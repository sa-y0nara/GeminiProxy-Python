"""上传服务模块

作为入口协调器，负责处理文件上传请求的路由和协调。
"""

import uuid
from pathlib import Path
from typing import Optional, Tuple

from fastapi import HTTPException, Request, Response
from fastapi.responses import JSONResponse

from app.core import manager
from app.core.file_manager import file_manager
from app.core.log_utils import Logger
from app.core.utils import parse_int_safe, first_non_empty
from app.schemas.gemini_files import InitialUploadRequest
from app.services.chunked_upload_handler import chunked_upload_handler
from app.services.file_resolver import file_resolver
from app.services.response_builder import response_builder
from app.services.stream_upload_handler import stream_upload_handler
from app.services.upload_processor import upload_processor


class UploadService:
    """上传服务 (入口协调器)

    职责：
    1. 初始化上传会话
    2. 处理可续传上传请求
    3. 处理 metadata-only 文件创建
    4. 同步远程文件到缓存
    5. 校验文件大小一致性
    """

    def enforce_size_consistency(
        self,
        metadata: dict,
        actual_size: int,
        header_size: Optional[str],
        *,
        request_id: str,
        session_id: str,
        file_path: Path,
        check_header: bool = True,
    ) -> None:
        """校验声明的文件大小与实际写入大小是否一致"""
        declared_size = metadata.get("size_bytes") or metadata.get("sizeBytes")
        declared_size_int = parse_int_safe(declared_size, label="sizeBytes")
        header_size_int = (
            parse_int_safe(header_size, label="Content-Length") if check_header else None
        )

        mismatch_errors = []
        if declared_size_int is not None and declared_size_int != actual_size:
            mismatch_errors.append(
                f"元数据声明大小 {declared_size_int} 与实际大小 {actual_size} 不一致"
            )
        if header_size_int is not None and header_size_int != actual_size:
            mismatch_errors.append(
                f"Content-Length {header_size_int} 与实际大小 {actual_size} 不一致"
            )

        if mismatch_errors:
            try:
                file_path.unlink(missing_ok=True)
            except Exception as cleanup_error:
                Logger.warning(
                    "清理不一致的缓存文件失败", exc=cleanup_error, path=str(file_path)
                )
            file_manager.upload_sessions.pop(session_id, None)
            Logger.warning(
                "上传被拒绝，声明大小与实际不符",
                request_id=request_id,
                errors="; ".join(mismatch_errors),
            )
            raise HTTPException(
                status_code=400,
                detail="; ".join(mismatch_errors),
            )

    def _extract_request_metadata(
        self,
        body: Optional[InitialUploadRequest],
        *,
        required: bool = False,
    ) -> dict:
        """统一处理上传入口处的 metadata 判空与提取"""
        metadata = (
            body.file.model_dump(by_alias=True, exclude_none=True)
            if body and body.file
            else {}
        )
        if required and not metadata:
            raise HTTPException(status_code=400, detail="file metadata is required")
        return metadata

    async def sync_remote_file_to_cache(
        self,
        *,
        request: Request,
        file_name: str,
        request_id: str,
        reason: str,
        preferred_client_id: Optional[str] = None,
        allow_deleted: bool = False,
    ) -> Tuple[Optional[dict], Optional[str], bool]:
        """获取远程文件元数据并更新缓存"""
        if not file_name:
            return None, None, False

        verify_request_id = f"{request_id}-{reason}"
        try:
            if preferred_client_id:
                response = await manager.send_command_to_client(
                    client_id=preferred_client_id,
                    command_type="get_file",
                    payload={"file_name": file_name},
                    request_id=verify_request_id,
                )
                verify_client_id = preferred_client_id
            else:
                async with manager.monitored_proxy_request(
                    verify_request_id, request
                ) as verify_client_id:
                    response = await manager.proxy_request(
                        command_type="get_file",
                        payload={"file_name": file_name},
                        request=request,
                        request_id=verify_request_id,
                    )
        except Exception as exc:
            Logger.warning(
                "远程文件校验失败",
                request_id=verify_request_id,
                file_name=file_name,
                exc=exc,
            )
            return None, None, False

        remote_file = response_builder.extract_file_payload(response)
        if remote_file:
            Logger.info(
                "远程文件校验成功",
                request_id=verify_request_id,
                file_name=file_name,
                mime=remote_file.get("mimeType"),
            )
        else:
            return None, None, False

        remote_sha = file_manager.extract_sha256_hex(remote_file)
        if remote_sha and not allow_deleted and file_manager.is_marked_deleted(remote_sha):
            return None, remote_sha, True

        remote_entry = file_manager.ensure_remote_entry(remote_file)
        cache_client_id = verify_client_id or preferred_client_id or "remote"
        if remote_entry:
            file_manager.update_replication_status(
                remote_entry.sha256,
                cache_client_id,
                "synced",
                remote_file,
            )
            file_manager.clear_deleted_flag(remote_entry.sha256)
            remote_sha = remote_entry.sha256

        return remote_file, remote_sha, False

    async def initiate_upload_session(
        self, request: Request, body: InitialUploadRequest
    ) -> Response:
        """初始化一个模拟的可续传上传会话"""
        session_id = str(uuid.uuid4())
        client_id = (
            request.headers.get("X-Client-ID")
            or request.headers.get("x-client-id")
            or "anonymous"
        )
        metadata = self._extract_request_metadata(body)
        file_manager.start_upload_session(session_id, client_id, metadata)

        proxy_upload_url = (
            f"{response_builder.determine_proxy_base_url(request)}/v1beta/files/upload/{session_id}"
        )

        return Response(
            headers={
                "X-Goog-Upload-URL": proxy_upload_url,
                "X-Goog-Upload-Status": "active",
                "X-Client-ID": client_id,
            },
        )

    async def handle_resumable_upload(
        self, request: Request, session_id: str
    ) -> JSONResponse:
        """接收文件内容，并触发完整的上传/同步逻辑"""
        client_id = request.headers.get("X-Client-ID") or request.headers.get("x-client-id")

        session = file_manager.get_upload_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Upload session not found.")

        if client_id and session.client_id != client_id:
            Logger.warning(
                "会话客户端验证失败，拒绝访问",
                session_id=session_id,
                expected_client=session.client_id,
                provided_client=client_id,
            )
            raise HTTPException(status_code=403, detail="Session client mismatch.")

        metadata = dict(session.metadata or {})
        filename_hint = (
            first_non_empty(metadata, "display_name", "displayName", "filename", "fileName")
            or "untitled"
        )

        request_id = str(uuid.uuid4())
        Logger.api_request(request_id, f"文件内容上传 | {filename_hint}")

        content_type = request.headers.get("content-type", "unknown")
        content_length = request.headers.get("content-length", "unknown")
        Logger.info(
            f"文件上传请求 - MIME: {content_type}, 大小: {content_length}",
            request_id=request_id,
        )

        # 从请求头提取文件名
        content_disposition = request.headers.get("content-disposition", "")
        header_filename = file_resolver.parse_filename_from_headers(content_disposition)
        if header_filename:
            filename_hint = header_filename
            Logger.info(f"从请求头中提取文件名: {filename_hint}", request_id=request_id)

        # 判断上传类型
        command_tokens = chunked_upload_handler.parse_upload_commands(
            request.headers.get("x-goog-upload-command") or ""
        )
        upload_offset_header = request.headers.get("x-goog-upload-offset")
        is_chunked = chunked_upload_handler.is_chunked_upload(
            command_tokens, upload_offset_header
        )
        finalize_requested = "finalize" in command_tokens

        if is_chunked:
            chunk_result = await chunked_upload_handler.handle_chunked_upload(
                request=request,
                session_id=session_id,
                metadata=metadata,
                request_id=request_id,
                upload_offset_header=upload_offset_header,
                finalize_requested=finalize_requested,
                size_validator=self.enforce_size_consistency,
            )
            if isinstance(chunk_result, Response):
                return chunk_result
            sha256, file_path, size_bytes = chunk_result
        else:
            sha256, file_path, size_bytes = await stream_upload_handler.handle_stream_upload(
                request=request,
                filename_hint=filename_hint,
                metadata=metadata,
                request_id=request_id,
                session_id=session_id,
                size_validator=self.enforce_size_consistency,
            )

        return await upload_processor.process_cached_file_upload(
            request=request,
            sha256=sha256,
            file_path=file_path,
            size_bytes=size_bytes,
            metadata=metadata,
            request_id=request_id,
            session_id=session_id,
            filename_hint=filename_hint,
            content_type_hint=request.headers.get("content-type"),
            sync_remote_callback=self.sync_remote_file_to_cache,
        )

    async def create_metadata_only_file(
        self, request: Request, body: InitialUploadRequest
    ) -> JSONResponse:
        """处理 metadata-only 文件创建请求"""
        request_id = str(uuid.uuid4())
        metadata = self._extract_request_metadata(body, required=True)

        Logger.api_request(request_id, "文件 metadata-only 创建")
        payload = {"metadata": {"file": metadata}}
        async with manager.monitored_proxy_request(request_id, request) as client_id:
            response_data = await manager.proxy_request(
                command_type="create_file_metadata",
                payload=payload,
                request=request,
                request_id=request_id,
            )

        remote_file = response_builder.extract_file_payload(response_data)
        if not isinstance(remote_file, dict):
            raise HTTPException(
                status_code=502, detail="Invalid response from frontend client"
            )

        entry = file_manager.ensure_remote_entry(remote_file)
        if entry:
            file_manager.update_replication_status(entry.sha256, client_id, "synced", remote_file)
            file_manager.clear_deleted_flag(entry.sha256)

        Logger.api_response(request_id, "metadata-only 文件创建成功")
        return response_builder.build_final_upload_response(remote_file, request=request)


upload_service = UploadService()
