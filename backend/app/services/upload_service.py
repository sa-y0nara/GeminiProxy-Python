from __future__ import annotations

import base64
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Tuple, Dict, Set

from fastapi import HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from app.core import manager
from app.core.config import settings
from app.core.file_manager import file_manager
from app.core.log_utils import Logger
from app.core.mime_utils import MimeUtils
from app.schemas.gemini_files import File, UploadFileResponse, InitialUploadRequest # Import if needed

FILENAME_RE = re.compile(r'filename[*]?=["\\"]?([^"\\\';\s]+)')

class UploadService:
    def __init__(self):
        pass

    def _parse_int_safe(self, value: Any, default: Optional[int] = None, label: str = "value") -> Optional[int]:
        """安全地将值转换为整数，带验证和日志"""
        if value is None:
            return default
        try:
            result = int(value)
            return result
        except (TypeError, ValueError):
            Logger.warning(f"{label} 无法转换为整数", value=value)
            return default

    def _extract_file_payload(self, response: Optional[dict]) -> Optional[dict]:
        if isinstance(response, dict):
            file_payload = response.get("file")
            if isinstance(file_payload, dict):
                return file_payload
            return response
        return None

    def _determine_proxy_base_url(self, request: Optional[Request]) -> Optional[str]:
        if request is not None:
            return str(request.base_url).rstrip("/")
        base = (settings.PROXY_BASE_URL or "").strip()
        return base.rstrip("/") if base else None


    def _build_proxy_uris(self, base_url: str, file_name: str) -> Tuple[str, str]:
        normalized_name = file_name.lstrip("/")
        if not normalized_name.startswith("files/"):
            normalized_name = f"files/{normalized_name}"
        metadata_uri = f"{base_url}/v1beta/{normalized_name}"
        download_uri = f"{metadata_uri}:download"
        return metadata_uri, download_uri

    def _first_non_empty(self, mapping: Optional[dict], *keys: str) -> Optional[str]:
        if not mapping:
            return None
        for key in keys:
            value = mapping.get(key)
            if value:
                return value
        return None

    def _sanitize_filename_hint(self, filename_hint: Optional[str]) -> str:
        """Sanitize filename to prevent directory traversal attacks."""
        if not filename_hint:
            return f"upload_{uuid.uuid4().hex}"
        
        sanitized = filename_hint.strip()
        sanitized = sanitized.replace("\\\\", "_").replace("/", "_").replace("..", "_")
        
        while sanitized.startswith("."):
            sanitized = sanitized[1:]
        
        if not sanitized or len(sanitized) == 0:
            return f"upload_{uuid.uuid4().hex}"
        
        return sanitized

    def _default_file_payload(self, entry, sha256: str, *, size_bytes: int) -> dict:
        now = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        fallback_name = f"files/{sha256}"
        return {
            "name": fallback_name,
            "displayName": entry.original_filename or "untitled",
            "mimeType": entry.mime_type or "application/octet-stream",
            "sizeBytes": str(size_bytes),
            "createTime": now,
            "updateTime": now,
            "sha256Hash": self.encode_sha256_base64(sha256) or sha256,
            "uri": fallback_name,
            "state": "ACTIVE",
            "source": "UPLOADED",
        }

    def prepare_file_for_response(self, file_data: File | dict, request: Optional[Request]) -> File:
        file_obj = file_data if isinstance(file_data, File) else File.model_validate(file_data)
        base_url = self._determine_proxy_base_url(request)

        if base_url:
            metadata_uri, download_uri = self._build_proxy_uris(base_url, file_obj.name)
            file_obj = file_obj.model_copy(update={"uri": metadata_uri, "download_uri": download_uri})
        elif not file_obj.download_uri and file_obj.uri:
            file_obj = file_obj.model_copy(update={"download_uri": file_obj.uri})

        return file_obj

    def encode_sha256_base64(self, sha256_hex: Optional[str]) -> Optional[str]:
        """将十六进制 sha256 转换为 base64 字符串"""
        if not sha256_hex:
            return None
        try:
            return base64.b64encode(bytes.fromhex(sha256_hex)).decode("ascii")
        except ValueError:
            Logger.warning("无法转换 sha256 为 base64", sha256=sha256_hex)
            return None

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
    ):
        """校验声明的文件大小与实际写入大小是否一致"""
        declared_size = metadata.get("size_bytes") or metadata.get("sizeBytes")
        declared_size_int = self._parse_int_safe(declared_size, label="sizeBytes")
        header_size_int = self._parse_int_safe(header_size, label="Content-Length") if check_header else None

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
                Logger.warning("清理不一致的缓存文件失败", exc=cleanup_error, path=str(file_path))
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

    def parse_filename_from_headers(self, *headers: Optional[str]) -> Optional[str]:
        """从若干 HTTP 头中提取文件名。"""
        for header in headers:
            if not header:
                continue
            match = FILENAME_RE.search(header)
            if match:
                return match.group(1)
        return None

    def _clear_upload_session(self, session_id: Optional[str]):
        if session_id:
            file_manager.upload_sessions.pop(session_id, None)

    def _ensure_not_deleted(self, name: str, sha256: Optional[str]):
        """确保请求的文件未被标记为已删除。"""
        if file_manager.is_name_marked_deleted(name):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")
        if sha256 and file_manager.is_marked_deleted(sha256):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found.")

    def _ensure_unique_entry(self, sha256: str, request_id: str):
        entry = file_manager.get_metadata_entry(sha256)
        if entry and file_manager.is_marked_deleted(sha256):
            Logger.info(
                "检测到已删除文件重新上传，正在重置旧的元数据",
                sha256=sha256[:8],
                request_id=request_id,
            )
            file_manager.delete_entry(sha256)
            return None
        return entry

    def _resolve_filename_and_mime(
        self,
        sha256: str,
        metadata: dict,
        filename_hint: Optional[str],
        content_type_hint: Optional[str],
        file_path: Path,
        request_id: str,
    ) -> Tuple[str, str]:
        normalized_hint = MimeUtils.normalize_filename(filename_hint)
        metadata_filename = MimeUtils.normalize_filename(
            self._first_non_empty(metadata, "display_name", "displayName", "filename", "fileName")
        )
        valid_names = [
            name
            for name in [normalized_hint, metadata_filename]
            if name and name.lower() not in {"untitled", "unknown", "unknown_file"}
        ]
        final_filename = valid_names[0] if valid_names else None

        header_mime = None
        if content_type_hint:
            header_mime = content_type_hint.split(";")[0].strip().lower()
            if header_mime == "application/octet-stream":
                header_mime = None

        metadata_mime = self._first_non_empty(metadata, "mime_type", "mimeType")
        if isinstance(metadata_mime, str):
            metadata_mime = metadata_mime.strip().lower()

        detected_mime = MimeUtils.detect_mime_type_from_content(file_path)
        inferred_mime_from_name = MimeUtils.infer_mime_type(final_filename) if final_filename else None

        candidate_mimes = [
            header_mime,
            metadata_mime,
            detected_mime,
            inferred_mime_from_name,
        ]
        final_mime = next((mime for mime in candidate_mimes if mime), "application/octet-stream")

        if not final_filename:
            final_filename = MimeUtils.build_fallback_filename(sha256, final_mime)
            Logger.info(f"使用基于类型的临时文件名: {final_filename}", request_id=request_id)
        else:
            suffix = Path(final_filename).suffix
            if not suffix:
                extension = MimeUtils.guess_extension_from_mime(final_mime, default="")
                if extension:
                    final_filename = f"{final_filename}{extension}"

        return final_filename, final_mime


    async def _sync_to_gemini(
        self,
        *,
        sha256: str,
        entry,
        request: Request,
        request_id: str,
        size_bytes: int,
    ) -> dict:
        # manager is passed in implicitly through closure or explicitly as arg, assuming explicit here for clarity
        gemini_file, client_id = await manager.upload_file_from_cache(sha256)
        Logger.api_response(request_id, f"文件同步上传成功 | {client_id}")

        remote_file, _, _ = await self._fetch_remote_file_and_update_cache(
            request=request,
            file_name=gemini_file.get("name"),
            request_id=request_id,
            reason="get",
            preferred_client_id=client_id,
            allow_deleted=True,
        )
        if remote_file:
            return self.build_file_response(remote_file, entry, size_bytes)

        file_manager.update_replication_status(
            sha256,
            client_id,
            "synced",
            gemini_file,
        )
        return self.build_file_response(gemini_file, entry, size_bytes)


    def _handle_offline_fallback(self, *, sha256: str, entry, request_id: str) -> dict:
        Logger.warning("没有可用的WebSocket客户端连接，但文件已保存到本地缓存", request_id=request_id)
        try:
            local_file_data = self._default_file_payload(entry, sha256, size_bytes=entry.size_bytes)
            file_manager.update_replication_status(sha256, "local", "synced", local_file_data)
            Logger.api_response(request_id, "文件已保存到本地缓存（离线模式）")
            return local_file_data
        except Exception as exc:
            Logger.error("创建本地文件条目失败", exc=exc, request_id=request_id)
            raise HTTPException(
                status_code=503,
                detail="No frontend clients available. Please ensure the browser client is connected.",
            ) from exc


    async def persist_body_to_cache(self, request: Request, filename_hint: Optional[str]):
        """将请求体保存到缓存，避免一次性读取全部内容。"""

        body_stream = request.stream()

        async def iterator():
            async for chunk in body_stream:
                if chunk:
                    yield chunk

        temp_name = self._sanitize_filename_hint(filename_hint)
        return await file_manager.save_stream_to_cache(iterator(), temp_name)


    def map_frontend_response_to_file_model(self, frontend_file: Optional[dict], entry, size_bytes: int) -> dict:
        """
        将前端返回的文件对象映射到后端File模型期望的格式

        Args:
            frontend_file: 前端返回的文件对象
            entry: 文件缓存条目，包含原始文件信息
            size_bytes: 文件大小

        Returns:
            符合File模型格式的字典
        """
        frontend_file = frontend_file or {}

        base_payload = self._default_file_payload(entry, entry.sha256, size_bytes=size_bytes)
        fallback_name = base_payload["name"]
        sha_base64 = (
            frontend_file.get("sha256Hash")
            or frontend_file.get("sha256_hash")
            or base_payload["sha256Hash"]
        )

        mapped_file = {
            **base_payload,
            "name": frontend_file.get("name") or fallback_name,
            "sizeBytes": str(frontend_file.get("size", size_bytes)),
            "uri": frontend_file.get("uri") or fallback_name,
            "sha256Hash": sha_base64,
        }

        if frontend_file.get("displayName"):
            mapped_file["displayName"] = frontend_file["displayName"]

        if "expirationTime" in frontend_file:
            mapped_file["expirationTime"] = frontend_file["expirationTime"]
        elif entry.gemini_file_expiration:
            mapped_file["expirationTime"] = entry.gemini_file_expiration.isoformat().replace('+00:00', 'Z')

        if "downloadUri" in frontend_file:
            mapped_file["downloadUri"] = frontend_file["downloadUri"]

        return mapped_file


    def _build_file_response(
        self,
        source_file: Optional[dict],
        entry,
        size_bytes: int,
    ) -> dict:
        """
        根据远端返回的数据或本地缓存构造 File 响应。
        """
        try:
            # 尝试直接验证远程文件数据
            if source_file:
                return File.model_validate(source_file).model_dump(by_alias=True, exclude_none=True)
        except Exception as exc:
            Logger.warning("远程文件数据无法直接验证，将使用本地映射", exc=exc)

        # 如果验证失败或没有 source_file，则使用本地映射
        mapped_file_data = self.map_frontend_response_to_file_model(source_file, entry, size_bytes)
        return File.model_validate(mapped_file_data).model_dump(by_alias=True, exclude_none=True)


    def build_final_upload_response(self, file_data: File | dict, request: Optional[Request] = None) -> JSONResponse:
        """
        构造带有 Google Upload 兼容头部的 JSON 响应
        """
        file_obj = self.prepare_file_for_response(file_data, request)

        payload = UploadFileResponse(file=file_obj).model_dump(by_alias=True, exclude_none=True)
        return JSONResponse(
            content=payload,
            headers={
                "X-Goog-Upload-Status": "final",
                "Content-Type": "application/json",
            },
            status_code=200,
        )


    async def _fetch_remote_file_and_update_cache(
        self,
        *,
        request: Request,
        file_name: str,
        request_id: str,
        reason: str,
        preferred_client_id: Optional[str] = None,
        allow_deleted: bool = False,
    ) -> Tuple[Optional[dict], Optional[str], bool]:
        """Fetch remote metadata, update cache, and report deletion status."""
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
                async with manager.monitored_proxy_request(verify_request_id, request) as verify_client_id:
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

        remote_file = self._extract_file_payload(response)
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


    def _extract_request_metadata(
        self,
        body: Optional[InitialUploadRequest],
        *,
        required: bool = False,
    ) -> dict:
        """统一处理上传入口处的 metadata 判空与提取。"""
        metadata = body.file.model_dump(by_alias=True, exclude_none=True) if body and body.file else {}
        if required and not metadata:
            raise HTTPException(status_code=400, detail="file metadata is required")
        return metadata


    def _parse_upload_commands(self, header_value: Optional[str]) -> Set[str]:
        if not header_value:
            return set()
        return {token.strip().lower() for token in header_value.split(",") if token.strip()}


    async def _handle_chunked_upload(
        self,
        *,
        request: Request,
        session_id: str,
        metadata: dict,
        request_id: str,
        upload_offset_header: Optional[str],
        finalize_requested: bool,
    ) -> Response | Tuple[str, Path, int]:
        try:
            upload_offset_int = int(upload_offset_header or 0)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Invalid X-Goog-Upload-Offset header.")

        chunk_data = await request.body()
        try:
            new_offset = file_manager.append_chunk_data(session_id, chunk_data, upload_offset_int)
        except ValueError as offset_error:
            file_manager.discard_chunk_upload(session_id)
            file_manager.upload_sessions.pop(session_id, None)
            raise HTTPException(status_code=400, detail=str(offset_error))

        if not finalize_requested:
            return Response(
                status_code=308,
                headers={
                    "X-Goog-Upload-Status": "active",
                    "X-Goog-Upload-Offset": str(new_offset),
                },
            )

        try:
            sha256, file_path, size_bytes = file_manager.finalize_chunk_upload(session_id)
        except ValueError as finalize_error:
            file_manager.upload_sessions.pop(session_id, None)
            raise HTTPException(status_code=400, detail=str(finalize_error))

        self.enforce_size_consistency(
            metadata,
            size_bytes,
            header_size=None,
            request_id=request_id,
            session_id=session_id,
            file_path=file_path,
            check_header=False,
        )
        return sha256, file_path, size_bytes


    async def _handle_stream_upload(
        self,
        *,
        request: Request,
        filename_hint: Optional[str],
        metadata: dict,
        request_id: str,
        session_id: str,
    ) -> Tuple[str, Path, int]:
        try:
            sha256, file_path, size_bytes = await self.persist_body_to_cache(request, filename_hint)
        except Exception as exc:
            Logger.error("保存文件到缓存失败", exc=exc)
            raise HTTPException(status_code=500, detail="Failed to save file to cache.") from exc

        self.enforce_size_consistency(
            metadata,
            size_bytes,
            header_size=request.headers.get("content-length"),
            request_id=request_id,
            session_id=session_id,
            file_path=file_path,
            check_header=True,
        )
        return sha256, file_path, size_bytes


    async def _process_cached_file_upload(
        self,
        *,
        request: Request,
        sha256: str,
        file_path: Path,
        size_bytes: int,
        metadata: Optional[dict],
        request_id: str,
        session_id: Optional[str],
        filename_hint: Optional[str],
        content_type_hint: Optional[str],
    ) -> JSONResponse:
        """共享的缓存文件上传处理逻辑"""
        metadata = metadata or {}
        entry = self._ensure_unique_entry(sha256, request_id)

        if entry:
            resolved_file = await self._ensure_valid_remote_entry(entry, request=request, request_id=request_id)
            Logger.api_response(request_id, f"文件已存在 (sha256: {sha256[:8]})")
            self._clear_upload_session(session_id)
            file_manager.clear_deleted_flag(sha256)
            return self.build_final_upload_response(resolved_file, request=request)

        final_filename, final_mime = self._resolve_filename_and_mime(
            sha256,
            metadata,
            filename_hint,
            content_type_hint,
            file_path,
            request_id,
        )

        entry = file_manager.create_metadata_entry(
            sha256=sha256,
            file_path=file_path,
            filename=final_filename,
            mime_type=final_mime,
            size_bytes=size_bytes,
        )

        Logger.info(
            f"创建文件元数据 - SHA256: {sha256[:8]}, 文件名: {final_filename}, MIME: {final_mime}",
            request_id=request_id,
        )

        try:
            file_data = await self._sync_to_gemini(
                sha256=sha256,
                entry=entry,
                request=request,
                request_id=request_id,
                size_bytes=size_bytes,
            )
        except HTTPException as exc:
            if exc.status_code == 503:
                file_data = self._handle_offline_fallback(
                    sha256=sha256,
                    entry=entry,
                    request_id=request_id,
                )
            else:
                raise
        except Exception as exc:
            Logger.error("上传过程中发生未预期的错误", exc=exc)
            raise HTTPException(status_code=500, detail=f"Upload failed: {str(exc)}")
        finally:
            self._clear_upload_session(session_id)

        file_manager.clear_deleted_flag(sha256)
        return self.build_final_upload_response(file_data, request=request)


    async def _ensure_valid_remote_entry(
        self,
        entry,
        *,
        request: Request,
        request_id: str,
    ) -> File:
        """
        Reuses an existing cached entry by verifying at least one remote replica.
        If none of the replicas are valid, synchronously rebuilds the remote copy.
        """
        sha256 = entry.sha256
        candidates: List[Tuple[str, str]] = [
            (client_id, data.get("name"))
            for client_id, data in entry.replication_map.items()
            if data.get("status") == "synced" and data.get("name")
        ]

        for client_id, remote_name in candidates:
            remote_file, _, _ = await self._fetch_remote_file_and_update_cache(
                request=request,
                file_name=remote_name,
                request_id=request_id,
                reason="dedup-verify",
                preferred_client_id=client_id,
            )
            if remote_file:
                return File.model_validate(remote_file)

        Logger.warning(
            "缓存命中但没有可用的远端副本，开始同步重建",
            sha256=sha256[:8],
            request_id=request_id,
        )

        file_manager.reset_replication_map(sha256)
        try:
            gemini_file, client_id = await manager.upload_file_from_cache(sha256)
            Logger.event(
                "REUSE_REBUILD",
                "缓存文件已重新上传",
                sha256=sha256[:8],
                client_id=client_id,
            )
            remote_file, _, _ = await self._fetch_remote_file_and_update_cache(
                request=request,
                file_name=gemini_file.get("name"),
                request_id=request_id,
                reason="dedup-rebuild",
                preferred_client_id=client_id,
            )
            if remote_file:
                return File.model_validate(remote_file)
            file_manager.update_replication_status(
                sha256,
                client_id,
                "synced",
                gemini_file,
            )
            return File.model_validate(gemini_file)
        except Exception as exc:
            Logger.error(
                "缓存文件重建失败，返回本地映射",
                exc=exc,
                sha256=sha256[:8],
                request_id=request_id,
            )
            fallback_file = self._build_file_response(None, entry, entry.size_bytes)
            return File.model_validate(fallback_file)
    
    async def initiate_upload_session(self, request: Request, body: InitialUploadRequest) -> Response:
        """
        初始化一个模拟的可续传上传会话，绑定到客户端以防止会话劫持。
        """
        session_id = str(uuid.uuid4())
        client_id = request.headers.get("X-Client-ID") or request.headers.get("x-client-id") or "anonymous"
        metadata = self._extract_request_metadata(body)
        file_manager.start_upload_session(session_id, client_id, metadata)

        proxy_upload_url = f"{self._determine_proxy_base_url(request)}/v1beta/files/upload/{session_id}"

        return Response(
            headers={
                "X-Goog-Upload-URL": proxy_upload_url,
                "X-Goog-Upload-Status": "active",
                "X-Client-ID": client_id,  # 返回客户端 ID 便于验证
            },
        )
    
    async def handle_resumable_upload(self, request: Request, session_id: str) -> JSONResponse:
        """
        接收文件内容，并触发完整的方案 B 上传/同步逻辑。
        支持自动重试机制以处理临时连接问题。
        """
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

        filename_hint = self._first_non_empty(metadata, "display_name", "displayName", "filename", "fileName") or "untitled"

        request_id = str(uuid.uuid4())
        Logger.api_request(request_id, f"文件内容上传 | {filename_hint}")

        content_type = request.headers.get("content-type", "unknown")
        content_length = request.headers.get("content-length", "unknown")
        Logger.info(f"文件上传请求 - MIME: {content_type}, 大小: {content_length}", request_id=request_id)

        content_disposition = request.headers.get("content-disposition", "")
        header_filename = self.parse_filename_from_headers(content_disposition)
        if header_filename:
            filename_hint = header_filename
            Logger.info(f"从请求头中提取文件名: {filename_hint}", request_id=request_id)

        command_tokens = self._parse_upload_commands(request.headers.get("x-goog-upload-command") or "")
        upload_offset_header = request.headers.get("x-goog-upload-offset")
        is_chunked_upload = bool(command_tokens) or upload_offset_header is not None
        finalize_requested = "finalize" in command_tokens

        if is_chunked_upload:
            chunk_result = await self._handle_chunked_upload(
                request=request,
                session_id=session_id,
                metadata=metadata,
                request_id=request_id,
                upload_offset_header=upload_offset_header,
                finalize_requested=finalize_requested,
            )
            if isinstance(chunk_result, Response):
                return chunk_result
            sha256, file_path, size_bytes = chunk_result
        else:
            sha256, file_path, size_bytes = await self._handle_stream_upload(
                request=request,
                filename_hint=filename_hint,
                metadata=metadata,
                request_id=request_id,
                session_id=session_id,
            )

        return await self._process_cached_file_upload(
            request=request,
            sha256=sha256,
            file_path=file_path,
            size_bytes=size_bytes,
            metadata=metadata,
            request_id=request_id,
            session_id=session_id,
            filename_hint=filename_hint,
            content_type_hint=request.headers.get("content-type"),
        )
    
    async def create_metadata_only_file(self, request: Request, body: InitialUploadRequest) -> JSONResponse:
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

        remote_file = self._extract_file_payload(response_data)
        if not isinstance(remote_file, dict):
            raise HTTPException(status_code=502, detail="Invalid response from frontend client")

        entry = file_manager.ensure_remote_entry(remote_file)
        if entry:
            file_manager.update_replication_status(entry.sha256, client_id, "synced", remote_file)
            file_manager.clear_deleted_flag(entry.sha256)

        Logger.api_response(request_id, "metadata-only 文件创建成功")
        return self.build_final_upload_response(remote_file, request=request)


upload_service = UploadService()
