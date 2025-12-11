"""上传后处理模块

负责处理文件上传后的同步、验证和响应构建。
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from app.core import manager
from app.core.file_manager import file_manager, FileCacheEntry
from app.core.log_utils import Logger
from app.schemas.gemini_files import File
from app.services.file_resolver import file_resolver
from app.services.response_builder import response_builder


class UploadProcessor:
    """上传后处理器
    
    职责：
    1. 处理缓存文件的上传后处理
    2. 同步到 Gemini
    3. 确保远程副本有效
    4. 处理离线回退
    """

    def ensure_unique_entry(self, sha256: str, request_id: str) -> Optional[FileCacheEntry]:
        """确保条目唯一，处理重新上传的情况"""
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

    async def sync_to_gemini(
        self,
        *,
        sha256: str,
        entry: FileCacheEntry,
        request: Request,
        request_id: str,
        size_bytes: int,
        sync_remote_callback,
    ) -> dict:
        """同步文件到 Gemini"""
        gemini_file, client_id = await manager.upload_file_from_cache(sha256)
        Logger.api_response(request_id, f"文件同步上传成功 | {client_id}")

        remote_file, _, _ = await sync_remote_callback(
            request=request,
            file_name=gemini_file.get("name"),
            request_id=request_id,
            reason="get",
            preferred_client_id=client_id,
            allow_deleted=True,
        )
        if remote_file:
            return response_builder.build_file_response(remote_file, entry, size_bytes)

        file_manager.update_replication_status(
            sha256,
            client_id,
            "synced",
            gemini_file,
        )
        return response_builder.build_file_response(gemini_file, entry, size_bytes)

    def handle_offline_fallback(
        self, *, sha256: str, entry: FileCacheEntry, request_id: str
    ) -> dict:
        """处理离线回退"""
        Logger.warning(
            "没有可用的WebSocket客户端连接，但文件已保存到本地缓存",
            request_id=request_id,
        )
        try:
            local_file_data = response_builder.default_file_payload(
                entry, sha256, size_bytes=entry.size_bytes
            )
            file_manager.update_replication_status(sha256, "local", "synced", local_file_data)
            Logger.api_response(request_id, "文件已保存到本地缓存（离线模式）")
            return local_file_data
        except Exception as exc:
            Logger.error("创建本地文件条目失败", exc=exc, request_id=request_id)
            raise HTTPException(
                status_code=503,
                detail="No frontend clients available. Please ensure the browser client is connected.",
            ) from exc

    async def ensure_valid_remote_entry(
        self,
        entry: FileCacheEntry,
        *,
        request: Request,
        request_id: str,
        sync_remote_callback,
    ) -> File:
        """验证或重建远程副本"""
        sha256 = entry.sha256
        candidates: list[tuple[str, str]] = [
            (client_id, data.get("name"))
            for client_id, data in entry.replication_map.items()
            if data.get("status") == "synced" and data.get("name")
        ]

        # 尝试验证现有副本
        for client_id, remote_name in candidates:
            remote_file, _, _ = await sync_remote_callback(
                request=request,
                file_name=remote_name,
                request_id=request_id,
                reason="dedup-verify",
                preferred_client_id=client_id,
            )
            if remote_file:
                try:
                    return File.model_validate(remote_file)
                except Exception as exc:
                    Logger.warning(
                        f"远程文件验证失败，尝试下一个副本: {exc}",
                        sha256=sha256[:8],
                        client_id=client_id,
                        request_id=request_id,
                    )
                    continue

        # 没有有效副本，需要重建
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
            remote_file, _, _ = await sync_remote_callback(
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
            fallback_file = response_builder.build_file_response(None, entry, entry.size_bytes)
            return File.model_validate(fallback_file)

    async def process_cached_file_upload(
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
        sync_remote_callback,
    ) -> JSONResponse:
        """处理缓存文件的上传后处理"""
        metadata = metadata or {}
        entry = self.ensure_unique_entry(sha256, request_id)

        if entry:
            resolved_file = await self.ensure_valid_remote_entry(
                entry, request=request, request_id=request_id, sync_remote_callback=sync_remote_callback
            )
            Logger.api_response(request_id, f"文件已存在 (sha256: {sha256[:8]})")
            if session_id:
                file_manager.sessions.remove_session(session_id)
            file_manager.clear_deleted_flag(sha256)
            return response_builder.build_final_upload_response(resolved_file, request=request)

        final_filename, final_mime = file_resolver.resolve_filename_and_mime(
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
            file_data = await self.sync_to_gemini(
                sha256=sha256,
                entry=entry,
                request=request,
                request_id=request_id,
                size_bytes=size_bytes,
                sync_remote_callback=sync_remote_callback,
            )
        except HTTPException as exc:
            if exc.status_code == 503:
                file_data = self.handle_offline_fallback(
                    sha256=sha256,
                    entry=entry,
                    request_id=request_id,
                )
            else:
                raise
        except Exception as exc:
            Logger.error("上传过程中发生未预期的错误", exc=exc)
            raise HTTPException(status_code=500, detail="Upload failed due to an internal error.") from exc
        finally:
            if session_id:
                file_manager.sessions.remove_session(session_id)

        file_manager.clear_deleted_flag(sha256)
        return response_builder.build_final_upload_response(file_data, request=request)


# 全局单例
upload_processor = UploadProcessor()
