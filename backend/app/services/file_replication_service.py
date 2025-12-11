"""文件复制服务模块

负责处理文件的上传、复制和后台复制任务。
"""

import uuid
from typing import Any, Optional, Tuple, TYPE_CHECKING

from fastapi import HTTPException, status

from app.core.background_request import build_background_request
from app.core.background_tasks import create_background_task
from app.core.exceptions import ApiException
from app.core.file_manager import file_manager
from app.core.log_utils import Logger

if TYPE_CHECKING:
    from app.core.interfaces import IConnectionManager


class FileReplicationService:
    """文件复制服务
    
    职责：
    1. 通过客户端上传文件
    2. 同步复制文件到指定客户端
    3. 管理后台复制任务
    4. 同步重建文件
    """

    async def upload_file_via_client(
        self,
        manager: "IConnectionManager",
        sha256: str,
        client_id: str,
        *,
        request_id: Optional[str] = None,
    ) -> dict:
        """指挥指定客户端从缓存下载并上传文件"""
        entry = file_manager.get_metadata_entry(sha256)
        if not entry:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="File not found in cache."
            )

        effective_request_id = request_id or f"upload-{sha256[:8]}-{client_id}"
        file_manager.update_replication_status(sha256, client_id, "pending_replication")

        try:
            file_bytes = entry.local_path.read_bytes()
            file_size = len(file_bytes)
        except Exception as exc:
            file_manager.update_replication_status(sha256, client_id, "failed")
            raise HTTPException(
                status_code=500, detail=f"Failed to read cached file: {exc}"
            ) from exc

        display_name = entry.original_filename or "untitled"
        mime_type = entry.mime_type or "application/octet-stream"
        size_bytes_str = str(entry.size_bytes)

        background_request = build_background_request()

        try:
            initiate_payload = {
                "metadata": {
                    "file": {
                        "displayName": display_name,
                        "mimeType": mime_type,
                        "sizeBytes": size_bytes_str,
                    }
                }
            }
            initiate_response = await manager._direct_proxy_request(
                command_type="initiate_resumable_upload",
                payload=initiate_payload,
                request_id=f"{effective_request_id}-init",
                client_id=client_id,
                request=background_request,
            )
            upload_url = initiate_response.get("upload_url")
            if not upload_url:
                raise ApiException(
                    status_code=500, detail="Failed to obtain upload URL from frontend."
                )

            chunk_payload = {
                "upload_url": upload_url,
                "upload_offset": 0,
                "content_length": file_size,
                "upload_command": "upload, finalize",
            }
            chunk_request_id = f"{effective_request_id}-chunk"
            chunk_command = {
                "id": chunk_request_id,
                "type": "upload_chunk",
                "payload": chunk_payload,
            }

            upload_response = await manager.send_binary_command(
                client_id=client_id,
                command=chunk_command,
                binary_body=file_bytes,
            )
        except Exception:
            file_manager.update_replication_status(sha256, client_id, "failed")
            raise

        gemini_file = upload_response.get("body") or upload_response.get("file")
        if isinstance(gemini_file, dict) and "file" in gemini_file and isinstance(
            gemini_file["file"], dict
        ):
            gemini_file = gemini_file["file"]
        if not gemini_file:
            file_manager.update_replication_status(sha256, client_id, "failed")
            raise ApiException(
                status_code=500,
                detail="Frontend did not return a file object after upload.",
            )

        file_manager.update_replication_status(sha256, client_id, "synced", gemini_file)
        Logger.event(
            "REPLICATION_SUCCESS",
            "文件上传/复制成功",
            sha256=sha256,
            client_id=client_id,
            request_id=effective_request_id,
        )
        return gemini_file

    async def replicate_files_to_client(
        self,
        manager: "IConnectionManager",
        client_id: str,
        sha_list: list[str],
        request_id: str,
    ) -> None:
        """同步等待客户端复制所有缺失文件"""
        if not sha_list:
            return

        Logger.info(
            "同步补全缺失文件",
            client_id=client_id,
            request_id=request_id,
            files=len(sha_list),
        )
        for sha in sha_list:
            await self.upload_file_via_client(
                manager,
                sha,
                client_id,
                request_id=f"replicate-{request_id}-{sha[:8]}",
            )

    def trigger_bulk_replication(
        self, manager: "IConnectionManager", client_id: str, sha_list: list[str]
    ) -> None:
        """触发后台任务，批量为客户端复制缺失文件"""
        if not sha_list:
            return
        task_id = f"heal-{client_id}-{uuid.uuid4().hex[:6]}"
        create_background_task(
            self.bulk_replication_task(manager, client_id, sha_list, task_id)
        )

    async def bulk_replication_task(
        self,
        manager: "IConnectionManager",
        client_id: str,
        sha_list: list[str],
        task_id: str,
    ) -> None:
        """后台批量复制任务"""
        Logger.event(
            "SELF_HEAL_START",
            "开始后台自愈复制",
            client_id=client_id,
            files=len(sha_list),
            task_id=task_id,
        )
        try:
            await self.replicate_files_to_client(manager, client_id, sha_list, task_id)
            Logger.event(
                "SELF_HEAL_SUCCESS",
                "后台自愈复制成功",
                client_id=client_id,
                files=len(sha_list),
                task_id=task_id,
            )
        except Exception as exc:
            Logger.warning(
                "后台自愈复制失败",
                client_id=client_id,
                files=len(sha_list),
                task_id=task_id,
                exc=exc,
            )

    async def synchronously_rebuild_file(
        self, manager: "IConnectionManager", sha256: str
    ) -> Tuple[dict, str]:
        """同步重建文件：轮询选择一个客户端，阻塞式地指挥它重新上传文件"""
        request_id = f"rebuild-{sha256[:8]}-{uuid.uuid4()}"
        Logger.event("REBUILD_START", "开始同步文件重建", sha256=sha256)

        client_id = manager.get_next_client()
        try:
            gemini_file = await self.upload_file_via_client(
                manager, sha256, client_id, request_id=request_id
            )
            Logger.event(
                "REBUILD_SUCCESS", "同步文件重建成功", sha256=sha256, client_id=client_id
            )
            return gemini_file, client_id
        except Exception as e:
            Logger.error(
                "同步文件重建失败", exc=e, sha256=sha256, client_id=client_id
            )
            raise


# 全局单例
file_replication_service = FileReplicationService()
