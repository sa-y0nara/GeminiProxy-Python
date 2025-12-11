"""文件校验服务模块

负责处理远程文件的校验和可用性检查。
"""

import asyncio
from typing import List, TYPE_CHECKING

from fastapi import HTTPException, status

from app.core.config import settings
from app.core.exceptions import ApiException
from app.core.file_manager import FileCacheEntry, file_manager
from app.core.log_utils import Logger

if TYPE_CHECKING:
    from app.core.interfaces import IConnectionManager
    from app.services.file_sync_service import FileReference


class FileVerificationService:
    """文件校验服务
    
    职责：
    1. 校验单个远程文件是否仍然有效
    2. 批量校验文件并触发修复
    3. 检查客户端同步状态
    """

    def is_client_synced(self, entry: FileCacheEntry, client_id: str) -> bool:
        """检查文件是否已同步到指定客户端"""
        replication_data = entry.replication_map.get(client_id)
        return bool(replication_data and replication_data.get("status") == "synced")

    def collect_missing_for_client(
        self, required_entries: dict[str, FileCacheEntry], client_id: str
    ) -> List[str]:
        """收集客户端缺失的文件列表"""
        missing: List[str] = []
        for sha256, entry in required_entries.items():
            if not self.is_client_synced(entry, client_id):
                missing.append(sha256)
        return missing

    async def verify_single_file(
        self,
        manager: "IConnectionManager",
        client_id: str,
        sha256: str,
        remote_name: str,
        request_id: str,
    ) -> bool:
        """验证单个文件是否在远端仍然有效"""
        verify_request_id = f"{request_id}-verify-{sha256[:8]}"
        try:
            response = await manager.send_command_to_client(
                client_id=client_id,
                command_type="get_file",
                payload={"file_name": remote_name},
                request_id=verify_request_id,
            )
            remote_file = response.get("file") if isinstance(response, dict) else response
            if isinstance(remote_file, dict):
                file_manager.update_replication_status(sha256, client_id, "synced", remote_file)
            return True
        except (HTTPException, ApiException) as exc:
            status_code = getattr(exc, "status_code", None) or status.HTTP_500_INTERNAL_SERVER_ERROR
            if status_code == status.HTTP_404_NOT_FOUND or status_code >= status.HTTP_500_INTERNAL_SERVER_ERROR:
                Logger.warning(
                    "远程文件校验失败",
                    client_id=client_id,
                    request_id=verify_request_id,
                    sha256=sha256[:8],
                )
                return False
            raise
        except Exception as exc:
            Logger.warning(
                "校验远端文件异常",
                exc=exc,
                client_id=client_id,
                sha256=sha256[:8],
            )
            return False

    async def ensure_remote_files_available(
        self,
        manager: "IConnectionManager",
        client_id: str,
        file_refs: List["FileReference"],
        request_id: str,
        replication_service: "FileReplicationServiceProtocol",
    ) -> None:
        """在发送请求前通过 get_file 校验所有引用是否仍然有效"""
        checked: set[str] = set()

        # 收集需要验证的文件
        to_verify = []
        for ref in file_refs:
            if ref.sha256 in checked:
                continue
            checked.add(ref.sha256)

            replication_data = ref.entry.replication_map.get(client_id)
            if not replication_data or replication_data.get("status") != "synced":
                continue
            remote_name = replication_data.get("name")
            if not remote_name:
                continue

            to_verify.append((ref.sha256, remote_name))

        if not to_verify:
            return

        semaphore = asyncio.Semaphore(settings.FILE_VERIFY_CONCURRENCY)

        async def verify_with_semaphore(sha256: str, remote_name: str):
            async with semaphore:
                return await self.verify_single_file(
                    manager, client_id, sha256, remote_name, request_id
                )

        verify_tasks = [
            verify_with_semaphore(sha256, remote_name) for sha256, remote_name in to_verify
        ]

        verify_results = await asyncio.gather(*verify_tasks, return_exceptions=True)

        needs_heal = []
        for (sha256, _), result in zip(to_verify, verify_results):
            if result is True:
                continue
            elif result is False:
                needs_heal.append(sha256)
            elif isinstance(result, Exception):
                needs_heal.append(sha256)

        if needs_heal:
            dedup = list(dict.fromkeys(needs_heal))
            Logger.warning(
                "检测到已失效的远端文件，将触发同步复制",
                client_id=client_id,
                request_id=request_id,
                files=[sha[:8] for sha in dedup],
            )
            for sha in dedup:
                file_manager.reset_replication_map(sha)
            await replication_service.replicate_files_to_client(
                manager, client_id, dedup, request_id
            )


class FileReplicationServiceProtocol:
    """FileReplicationService 协议类型，用于类型提示"""
    async def replicate_files_to_client(
        self,
        manager: "IConnectionManager",
        client_id: str,
        sha_list: list[str],
        request_id: str,
    ) -> None:
        ...


# 全局单例
file_verification_service = FileVerificationService()
