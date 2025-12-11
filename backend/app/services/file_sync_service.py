"""文件同步服务模块

作为协调器，负责处理文件引用解析、客户端选择和请求重试逻辑。
使用 FileReferenceResolver 和 ClientSelector 进行职责分离。
"""

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from fastapi import HTTPException, Request

from app.core.background_request import build_background_request
from app.core.background_tasks import create_background_task
from app.core.file_manager import file_manager
from app.core.log_utils import Logger
from app.services.client_selector import client_selector
from app.services.file_reference_resolver import file_reference_resolver, FileReference
from app.services.file_replication_service import file_replication_service
from app.services.file_verification_service import file_verification_service

if TYPE_CHECKING:
    from app.core.interfaces import IConnectionManager


class FileSyncService:
    """文件同步服务 (协调器)

    职责：
    1. 协调文件引用解析和客户端选择
    2. 确保文件引用就绪
    3. 协调请求重试逻辑
    4. 管理远程文件删除任务
    """

    # =========================================================================
    # 文件解析和客户端选择
    # =========================================================================

    async def resolve_client_and_files(
        self,
        manager: "IConnectionManager",
        *,
        payload: Any,
        request_id: str,
        initial_client_id: str,
    ) -> Tuple[str, Dict[str, str], Optional[str]]:
        """确定最佳客户端并确保文件引用就绪"""
        alias_map: Dict[str, str] = {}
        fallback_alias: Optional[str] = None

        file_refs: List[FileReference] = []
        try:
            file_refs = file_reference_resolver.extract_file_references(payload, request_id)
            if file_refs:
                fallback_alias = file_refs[0].alias
                alias_list = [
                    ref.alias
                    or ref.entry.replication_map.get("local", {}).get("name")
                    or ref.sha256[:8]
                    for ref in file_refs
                ]
                Logger.info(
                    f"共解析出 {len(file_refs)} 个文件引用: {alias_list}",
                    request_id=request_id,
                )
        except HTTPException:
            raise
        except Exception as exc:
            Logger.warning("遍历 payload 文件引用失败", exc=exc, request_id=request_id)

        client_id = initial_client_id
        if not file_refs:
            return client_id, alias_map, fallback_alias

        required_entries = {ref.sha256: ref.entry for ref in file_refs}
        missing_for_initial = file_verification_service.collect_missing_for_client(
            required_entries, client_id
        )

        if missing_for_initial:
            best_client_id, missing_for_best, initial_missing = client_selector.select_best_client(
                manager, required_entries, client_id
            )

            if missing_for_best:
                await file_replication_service.replicate_files_to_client(
                    manager, best_client_id, missing_for_best, request_id
                )

            if best_client_id != client_id and initial_missing:
                file_replication_service.trigger_bulk_replication(
                    manager, client_id, initial_missing
                )

            client_id = best_client_id

        await file_verification_service.ensure_remote_files_available(
            manager, client_id, file_refs, request_id, file_replication_service
        )
        file_reference_resolver.rewrite_file_references(file_refs, client_id, request_id, alias_map)
        return client_id, alias_map, fallback_alias

    # =========================================================================
    # 远程文件删除
    # =========================================================================

    def trigger_delete_task(
        self, manager: "IConnectionManager", client_id: str, file_name: str
    ) -> None:
        """触发一个后台任务来异步删除远程文件"""
        create_background_task(self._delete_file_task(manager, client_id, file_name))

    async def _delete_file_task(
        self, manager: "IConnectionManager", client_id: str, file_name: str
    ) -> None:
        """异步删除远程文件的实际后台任务"""
        request_id = f"delete-{file_name.replace('/', '-')}"
        Logger.event(
            "DELETE_START", "开始异步远程文件删除", client_id=client_id, file_name=file_name
        )
        try:
            await manager._direct_proxy_request(
                command_type="delete_file",
                payload={"file_name": file_name},
                request_id=request_id,
                client_id=client_id,
                request=build_background_request(),
            )
            Logger.event(
                "DELETE_SUCCESS",
                "异步远程文件删除成功",
                client_id=client_id,
                file_name=file_name,
            )
        except Exception as e:
            Logger.warning(
                "异步远程文件删除失败", exc=e, client_id=client_id, file_name=file_name
            )

    # =========================================================================
    # 请求重试
    # =========================================================================

    async def execute_proxy_request_with_retry(
        self,
        manager: "IConnectionManager",
        *,
        command_type: str,
        effective_payload: Any,
        request: Request,
        request_id: str,
        client_id: str,
        is_streaming: bool,
        original_file_name: Optional[str] = None,
    ) -> Any:
        """执行代理请求，并封装了文件过期/未找到时的自动重建与重试逻辑"""
        from app.services.payload_service import payload_service

        try:
            return await manager.proxy_request(
                command_type=command_type,
                payload=effective_payload,
                request=request,
                request_id=request_id,
                is_streaming=is_streaming,
            )
        except Exception as exc:
            if not (hasattr(exc, "is_resettable") and getattr(exc, "is_resettable", False)):
                raise

            sha256_to_reset = (
                file_manager.get_sha256_by_filename(original_file_name)
                if original_file_name
                else None
            )

            if not sha256_to_reset:
                raise

            Logger.error("尝试使用重建的文件重试请求", request_id=request_id)
            try:
                if hasattr(manager, "request_manager"):
                    manager.request_manager.cleanup_request(request_id)

                new_file, new_client_id = await file_replication_service.synchronously_rebuild_file(
                    manager, sha256_to_reset
                )

                new_file_uri = new_file.get("uri") or new_file.get("name")
                if new_file_uri and original_file_name:
                    effective_payload = payload_service.update_file_uri_in_payload(
                        effective_payload,
                        original_file_name,
                        new_file_uri,
                        request_id,
                    )

                if hasattr(manager, "request_manager"):
                    manager.request_manager.register_request(request_id, new_client_id)

                return await manager.proxy_request(
                    command_type=command_type,
                    payload=effective_payload,
                    request=request,
                    request_id=request_id,
                    is_streaming=is_streaming,
                )
            except Exception as rebuild_exc:
                if hasattr(manager, "request_manager"):
                    manager.request_manager.cleanup_request(request_id)

                raise HTTPException(
                    status_code=500,
                    detail=f"File expired, and reconstruction failed: {rebuild_exc}",
                )

    # =========================================================================
    # 兼容旧代码
    # =========================================================================

    async def synchronously_rebuild_file(
        self, manager: "IConnectionManager", sha256: str
    ) -> Tuple[dict, str]:
        """同步重建文件 - 委托给 file_replication_service"""
        return await file_replication_service.synchronously_rebuild_file(manager, sha256)


file_sync_service = FileSyncService()
