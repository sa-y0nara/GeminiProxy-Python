import asyncio
import json
import random
import uuid
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Optional, Dict, List, Tuple

from fastapi import HTTPException, status

from app.core.background_tasks import create_background_task
from app.core.config import settings
from app.core.exceptions import ApiException
from app.core.file_manager import FileCacheEntry, file_manager
from app.core.log_utils import Logger
from app.core.interfaces import IConnectionManager # Use Interface

@dataclass
class FileReference:
    """指向 payload 中一个 fileData 节点的引用"""

    sha256: str
    entry: FileCacheEntry
    file_dict: dict
    alias: Optional[str] = None


class FileSyncService:
    """
    负责处理文件同步、复制、校验和上传的业务逻辑服务。
    """

    def __init__(self):
        pass

    def _resolve_sha_from_file_dict(self, file_dict: dict) -> Tuple[Optional[str], Optional[str]]:
        for key in ("fileUri", "file_uri", "fileName", "file_name", "fileId", "file_id"):
            value = file_dict.get(key)
            if not value or not isinstance(value, str):
                continue
            sha256 = file_manager.get_sha256_by_filename(value)
            if sha256:
                return sha256, value
        return None, None

    def extract_file_references(self, payload: Any, request_id: str) -> List[FileReference]:
        """遍历 payload，收集所有 fileData 节点"""
        references: List[FileReference] = []

        def _walk(node: Any):
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in ("fileData", "file_data") and isinstance(value, dict):
                        sha256, alias = self._resolve_sha_from_file_dict(value)
                        if not sha256:
                            Logger.warning("fileData 无法解析 sha256", request_id=request_id, file_data=value)
                            continue
                        entry = file_manager.get_metadata_entry(sha256)
                        if not entry:
                            # 即使文件在本地未找到，我们也记录日志但不中断整个遍历
                            # 实际请求发送时会再次检查
                            # 但为了保持兼容性，这里抛出 404
                            raise HTTPException(
                                status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"File {value.get('fileName') or value.get('fileUri')} not found in cache.",
                            )
                        references.append(FileReference(sha256=sha256, entry=entry, file_dict=value, alias=alias))
                    else:
                        _walk(value)
            elif isinstance(node, list):
                for item in node:
                    _walk(item)

        _walk(payload)
        return references

    def is_client_synced(self, entry: FileCacheEntry, client_id: str) -> bool:
        replication_data = entry.replication_map.get(client_id)
        return bool(replication_data and replication_data.get("status") == "synced")

    def collect_missing_for_client(self, required_entries: Dict[str, FileCacheEntry], client_id: str) -> List[str]:
        missing: List[str] = []
        for sha256, entry in required_entries.items():
            if not self.is_client_synced(entry, client_id):
                missing.append(sha256)
        return missing

    def select_best_client(
        self,
        manager: IConnectionManager,
        required_entries: Dict[str, FileCacheEntry],
        preferred_client: str,
    ) -> Tuple[str, List[str], List[str]]:
        """扫描所有客户端，选择缺失文件最少的客户端"""
        active_clients = manager.get_all_clients()
        if not active_clients:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No frontend clients connected",
            )

        best_clients: List[str] = []
        best_missing_count: Optional[int] = None
        missing_map: Dict[str, List[str]] = {}

        for client_id in active_clients:
            missing = self.collect_missing_for_client(required_entries, client_id)
            missing_map[client_id] = missing
            missing_count = len(missing)
            if best_missing_count is None or missing_count < best_missing_count:
                best_missing_count = missing_count
                best_clients = [client_id]
            elif missing_count == best_missing_count:
                best_clients.append(client_id)

        if not best_clients:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No frontend clients available for scheduling",
            )

        if preferred_client in best_clients:
            selected = preferred_client
        else:
            selected = random.choice(best_clients)

        return selected, missing_map.get(selected, []), missing_map.get(preferred_client, [])

    def rewrite_file_references(
        self,
        file_refs: List[FileReference],
        client_id: str,
        request_id: str,
        alias_map: Optional[Dict[str, str]] = None,
    ):
        """将 payload 中的 fileData 替换为客户端对应的 fileUri"""
        for ref in file_refs:
            replication_data = ref.entry.replication_map.get(client_id)
            if not replication_data or replication_data.get("status") != "synced":
                # 这里理论上不应发生，因为之前已经 ensure_remote_files_available
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Client {client_id} does not have required file {ref.sha256[:8]}",
                )

            final_file_name = replication_data.get("name")
            final_uri = replication_data.get("uri") or final_file_name
            if not final_uri:
                Logger.warning("复制数据缺少可用的 fileUri", client_id=client_id, sha256=ref.sha256)
                continue

            ref.file_dict["fileUri"] = final_uri
            ref.file_dict.pop("fileName", None)
            ref.file_dict.pop("file_name", None)
            ref.file_dict.pop("file_uri", None)

            if alias_map is not None:
                if final_file_name:
                    alias_map[final_file_name] = ref.sha256
                if final_uri:
                    alias_map[final_uri] = ref.sha256
                if ref.alias:
                    alias_map.setdefault(ref.alias, ref.sha256)

            Logger.debug(
                "已改写 fileData 引用",
                request_id=request_id,
                client_id=client_id,
                sha256=ref.sha256,
                file_uri=final_uri,
            )

    async def ensure_remote_files_available(
        self,
        manager: IConnectionManager,
        client_id: str,
        file_refs: List[FileReference],
        request_id: str,
    ):
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
        
        semaphore = asyncio.Semaphore(10)
        
        async def verify_with_semaphore(sha256: str, remote_name: str):
            async with semaphore:
                return await self.verify_single_file(manager, client_id, sha256, remote_name, request_id)
        
        verify_tasks = [
            verify_with_semaphore(sha256, remote_name)
            for sha256, remote_name in to_verify
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
            await self.replicate_files_to_client(manager, client_id, dedup, request_id)

    async def verify_single_file(
        self,
        manager: IConnectionManager,
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
            # 兼容不同类型的异常对象
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
            
    def _build_background_request(self) -> SimpleNamespace:
        async def _always_connected():
            return False
        return SimpleNamespace(is_disconnected=_always_connected)

    async def upload_file_via_client(
        self,
        manager: IConnectionManager,
        sha256: str,
        client_id: str,
        *,
        request_id: Optional[str] = None,
    ) -> dict:
        """指挥指定客户端从缓存下载并上传文件"""
        entry = file_manager.get_metadata_entry(sha256)
        if not entry:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found in cache.")

        effective_request_id = request_id or f"upload-{sha256[:8]}-{client_id}"
        file_manager.update_replication_status(sha256, client_id, "pending_replication")

        try:
            file_bytes = entry.local_path.read_bytes()
            file_size = len(file_bytes)
        except Exception as exc:
            file_manager.update_replication_status(sha256, client_id, "failed")
            raise HTTPException(status_code=500, detail=f"Failed to read cached file: {exc}") from exc

        display_name = entry.original_filename or "untitled"
        mime_type = entry.mime_type or "application/octet-stream"
        size_bytes_str = str(entry.size_bytes)

        background_request = self._build_background_request()

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
            # 使用 _direct_proxy_request
            initiate_response = await manager._direct_proxy_request(
                command_type="initiate_resumable_upload",
                payload=initiate_payload,
                request_id=f"{effective_request_id}-init",
                client_id=client_id,
                request=background_request, # type: ignore
            )
            upload_url = initiate_response.get("upload_url")
            if not upload_url:
                raise ApiException(status_code=500, detail="Failed to obtain upload URL from frontend.")

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
            
            # 使用 send_binary_command
            upload_response = await manager.send_binary_command(
                client_id=client_id,
                command=chunk_command,
                binary_body=file_bytes,
            )
        except Exception:
            file_manager.update_replication_status(sha256, client_id, "failed")
            raise

        gemini_file = upload_response.get("body") or upload_response.get("file")
        if isinstance(gemini_file, dict) and "file" in gemini_file and isinstance(gemini_file["file"], dict):
            gemini_file = gemini_file["file"]
        if not gemini_file:
            file_manager.update_replication_status(sha256, client_id, "failed")
            raise ApiException(status_code=500, detail="Frontend did not return a file object after upload.")

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
        manager: IConnectionManager,
        client_id: str,
        sha_list: List[str],
        request_id: str,
    ):
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

    async def resolve_client_and_files(
        self,
        manager: IConnectionManager,
        *,
        payload: Any,
        request_id: str,
        initial_client_id: str,
    ) -> Tuple[str, Dict[str, str], Optional[str]]:
        """Determine the best client and ensure file references are ready."""
        alias_map: Dict[str, str] = {}
        fallback_alias: Optional[str] = None

        file_refs: List[FileReference] = []
        try:
            file_refs = self.extract_file_references(payload, request_id)
            if file_refs:
                fallback_alias = file_refs[0].alias
                alias_list = [
                    ref.alias or ref.entry.replication_map.get("local", {}).get("name") or ref.sha256[:8]
                    for ref in file_refs
                ]
                Logger.info(
                    f"[调试] 共解析出 {len(file_refs)} 个文件引用: {alias_list}",
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
        missing_for_initial = self.collect_missing_for_client(required_entries, client_id)

        if missing_for_initial:
            best_client_id, missing_for_best, initial_missing = self.select_best_client(
                manager, required_entries, client_id
            )

            if missing_for_best:
                await self.replicate_files_to_client(manager, best_client_id, missing_for_best, request_id)

            if best_client_id != client_id and initial_missing:
                self.trigger_bulk_replication(manager, client_id, initial_missing)

            client_id = best_client_id

        await self.ensure_remote_files_available(manager, client_id, file_refs, request_id)
        self.rewrite_file_references(file_refs, client_id, request_id, alias_map)
        return client_id, alias_map, fallback_alias

    def trigger_bulk_replication(self, manager: IConnectionManager, client_id: str, sha_list: List[str]):
        """触发后台任务，批量为客户端复制缺失文件"""
        if not sha_list:
            return
        task_id = f"heal-{client_id}-{uuid.uuid4().hex[:6]}"
        create_background_task(self.bulk_replication_task(manager, client_id, sha_list, task_id))

    async def bulk_replication_task(self, manager: IConnectionManager, client_id: str, sha_list: List[str], task_id: str):
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

    async def synchronously_rebuild_file(self, manager: IConnectionManager, sha256: str) -> Tuple[dict, str]:
        """
        同步重建文件：轮询选择一个客户端，阻塞式地指挥它重新上传文件。
        """
        request_id = f"rebuild-{sha256[:8]}-{uuid.uuid4()}"
        Logger.event("REBUILD_START", "开始同步文件重建", sha256=sha256)

        client_id = manager.get_next_client()
        try:
            gemini_file = await self.upload_file_via_client(manager, sha256, client_id, request_id=request_id)
            Logger.event("REBUILD_SUCCESS", "同步文件重建成功", sha256=sha256, client_id=client_id)
            return gemini_file, client_id

        except Exception as e:
            Logger.error("同步文件重建失败", exc=e, sha256=sha256, client_id=client_id)
            raise  # 将异常向上抛出

    def trigger_delete_task(self, manager: IConnectionManager, client_id: str, file_name: str):
        """触发一个后台任务来异步删除远程文件"""
        create_background_task(self.delete_file_task(manager, client_id, file_name))

    async def delete_file_task(self, manager: IConnectionManager, client_id: str, file_name: str):
        """异步删除远程文件的实际后台任务"""
        request_id = f"delete-{file_name.replace('/', '-')}"
        Logger.event("DELETE_START", "开始异步远程文件删除", client_id=client_id, file_name=file_name)
        try:
            await manager._direct_proxy_request(
                command_type="delete_file",
                payload={"file_name": file_name},
                request_id=request_id,
                client_id=client_id,
                request=self._build_background_request(), # type: ignore
            )
            Logger.event("DELETE_SUCCESS", "异步远程文件删除成功", client_id=client_id, file_name=file_name)
        except Exception as e:
            # 忽略错误，因为最终文件会被 TTL 清理
            Logger.warning("异步远程文件删除失败", exc=e, client_id=client_id, file_name=file_name)


file_sync_service = FileSyncService()
