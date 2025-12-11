"""API 请求处理模块

负责处理高层 API 请求的入口逻辑，包括文件同步和重试。
"""

import uuid
from typing import Any, Optional, TYPE_CHECKING

from fastapi import Request

from app.core.log_utils import Logger
from app.services.file_sync_service import file_sync_service
from app.services.payload_service import payload_service

if TYPE_CHECKING:
    from app.core.request_manager import RequestManager


class ApiRequestHandler:
    """API 请求处理器
    
    职责：
    1. 处理 API 请求入口
    2. 协调 payload 准备
    3. 协调文件同步
    4. 执行带重试的代理请求
    """

    def __init__(self, request_manager: "RequestManager") -> None:
        self.request_manager = request_manager

    async def handle_api_request(
        self,
        manager: Any,  # ConnectionManager, avoid circular import
        *,
        command_type: str,
        payload: Any,
        request: Optional[Request] = None,
        is_streaming: bool = False,
    ) -> Any:
        """API 请求处理入口"""
        request_id = str(uuid.uuid4())
        effective_payload = await payload_service.prepare_payload(command_type, payload, request_id)
        original_file_name = payload_service.find_original_file_name(effective_payload, request_id)

        client_id = manager.get_next_client()

        # 委托给 FileSyncService 处理文件同步
        client_id, alias_map, fallback_alias = await file_sync_service.resolve_client_and_files(
            manager,
            payload=effective_payload,
            request_id=request_id,
            initial_client_id=client_id,
        )
        if not original_file_name and fallback_alias:
            original_file_name = fallback_alias

        if alias_map:
            self.request_manager.request_file_aliases[request_id] = alias_map

        try:
            self.request_manager.register_request(request_id, client_id)
            return await file_sync_service.execute_proxy_request_with_retry(
                manager,
                command_type=command_type,
                effective_payload=effective_payload,
                request=request,
                request_id=request_id,
                client_id=client_id,
                is_streaming=is_streaming,
                original_file_name=original_file_name,
            )
        except Exception:
            self.request_manager.cleanup_request(request_id)
            raise
