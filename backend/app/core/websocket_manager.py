"""WebSocket 连接管理器模块

作为 Facade 协调各个子模块，提供统一的 WebSocket 连接管理接口。
"""

import asyncio
import uuid
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request, WebSocket, status
from pydantic import BaseModel
from contextlib import asynccontextmanager

from app.core.config import settings
from app.core.background_request import build_background_request
from app.core.connection_registry import ConnectionRegistry
from app.core.log_utils import Logger
from app.core.proxy_handler import ProxyHandler
from app.core.request_manager import RequestManager
from app.core.streaming_handler import StreamingHandler
from app.core.api_request_handler import ApiRequestHandler
from app.services.file_sync_service import file_sync_service


class ConnectionManager:
    """WebSocket 连接管理器 (Facade)

    协调以下组件：
    - ConnectionRegistry: 管理连接池
    - RequestManager: 管理请求生命周期
    - StreamingHandler: 处理流式请求
    - ProxyHandler: 处理代理请求
    - ApiRequestHandler: 处理 API 请求入口
    """

    def __init__(self) -> None:
        self.registry = ConnectionRegistry()
        self.request_manager = RequestManager()
        self.streaming_handler = StreamingHandler(self.request_manager)
        self.proxy_handler = ProxyHandler(self.request_manager)
        self.api_handler = ApiRequestHandler(self.request_manager)

    # =========================================================================
    # 属性 (兼容旧代码)
    # =========================================================================

    @property
    def active_connections(self) -> Dict[str, WebSocket]:
        """兼容旧代码访问 active_connections"""
        return self.registry.active_connections

    @property
    def request_to_client(self) -> Dict[str, str]:
        """兼容旧代码访问 request_to_client"""
        return self.request_manager.request_to_client

    # =========================================================================
    # 连接管理
    # =========================================================================

    async def connect(self, websocket: WebSocket, client_id: str) -> None:
        await self.registry.register(websocket, client_id)

    async def disconnect(self, client_id: str) -> None:
        """断开客户端连接，清理所有活跃请求"""
        request_ids = self.request_manager.get_active_requests_for_client(client_id)
        if request_ids:
            Logger.event("DISCONNECT", f"取消 {len(request_ids)} 个请求", client_id=client_id)
            for request_id in request_ids:
                self.cancel_request(request_id)
        await self.registry.unregister(client_id)

    def get_next_client(self) -> str:
        return self.registry.get_next_client()

    def get_all_clients(self) -> list[str]:
        return self.registry.get_all_clients()

    # =========================================================================
    # 消息处理
    # =========================================================================

    async def handle_message(self, message: dict[str, Any]) -> None:
        """处理从前端收到的响应消息"""
        payload = message.get("payload", {})
        request_id = message.get("id")

        if request_id:
            is_finished = payload.get("is_finished", "N/A")
            status_info = message.get("status", {})
            Logger.debug(f"接收消息 {request_id} | 完成: {is_finished} | 状态: {status_info}")

        if not request_id:
            return

        # 路由到相应的处理器
        if self.request_manager.get_stream_queue(request_id):
            await self.streaming_handler.handle_streaming_message(request_id, payload, message)
            return

        if self.request_manager.get_future(request_id):
            await self.proxy_handler.handle_non_streaming_message(request_id, payload, message)

    # =========================================================================
    # 代理请求
    # =========================================================================

    @asynccontextmanager
    async def monitored_proxy_request(self, request_id: str, request: Request):
        """监控代理请求的上下文管理器"""
        client_id = self.get_next_client()
        self.request_manager.register_request(request_id, client_id)
        Logger.debug(f"注册请求 {request_id} → {client_id}")

        try:
            yield client_id
        finally:
            # 注意：is_disconnected() 在某些边缘情况下可能失败，
            # 但这里已经处于 finally 清理阶段，安全忽略异常
            try:
                if await request.is_disconnected():
                    Logger.event("DISCONNECT", "客户端断开连接", request_id=request_id)
                    await self.cancel_request(request_id)
                else:
                    self.request_manager.cleanup_request(request_id)
            except (ConnectionError, RuntimeError, OSError):
                # 连接可能已销毁或底层 socket 异常，静默处理
                self.request_manager.cleanup_request(request_id)

    async def proxy_request(
        self,
        command_type: str,
        payload: Any,
        request: Request,
        request_id: str,
        is_streaming: bool = False,
    ) -> Any:
        """核心代理方法"""
        client_id = self.request_manager.get_client_for_request(request_id)

        if not client_id:
            client_id = self.get_next_client()
            self.request_manager.register_request(request_id, client_id)

        websocket = self.registry.get_socket(client_id)
        if not websocket:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Client {client_id} is no longer connected",
            )

        payload_to_send = self._serialize_payload(payload)
        command: dict[str, Any] = {
            "id": request_id,
            "type": command_type,
            "payload": payload_to_send,
        }

        Logger.ws_send(request_id, client_id, command_type, command=command)

        if is_streaming:
            return await self.streaming_handler.handle_streaming_request(
                websocket, command, request_id, request
            )

        return await self.proxy_handler.handle_non_streaming_request(
            websocket, command, request_id
        )

    async def _direct_proxy_request(
        self,
        command_type: str,
        payload: Any,
        request_id: str,
        client_id: str,
        request: Optional[Request] = None,
        is_streaming: bool = False,
    ) -> Any:
        """直接代理方法：指定客户端发送命令"""
        websocket = self.registry.get_socket(client_id)
        if not websocket:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Client {client_id} not connected.",
            )

        payload_to_send = self._serialize_payload(payload)
        command: dict[str, Any] = {
            "id": request_id,
            "type": command_type,
            "payload": payload_to_send,
        }

        Logger.ws_send(request_id, client_id, command_type, command=command)
        self.request_manager.register_request(request_id, client_id)

        if is_streaming:
            if not request:
                raise ValueError("Streaming requests require a 'request' object.")
            return await self.streaming_handler.handle_streaming_request(
                websocket, command, request_id, request
            )

        return await self.proxy_handler.handle_non_streaming_request(
            websocket, command, request_id
        )

    async def send_binary_command(
        self,
        *,
        client_id: str,
        command: dict[str, Any],
        binary_body: bytes,
    ) -> Any:
        """发送二进制命令"""
        websocket = self.registry.get_socket(client_id)
        if not websocket:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Client {client_id} not connected.",
            )

        request_id = command.get("id")
        if not request_id:
            raise ValueError("Binary commands require an 'id' field.")

        # 验证大小
        max_binary_bytes = settings.MAX_BINARY_SIZE_MB * 1024 * 1024
        if binary_body and len(binary_body) > max_binary_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_PAYLOAD_TOO_LARGE,
                detail=f"Binary payload exceeds maximum size of {settings.MAX_BINARY_SIZE_MB}MB",
            )

        Logger.ws_send(request_id, client_id, command.get("type", "unknown"), command=command)
        self.request_manager.register_request(request_id, client_id)

        packet = self.proxy_handler.build_binary_packet(command, binary_body)
        return await self.proxy_handler.await_response(
            request_id,
            command,
            lambda: websocket.send_bytes(packet),
        )



    # =========================================================================
    # API 请求入口 (委托给 ApiRequestHandler)
    # =========================================================================

    async def handle_api_request(
        self,
        *,
        command_type: str,
        payload: Any,
        request: Optional[Request] = None,
        is_streaming: bool = False,
    ) -> Any:
        """API 请求处理入口"""
        return await self.api_handler.handle_api_request(
            self,
            command_type=command_type,
            payload=payload,
            request=request,
            is_streaming=is_streaming,
        )

    # =========================================================================
    # 请求取消
    # =========================================================================

    def cancel_request(self, request_id: str) -> bool:
        """取消请求"""
        client_id = self.request_manager.get_client_for_request(request_id)
        if not client_id:
            return False

        self.request_manager.cleanup_request(request_id)

        if self.registry.is_connected(client_id):
            # Fire-and-forget: 发送取消信号但不阻塞
            task = asyncio.create_task(self._send_cancel_signal_async(client_id, request_id))
            task.add_done_callback(self._handle_cancel_task_exception)

        return True

    def _handle_cancel_task_exception(self, task: asyncio.Task) -> None:
        """处理取消任务中的异常，避免未处理的异常警告"""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            Logger.debug(f"取消信号任务异常 (可忽略): {type(exc).__name__}")

    async def _send_cancel_signal_async(self, websocket_id: str, request_id: str) -> None:
        try:
            websocket = self.registry.get_socket(websocket_id)
            if websocket:
                await websocket.send_json({"type": "cancel_task", "id": request_id})
                Logger.event("CANCEL", "发送取消信号", request_id=request_id, client_id=websocket_id)
        except Exception as e:
            Logger.debug(f"发送取消信号失败 (可忽略): {type(e).__name__}", request_id=request_id, client_id=websocket_id)

    # =========================================================================
    # 工具方法
    # =========================================================================

    def _serialize_payload(self, payload: Any) -> Any:
        """序列化 payload"""
        if isinstance(payload, BaseModel):
            return payload.model_dump(by_alias=True, exclude_none=True)
        return payload or {}

    # =========================================================================
    # 文件同步快捷方法
    # =========================================================================

    async def upload_file_from_cache(self, sha256: str) -> tuple[dict, str]:
        return await file_sync_service.synchronously_rebuild_file(self, sha256)

    async def send_command_to_client(
        self,
        *,
        client_id: str,
        command_type: str,
        payload: Any,
        request_id: Optional[str] = None,
        is_streaming: bool = False,
    ) -> Any:
        effective_request_id = request_id or f"direct-{command_type}-{uuid.uuid4()}"
        return await self._direct_proxy_request(
            command_type=command_type,
            payload=payload,
            request_id=effective_request_id,
            client_id=client_id,
            request=build_background_request(),
            is_streaming=is_streaming,
        )

    def trigger_delete_task(self, client_id: str, file_name: str) -> None:
        file_sync_service.trigger_delete_task(self, client_id, file_name)


manager = ConnectionManager()
