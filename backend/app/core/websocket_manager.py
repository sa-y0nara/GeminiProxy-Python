import asyncio
import json
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, Optional, Dict

from fastapi import HTTPException, Request, WebSocket, status
from pydantic import BaseModel

from app.core.config import settings
from app.core.exceptions import ApiException
from app.core.file_manager import file_manager
from app.core.log_utils import Logger
from app.services.file_sync_service import file_sync_service
from app.services.payload_service import payload_service

# 引入新组件
from app.core.connection_registry import ConnectionRegistry
from app.core.request_manager import RequestManager

class ConnectionManager:
    """
    WebSocket 连接管理器 (重构版)。
    作为 Facade 协调 ConnectionRegistry 和 RequestManager。
    """
    def __init__(self) -> None:
        self.registry = ConnectionRegistry()
        self.request_manager = RequestManager()

    @property
    def active_connections(self) -> Dict[str, WebSocket]:
        """兼容旧代码访问 active_connections"""
        return self.registry.active_connections

    @property
    def request_to_client(self) -> Dict[str, str]:
        """兼容旧代码访问 request_to_client (主要供测试或调试用)"""
        return self.request_manager.request_to_client

    async def connect(self, websocket: WebSocket, client_id: str):
        await self.registry.register(websocket, client_id)

    async def disconnect(self, client_id: str):
        """断开客户端连接，清理所有活跃请求"""
        # 获取该客户端所有正在进行的请求
        request_ids = self.request_manager.get_active_requests_for_client(client_id)
        if request_ids:
            Logger.event("DISCONNECT", f"取消 {len(request_ids)} 个请求", client_id=client_id)
            for request_id in request_ids:
                self.cancel_request(request_id)

        await self.registry.unregister(client_id)

    async def handle_message(self, message: dict[str, Any]):
        """处理从前端收到的响应消息"""
        payload = message.get("payload", {})
        request_id = message.get("id")

        if request_id:
            is_finished = payload.get("is_finished", "N/A")
            status_info = message.get("status", {})
            # 降低日志级别避免刷屏，或者保持 debug
            Logger.debug(f"接收消息 {request_id} | 完成: {is_finished} | 状态: {status_info}")

        if not request_id:
            return

        # 检查是否是流式响应
        if self.request_manager.get_stream_queue(request_id):
            await self._handle_streaming_message(request_id, payload, message)
            return

        # 检查是否是非流式响应
        if self.request_manager.get_future(request_id):
            await self._handle_non_streaming_message(request_id, payload, message)

    async def _handle_streaming_message(self, request_id: str, payload: dict, message: dict):
        """处理流式响应消息"""
        if not payload.get("is_streaming"):
            return

        queue = self.request_manager.get_stream_queue(request_id)
        if not queue:
            return

        chunk_num = self.request_manager.increment_chunk_count(request_id)

        if "chunk" in payload:
            try:
                queue.put_nowait(payload["chunk"])
            except asyncio.QueueFull:
                Logger.warning(f"流式队列已满，丢弃 chunk", request_id=request_id)

        client_id = self.request_manager.get_client_for_request(request_id) or "unknown"

        if payload.get("is_finished"):
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
            Logger.ws_receive(request_id, client_id, is_stream_end=True, total_chunks=chunk_num, data=message)
            self.request_manager.cleanup_request(request_id)
        elif chunk_num == 1:
            Logger.ws_receive(request_id, client_id, is_stream_start=True, data=message)
        else:
            Logger.ws_receive(request_id, client_id, is_stream_middle=True, data=message)

    async def _handle_non_streaming_message(self, request_id: str, payload: dict, message: dict):
        """处理非流式响应消息"""
        client_id = self.request_manager.get_client_for_request(request_id) or "unknown"
        Logger.ws_receive(request_id, client_id, data=message)
        
        future = self.request_manager.get_future(request_id)
        if not future or future.done():
            return

        # 从 request_manager 移除 future，防止重复处理
        self.request_manager.remove_future(request_id)

        error_info = message.get("status", {}).get("error")
        if error_info:
            if isinstance(error_info, dict):
                code = error_info.get("code", 500)
                detail = error_info
            else:
                code = 500
                detail = {"message": str(error_info)}

            exception = ApiException(status_code=code, detail=detail)
            future.set_exception(exception)
        else:
            future.set_result(payload)

    def get_next_client(self) -> str:
        return self.registry.get_next_client()

    def get_all_clients(self) -> list[str]:
        return self.registry.get_all_clients()

    @asynccontextmanager
    async def monitored_proxy_request(self, request_id: str, request: Request):
        """
        监控代理请求的上下文管理器。
        """
        client_id = self.get_next_client()
        self.request_manager.register_request(request_id, client_id)
        Logger.debug(f"注册请求 {request_id} → {client_id}")

        try:
            yield client_id
        finally:
            if await request.is_disconnected():
                Logger.event("DISCONNECT", "客户端断开连接", request_id=request_id)
                await self.cancel_request(request_id)
            else:
                # 兜底清理
                self.request_manager.cleanup_request(request_id)

    async def _direct_proxy_request(
        self,
        command_type: str,
        payload: Any,
        request_id: str,
        client_id: str,
        request: Optional[Request] = None,
        is_streaming: bool = False,
    ) -> Any:
        """
        直接代理方法：指定客户端发送命令。
        保留此方法以兼容 FileSyncService。
        """
        websocket = self.registry.get_socket(client_id)
        if not websocket:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Client {client_id} not connected.")

        if isinstance(payload, BaseModel):
            payload_to_send = payload.model_dump(by_alias=True, exclude_none=True)
        else:
            payload_to_send = payload or {}

        command: dict[str, Any] = {
            "id": request_id,
            "type": command_type,
            "payload": payload_to_send,
        }

        # 发送命令
        Logger.ws_send(request_id, client_id, command_type, command=command)

        if is_streaming:
            if not request:
                raise ValueError("Streaming requests require a 'request' object.")
            # 对于 direct 请求，我们也需要注册，以便接收响应
            self.request_manager.register_request(request_id, client_id)
            return await self._handle_streaming_request(websocket, command, request_id, request)

        # 注册非流式请求
        self.request_manager.register_request(request_id, client_id)
        return await self._handle_non_streaming_request(websocket, command, request_id)

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
        
        # 注册请求
        self.request_manager.register_request(request_id, client_id)
        
        packet = self._build_binary_packet(command, binary_body)
        return await self._await_response(
            request_id,
            command,
            lambda: websocket.send_bytes(packet),
        )
    
    # 兼容旧代码的方法别名
    _send_binary_command = send_binary_command

    async def proxy_request(
        self,
        command_type: str,
        payload: Any,
        request: Request,
        request_id: str,
        is_streaming: bool = False,
    ) -> Any:
        """核心代理方法"""
        # 注意：这里假设调用者已经通过 monitored_proxy_request 或其他方式注册了 client
        # 但为了安全起见，如果 request_manager 中没有映射，我们尝试获取
        client_id = self.request_manager.get_client_for_request(request_id)
        
        if not client_id:
             # 如果没有注册（这是不应该发生的，如果使用了 monitored_proxy_request），我们尝试分配一个
             # 这主要为了兼容未经过 monitored_proxy_request 的直接调用（如果有）
             client_id = self.get_next_client()
             self.request_manager.register_request(request_id, client_id)

        websocket = self.registry.get_socket(client_id)
        if not websocket:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Client {client_id} is no longer connected"
            )

        if isinstance(payload, BaseModel):
            payload_to_send = payload.model_dump(by_alias=True, exclude_none=True)
        else:
            payload_to_send = payload or {}

        command: dict[str, Any] = {
            "id": request_id,
            "type": command_type,
            "payload": payload_to_send,
        }

        Logger.ws_send(request_id, client_id, command_type, command=command)

        if is_streaming:
            if not request:
                raise ValueError("Streaming requests require a 'request' object.")
            return await self._handle_streaming_request(websocket, command, request_id, request)

        return await self._handle_non_streaming_request(websocket, command, request_id)

    async def _handle_non_streaming_request(
        self, websocket: WebSocket, command: dict[str, Any], request_id: str
    ) -> Any:
        return await self._await_response(
            request_id,
            command,
            lambda: websocket.send_json(command),
        )

    async def _await_response(
        self,
        request_id: str,
        command: dict[str, Any],
        sender,
    ) -> Any:
        """发送并等待响应"""
        # 创建 future 并注册
        future = self.request_manager.create_future(request_id)
        
        try:
            await sender()
            return await asyncio.wait_for(future, timeout=settings.WEBSOCKET_TIMEOUT)
        except asyncio.TimeoutError:
            self.request_manager.cleanup_request(request_id)
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Request to frontend client timed out",
            )
        except ApiException as exc:
            self.request_manager.cleanup_request(request_id)
            self._mark_resettable_if_needed(exc, command, request_id)
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        except Exception as exc:
            self.request_manager.cleanup_request(request_id)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error communicating with frontend client: {exc}",
            )

    def _mark_resettable_if_needed(self, exc: ApiException, command: dict[str, Any], request_id: str):
        """检查错误是否可重置"""
        error_detail = exc.detail or {}
        if not isinstance(error_detail, dict):
            error_detail = {}
        error_message = str(error_detail.get("message", "")).lower() if error_detail else ""
        
        # 优化：不只是依赖字符串，未来可以扩展错误码检查
        if "not found" not in error_message and "file not found" not in error_message:
            return

        file_name = command.get("payload", {}).get("fileName")
        if not file_name:
             # 尝试从别名映射中恢复
             alias_map = self.request_manager.request_file_aliases.get(request_id) or {}
             if len(alias_map) == 1:
                file_name = next(iter(alias_map.keys()))
        
        if not file_name:
            return

        sha256 = file_manager.get_sha256_by_filename(file_name)
        if not sha256:
            return
            
        Logger.warning("检测到文件过期/未找到，触发全局重置", file_name=file_name, sha256=sha256)
        file_manager.reset_replication_map(sha256)
        exc.is_resettable = True

    def _build_binary_packet(self, command: dict[str, Any], binary_body: bytes) -> bytes:
        metadata_bytes = json.dumps(
            command,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        header_bytes = len(metadata_bytes).to_bytes(4, byteorder="big", signed=False)
        return header_bytes + metadata_bytes + (binary_body or b"")

    async def handle_api_request(
        self,
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
        
        # 获取初始客户端
        client_id = self.get_next_client()

        # 委托给 FileSyncService 处理文件同步
        client_id, alias_map, fallback_alias = await file_sync_service.resolve_client_and_files(
            self,
            payload=effective_payload,
            request_id=request_id,
            initial_client_id=client_id,
        )
        if not original_file_name and fallback_alias:
            original_file_name = fallback_alias

        if alias_map:
            self.request_manager.request_file_aliases[request_id] = alias_map

        try:
            # 注册请求并绑定到客户端
            self.request_manager.register_request(request_id, client_id)
            
            # 由于 handle_api_request 没有使用 monitored_proxy_request 上下文管理器，
            # 我们需要手动管理流式请求的生命周期，或者在这里使用 try/finally 块确保清理
            # 这里的逻辑稍微有点 tricky：
            # 如果是流式，proxy_request 返回生成器，生成器结束时需要清理。
            # 如果是非流式，await response 后清理。
            
            # 为了简化，我们调用 proxy_request，它内部会调用 _handle_streaming_request
            # _handle_streaming_request 返回的 generator 在结束时会清理。
            
            return await self.proxy_request(
                command_type=command_type,
                payload=effective_payload,
                request=request,
                request_id=request_id,
                is_streaming=is_streaming,
            )

        except Exception as exc:
            # 错误恢复逻辑
            if hasattr(exc, 'is_resettable') and getattr(exc, 'is_resettable', False):
                sha256_to_reset = file_manager.get_sha256_by_filename(original_file_name) if original_file_name else None
                if sha256_to_reset:
                    Logger.error("尝试使用重建的文件重试请求", request_id=request_id)
                    try:
                        # 清理旧的请求绑定
                        self.request_manager.cleanup_request(request_id)
                        
                        new_file, new_client_id = await file_sync_service.synchronously_rebuild_file(self, sha256_to_reset)
                        
                        # 更新 payload 中的文件 URI（递归更新所有匹配的引用）
                        new_file_uri = new_file.get("uri") or new_file.get("name")
                        if new_file_uri and original_file_name:
                            effective_payload = payload_service.update_file_uri_in_payload(
                                effective_payload,
                                original_file_name,
                                new_file_uri,
                                request_id,
                            )
                        
                        # 重新注册请求到新客户端
                        self.request_manager.register_request(request_id, new_client_id)
                        return await self.proxy_request(
                            command_type=command_type,
                            payload=effective_payload,
                            request=request,
                            request_id=request_id,
                            is_streaming=is_streaming,
                        )
                    except Exception as rebuild_exc:
                         # 确保清理
                        self.request_manager.cleanup_request(request_id)
                        raise HTTPException(
                            status_code=500,
                            detail=f"File expired, and reconstruction failed: {rebuild_exc}",
                        )
            # 发生其他异常也要清理
            # 注意：流式请求在生成器中抛出异常时也会清理吗？
            # 如果 proxy_request 抛出异常（比如发送命令失败），我们需要在这里清理
            self.request_manager.cleanup_request(request_id)
            raise

    async def _handle_streaming_request(
        self,
        websocket: WebSocket,
        command: dict[str, Any],
        request_id: str,
        request: Request,
    ) -> AsyncGenerator[Any, None]:
        """处理流式请求，返回生成器"""
        queue = self.request_manager.create_stream_queue(request_id)

        async def stream_generator() -> AsyncGenerator[Any, None]:
            try:
                await websocket.send_json(command)
                while True:
                    if await request.is_disconnected():
                        Logger.event("DISCONNECT", "流式传输中断", request_id=request_id)
                        break

                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    if item is None:
                        break
                    yield item
            finally:
                self.request_manager.cleanup_request(request_id)

        return stream_generator()

    def cancel_request(self, request_id: str) -> bool:
        """取消请求"""
        client_id = self.request_manager.get_client_for_request(request_id)
        if not client_id:
            return False

        self.request_manager.cleanup_request(request_id)

        # 发送取消信号
        if self.registry.is_connected(client_id):
            asyncio.create_task(self._send_cancel_signal_async(client_id, request_id))

        return True

    async def _send_cancel_signal_async(self, websocket_id: str, request_id: str):
        try:
            websocket = self.registry.get_socket(websocket_id)
            if websocket:
                await websocket.send_json({"type": "cancel_task", "id": request_id})
                Logger.event("CANCEL", "发送取消信号", request_id=request_id, client_id=websocket_id)
        except Exception:
            pass
            
    # FileSyncService 兼容方法
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
            request=self._build_background_request(),
            is_streaming=is_streaming,
        )
        
    def _build_background_request(self) -> SimpleNamespace:
        async def _always_connected():
            return False
        return SimpleNamespace(is_disconnected=_always_connected)
        
    def trigger_delete_task(self, client_id: str, file_name: str):
        file_sync_service.trigger_delete_task(self, client_id, file_name)

manager = ConnectionManager()
