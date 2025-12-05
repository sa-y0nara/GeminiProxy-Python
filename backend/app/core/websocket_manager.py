import asyncio
import json
import logging
import random
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional
from types import SimpleNamespace

from app.core.background_tasks import create_background_task
from app.core.config import settings
from app.core.exceptions import ApiException
from app.core.file_manager import FileCacheEntry, file_manager
from app.core.log_utils import Logger
from app.services.file_sync_service import file_sync_service
from app.services.payload_service import payload_service
from fastapi import HTTPException, Request, WebSocket, status
from pydantic import BaseModel


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: dict[str, WebSocket] = {}
        self.pending_responses: dict[str, asyncio.Future] = {}
        self.streaming_responses: dict[str, asyncio.Queue] = {}
        self.streaming_chunk_count: dict[str, int] = {}

        # 新增：追踪 request_id 到 client_id 的映射
        self.request_to_client: dict[str, str] = {}

        # 新增：追踪每个 client 正在处理的请求集合
        self.client_active_requests: dict[str, set[str]] = {}

        # 请求与文件别名映射，用于错误回退
        self.request_file_aliases: dict[str, dict[str, str]] = {}

        self._client_ids: list[str] = []
        self._next_client_index: int = 0

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        self._client_ids.append(client_id)
        self.client_active_requests[client_id] = set()

    async def disconnect(self, client_id: str):
        """断开客户端连接，清理所有活跃请求"""
        if client_id in self.client_active_requests:
            request_ids = list(self.client_active_requests[client_id])
            Logger.event("DISCONNECT", f"取消 {len(request_ids)} 个请求", client_id=client_id)

            for request_id in request_ids:
                self.cancel_request(request_id)

            self.client_active_requests.pop(client_id, None)

        if client_id in self.active_connections:
            del self.active_connections[client_id]
        if client_id in self._client_ids:
            self._client_ids.remove(client_id)

    async def handle_message(self, message: dict[str, Any]):
        """处理从前端收到的响应消息"""
        payload = message.get("payload", {})
        request_id = message.get("id")

        if request_id:
            is_finished = payload.get("is_finished", "N/A")
            status = message.get("status", {})
            Logger.debug(f"接收消息 {request_id} | 完成: {is_finished} | 状态: {status}")
            Logger.debug(f"完整消息内容: {message}")

        # 处理流式响应
        if request_id in self.streaming_responses:
            await self._handle_streaming_message(request_id, payload, message)
            return

        # 处理非流式响应
        if request_id and request_id in self.pending_responses:
            await self._handle_non_streaming_message(request_id, payload, message)

    async def _handle_streaming_message(self, request_id: str, payload: dict, message: dict):
        """处理流式响应消息"""
        if not payload.get("is_streaming"):
            return

        queue = self.streaming_responses[request_id]
        if request_id not in self.streaming_chunk_count:
            self.streaming_chunk_count[request_id] = 0
        self.streaming_chunk_count[request_id] += 1
        chunk_num = self.streaming_chunk_count[request_id]

        if "chunk" in payload:
            queue.put_nowait(payload["chunk"])

        client_id = self.request_to_client.get(request_id, "unknown")

        if payload.get("is_finished"):
            queue.put_nowait(None)
            Logger.ws_receive(request_id, client_id, is_stream_end=True, total_chunks=chunk_num, data=message)
            self._cleanup_request(request_id)
        elif chunk_num == 1:
            Logger.ws_receive(request_id, client_id, is_stream_start=True, data=message)
        else:
            Logger.ws_receive(request_id, client_id, is_stream_middle=True, data=message)

    async def _handle_non_streaming_message(self, request_id: str, payload: dict, message: dict):
        """处理非流式响应消息"""
        client_id = self.request_to_client.get(request_id, "unknown")
        Logger.ws_receive(request_id, client_id, data=message)
        future = self.pending_responses.pop(request_id)
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
        """轮询算法，获取下一个健康的客户端ID"""
        if not self._client_ids:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No frontend clients connected",
            )
        client_id = self._client_ids[self._next_client_index]
        self._next_client_index = (self._next_client_index + 1) % len(self._client_ids)
        return client_id

    def get_all_clients(self) -> list[str]:
        """获取所有连接的客户端ID列表"""
        return list(self.active_connections.keys())

    @asynccontextmanager
    async def monitored_proxy_request(self, request_id: str, request: Request):
        """
        An async context manager to monitor and clean up a proxy request.
        It handles request registration and cancellation/cleanup upon exit.
        """
        client_id = self.get_next_client()
        self.request_to_client[request_id] = client_id
        self.client_active_requests[client_id].add(request_id)
        Logger.debug(f"注册请求 {request_id} → {client_id}")

        try:
            yield client_id
        finally:
            if await request.is_disconnected():
                Logger.event("DISCONNECT", "客户端断开连接", request_id=request_id)
                await self.cancel_request(request_id)
            else:
                # For non-streaming requests, the future is cleaned up when the response is received.
                # For streaming, it's cleaned up when the stream ends.
                # This is a fallback for unexpected exits.
                if request_id in self.pending_responses or request_id in self.streaming_responses:
                    self._cleanup_request(request_id)

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
        直接代理方法：指定客户端发送命令，用于后台任务。
        不通过 monitored_proxy_request 上下文管理器。
        """
        if client_id not in self.active_connections:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Client {client_id} not connected.")

        websocket = self.active_connections[client_id]

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

    async def send_binary_command(
        self,
        *,
        client_id: str,
        command: dict[str, Any],
        binary_body: bytes,
    ) -> Any:
        """
        发送携带原始文件数据的二进制命令。
        """
        if client_id not in self.active_connections:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Client {client_id} not connected.",
            )

        request_id = command.get("id")
        if not request_id:
            raise ValueError("Binary commands require an 'id' field.")

        # 验证二进制数据大小
        max_binary_bytes = settings.MAX_BINARY_SIZE_MB * 1024 * 1024
        if binary_body and len(binary_body) > max_binary_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_PAYLOAD_TOO_LARGE,
                detail=f"Binary payload exceeds maximum size of {settings.MAX_BINARY_SIZE_MB}MB",
            )

        websocket = self.active_connections[client_id]
        Logger.ws_send(request_id, client_id, command.get("type", "unknown"), command=command)
        return await self._handle_binary_request(websocket, command, binary_body, request_id)

    async def proxy_request(
        self,
        command_type: str,
        payload: Any,
        request: Request,
        request_id: str,
        is_streaming: bool = False,
    ) -> Any:
        """
        核心代理方法：选择一个客户端，发送命令，并等待响应。
        对于流式请求，返回异步生成器。
        实际的注册和清理由 `monitored_proxy_request` 上下文管理器处理。
        """
        client_id = self.request_to_client.get(request_id)
        if not client_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Request not properly registered with a client"
            )
        
        websocket = self.active_connections.get(client_id)
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
        """Handles a non-streaming request."""
        return await self._await_response(
            request_id,
            command,
            lambda: websocket.send_json(command),
        )

    async def _handle_binary_request(
        self,
        websocket: WebSocket,
        command: dict[str, Any],
        binary_body: bytes,
        request_id: str,
    ) -> Any:
        """Handles a binary upload request."""
        packet = self._build_binary_packet(command, binary_body)
        return await self._await_response(
            request_id,
            command,
            lambda: websocket.send_bytes(packet),
        )

    async def _await_response(
        self,
        request_id: str,
        command: dict[str, Any],
        sender,
    ) -> Any:
        """Send a command and wait for the registered response future."""
        future = asyncio.get_running_loop().create_future()
        self.pending_responses[request_id] = future
        try:
            await sender()
            return await asyncio.wait_for(future, timeout=settings.WEBSOCKET_TIMEOUT)
        except asyncio.TimeoutError:
            self._cleanup_request(request_id)
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Request to frontend client timed out",
            )
        except ApiException as exc:
            self._cleanup_request(request_id)
            self._mark_resettable_if_needed(exc, command, request_id)
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        except Exception as exc:
            self._cleanup_request(request_id)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error communicating with frontend client: {exc}",
            )

    def _mark_resettable_if_needed(self, exc: ApiException, command: dict[str, Any], request_id: str):
        """Inspect ApiException and mark it resettable if it's a missing-file scenario."""
        error_detail = exc.detail or {}
        # 验证 error_detail 是字典类型，避免属性访问错误
        if not isinstance(error_detail, dict):
            error_detail = {}
        error_message = str(error_detail.get("message", "")).lower() if error_detail else ""
        if "not found" not in error_message and "file not found" not in error_message:
            return

        file_name = command.get("payload", {}).get("fileName")
        if not file_name and request_id in self.request_file_aliases:
            alias_map = self.request_file_aliases.get(request_id) or {}
            if len(alias_map) == 1:
                file_name = next(iter(alias_map.keys()))
            elif alias_map:
                Logger.warning(
                    "无法确定具体缺失的文件，存在多个候选",
                    request_id=request_id,
                    aliases=list(alias_map.keys()),
                )
        if not file_name:
            return

        sha256 = file_manager.get_sha256_by_filename(file_name)
        if not sha256:
            return
        Logger.warning("检测到文件过期/未找到，触发全局重置", file_name=file_name, sha256=sha256)
        file_manager.reset_replication_map(sha256)
        exc.is_resettable = True

    def _build_binary_packet(self, command: dict[str, Any], binary_body: bytes) -> bytes:
        """构造前端约定的二进制帧: [len][json][binary]."""
        metadata_bytes = json.dumps(
            command,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        header_bytes = len(metadata_bytes).to_bytes(4, byteorder="big", signed=False)
        return header_bytes + metadata_bytes + (binary_body or b"")

    @asynccontextmanager
    async def _bind_request_to_client(self, request_id: str, client_id: str):
        """Register the current request to a client for cleanup and tracing."""
        self.request_to_client[request_id] = client_id
        if client_id not in self.client_active_requests:
            self.client_active_requests[client_id] = set()
        self.client_active_requests[client_id].add(request_id)
        try:
            yield
        finally:
            self.request_to_client.pop(request_id, None)
            if client_id in self.client_active_requests:
                self.client_active_requests[client_id].discard(request_id)

    async def handle_api_request(
        self,
        *,
        command_type: str,
        payload: Any,
        request: Optional[Request] = None,
        is_streaming: bool = False,
    ) -> Any:
        """
        处理 API 请求的统一入口 (方案 B)。
        集成了文件查找、客户端选择、回退、复制和容错逻辑。
        """
        request_id = str(uuid.uuid4())
        effective_payload = await payload_service.prepare_payload(command_type, payload, request_id)

        Logger.debug(f"原始 payload 已接收", request_id=request_id)
        Logger.debug(f"有效 payload 已准备", request_id=request_id)

        original_file_name = payload_service.find_original_file_name(effective_payload, request_id)
        client_id = self.get_next_client()

        # 委托给 FileSyncService 处理文件同步和客户端选择逻辑
        client_id, alias_map, fallback_alias = await file_sync_service.resolve_client_and_files(
            self,
            payload=effective_payload,
            request_id=request_id,
            initial_client_id=client_id,
        )
        if not original_file_name and fallback_alias:
            original_file_name = fallback_alias

        alias_map = alias_map or {}
        self.request_file_aliases[request_id] = alias_map

        try:
            # For streaming, keep the mapping alive until stream ends (cleanup happens in stream finalizer)
            # For non-streaming, bind briefly and cleanup when response is ready
            if is_streaming:
                self.request_to_client[request_id] = client_id
                if client_id not in self.client_active_requests:
                    self.client_active_requests[client_id] = set()
                self.client_active_requests[client_id].add(request_id)
                return await self.proxy_request(
                    command_type=command_type,
                    payload=effective_payload,
                    request=request,
                    request_id=request_id,
                    is_streaming=is_streaming,
                )
            else:
                async with self._bind_request_to_client(request_id, client_id):
                    return await self.proxy_request(
                        command_type=command_type,
                        payload=effective_payload,
                        request=request,
                        request_id=request_id,
                        is_streaming=is_streaming,
                    )
        except Exception as exc:
            if hasattr(exc, 'is_resettable') and getattr(exc, 'is_resettable', False):
                sha256_to_reset = file_manager.get_sha256_by_filename(original_file_name) if original_file_name else None
                if sha256_to_reset:
                    Logger.error(
                        "捕获到可重置的文件错误，将尝试同步重建",
                        request_id=request_id,
                        sha256=sha256_to_reset,
                    )
                    try:
                        # 委托给 FileSyncService 进行重建
                        new_file, new_client_id = await file_sync_service.synchronously_rebuild_file(self, sha256_to_reset)
                        
                        if (isinstance(payload, dict) and "payload" in payload and 
                            "contents" in payload["payload"] and len(payload["payload"]["contents"]) > 0):
                            content = payload["payload"]["contents"][0]
                            if isinstance(content, dict):
                                file_data = content.get("fileData") or content.get("file_data")
                                if isinstance(file_data, dict):
                                    file_data["fileName"] = new_file["name"]
                        if new_file and isinstance(new_file, dict):
                            alias_map[new_file.get("name")] = sha256_to_reset

                        Logger.event("RETRY_REQUEST", "使用重建的文件重试请求", request_id=request_id)
                        async with self._bind_request_to_client(request_id, new_client_id):
                            return await self.proxy_request(
                                command_type=command_type,
                                payload=payload,
                                request=request,
                                request_id=request_id,
                                is_streaming=is_streaming,
                            )
                    except Exception as rebuild_exc:
                        Logger.error("重试请求在同步重建后失败", exc=rebuild_exc, request_id=request_id)
                        raise HTTPException(
                            status_code=500,
                            detail=f"File expired, and reconstruction failed: {rebuild_exc}",
                        )
            raise
        finally:
            self.request_file_aliases.pop(request_id, None)

    async def _handle_streaming_request(

        self,
        websocket: WebSocket,
        command: dict[str, Any],
        request_id: str,
        request: Request,
    ) -> AsyncGenerator[Any, None]:
        """Handles a streaming request and returns an async generator."""
        queue: asyncio.Queue = asyncio.Queue()
        self.streaming_responses[request_id] = queue

        async def stream_generator() -> AsyncGenerator[Any, None]:
            try:
                await websocket.send_json(command)
                while True:
                    # Check for disconnect before waiting for the next item
                    if await request.is_disconnected():
                        Logger.event("DISCONNECT", "流式传输中断", request_id=request_id)
                        # No need to call cancel_request here, the context manager will handle it
                        break

                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        # Timeout allows us to re-check the disconnect status
                        continue

                    if item is None:  # End of stream signal
                        break
                    yield item
            finally:
                # The context manager will ultimately handle the final cleanup
                pass

        return stream_generator()

    def cancel_request(self, request_id: str) -> bool:
        """
        取消指定的请求（同步入口点）

        职责：
        1. 检查请求是否存在
        2. 清理后端资源
        3. 异步发送取消信号给前端

        Args:
            request_id: 要取消的请求ID

        Returns:
            bool: 取消操作是否成功启动
        """
        Logger.debug(f"尝试取消请求 {request_id}")

        # 步骤 1：幂等性检查
        if request_id not in self.request_to_client:
            Logger.debug(f"请求 {request_id} 未找到或已取消")
            return False

        # 步骤 2：获取处理该请求的客户端
        client_id = self.request_to_client[request_id]

        # 步骤 3：清理后端资源（必须执行）
        self._cleanup_request(request_id)

        # 步骤 4：异步发送取消信号（best effort）
        if client_id in self.active_connections:
            try:
                asyncio.create_task(self._send_cancel_signal_async(websocket_id=client_id, request_id=request_id))
            except RuntimeError:
                Logger.warning("无法创建后台任务发送取消信号", request_id=request_id, client_id=client_id)
        else:
            Logger.debug("客户端未连接，跳过取消信号", client_id=client_id)

        return True

    async def _send_cancel_signal_async(self, websocket_id: str, request_id: str):
        """异步发送取消信号"""
        try:
            if websocket_id not in self.active_connections:
                return
            websocket = self.active_connections[websocket_id]
            cancel_message = {"type": "cancel_task", "id": request_id}
            await websocket.send_json(cancel_message)
            Logger.event("CANCEL", "发送取消信号", request_id=request_id, client_id=websocket_id)
        except (RuntimeError, ConnectionError, Exception) as e:
            Logger.debug(f"发送取消信号失败 [{type(e).__name__}]", request_id=request_id, client_id=websocket_id)

    def _cleanup_request(self, request_id: str):
        """
        清理与请求相关的所有内部资源（内部方法）

        注意：此方法是幂等的，可以安全地多次调用
        """
        cleaned_items = []

        # 清理 1：流式响应队列
        if request_id in self.streaming_responses:
            queue = self.streaming_responses.pop(request_id)
            # 确保队列中的等待者被释放
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
            cleaned_items.append("queue")

        # 清理 2：请求映射关系
        if request_id in self.request_to_client:
            client_id = self.request_to_client.pop(request_id)
            if client_id in self.client_active_requests:
                self.client_active_requests[client_id].discard(request_id)
            cleaned_items.append("mapping")

        # 清理 3：非流式响应的 Future
        if request_id in self.pending_responses:
            future = self.pending_responses.pop(request_id)
            if not future.done():
                future.cancel()
            cleaned_items.append("future")

        # 清理 4：流式包计数
        if request_id in self.streaming_chunk_count:
            self.streaming_chunk_count.pop(request_id)
            cleaned_items.append("chunk_count")

        if cleaned_items:
            Logger.debug(f"清理资源 {request_id} | {', '.join(cleaned_items)}")

    def _build_background_request(self) -> SimpleNamespace:
        async def _always_connected():
            return False

        return SimpleNamespace(is_disconnected=_always_connected)

    async def upload_file_from_cache(self, sha256: str) -> tuple[dict, str]:
        """
        供 API 调用：同步地选择客户端并上传缓存文件。
        """
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
        """
        直接向指定客户端发送命令（供 API 或后台逻辑复用）。
        """
        effective_request_id = request_id or f"direct-{command_type}-{uuid.uuid4()}"
        return await self._direct_proxy_request(
            command_type=command_type,
            payload=payload,
            request_id=effective_request_id,
            client_id=client_id,
            request=self._build_background_request(),
            is_streaming=is_streaming,
        )

    def trigger_delete_task(self, client_id: str, file_name: str):
        """触发一个后台任务来异步删除远程文件"""
        file_sync_service.trigger_delete_task(self, client_id, file_name)


manager = ConnectionManager()