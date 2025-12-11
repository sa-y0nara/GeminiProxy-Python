"""代理请求处理模块

负责处理 WebSocket 代理请求的发送、响应等待和错误处理。
"""

import asyncio
import json
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

from fastapi import HTTPException, WebSocket, status

from app.core.config import settings
from app.core.exceptions import ApiException
from app.core.file_manager import file_manager
from app.core.log_utils import Logger

if TYPE_CHECKING:
    from app.core.request_manager import RequestManager


class ProxyHandler:
    """代理请求处理器
    
    职责：
    1. 发送请求并等待响应
    2. 处理非流式响应消息
    3. 构建二进制数据包
    4. 处理错误恢复标记
    """

    def __init__(self, request_manager: "RequestManager"):
        self.request_manager = request_manager

    async def handle_non_streaming_message(
        self, request_id: str, payload: dict, message: dict
    ) -> None:
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

    async def handle_non_streaming_request(
        self, websocket: WebSocket, command: dict[str, Any], request_id: str
    ) -> Any:
        """处理非流式请求"""
        return await self.await_response(
            request_id,
            command,
            lambda: websocket.send_json(command),
        )

    async def await_response(
        self,
        request_id: str,
        command: dict[str, Any],
        sender: Callable,
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
        except asyncio.CancelledError:
            # 正常取消（如客户端断开连接）
            self.request_manager.cleanup_request(request_id)
            Logger.info("请求已取消", request_id=request_id)
            # 使用 499 状态码表示 Client Closed Request，避免 uvicorn 报 500
            raise HTTPException(status_code=499, detail="Client Closed Request")
        except ApiException as exc:
            self.request_manager.cleanup_request(request_id)
            self.mark_resettable_if_needed(exc, command, request_id)
            raise HTTPException(status_code=exc.status_code, detail=exc.detail)
        except Exception as exc:
            self.request_manager.cleanup_request(request_id)
            Logger.error("后端通信异常", exc=exc, request_id=request_id)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Error communicating with frontend client: {exc}",
            )

    def mark_resettable_if_needed(
        self, exc: ApiException, command: dict[str, Any], request_id: str
    ) -> None:
        """检查错误是否可重置"""
        error_detail = exc.detail or {}
        if not isinstance(error_detail, dict):
            error_detail = {}
        error_message = (
            str(error_detail.get("message", "")).lower() if error_detail else ""
        )

        # 检查是否为文件未找到错误
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

        Logger.warning(
            "检测到文件过期/未找到，触发全局重置", file_name=file_name, sha256=sha256
        )
        file_manager.reset_replication_map(sha256)
        exc.is_resettable = True

    def build_binary_packet(self, command: dict[str, Any], binary_body: bytes) -> bytes:
        """构建二进制数据包"""
        metadata_bytes = json.dumps(
            command,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        header_bytes = len(metadata_bytes).to_bytes(4, byteorder="big", signed=False)
        return header_bytes + metadata_bytes + (binary_body or b"")
