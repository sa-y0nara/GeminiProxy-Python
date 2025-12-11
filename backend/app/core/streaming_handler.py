"""流式请求处理模块

负责处理 WebSocket 流式响应的创建、队列管理和消息分发。
"""

import asyncio
from collections.abc import AsyncGenerator
from typing import Any, Dict, Optional, TYPE_CHECKING

from fastapi import Request, WebSocket

from app.core.log_utils import Logger

if TYPE_CHECKING:
    from app.core.request_manager import RequestManager


class StreamingHandler:
    """流式请求处理器
    
    职责：
    1. 创建和管理流式响应队列
    2. 处理流式消息分发
    3. 生成流式响应迭代器
    """

    def __init__(self, request_manager: "RequestManager"):
        self.request_manager = request_manager

    async def handle_streaming_message(
        self, request_id: str, payload: dict, message: dict
    ) -> None:
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
            Logger.ws_receive(
                request_id, client_id, is_stream_end=True, total_chunks=chunk_num, data=message
            )
            self.request_manager.cleanup_request(request_id)
        elif chunk_num == 1:
            Logger.ws_receive(request_id, client_id, is_stream_start=True, data=message)
        else:
            Logger.ws_receive(request_id, client_id, is_stream_middle=True, data=message)

    async def handle_streaming_request(
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

                # 创建一个断开连接的监控任务
                disconnect_task = asyncio.create_task(request.is_disconnected())
                # 创建一个队列获取任务
                queue_task = asyncio.create_task(queue.get())

                pending = {disconnect_task, queue_task}

                while True:
                    # 等待任意一个任务完成
                    done, pending = await asyncio.wait(
                        pending, return_when=asyncio.FIRST_COMPLETED
                    )

                    if disconnect_task in done:
                        # 客户端断开连接
                        Logger.event(
                            "DISCONNECT", "流式传输中断，客户端已断开", request_id=request_id
                        )
                        queue_task.cancel()  # 取消正在等待的队列任务
                        break

                    if queue_task in done:
                        # 队列有新数据
                        try:
                            item = queue_task.result()
                        except asyncio.CancelledError:
                            break

                        if item is None:
                            # 流结束信号
                            disconnect_task.cancel()
                            break

                        yield item

                        # 重新调度下一个队列任务
                        queue_task = asyncio.create_task(queue.get())
                        pending.add(queue_task)

            except asyncio.CancelledError:
                Logger.warning("流式生成器被取消", request_id=request_id)
                raise
            except Exception as e:
                Logger.error("流式传输发生异常", exc=e, request_id=request_id)
                raise
            finally:
                # 确保所有 pending 任务都被取消
                for task in pending:
                    if not task.done():
                        task.cancel()
                self.request_manager.cleanup_request(request_id)

        return stream_generator()
