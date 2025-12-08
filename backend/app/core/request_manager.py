import asyncio
from typing import Dict, Any, Optional, Set, List
from app.core.log_utils import Logger
from app.core.exceptions import ApiException
from fastapi import HTTPException, status

class RequestManager:
    """
    负责管理请求的生命周期，包括挂起的响应、流式队列和请求-客户端映射。
    """
    def __init__(self):
        self.pending_responses: Dict[str, asyncio.Future] = {}
        self.streaming_responses: Dict[str, asyncio.Queue] = {}
        self.streaming_chunk_count: Dict[str, int] = {}
        
        # request_id -> client_id
        self.request_to_client: Dict[str, str] = {}
        # client_id -> set[request_id]
        self.client_active_requests: Dict[str, Set[str]] = {}

        # 请求与文件别名映射 (用于错误恢复)
        self.request_file_aliases: Dict[str, Dict[str, str]] = {}

    def register_request(self, request_id: str, client_id: str):
        """注册请求与客户端的绑定关系"""
        self.request_to_client[request_id] = client_id
        if client_id not in self.client_active_requests:
            self.client_active_requests[client_id] = set()
        self.client_active_requests[client_id].add(request_id)

    def unregister_request(self, request_id: str):
        """解除请求绑定"""
        if request_id in self.request_to_client:
            client_id = self.request_to_client.pop(request_id)
            if client_id in self.client_active_requests:
                self.client_active_requests[client_id].discard(request_id)
                # 清理空的集合
                if not self.client_active_requests[client_id]:
                    del self.client_active_requests[client_id]
        
        self.request_file_aliases.pop(request_id, None)

    def get_client_for_request(self, request_id: str) -> Optional[str]:
        return self.request_to_client.get(request_id)

    def create_future(self, request_id: str) -> asyncio.Future:
        """创建一个用于非流式响应的 Future"""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_responses[request_id] = future
        return future

    def get_future(self, request_id: str) -> Optional[asyncio.Future]:
        return self.pending_responses.get(request_id)

    def remove_future(self, request_id: str):
        if request_id in self.pending_responses:
            del self.pending_responses[request_id]

    def create_stream_queue(self, request_id: str) -> asyncio.Queue:
        """创建一个用于流式响应的 Queue"""
        queue = asyncio.Queue()
        self.streaming_responses[request_id] = queue
        self.streaming_chunk_count[request_id] = 0
        return queue

    def get_stream_queue(self, request_id: str) -> Optional[asyncio.Queue]:
        return self.streaming_responses.get(request_id)

    def increment_chunk_count(self, request_id: str) -> int:
        if request_id not in self.streaming_chunk_count:
            self.streaming_chunk_count[request_id] = 0
        self.streaming_chunk_count[request_id] += 1
        return self.streaming_chunk_count[request_id]

    def cleanup_request(self, request_id: str):
        """清理所有与请求相关的资源"""
        cleaned_items = []

        # 清理流式响应
        if request_id in self.streaming_responses:
            queue = self.streaming_responses.pop(request_id)
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
            cleaned_items.append("queue")

        # 解除映射
        self.unregister_request(request_id)
        cleaned_items.append("mapping")

        # 清理 Future
        if request_id in self.pending_responses:
            future = self.pending_responses.pop(request_id)
            if not future.done():
                future.cancel()
            cleaned_items.append("future")

        # 清理计数器
        if request_id in self.streaming_chunk_count:
            del self.streaming_chunk_count[request_id]
            cleaned_items.append("chunk_count")

        if cleaned_items:
            Logger.debug(f"清理资源 {request_id} | {', '.join(cleaned_items)}")

    def get_active_requests_for_client(self, client_id: str) -> List[str]:
        """获取指定客户端的所有活跃请求ID"""
        return list(self.client_active_requests.get(client_id, set()))
