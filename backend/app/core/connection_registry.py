from typing import Dict, List, Optional
from fastapi import WebSocket, HTTPException, status
from app.core.log_utils import Logger

class ConnectionRegistry:
    """
    负责管理 WebSocket 连接池和客户端负载均衡。
    """
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self._client_ids: List[str] = []
        self._next_client_index: int = 0

    async def register(self, websocket: WebSocket, client_id: str):
        """注册一个新的 WebSocket 连接"""
        await websocket.accept()
        self.active_connections[client_id] = websocket
        if client_id not in self._client_ids:
            self._client_ids.append(client_id)
        Logger.info(f"客户端已注册: {client_id}")

    async def unregister(self, client_id: str):
        """注销一个 WebSocket 连接"""
        if client_id in self.active_connections:
            del self.active_connections[client_id]
        if client_id in self._client_ids:
            self._client_ids.remove(client_id)
        Logger.info(f"客户端已注销: {client_id}")

    def get_socket(self, client_id: str) -> Optional[WebSocket]:
        """获取指定客户端的 WebSocket 实例"""
        return self.active_connections.get(client_id)

    def get_next_client(self) -> str:
        """轮询算法，获取下一个可用的客户端ID"""
        if not self._client_ids:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No frontend clients connected",
            )
        
        # 简单的轮询
        client_id = self._client_ids[self._next_client_index]
        self._next_client_index = (self._next_client_index + 1) % len(self._client_ids)
        return client_id

    def get_all_clients(self) -> List[str]:
        """获取所有活跃客户端 ID"""
        return list(self.active_connections.keys())

    def is_connected(self, client_id: str) -> bool:
        """检查客户端是否连接"""
        return client_id in self.active_connections
