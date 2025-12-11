"""接口定义模块

定义用于解耦模块间依赖的协议接口。
"""

from types import SimpleNamespace
from typing import Optional, Protocol, Any, List, Union

from fastapi import Request


# 后台请求类型：用于后台任务中模拟 Request 对象
BackgroundRequest = SimpleNamespace


class IConnectionManager(Protocol):
    """ConnectionManager 接口协议
    
    用于解耦 FileSyncService 等服务与 ConnectionManager 的依赖。
    """

    def get_all_clients(self) -> List[str]:
        """获取所有已连接的客户端 ID"""
        ...
    
    def get_next_client(self) -> str:
        """获取下一个可用的客户端 ID（轮询）"""
        ...
    
    async def send_command_to_client(
        self,
        *,
        client_id: str,
        command_type: str,
        payload: Any,
        request_id: Optional[str] = None,
        is_streaming: bool = False,
    ) -> Any:
        """发送命令到指定客户端"""
        ...

    async def _direct_proxy_request(
        self,
        command_type: str,
        payload: Any,
        request_id: str,
        client_id: str,
        request: Optional[Union[Request, BackgroundRequest]] = None,
        is_streaming: bool = False,
    ) -> Any:
        """直接代理请求到指定客户端"""
        ...
    
    async def send_binary_command(
        self,
        *,
        client_id: str,
        command: dict[str, Any],
        binary_body: bytes,
    ) -> Any:
        """发送二进制命令到指定客户端"""
        ...
