"""后台请求工具模块

提供共享的后台请求构建工具，用于后台任务中模拟 Request 对象。
"""

from types import SimpleNamespace
from typing import Any


def build_background_request() -> SimpleNamespace:
    """创建用于后台任务的模拟 Request 对象。
    
    后台任务（如文件复制、删除等）需要一个模拟的 Request 对象，
    其 is_disconnected 方法始终返回 False，表示"连接永不断开"。
    
    Returns:
        SimpleNamespace: 模拟的 Request 对象，包含 is_disconnected 方法
    """
    async def _always_connected() -> bool:
        return False
    return SimpleNamespace(is_disconnected=_always_connected)
