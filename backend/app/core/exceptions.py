"""异常定义模块

异常使用规范：
- HTTPException: 用于 API 层直接返回给客户端的 HTTP 错误
  适用于：路由处理器、验证错误、权限错误
  
- ApiException: 用于 WebSocket 相关的内部业务逻辑错误
  适用于：WebSocket 通信错误、前端客户端响应错误
  会在 websocket_manager 中被捕获并转换为 HTTPException
"""

from typing import Optional, Union

class ApiException(Exception):
    def __init__(self, status_code: int, detail: Union[dict, str, None]):
        self.status_code = status_code
        self.detail = detail
        self.sha256_to_reset: Optional[str] = None  # 用于携带需要重置的文件的sha256
        self.is_resettable: bool = False  # 标记异常是否可以触发重置
