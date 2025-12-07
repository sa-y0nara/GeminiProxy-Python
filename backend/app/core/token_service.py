"""安全令牌服务模块

提供用于内部文件下载端点的安全令牌生成和验证。
采用 HMAC-SHA256 签名方案。
"""

import hashlib
import hmac
import time
from typing import Optional

from app.core.config import settings
from app.core.log_utils import Logger


class TokenService:
    """安全令牌服务

    用于生成和验证内部文件下载的安全令牌。
    令牌格式: HMAC-SHA256(secret, sha256 + timestamp)[:32]
    """

    def __init__(self):
        self._secret = self._get_secret()
        Logger.event("INIT", "TokenService 初始化")

    def _get_secret(self) -> bytes:
        """获取密钥，优先使用配置"""
        secret = getattr(settings, "DOWNLOAD_TOKEN_SECRET", "default-secret-change-in-production")
        return secret.encode("utf-8")

    def _get_ttl(self) -> int:
        """获取令牌有效期（秒）"""
        return getattr(settings, "DOWNLOAD_TOKEN_TTL", 300)

    def generate_download_token(self, sha256: str) -> str:
        """生成下载令牌

        Args:
            sha256: 文件的 SHA256 哈希值

        Returns:
            生成的令牌字符串
        """
        timestamp = int(time.time())
        message = f"{sha256}:{timestamp}".encode("utf-8")
        signature = hmac.new(self._secret, message, hashlib.sha256).hexdigest()[:32]
        # 令牌格式: timestamp.signature
        token = f"{timestamp}.{signature}"
        return token

    def validate_download_token(self, sha256: str, token: str) -> bool:
        """验证下载令牌

        Args:
            sha256: 文件的 SHA256 哈希值
            token: 要验证的令牌

        Returns:
            令牌是否有效
        """
        if not token or "." not in token:
            Logger.debug("令牌格式无效", token=token[:20] if token else None)
            return False

        try:
            parts = token.split(".", 1)
            if len(parts) != 2:
                return False

            timestamp_str, provided_signature = parts
            timestamp = int(timestamp_str)
        except (ValueError, IndexError):
            Logger.debug("令牌解析失败", token=token[:20])
            return False

        # 检查是否过期
        current_time = int(time.time())
        ttl = self._get_ttl()
        if current_time - timestamp > ttl:
            Logger.debug("令牌已过期", age=current_time - timestamp, ttl=ttl)
            return False

        # 验证签名
        message = f"{sha256}:{timestamp}".encode("utf-8")
        expected_signature = hmac.new(self._secret, message, hashlib.sha256).hexdigest()[:32]

        if not hmac.compare_digest(provided_signature, expected_signature):
            Logger.debug("令牌签名不匹配")
            return False

        return True


# 全局单例
token_service = TokenService()
