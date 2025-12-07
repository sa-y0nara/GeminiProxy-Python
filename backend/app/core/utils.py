"""通用工具函数模块

提供在多个模块间共享的工具函数。
"""

import base64
from typing import Any, Optional

from app.core.log_utils import Logger


def parse_int_safe(value: Any, default: Optional[int] = None, label: str = "value") -> Optional[int]:
    """安全地将值转换为整数，带验证和日志

    Args:
        value: 要转换的值
        default: 转换失败时的默认值
        label: 用于日志的标签

    Returns:
        转换后的整数，或默认值
    """
    if value is None:
        return default
    try:
        result = int(value)
        return result
    except (TypeError, ValueError):
        Logger.warning(f"{label} 无法转换为整数", value=value)
        return default


def first_non_empty(mapping: Optional[dict], *keys: str) -> Optional[str]:
    """从字典中获取第一个非空值

    Args:
        mapping: 源字典
        *keys: 要尝试的键名列表

    Returns:
        第一个非空的值，或 None
    """
    if not mapping:
        return None
    for key in keys:
        value = mapping.get(key)
        if value:
            return value
    return None


def encode_sha256_base64(sha256_hex: Optional[str]) -> Optional[str]:
    """将十六进制 sha256 转换为 base64 字符串

    Args:
        sha256_hex: 十六进制格式的 SHA256 哈希值

    Returns:
        base64 编码的字符串，或 None
    """
    if not sha256_hex:
        return None
    try:
        return base64.b64encode(bytes.fromhex(sha256_hex)).decode("ascii")
    except ValueError:
        Logger.warning("无法转换 sha256 为 base64", sha256=sha256_hex)
        return None


def short_sha(sha256: str, length: int = 8) -> str:
    """返回 SHA256 的短字符串用于日志显示

    Args:
        sha256: 完整的 SHA256 哈希值
        length: 截取长度，默认为 8

    Returns:
        截取后的短哈希字符串
    """
    if not sha256:
        return ""
    return sha256[:length]

