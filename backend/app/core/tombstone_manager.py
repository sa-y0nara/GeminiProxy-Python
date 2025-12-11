"""墓碑管理模块

负责管理已删除文件的标记，防止已删除文件被重新访问。
"""

from typing import Callable, Dict, Optional, Set

from app.core.log_utils import Logger


class TombstoneManager:
    """墓碑（删除标记）管理器
    
    职责：
    1. 维护已删除文件的 sha256 集合
    2. 维护已删除文件别名的映射
    3. 提供删除状态查询
    """

    def __init__(self, canonical_aliases_fn: Callable[[Optional[str]], Set[str]]) -> None:
        """
        Args:
            canonical_aliases_fn: 规范化别名的函数，来自 AliasManager
        """
        self._canonical_aliases = canonical_aliases_fn
        self.deleted_shas: Set[str] = set()
        self.deleted_alias_map: Dict[str, str] = {}  # alias -> sha256

    def mark_deleted(self, sha256: Optional[str], aliases: Optional[Set[str]] = None) -> None:
        """标记文件为已删除"""
        if not sha256:
            return
        self.deleted_shas.add(sha256)
        tombstone_aliases: Set[str] = set()
        tombstone_aliases.update(self._normalize_aliases_for_tombstone(sha256))
        tombstone_aliases.update(self._normalize_aliases_for_tombstone(f"files/{sha256}"))
        if aliases:
            for alias in aliases:
                tombstone_aliases.update(self._normalize_aliases_for_tombstone(alias))
        for alias in tombstone_aliases:
            self.deleted_alias_map[alias] = sha256
        Logger.debug("标记文件为已删除", sha256=sha256[:8])

    def clear_deleted_flag(self, sha256: Optional[str]) -> None:
        """清除删除标记"""
        if not sha256:
            return
        if sha256 in self.deleted_shas:
            self.deleted_shas.discard(sha256)
        aliases_to_remove = [alias for alias, value in self.deleted_alias_map.items() if value == sha256]
        for alias in aliases_to_remove:
            self.deleted_alias_map.pop(alias, None)
        if aliases_to_remove:
            Logger.debug("清除已删除标记", sha256=sha256[:8])

    def is_marked_deleted(self, sha256: Optional[str]) -> bool:
        """检查 sha256 是否被标记为已删除"""
        return bool(sha256 and sha256 in self.deleted_shas)

    def is_name_marked_deleted(self, name: Optional[str]) -> bool:
        """检查名称是否被标记为已删除"""
        if not name:
            return False
        aliases = self._normalize_aliases_for_tombstone(name)
        return any(alias in self.deleted_alias_map for alias in aliases)

    def _normalize_aliases_for_tombstone(self, alias: Optional[str]) -> Set[str]:
        """规范化别名以用于墓碑检查"""
        normalized_aliases: Set[str] = set()
        for token in self._canonical_aliases(alias):
            if not token:
                continue
            normalized_aliases.add(token)
            if token.startswith("files/"):
                tail = token.split("files/", 1)[-1]
                if tail and tail != token:
                    normalized_aliases.add(tail)
                    normalized_aliases.add(f"files/{tail}")
            else:
                normalized_aliases.add(f"files/{token}")
        return normalized_aliases
