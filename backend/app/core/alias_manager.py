"""别名管理模块

负责管理文件名与 SHA256 的双向映射关系。
"""

import string
from typing import Dict, List, Optional, Set, TYPE_CHECKING

from app.core.config import settings
from app.core.log_utils import Logger

if TYPE_CHECKING:
    from app.core.metadata_store import FileCacheEntry


class AliasManager:
    """别名管理器
    
    职责：
    1. 维护文件名/URI 到 sha256 的反向映射
    2. 支持多种别名形式的规范化
    3. 回退查找策略
    """

    def __init__(self) -> None:
        # alias -> sha256
        self.reverse_mapping: Dict[str, str] = {}

    def register_aliases(self, sha256: str, *aliases: str) -> None:
        """注册文件别名"""
        for alias in aliases:
            for normalized in self._canonical_aliases(alias):
                self.reverse_mapping[normalized] = sha256
                Logger.debug("注册文件别名", alias=normalized, sha256=sha256[:8])

    def remove_aliases(self, *aliases: str) -> None:
        """移除文件别名"""
        for alias in aliases:
            for normalized in self._canonical_aliases(alias):
                if self.reverse_mapping.pop(normalized, None):
                    Logger.debug("移除文件别名", alias=normalized)

    def get_sha256_by_alias(
        self,
        file_name: str,
        metadata_store: Dict[str, "FileCacheEntry"],
    ) -> Optional[str]:
        """通过别名查找 sha256"""
        if not file_name:
            return None

        # 直接查找
        mapped = self.reverse_mapping.get(file_name)
        if mapped:
            Logger.debug("命中文件别名", alias=file_name, sha256=mapped[:8])
            return mapped

        # 标准化查找
        normalized_forms = self._extract_normalized_forms(file_name, metadata_store)
        for form in normalized_forms:
            mapped = self.reverse_mapping.get(form)
            if mapped:
                Logger.debug("命中文件别名", alias=form, sha256=mapped[:8])
                return mapped

        # 回退查找
        result = self._fallback_lookup(file_name, normalized_forms, metadata_store)
        if result:
            return result

        Logger.debug("文件别名未找到", alias=file_name)
        return None

    def _canonical_aliases(self, alias: Optional[str]) -> Set[str]:
        """规范化别名为所有可能的形式"""
        if not alias:
            return set()
        token = alias.strip()
        if not token:
            return set()
        
        variants = {token}
        if "/" in token:
            tail = token.rsplit("/", 1)[-1]
            if tail and tail != token:
                variants.add(tail)
        return variants

    def _extract_normalized_forms(
        self,
        file_name: str,
        metadata_store: Dict[str, "FileCacheEntry"],
    ) -> List[str]:
        """提取文件名的各种标准化形式"""
        forms = []
        normalized = file_name.strip().split(":download", 1)[0]
        forms.append(normalized)
        
        if "files/" in normalized:
            suffix = normalized[normalized.index("files/"):]
            suffix = suffix.split(":download", 1)[0]
            forms.append(suffix)
            tail = suffix.split("files/", 1)[-1]
            if tail:
                forms.append(tail)
        
        # 检查是否为完整的 sha256
        candidate = file_name.split('/')[-1].split(":download", 1)[0]
        if len(candidate) == 64 and all(c in string.hexdigits for c in candidate):
            if candidate in metadata_store:
                return [candidate]
        
        return forms

    def _fallback_lookup(
        self,
        file_name: str,
        normalized_forms: List[str],
        metadata_store: Dict[str, "FileCacheEntry"],
    ) -> Optional[str]:
        """在 replication_map 中回退查找"""
        fallback_candidates = set(normalized_forms)
        for sha, entry in metadata_store.items():
            for data in entry.replication_map.values():
                remote_name = data.get("name")
                if remote_name and remote_name in fallback_candidates:
                    Logger.debug("通过 replication_map 找到文件", alias=file_name, sha256=sha[:8])
                    self.register_aliases(sha, remote_name)
                    return sha
                uri = data.get("uri")
                if uri and "files/" in uri:
                    uri_tail = uri.split("files/", 1)[-1]
                    if uri_tail and uri_tail in fallback_candidates:
                        Logger.debug("通过 uri 找到文件", alias=file_name, sha256=sha[:8])
                        self.register_aliases(sha, uri, uri_tail)
                        return sha
        return None

    def register_standard_aliases(self, sha256: str) -> None:
        """注册标准别名（sha256 本身和短形式）"""
        short_sha = sha256[:settings.SHA256_ALIAS_LENGTH]
        self.register_aliases(
            sha256,
            sha256,
            short_sha,
            f"files/{sha256}",
            f"files/{short_sha}",
        )

    def remove_standard_aliases(self, sha256: str) -> None:
        """移除标准别名"""
        short_sha = sha256[:settings.SHA256_ALIAS_LENGTH]
        self.remove_aliases(sha256, short_sha, f"files/{sha256}", f"files/{short_sha}")
