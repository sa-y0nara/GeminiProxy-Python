"""元数据存储模块

负责管理内存中的文件元数据、复制状态。
使用 AliasManager 和 TombstoneManager 处理别名和删除标记。
"""

import base64
import string
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from app.core.alias_manager import AliasManager
from app.core.config import settings
from app.core.log_utils import Logger
from app.core.tombstone_manager import TombstoneManager

ISO_TIMESTAMP_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T"
    r"(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<frac>\d+))?"
    r"(?P<tz>Z|[+-]\d{2}:\d{2})?$"
)


@dataclass
class FileCacheEntry:
    """文件缓存元数据条目"""
    sha256: str
    local_path: Path
    original_filename: str
    mime_type: Optional[str]
    size_bytes: int
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_accessed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    gemini_file_expiration: Optional[datetime] = None
    replication_map: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class MetadataStore:
    """元数据存储
    
    职责：
    1. 管理文件元数据条目的 CRUD
    2. 管理复制状态
    3. 协调 AliasManager 和 TombstoneManager
    """

    def __init__(self, file_cache_dir: Path) -> None:
        self.file_cache_dir = file_cache_dir
        self.metadata_store: Dict[str, FileCacheEntry] = {}
        
        # 初始化子模块
        self.alias_manager = AliasManager()
        self.tombstone_manager = TombstoneManager(self.alias_manager._canonical_aliases)
        
        Logger.event("INIT", "MetadataStore 初始化")

    # =========================================================================
    # 条目 CRUD
    # =========================================================================

    def get_entry(self, sha256: str) -> Optional[FileCacheEntry]:
        """获取条目并更新访问时间"""
        entry = self.metadata_store.get(sha256)
        if entry:
            entry.last_accessed_at = datetime.now(timezone.utc)
        return entry

    def create_entry(
        self, *, sha256: str, file_path: Path, filename: str, mime_type: Optional[str], size_bytes: int
    ) -> FileCacheEntry:
        """创建新的元数据条目"""
        entry = FileCacheEntry(
            sha256=sha256,
            local_path=file_path,
            original_filename=filename,
            mime_type=mime_type,
            size_bytes=size_bytes,
        )
        self.metadata_store[sha256] = entry
        self.alias_manager.register_standard_aliases(sha256)
        Logger.event("METADATA_CREATE", "创建文件元数据", sha256=sha256)
        return entry

    def delete_entry(self, sha256: str) -> Optional[FileCacheEntry]:
        """删除元数据，清理映射，返回被删除的条目"""
        entry = self.metadata_store.pop(sha256, None)
        if not entry:
            return None

        self.alias_manager.remove_standard_aliases(sha256)
        for client_id, data in entry.replication_map.items():
            if "name" in data:
                self.alias_manager.remove_aliases(data["name"])
        
        Logger.event("METADATA_DELETE", "元数据已删除", sha256=sha256)
        return entry

    def get_all_entries(self) -> List[FileCacheEntry]:
        """获取所有条目"""
        return list(self.metadata_store.values())

    # =========================================================================
    # 别名管理 (委托给 AliasManager)
    # =========================================================================

    def get_sha256_by_filename(self, file_name: str) -> Optional[str]:
        """通过文件名查找 sha256"""
        return self.alias_manager.get_sha256_by_alias(file_name, self.metadata_store)

    def register_aliases(self, sha256: str, *aliases: str) -> None:
        """注册文件别名"""
        self.alias_manager.register_aliases(sha256, *aliases)

    # =========================================================================
    # 复制状态管理
    # =========================================================================

    def update_replication_status(
        self, sha256: str, client_id: str, status: str, gemini_file: Optional[Dict] = None
    ) -> None:
        """更新复制状态"""
        entry = self.get_entry(sha256)
        if not entry:
            return

        replication_data = {"status": status}
        if gemini_file:
            replication_data.update(gemini_file)
            file_name = gemini_file.get("name")
            if file_name:
                self.alias_manager.register_aliases(sha256, file_name)
            uri_value = gemini_file.get("uri")
            if uri_value:
                replication_data["uri"] = uri_value
            
            if not entry.gemini_file_expiration:
                expiration_time = gemini_file.get("expirationTime")
                if expiration_time:
                    parsed_expiration = self._parse_iso_timestamp(expiration_time)
                    if parsed_expiration:
                        entry.gemini_file_expiration = parsed_expiration

        entry.replication_map[client_id] = replication_data
        Logger.debug("更新复制状态", sha256=sha256, client_id=client_id, status=status)

    def reset_replication_map(self, sha256: str) -> None:
        """重置复制映射"""
        entry = self.get_entry(sha256)
        if not entry:
            return
        
        for client_id, data in entry.replication_map.items():
            if "name" in data:
                self.alias_manager.reverse_mapping.pop(data["name"], None)

        entry.replication_map.clear()
        entry.gemini_file_expiration = None
        Logger.event("REPLICATION_RESET", "文件复制地图已重置", sha256=sha256)

    # =========================================================================
    # 远程文件处理
    # =========================================================================

    def extract_sha256_hex(self, remote_file: Dict[str, Any]) -> Optional[str]:
        """从远程文件对象提取 sha256"""
        sha_candidates = (
            remote_file.get("sha256Hash"),
            remote_file.get("sha256_hash"),
            remote_file.get("sha256"),
        )
        for candidate in sha_candidates:
            if not candidate:
                continue
            candidate = str(candidate).strip()
            if not candidate:
                continue
            if len(candidate) == 64 and all(c in string.hexdigits for c in candidate):
                return candidate.lower()
            try:
                decoded = base64.b64decode(candidate)
                return decoded.hex()
            except Exception:
                Logger.warning("远程 sha256Hash 解析失败", value=candidate, file_name=remote_file.get("name"))
        return None

    def ensure_remote_entry(self, remote_file: Dict[str, Any]) -> Optional[FileCacheEntry]:
        """确保远程文件有对应的元数据条目"""
        sha256_hex = self.extract_sha256_hex(remote_file)
        if not sha256_hex:
            Logger.warning("远程文件缺少 sha256Hash，无法登记", file_name=remote_file.get("name"))
            return None

        entry = self.metadata_store.get(sha256_hex)
        if entry:
            return entry

        placeholder_path = self.file_cache_dir / "remote_stub" / f"{sha256_hex}.bin"
        placeholder_path.parent.mkdir(parents=True, exist_ok=True)

        size_value = remote_file.get("sizeBytes") or remote_file.get("size_bytes")
        try:
            size_int = int(size_value) if size_value is not None else 0
        except (TypeError, ValueError):
            Logger.warning("远程文件 sizeBytes 无法解析", size=size_value, file_name=remote_file.get("name"))
            size_int = 0

        display_name = remote_file.get("displayName") or remote_file.get("display_name") or remote_file.get("name")
        mime_type = remote_file.get("mimeType") or remote_file.get("mime_type")

        entry = FileCacheEntry(
            sha256=sha256_hex,
            local_path=placeholder_path,
            original_filename=display_name or sha256_hex,
            mime_type=mime_type,
            size_bytes=size_int,
        )

        expiration_raw = remote_file.get("expirationTime") or remote_file.get("expiration_time")
        if expiration_raw:
            parsed = self._parse_iso_timestamp(expiration_raw)
            if parsed:
                entry.gemini_file_expiration = parsed

        self.metadata_store[sha256_hex] = entry
        Logger.event("METADATA_REMOTE", "创建远程文件元数据占位", sha256=sha256_hex[:8], file_name=remote_file.get("name"))
        return entry

    # =========================================================================
    # 删除标记 (委托给 TombstoneManager)
    # =========================================================================

    def mark_deleted(self, sha256: Optional[str], aliases: Optional[Set[str]] = None) -> None:
        """标记文件为已删除"""
        self.tombstone_manager.mark_deleted(sha256, aliases)

    def clear_deleted_flag(self, sha256: Optional[str]) -> None:
        """清除删除标记"""
        self.tombstone_manager.clear_deleted_flag(sha256)

    def is_marked_deleted(self, sha256: Optional[str]) -> bool:
        """检查是否被标记为已删除"""
        return self.tombstone_manager.is_marked_deleted(sha256)

    def is_name_marked_deleted(self, name: Optional[str]) -> bool:
        """检查名称是否被标记为已删除"""
        return self.tombstone_manager.is_name_marked_deleted(name)

    # =========================================================================
    # 工具方法
    # =========================================================================

    def _parse_iso_timestamp(self, value: Optional[str]) -> Optional[datetime]:
        """解析 ISO 时间戳"""
        if not value:
            return None
        match = ISO_TIMESTAMP_RE.match(value.strip())
        if not match:
            Logger.warning("无法解析时间戳", timestamp=value)
            return None

        base = f"{match.group('date')}T{match.group('time')}"
        frac = match.group('frac')
        if frac:
            frac = (frac + "000000")[:6]
            base = f"{base}.{frac}"

        tz = match.group('tz') or "+00:00"
        if tz == "Z":
            tz = "+00:00"

        try:
            return datetime.fromisoformat(base + tz)
        except ValueError as exc:
            Logger.warning("时间戳解析失败", timestamp=value, exc=exc)
            return None
