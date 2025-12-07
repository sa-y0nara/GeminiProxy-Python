import base64
import string
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, List

from app.core.config import settings
from app.core.log_utils import Logger

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
    """
    负责管理内存中的文件元数据、复制状态和反向映射。
    """
    def __init__(self, file_cache_dir: Path):
        self.file_cache_dir = file_cache_dir
        self.metadata_store: Dict[str, FileCacheEntry] = {}
        self.reverse_mapping: Dict[str, str] = {}
        self.deleted_shas: Set[str] = set()
        self.deleted_alias_map: Dict[str, str] = {}
        Logger.event("INIT", "MetadataStore 初始化")

    def get_entry(self, sha256: str) -> Optional[FileCacheEntry]:
        """获取条目并更新访问时间"""
        entry = self.metadata_store.get(sha256)
        if entry:
            entry.last_accessed_at = datetime.now(timezone.utc)
        return entry

    def create_entry(
        self, *, sha256: str, file_path: Path, filename: str, mime_type: Optional[str], size_bytes: int
    ) -> FileCacheEntry:
        entry = FileCacheEntry(
            sha256=sha256,
            local_path=file_path,
            original_filename=filename,
            mime_type=mime_type,
            size_bytes=size_bytes,
        )
        self.metadata_store[sha256] = entry
        short_sha = sha256[:settings.SHA256_ALIAS_LENGTH]
        self.register_aliases(
            sha256,
            sha256,
            short_sha,
            f"files/{sha256}",
            f"files/{short_sha}",
        )
        Logger.event("METADATA_CREATE", "创建文件元数据", sha256=sha256)
        return entry

    def delete_entry(self, sha256: str) -> Optional[FileCacheEntry]:
        """删除元数据，清理映射，返回被删除的条目(以便调用者清理物理文件)"""
        entry = self.metadata_store.pop(sha256, None)
        if not entry:
            return None

        short_sha = sha256[:settings.SHA256_ALIAS_LENGTH]
        self._remove_aliases(sha256, short_sha, f"files/{sha256}", f"files/{short_sha}")
        for client_id, data in entry.replication_map.items():
            if "name" in data:
                self._remove_aliases(data["name"])
        
        Logger.event("METADATA_DELETE", "元数据已删除", sha256=sha256)
        return entry

    def get_sha256_by_filename(self, file_name: str) -> Optional[str]:
        if not file_name:
            return None

        mapped = self.reverse_mapping.get(file_name)
        if mapped:
            Logger.debug("命中文件别名", alias=file_name, sha256=mapped[:8])
            return mapped

        normalized_forms = self._extract_normalized_forms(file_name)
        for form in normalized_forms:
            mapped = self.reverse_mapping.get(form)
            if mapped:
                Logger.debug("命中文件别名", alias=form, sha256=mapped[:8])
                return mapped

        result = self._fallback_lookup(file_name, normalized_forms)
        if result:
            return result

        Logger.debug("文件别名未找到", alias=file_name)
        return None

    def register_aliases(self, sha256: str, *aliases: str):
        for alias in aliases:
            for normalized in self._canonical_aliases(alias):
                self.reverse_mapping[normalized] = sha256
                Logger.debug("注册文件别名", alias=normalized, sha256=sha256[:8])

    def _remove_aliases(self, *aliases: str):
        for alias in aliases:
            for normalized in self._canonical_aliases(alias):
                if self.reverse_mapping.pop(normalized, None):
                    Logger.debug("移除文件别名", alias=normalized)

    def _canonical_aliases(self, alias: Optional[str]) -> Set[str]:
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

    def _extract_normalized_forms(self, file_name: str) -> List[str]:
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
        
        candidate = file_name.split('/')[-1].split(":download", 1)[0]
        if len(candidate) == 64 and all(c in string.hexdigits for c in candidate):
            if candidate in self.metadata_store:
                return [candidate]
        
        return forms

    def _fallback_lookup(self, file_name: str, normalized_forms: List[str]) -> Optional[str]:
        fallback_candidates = set(normalized_forms)
        for sha, entry in self.metadata_store.items():
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

    def update_replication_status(
        self, sha256: str, client_id: str, status: str, gemini_file: Optional[Dict] = None
    ):
        entry = self.get_entry(sha256)
        if not entry:
            return

        replication_data = {"status": status}
        if gemini_file:
            replication_data.update(gemini_file)
            file_name = gemini_file.get("name")
            if file_name:
                self.register_aliases(sha256, file_name)
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

    def reset_replication_map(self, sha256: str):
        entry = self.get_entry(sha256)
        if not entry:
            return
        
        for client_id, data in entry.replication_map.items():
            if "name" in data:
                self.reverse_mapping.pop(data["name"], None)

        entry.replication_map.clear()
        entry.gemini_file_expiration = None
        Logger.event("REPLICATION_RESET", "文件复制地图已重置", sha256=sha256)

    def extract_sha256_hex(self, remote_file: Dict[str, Any]) -> Optional[str]:
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

    def _parse_iso_timestamp(self, value: Optional[str]) -> Optional[datetime]:
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

    # Deletion Flags
    def mark_deleted(self, sha256: Optional[str], aliases: Optional[Set[str]] = None):
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

    def clear_deleted_flag(self, sha256: Optional[str]):
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
        return bool(sha256 and sha256 in self.deleted_shas)

    def is_name_marked_deleted(self, name: Optional[str]) -> bool:
        if not name:
            return False
        aliases = self._normalize_aliases_for_tombstone(name)
        return any(alias in self.deleted_alias_map for alias in aliases)

    def _normalize_aliases_for_tombstone(self, alias: Optional[str]) -> Set[str]:
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
    
    def get_all_entries(self) -> List[FileCacheEntry]:
        return list(self.metadata_store.values())
