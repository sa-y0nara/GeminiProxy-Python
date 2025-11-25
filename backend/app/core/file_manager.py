"""文件管理模块 (方案 B)

负责管理文件缓存、元数据、后台清理和 sha256 计算。
"""

import asyncio
import base64
import hashlib
import logging
import os
import re
import shutil
import string
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Optional, Set, Tuple

from app.core.config import settings
from app.core.log_utils import Logger


ISO_TIMESTAMP_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T"
    r"(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<frac>\d+))?"
    r"(?P<tz>Z|[+-]\d{2}:\d{2})?$"
)

# ============================================================================
# 数据类 (方案 B)
# ============================================================================


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


@dataclass
class ChunkUploadState:
    temp_path: Path
    sha256: hashlib._hashlib.HASH = field(default_factory=hashlib.sha256)
    size_bytes: int = 0


@dataclass
class UploadSession:
    metadata: Dict[str, Any]
    client_id: str  # 绑定到特定客户端，防止会话劫持
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ============================================================================
# 文件管理器 (方案 B)
# ============================================================================


class FileManager:
    """文件管理器

    职责:
    1.  将上传的文件内容存储到本地缓存。
    2.  计算文件的 sha256 作为唯一标识。
    3.  管理文件的核心元数据（包括复制状态）。
    4.  提供后台任务进行缓存清理 (TTL + LRU)。
    """

    def __init__(self) -> None:
        """初始化文件管理器"""
        # 文件缓存目录
        self.file_cache_dir: Path = Path(settings.FILE_CACHE_DIR).resolve()
        # 确保缓存目录在项目根目录之外，或者在 .gitignore 中，避免触发重载
        self.file_cache_dir.mkdir(parents=True, exist_ok=True)

        # 核心元数据存储 (sha256 -> FileCacheEntry)
        self.metadata_store: Dict[str, FileCacheEntry] = {}
        # 反向映射 (gemini_file_name -> sha256)
        self.reverse_mapping: Dict[str, str] = {}
        # 临时上传会话 (session_id -> UploadSession)
        self.upload_sessions: Dict[str, UploadSession] = {}
        # 分块上传状态 (session_id -> ChunkUploadState)
        self.chunk_upload_states: Dict[str, ChunkUploadState] = {}
        # 记录被显式删除的 sha256，防止异步删除期间被再次引用
        self.deleted_shas: Set[str] = set()
        # 记录被删除的文件别名 (files/<id>、裸id 等) -> sha256
        self.deleted_alias_map: Dict[str, str] = {}

        Logger.event("INIT", "文件管理器(方案 B)初始化", cache_dir=str(self.file_cache_dir))

    def start_upload_session(self, session_id: str, client_id: str, metadata: Optional[Dict[str, Any]] = None) -> UploadSession:
        """创建绑定到特定客户端的上传会话，防止会话劫持。"""
        session = UploadSession(metadata=dict(metadata or {}), client_id=client_id)
        self.upload_sessions[session_id] = session
        return session

    def _extract_metadata_and_timestamp(self, session_data: Any) -> Tuple[Dict[str, Any], datetime]:
        """从多种数据格式中提取元数据和时间戳"""
        metadata: Dict[str, Any] = {}
        created_at = datetime.now(timezone.utc)

        if isinstance(session_data, dict):
            metadata = dict(session_data.get("metadata") or session_data or {})
            raw_created = session_data.get("created_at")
        elif isinstance(session_data, tuple) and len(session_data) == 2:
            metadata = dict(session_data[0] or {})
            raw_created = session_data[1]
        else:
            raw_created = None

        if isinstance(raw_created, datetime):
            created_at = raw_created if raw_created.tzinfo else raw_created.replace(tzinfo=timezone.utc)

        return metadata, created_at

    def _normalize_upload_session(self, session_id: str, session_data: Any, client_id: Optional[str] = None) -> Optional[UploadSession]:
        """规范化上传会话数据格式"""
        if isinstance(session_data, UploadSession):
            if client_id and session_data.client_id != client_id:
                Logger.warning("会话客户端不匹配，拒绝访问", session_id=session_id)
                return None
            return session_data
        if session_data is None:
            return None

        metadata, created_at = self._extract_metadata_and_timestamp(session_data)
        bound_client_id = client_id or "unknown"
        session = UploadSession(metadata=metadata, client_id=bound_client_id, created_at=created_at)
        self.upload_sessions[session_id] = session
        return session

    def get_upload_session(self, session_id: str) -> Optional[UploadSession]:
        session_data = self.upload_sessions.get(session_id)
        return self._normalize_upload_session(session_id, session_data)

    def get_upload_metadata(self, session_id: str) -> Dict[str, Any]:
        session = self.get_upload_session(session_id)
        return dict(session.metadata) if session else {}

    def _get_cache_path(self, sha256: str) -> Path:
        """根据 sha256 生成分层的文件缓存路径"""
        # 使用前4个字符创建两级子目录，避免单个目录下文件过多
        # 例如: d29a...f2 -> /.../file_cache/d2/9a/d29a...f2.bin
        if len(sha256) < 4:
            raise ValueError("sha256 hash must be at least 4 characters long")
        sub_dir1 = sha256[:2]
        sub_dir2 = sha256[2:4]
        return self.file_cache_dir / sub_dir1 / sub_dir2 / f"{sha256}.bin"

    async def save_stream_to_cache(
        self, stream: AsyncGenerator[bytes, None], filename: str
    ) -> Tuple[str, Path, int]:
        """将异步字节流保存到缓存，使用后台线程执行阻塞 I/O。"""

        sha256 = hashlib.sha256()
        size_bytes = 0
        temp_path = self.file_cache_dir / f"temp_{filename}"

        try:
            with open(temp_path, "wb") as temp_file:
                async for chunk in stream:
                    if not chunk:
                        continue
                    sha256.update(chunk)
                    size_bytes += len(chunk)
                    await asyncio.to_thread(temp_file.write, chunk)

            sha256_hex = sha256.hexdigest()
            final_path = self._get_cache_path(sha256_hex)

            # 创建目标子目录
            final_path.parent.mkdir(parents=True, exist_ok=True)

            # 将临时文件移动到最终位置
            await asyncio.to_thread(shutil.move, temp_path, final_path)

            Logger.event(
                "FILE_CACHE_SAVE",
                "文件已保存到缓存",
                sha256=sha256_hex,
                path=str(final_path),
                size=size_bytes,
            )
            return sha256_hex, final_path, size_bytes
        finally:
            if os.path.exists(temp_path):
                await asyncio.to_thread(os.remove, temp_path)

    # ========================================================================
    # 分块上传管理
    # ========================================================================

    def _create_chunk_state(self, session_id: str) -> ChunkUploadState:
        temp_path = self.file_cache_dir / f"chunk_{session_id}"
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError as e:
                logging.warning(f"Failed to remove existing chunk temp file {temp_path}: {e}")
        state = ChunkUploadState(temp_path=temp_path)
        self.chunk_upload_states[session_id] = state
        return state

    def append_chunk_data(self, session_id: str, data: bytes, expected_offset: int) -> int:
        state = self.chunk_upload_states.get(session_id)
        if not state:
            state = self._create_chunk_state(session_id)

        if state.size_bytes != expected_offset:
            raise ValueError(f"Offset mismatch: expected {state.size_bytes}, got {expected_offset}")

        with open(state.temp_path, "ab") as f:
            f.write(data)
        state.sha256.update(data)
        state.size_bytes += len(data)
        return state.size_bytes

    def finalize_chunk_upload(self, session_id: str) -> Tuple[str, Path, int]:
        state = self.chunk_upload_states.pop(session_id, None)
        if not state:
            raise ValueError("Chunk upload state not found")

        sha256_hex = state.sha256.hexdigest()
        final_path = self._get_cache_path(sha256_hex)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(state.temp_path, final_path)

        Logger.event(
            "FILE_CACHE_SAVE",
            "文件已保存到缓存",
            sha256=sha256_hex,
            path=str(final_path),
            size=state.size_bytes,
        )
        return sha256_hex, final_path, state.size_bytes

    def discard_chunk_upload(self, session_id: str):
        state = self.chunk_upload_states.pop(session_id, None)
        if state and state.temp_path.exists():
            try:
                state.temp_path.unlink()
            except OSError as e:
                Logger.warning(f"Failed to delete temp file {state.temp_path}: {e}")

    # ========================================================================
    # 元数据管理
    # ========================================================================

    def _register_aliases(self, sha256: str, *aliases: str):
        """注册反向映射别名，兼容 files/<id> 及末尾裸 ID"""
        for alias in aliases:
            for normalized in self._canonical_aliases(alias):
                self.reverse_mapping[normalized] = sha256
                Logger.debug("注册文件别名", alias=normalized, sha256=sha256[:8])

    def _remove_aliases(self, *aliases: str):
        """移除反向映射别名，同时移除裸 ID 映射"""
        for alias in aliases:
            for normalized in self._canonical_aliases(alias):
                if self.reverse_mapping.pop(normalized, None):
                    Logger.debug("移除文件别名", alias=normalized)

    def _canonical_aliases(self, alias: Optional[str]) -> Set[str]:
        """生成别名的所有规范形式，使用集合推导式优化"""
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

    def extract_sha256_hex(self, remote_file: Dict[str, Any]) -> Optional[str]:
        """从远端文件响应中解析 sha256（支持 base64 或 hex 格式）"""
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
        """
        当本地不存在缓存条目时，根据远端 files.get 响应创建一个占位的元数据条目。
        """
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
        Logger.event(
            "METADATA_REMOTE",
            "创建远程文件元数据占位",
            sha256=sha256_hex[:8],
            file_name=remote_file.get("name"),
        )
        return entry

    def _parse_iso_timestamp(self, value: Optional[str]) -> Optional[datetime]:
        """兼容 Google 返回的纳秒时间戳，转换为 Python datetime"""
        if not value:
            return None

        match = ISO_TIMESTAMP_RE.match(value.strip())
        if not match:
            Logger.warning("无法解析时间戳", timestamp=value)
            return None

        base = f"{match.group('date')}T{match.group('time')}"
        frac = match.group('frac')
        if frac:
            frac = (frac + "000000")[:6]  # Python datetime 仅支持微秒
            base = f"{base}.{frac}"

        tz = match.group('tz') or "+00:00"
        if tz == "Z":
            tz = "+00:00"

        try:
            return datetime.fromisoformat(base + tz)
        except ValueError as exc:
            Logger.warning("时间戳解析失败", timestamp=value, exc=exc)
            return None

    def get_metadata_entry(self, sha256: str) -> Optional[FileCacheEntry]:
        """通过 sha256 获取元数据条目，并更新访问时间"""
        entry = self.metadata_store.get(sha256)
        if entry:
            entry.last_accessed_at = datetime.now(timezone.utc)
        return entry

    def get_sha256_by_filename(self, file_name: str) -> Optional[str]:
        """通过 gemini file name 或冗余 fileUri 获取 sha256，优化查询流程"""
        if not file_name:
            return None

        # 第一步：直接查询反向映射（O(1) 查询）
        mapped = self.reverse_mapping.get(file_name)
        if mapped:
            Logger.debug("命中文件别名", alias=file_name, sha256=mapped[:8])
            return mapped

        # 第二步：提取规范化形式并查询
        normalized_forms = self._extract_normalized_forms(file_name)
        for form in normalized_forms:
            mapped = self.reverse_mapping.get(form)
            if mapped:
                Logger.debug("命中文件别名", alias=form, sha256=mapped[:8])
                return mapped

        # 第三步：仅当前两步都失败时才进行全表扫描（带缓存）
        result = self._fallback_lookup(file_name, normalized_forms)
        if result:
            return result

        Logger.debug("文件别名未找到", alias=file_name)
        return None

    def _extract_normalized_forms(self, file_name: str) -> list[str]:
        """提取可能的规范化形式"""
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
        
        # 检查是否为裸 sha256
        candidate = file_name.split('/')[-1].split(":download", 1)[0]
        if len(candidate) == 64 and all(c in string.hexdigits for c in candidate):
            if candidate in self.metadata_store:
                return [candidate]
        
        return forms

    def _fallback_lookup(self, file_name: str, normalized_forms: list[str]) -> Optional[str]:
        """回退查询：扫描 replication_map，仅在前两步失败时执行"""
        fallback_candidates = set(normalized_forms)
        
        for sha, entry in self.metadata_store.items():
            for data in entry.replication_map.values():
                remote_name = data.get("name")
                if remote_name and remote_name in fallback_candidates:
                    Logger.debug("通过 replication_map 找到文件", alias=file_name, sha256=sha[:8])
                    self._register_aliases(sha, remote_name)
                    return sha
                uri = data.get("uri")
                if uri and "files/" in uri:
                    uri_tail = uri.split("files/", 1)[-1]
                    if uri_tail and uri_tail in fallback_candidates:
                        Logger.debug("通过 uri 找到文件", alias=file_name, sha256=sha[:8])
                        self._register_aliases(sha, uri, uri_tail)
                        return sha
        
        return None

    def create_metadata_entry(
        self, *, sha256: str, file_path: Path, filename: str, mime_type: Optional[str], size_bytes: int
    ) -> FileCacheEntry:
        """创建一个新的元数据条目"""
        entry = FileCacheEntry(
            sha256=sha256,
            local_path=file_path,
            original_filename=filename,
            mime_type=mime_type,
            size_bytes=size_bytes,
        )
        self.metadata_store[sha256] = entry
        # 注册本地 fallback fileUri，便于在复制前使用
        short_sha = sha256[:32]
        self._register_aliases(
            sha256,
            sha256,
            short_sha,
            f"files/{sha256}",
            f"files/{short_sha}",
        )
        Logger.event("METADATA_CREATE", "创建文件元数据", sha256=sha256)
        return entry

    def update_replication_status(
        self, sha256: str, client_id: str, status: str, gemini_file: Optional[Dict] = None
    ):
        """更新文件的复制状态"""
        entry = self.get_metadata_entry(sha256)
        if not entry:
            return

        replication_data = {"status": status}
        if gemini_file:
            replication_data.update(gemini_file)
            file_name = gemini_file.get("name")
            if file_name:
                # 更新反向映射
                self._register_aliases(sha256, file_name)
            uri_value = gemini_file.get("uri")
            if uri_value:
                replication_data["uri"] = uri_value
            # 如果这是第一次成功上传，记录过期时间
            if not entry.gemini_file_expiration:
                expiration_time = gemini_file.get("expirationTime")
                if expiration_time:
                    parsed_expiration = self._parse_iso_timestamp(expiration_time)
                    if parsed_expiration:
                        entry.gemini_file_expiration = parsed_expiration

        entry.replication_map[client_id] = replication_data
        Logger.debug(
            "更新复制状态",
            sha256=sha256,
            client_id=client_id,
            status=status,
        )

    def reset_replication_map(self, sha256: str):
        """全局重置：清空文件的复制地图"""
        entry = self.get_metadata_entry(sha256)
        if not entry:
            return

        # 从反向映射中删除所有相关的旧 file_name
        for client_id, data in entry.replication_map.items():
            if "name" in data:
                self.reverse_mapping.pop(data["name"], None)

        entry.replication_map.clear()
        entry.gemini_file_expiration = None  # 重置过期时间
        Logger.event("REPLICATION_RESET", "文件复制地图已重置", sha256=sha256)

    def _delete_entry(self, sha256: str):
        """内部方法：删除一个缓存条目及其关联的所有数据"""
        entry = self.metadata_store.pop(sha256, None)
        if not entry:
            return

        # 从反向映射中删除
        short_sha = sha256[:32]
        self._remove_aliases(sha256, short_sha, f"files/{sha256}", f"files/{short_sha}")
        for client_id, data in entry.replication_map.items():
            if "name" in data:
                self._remove_aliases(data["name"])

        # 删除物理文件
        try:
            if entry.local_path.exists():
                os.remove(entry.local_path)
                # 尝试删除空的父目录
                try:
                    entry.local_path.parent.rmdir()
                    entry.local_path.parent.parent.rmdir()
                except OSError:
                    # 目录非空，忽略错误
                    pass
        except OSError as e:
            Logger.error("删除缓存文件失败", exc=e, path=str(entry.local_path))

        Logger.event("FILE_CACHE_DELETE", "文件已从缓存中删除", sha256=sha256)

    def delete_entry(self, sha256: str):
        """公开的缓存删除接口，供其他模块使用。"""
        self._delete_entry(sha256)

    # ========================================================================
    # 删除标记管理
    # ========================================================================

    def _normalize_aliases_for_tombstone(self, alias: Optional[str]) -> Set[str]:
        """生成一组可用于 tombstone 的别名形式"""
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

    def mark_deleted(self, sha256: Optional[str], aliases: Optional[Set[str]] = None):
        """记录一个 sha256 被显式删除，并记住相关别名"""
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
        Logger.debug(
            "标记文件为已删除",
            sha256=sha256[:8],
            aliases=list(sorted(tombstone_aliases))[:3],
        )

    def clear_deleted_flag(self, sha256: Optional[str]):
        """清除显式删除标记"""
        if not sha256:
            return
        if sha256 in self.deleted_shas:
            self.deleted_shas.discard(sha256)
        aliases_to_remove = [alias for alias, value in self.deleted_alias_map.items() if value == sha256]
        for alias in aliases_to_remove:
            self.deleted_alias_map.pop(alias, None)
        if aliases_to_remove:
            Logger.debug("清除已删除标记", sha256=sha256[:8], aliases=aliases_to_remove[:3])

    def is_marked_deleted(self, sha256: Optional[str]) -> bool:
        """判断一个 sha256 是否仍处于显式删除状态"""
        return bool(sha256 and sha256 in self.deleted_shas)

    def is_name_marked_deleted(self, name: Optional[str]) -> bool:
        """判断一个文件名/URI 是否被标记为已删除"""
        if not name:
            return False
        aliases = self._normalize_aliases_for_tombstone(name)
        return any(alias in self.deleted_alias_map for alias in aliases)

    def _collect_ttl_expired_files(self, now: datetime) -> list[str]:
        """收集所有 TTL 过期的文件"""
        return [sha256 for sha256, entry in self.metadata_store.items()
                if entry.gemini_file_expiration and now > entry.gemini_file_expiration]

    def _collect_lru_candidates(self, now: datetime, exclude_shas: set) -> list[tuple]:
        """收集 LRU 清理候选文件"""
        candidates = []
        for sha256, entry in self.metadata_store.items():
            if sha256 not in exclude_shas:
                candidates.append((entry.last_accessed_at, sha256, entry.size_bytes))
        return candidates

    def _select_lru_to_delete(self, lru_candidates: list, quota_bytes: int) -> set[str]:
        """基于 LRU 策略选择要删除的文件"""
        to_delete = set()
        total_size = sum(size for _, _, size in lru_candidates)
        
        if total_size <= quota_bytes:
            return to_delete
        
        Logger.info(f"LRU清理: 缓存超出配额 ({total_size / 1024 / 1024:.2f}MB > {quota_bytes / 1024 / 1024:.2f}MB)。")
        lru_candidates.sort()
        
        for last_accessed, sha256, size_bytes in lru_candidates:
            if total_size <= quota_bytes:
                break
            to_delete.add(sha256)
            total_size -= size_bytes
        
        return to_delete

    def _cleanup_expired_sessions(self, now: datetime) -> int:
        """清理过期的上传会话，返回清理数量"""
        expired_sessions = []
        session_timeout = timedelta(hours=settings.SESSION_EXPIRATION_HOURS)
        
        for session_id, session_data in list(self.upload_sessions.items()):
            session = session_data if isinstance(session_data, UploadSession) else self._normalize_upload_session(session_id, session_data)
            if not session or (now - session.created_at) > session_timeout:
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            self.upload_sessions.pop(session_id, None)
        
        if expired_sessions:
            Logger.info(f"清理 {len(expired_sessions)} 个过期的上传会话...")
        
        return len(expired_sessions)

    async def periodic_cleanup_task(self):
        """后台定期清理任务，结合 TTL 和 LRU 策略。"""
        while True:
            await asyncio.sleep(settings.FILE_CACHE_CLEANUP_INTERVAL)
            Logger.info("开始执行文件缓存清理任务...")

            try:
                now = datetime.now(timezone.utc)
                to_delete = set()

                # 1. TTL 清理
                ttl_expired = self._collect_ttl_expired_files(now)
                if ttl_expired:
                    Logger.info(f"TTL清理: 发现 {len(ttl_expired)} 个过期文件。")
                    to_delete.update(ttl_expired)

                # 2. LRU 清理
                quota_bytes = settings.FILE_CACHE_QUOTA_MB * 1024 * 1024
                lru_candidates = self._collect_lru_candidates(now, to_delete)
                lru_to_delete = self._select_lru_to_delete(lru_candidates, quota_bytes)
                to_delete.update(lru_to_delete)

                # 3. 清理过期会话
                self._cleanup_expired_sessions(now)

                # 4. 执行删除
                if to_delete:
                    Logger.info(f"准备删除 {len(to_delete)} 个缓存条目...")
                    for sha256 in list(to_delete):
                        self._delete_entry(sha256)
                    Logger.info("缓存清理任务完成。")
                else:
                    Logger.info("缓存状态正常，无需清理。")
            except Exception as e:
                Logger.error("缓存清理任务失败", exc=e)

    def cleanup_all_cache_files(self):
        """
        删除整个文件缓存目录。
        在应用关闭时调用，用于清理。
        """
        try:
            if self.file_cache_dir.exists():
                shutil.rmtree(self.file_cache_dir)
                Logger.event("SHUTDOWN_CLEANUP", "删除文件缓存目录", cache_dir=str(self.file_cache_dir))
        except OSError as e:
            Logger.error("删除文件缓存目录失败", exc=e, cache_dir=str(self.file_cache_dir))


# ============================================================================
# 全局文件管理器实例
# ============================================================================

file_manager = FileManager()
