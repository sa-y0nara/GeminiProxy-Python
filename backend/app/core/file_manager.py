"""文件管理模块 (方案 B - 重构版)

负责管理文件缓存、元数据、后台清理和 sha256 计算。
作为 Facade 协调 FileStorage, MetadataStore 和 SessionManager。
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Optional, Set, Tuple, List

from app.core.config import settings
from app.core.log_utils import Logger

from app.core.file_storage import FileStorage
from app.core.metadata_store import MetadataStore, FileCacheEntry
from app.core.session_manager import SessionManager, UploadSession

# ============================================================================
# 文件管理器 (Facade)
# ============================================================================


class FileManager:
    """文件管理器 (Facade)

    职责:
    1.  协调物理存储 (FileStorage)
    2.  协调元数据管理 (MetadataStore)
    3.  协调会话管理 (SessionManager)
    4.  执行后台清理任务
    """

    def __init__(self) -> None:
        """初始化文件管理器"""
        self.storage = FileStorage()
        self.metadata = MetadataStore(self.storage.file_cache_dir)
        self.sessions = SessionManager()

        Logger.event("INIT", "FileManager(Facade) 初始化")
    
    # --- 代理属性，保持兼容性 ---
    @property
    def file_cache_dir(self) -> Path:
        return self.storage.file_cache_dir
        
    @property
    def metadata_store(self) -> Dict[str, FileCacheEntry]:
        # 兼容旧代码直接访问 metadata_store 属性
        return self.metadata.metadata_store
        
    @property
    def upload_sessions(self) -> Dict[str, UploadSession]:
        return self.sessions.upload_sessions

    # --- 会话管理代理 ---

    def start_upload_session(self, session_id: str, client_id: str, metadata: Optional[Dict[str, Any]] = None) -> UploadSession:
        return self.sessions.start_session(session_id, client_id, metadata)

    def get_upload_session(self, session_id: str) -> Optional[UploadSession]:
        return self.sessions.get_session(session_id)

    def get_upload_metadata(self, session_id: str) -> Dict[str, Any]:
        return self.sessions.get_metadata(session_id)

    # --- 存储操作代理 ---

    async def save_stream_to_cache(
        self, stream: AsyncGenerator[bytes, None], filename: str
    ) -> Tuple[str, Path, int]:
        return await self.storage.save_stream_to_cache(stream, filename)

    def append_chunk_data(self, session_id: str, data: bytes, expected_offset: int) -> int:
        return self.storage.append_chunk_data(session_id, data, expected_offset)

    def finalize_chunk_upload(self, session_id: str) -> Tuple[str, Path, int]:
        return self.storage.finalize_chunk_upload(session_id)

    def discard_chunk_upload(self, session_id: str):
        self.storage.discard_chunk_upload(session_id)

    # --- 元数据操作代理 ---

    def extract_sha256_hex(self, remote_file: Dict[str, Any]) -> Optional[str]:
        return self.metadata.extract_sha256_hex(remote_file)

    def ensure_remote_entry(self, remote_file: Dict[str, Any]) -> Optional[FileCacheEntry]:
        return self.metadata.ensure_remote_entry(remote_file)

    def get_metadata_entry(self, sha256: str) -> Optional[FileCacheEntry]:
        return self.metadata.get_entry(sha256)

    def get_sha256_by_filename(self, file_name: str) -> Optional[str]:
        return self.metadata.get_sha256_by_filename(file_name)

    def create_metadata_entry(
        self, *, sha256: str, file_path: Path, filename: str, mime_type: Optional[str], size_bytes: int
    ) -> FileCacheEntry:
        return self.metadata.create_entry(
            sha256=sha256,
            file_path=file_path,
            filename=filename,
            mime_type=mime_type,
            size_bytes=size_bytes,
        )

    def update_replication_status(
        self, sha256: str, client_id: str, status: str, gemini_file: Optional[Dict] = None
    ):
        self.metadata.update_replication_status(sha256, client_id, status, gemini_file)

    def reset_replication_map(self, sha256: str):
        self.metadata.reset_replication_map(sha256)

    def delete_entry(self, sha256: str):
        """删除元数据和物理文件"""
        entry = self.metadata.delete_entry(sha256)
        if entry:
            self.storage.delete_file(entry.local_path)
            Logger.event("FILE_DELETE_COMPLETE", "文件已完全删除", sha256=sha256)

    # --- 删除标记代理 ---

    def mark_deleted(self, sha256: Optional[str], aliases: Optional[Set[str]] = None):
        self.metadata.mark_deleted(sha256, aliases)

    def clear_deleted_flag(self, sha256: Optional[str]):
        self.metadata.clear_deleted_flag(sha256)

    def is_marked_deleted(self, sha256: Optional[str]) -> bool:
        return self.metadata.is_marked_deleted(sha256)

    def is_name_marked_deleted(self, name: Optional[str]) -> bool:
        return self.metadata.is_name_marked_deleted(name)

    # --- 清理任务 ---

    def _collect_ttl_expired_files(self, now: datetime) -> list[str]:
        """收集所有 TTL 过期的文件"""
        return [sha256 for sha256, entry in self.metadata.metadata_store.items()
                if entry.gemini_file_expiration and now > entry.gemini_file_expiration]

    def _collect_lru_candidates(self, now: datetime, exclude_shas: set) -> list[tuple]:
        """收集 LRU 清理候选文件"""
        candidates = []
        for sha256, entry in self.metadata.metadata_store.items():
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
                self.sessions.cleanup_expired_sessions(now)

                # 4. 执行删除
                if to_delete:
                    Logger.info(f"准备删除 {len(to_delete)} 个缓存条目...")
                    for sha256 in list(to_delete):
                        self.delete_entry(sha256)
                    Logger.info("缓存清理任务完成。")
                else:
                    Logger.info("缓存状态正常，无需清理。")
            except Exception as e:
                Logger.error("缓存清理任务失败", exc=e)

    def cleanup_all_cache_files(self):
        self.storage.cleanup_all_files()


# ============================================================================
# 全局文件管理器实例
# ============================================================================

file_manager = FileManager()