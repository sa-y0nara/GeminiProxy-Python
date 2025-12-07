import asyncio
import hashlib
import logging
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncGenerator, Dict, Tuple

from app.core.config import settings
from app.core.log_utils import Logger

@dataclass
class ChunkUploadState:
    temp_path: Path
    sha256: hashlib._hashlib.HASH = field(default_factory=hashlib.sha256)
    size_bytes: int = 0

class FileStorage:
    """
    负责物理文件存储操作：保存、分块写入、移动、删除、清理。
    不包含任何业务元数据逻辑。
    """
    def __init__(self):
        self.file_cache_dir: Path = Path(settings.FILE_CACHE_DIR).resolve()
        self.file_cache_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_upload_states: Dict[str, ChunkUploadState] = {}
        Logger.event("INIT", "FileStorage 初始化", cache_dir=str(self.file_cache_dir))

    def get_cache_path(self, sha256: str) -> Path:
        """根据 sha256 生成分层的文件缓存路径"""
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
            final_path = self.get_cache_path(sha256_hex)
            final_path.parent.mkdir(parents=True, exist_ok=True)
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
        final_path = self.get_cache_path(sha256_hex)
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

    def delete_file(self, path: Path):
        """删除物理文件"""
        try:
            if path.exists():
                os.remove(path)
                try:
                    path.parent.rmdir()
                    path.parent.parent.rmdir()
                except OSError:
                    pass
        except OSError as e:
            Logger.error("删除缓存文件失败", exc=e, path=str(path))

    def cleanup_all_files(self):
        """删除整个缓存目录"""
        try:
            if self.file_cache_dir.exists():
                shutil.rmtree(self.file_cache_dir)
                Logger.event("SHUTDOWN_CLEANUP", "删除文件缓存目录", cache_dir=str(self.file_cache_dir))
        except OSError as e:
            Logger.error("删除文件缓存目录失败", exc=e, cache_dir=str(self.file_cache_dir))
