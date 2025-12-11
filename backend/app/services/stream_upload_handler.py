"""流式上传处理模块

负责处理流式上传（stream upload）的逻辑。
"""

from pathlib import Path
from typing import Optional, Tuple

from fastapi import HTTPException, Request

from app.core.file_manager import file_manager
from app.core.log_utils import Logger
from app.services.file_resolver import file_resolver


class StreamUploadHandler:
    """流式上传处理器
    
    职责：
    1. 将请求体保存到缓存
    2. 处理流式上传
    """

    async def persist_body_to_cache(
        self, request: Request, filename_hint: Optional[str]
    ) -> Tuple[str, Path, int]:
        """将请求体保存到缓存，避免一次性读取全部内容"""
        body_stream = request.stream()

        async def iterator():
            async for chunk in body_stream:
                if chunk:
                    yield chunk

        temp_name = file_resolver.sanitize_filename_hint(filename_hint)
        return await file_manager.save_stream_to_cache(iterator(), temp_name)

    async def handle_stream_upload(
        self,
        *,
        request: Request,
        filename_hint: Optional[str],
        metadata: dict,
        request_id: str,
        session_id: str,
        size_validator,
    ) -> Tuple[str, Path, int]:
        """处理流式上传请求
        
        Args:
            request: FastAPI 请求对象
            filename_hint: 文件名提示
            metadata: 文件元数据
            request_id: 请求 ID
            session_id: 上传会话 ID
            size_validator: 大小校验回调函数
            
        Returns:
            (sha256, file_path, size_bytes) 元组
        """
        try:
            sha256, file_path, size_bytes = await self.persist_body_to_cache(
                request, filename_hint
            )
        except Exception as exc:
            Logger.error("保存文件到缓存失败", exc=exc)
            raise HTTPException(
                status_code=500, detail="Failed to save file to cache."
            ) from exc

        # 调用大小校验
        size_validator(
            metadata,
            size_bytes,
            header_size=request.headers.get("content-length"),
            request_id=request_id,
            session_id=session_id,
            file_path=file_path,
            check_header=True,
        )

        return sha256, file_path, size_bytes


# 全局单例
stream_upload_handler = StreamUploadHandler()
