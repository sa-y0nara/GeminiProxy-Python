"""分块上传处理模块

负责处理分块上传（chunked upload）的逻辑。
"""

from pathlib import Path
from typing import Optional, Set, Tuple, Union

from fastapi import HTTPException, Request, Response

from app.core.file_manager import file_manager
from app.core.log_utils import Logger


class ChunkedUploadHandler:
    """分块上传处理器
    
    职责：
    1. 解析上传命令
    2. 处理分块数据追加
    3. 完成分块上传
    """

    def parse_upload_commands(self, header_value: Optional[str]) -> Set[str]:
        """解析 X-Goog-Upload-Command 头部"""
        if not header_value:
            return set()
        return {token.strip().lower() for token in header_value.split(",") if token.strip()}

    async def handle_chunked_upload(
        self,
        *,
        request: Request,
        session_id: str,
        metadata: dict,
        request_id: str,
        upload_offset_header: Optional[str],
        finalize_requested: bool,
        size_validator,
    ) -> Union[Response, Tuple[str, Path, int]]:
        """处理分块上传请求
        
        Args:
            request: FastAPI 请求对象
            session_id: 上传会话 ID
            metadata: 文件元数据
            request_id: 请求 ID
            upload_offset_header: X-Goog-Upload-Offset 头部值
            finalize_requested: 是否请求完成上传
            size_validator: 大小校验回调函数
            
        Returns:
            如果未完成，返回 308 Response
            如果完成，返回 (sha256, file_path, size_bytes) 元组
        """
        try:
            upload_offset_int = int(upload_offset_header or 0)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail="Invalid X-Goog-Upload-Offset header."
            )

        chunk_data = await request.body()
        try:
            new_offset = file_manager.append_chunk_data(
                session_id, chunk_data, upload_offset_int
            )
        except ValueError as offset_error:
            file_manager.discard_chunk_upload(session_id)
            file_manager.upload_sessions.pop(session_id, None)
            raise HTTPException(status_code=400, detail=str(offset_error))

        if not finalize_requested:
            return Response(
                status_code=308,
                headers={
                    "X-Goog-Upload-Status": "active",
                    "X-Goog-Upload-Offset": str(new_offset),
                },
            )

        try:
            sha256, file_path, size_bytes = file_manager.finalize_chunk_upload(session_id)
        except ValueError as finalize_error:
            file_manager.upload_sessions.pop(session_id, None)
            raise HTTPException(status_code=400, detail=str(finalize_error))

        # 调用大小校验
        size_validator(
            metadata,
            size_bytes,
            header_size=None,
            request_id=request_id,
            session_id=session_id,
            file_path=file_path,
            check_header=False,
        )

        return sha256, file_path, size_bytes

    def is_chunked_upload(
        self, command_tokens: Set[str], upload_offset_header: Optional[str]
    ) -> bool:
        """判断是否为分块上传"""
        return bool(command_tokens) or upload_offset_header is not None


# 全局单例
chunked_upload_handler = ChunkedUploadHandler()
