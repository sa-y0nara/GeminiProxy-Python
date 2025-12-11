"""文件解析器模块

负责文件名处理、MIME 类型解析和相关工具函数。
"""

import re
import uuid
from pathlib import Path
from typing import Optional, Tuple

from app.core.log_utils import Logger
from app.core.mime_utils import MimeUtils
from app.core.utils import first_non_empty

FILENAME_RE = re.compile(r'filename\*?=["\']?([^"\'\s;]+)')


class FileResolver:
    """文件解析器

    负责：
    - 文件名清理和规范化
    - MIME 类型解析
    - 从 HTTP 头中提取文件名
    """

    def sanitize_filename_hint(self, filename_hint: Optional[str]) -> str:
        """清理文件名，防止目录遍历攻击

        Args:
            filename_hint: 原始文件名提示

        Returns:
            清理后的安全文件名
        """
        if not filename_hint:
            return f"upload_{uuid.uuid4().hex}"

        sanitized = filename_hint.strip()
        
        # 使用 Path.name 提取最终文件名部分，防止路径遍历
        # 这会自动处理 /, \\, .., ... 等所有路径遍历变体
        sanitized = Path(sanitized).name
        
        # 额外处理：移除可能残留的路径分隔符
        sanitized = sanitized.replace("\\", "_").replace("/", "_")
        
        # 移除开头的点（隐藏文件），但保留扩展名中的点
        sanitized = sanitized.lstrip(".")

        if not sanitized:
            return f"upload_{uuid.uuid4().hex}"

        return sanitized

    def parse_filename_from_headers(self, *headers: Optional[str]) -> Optional[str]:
        """从若干 HTTP 头中提取文件名

        Args:
            *headers: 要检查的 HTTP 头值列表

        Returns:
            提取的文件名，或 None
        """
        for header in headers:
            if not header:
                continue
            match = FILENAME_RE.search(header)
            if match:
                return match.group(1)
        return None

    def resolve_filename_and_mime(
        self,
        sha256: str,
        metadata: dict,
        filename_hint: Optional[str],
        content_type_hint: Optional[str],
        file_path: Path,
        request_id: str,
    ) -> Tuple[str, str]:
        """解析最终的文件名和 MIME 类型

        根据多个来源（HTTP 头、元数据、文件内容检测）确定最佳的文件名和 MIME 类型。

        Args:
            sha256: 文件的 SHA256 哈希值
            metadata: 文件元数据字典
            filename_hint: 文件名提示
            content_type_hint: Content-Type 头
            file_path: 文件路径
            request_id: 请求 ID（用于日志）

        Returns:
            (文件名, MIME 类型) 元组
        """
        normalized_hint = MimeUtils.normalize_filename(filename_hint)
        metadata_filename = MimeUtils.normalize_filename(
            first_non_empty(metadata, "display_name", "displayName", "filename", "fileName")
        )
        valid_names = [
            name
            for name in [normalized_hint, metadata_filename]
            if name and name.lower() not in {"untitled", "unknown", "unknown_file"}
        ]
        final_filename = valid_names[0] if valid_names else None

        header_mime = None
        if content_type_hint:
            header_mime = content_type_hint.split(";")[0].strip().lower()
            if header_mime == "application/octet-stream":
                header_mime = None

        metadata_mime = first_non_empty(metadata, "mime_type", "mimeType")
        if isinstance(metadata_mime, str):
            metadata_mime = metadata_mime.strip().lower()

        detected_mime = MimeUtils.detect_mime_type_from_content(file_path)
        inferred_mime_from_name = MimeUtils.infer_mime_type(final_filename) if final_filename else None

        candidate_mimes = [
            header_mime,
            metadata_mime,
            detected_mime,
            inferred_mime_from_name,
        ]
        final_mime = next((mime for mime in candidate_mimes if mime), "application/octet-stream")

        if not final_filename:
            final_filename = MimeUtils.build_fallback_filename(sha256, final_mime)
            Logger.info(f"使用基于类型的临时文件名: {final_filename}", request_id=request_id)
        else:
            suffix = Path(final_filename).suffix
            if not suffix:
                extension = MimeUtils.guess_extension_from_mime(final_mime, default="")
                if extension:
                    final_filename = f"{final_filename}{extension}"

        return final_filename, final_mime


# 全局单例
file_resolver = FileResolver()
