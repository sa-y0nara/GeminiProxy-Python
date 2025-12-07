"""响应构建器模块

负责构建文件 API 的响应对象。
"""

from datetime import datetime, timezone
from typing import Optional, Tuple

from fastapi import Request
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.log_utils import Logger
from app.core.utils import encode_sha256_base64
from app.core.metadata_store import FileCacheEntry
from app.schemas.gemini_files import File, UploadFileResponse


class ResponseBuilder:
    """响应构建器

    负责：
    - 构建代理 URI
    - 构造文件响应对象
    - 处理响应格式转换
    """

    def extract_file_payload(self, response: Optional[dict]) -> Optional[dict]:
        """从响应中提取文件 payload
        
        Args:
            response: API 响应字典
            
        Returns:
            文件 payload 字典，或 None
        """
        if isinstance(response, dict):
            file_payload = response.get("file")
            if isinstance(file_payload, dict):
                return file_payload
            return response
        return None

    def determine_proxy_base_url(self, request: Optional[Request]) -> Optional[str]:
        """确定代理基础 URL

        Args:
            request: FastAPI 请求对象

        Returns:
            代理基础 URL，或 None
        """
        if request is not None:
            return str(request.base_url).rstrip("/")
        base = (settings.PROXY_BASE_URL or "").strip()
        return base.rstrip("/") if base else None

    def build_proxy_uris(self, base_url: str, file_name: str) -> Tuple[str, str]:
        """构建代理 URI

        Args:
            base_url: 基础 URL
            file_name: 文件名

        Returns:
            (元数据 URI, 下载 URI) 元组
        """
        normalized_name = file_name.lstrip("/")
        if not normalized_name.startswith("files/"):
            normalized_name = f"files/{normalized_name}"
        metadata_uri = f"{base_url}/v1beta/{normalized_name}"
        download_uri = f"{metadata_uri}:download"
        return metadata_uri, download_uri

    def default_file_payload(self, entry: FileCacheEntry, sha256: str, *, size_bytes: int) -> dict:
        """构建默认文件 payload

        Args:
            entry: 文件缓存条目
            sha256: SHA256 哈希值
            size_bytes: 文件大小

        Returns:
            文件 payload 字典
        """
        now = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        fallback_name = f"files/{sha256}"
        return {
            "name": fallback_name,
            "displayName": entry.original_filename or "untitled",
            "mimeType": entry.mime_type or "application/octet-stream",
            "sizeBytes": str(size_bytes),
            "createTime": now,
            "updateTime": now,
            "sha256Hash": encode_sha256_base64(sha256) or sha256,
            "uri": fallback_name,
            "state": "ACTIVE",
            "source": "UPLOADED",
        }

    def prepare_file_for_response(self, file_data: File | dict, request: Optional[Request]) -> File:
        """为响应准备文件对象

        Args:
            file_data: 文件数据（File 或 dict）
            request: 请求对象

        Returns:
            处理后的 File 对象
        """
        file_obj = file_data if isinstance(file_data, File) else File.model_validate(file_data)
        base_url = self.determine_proxy_base_url(request)

        if base_url:
            metadata_uri, download_uri = self.build_proxy_uris(base_url, file_obj.name)
            file_obj = file_obj.model_copy(update={"uri": metadata_uri, "download_uri": download_uri})
        elif not file_obj.download_uri and file_obj.uri:
            file_obj = file_obj.model_copy(update={"download_uri": file_obj.uri})

        return file_obj

    def map_frontend_response_to_file_model(
        self, frontend_file: Optional[dict], entry: FileCacheEntry, size_bytes: int
    ) -> dict:
        """将前端返回的文件对象映射到后端 File 模型期望的格式

        Args:
            frontend_file: 前端返回的文件对象
            entry: 文件缓存条目
            size_bytes: 文件大小

        Returns:
            符合 File 模型格式的字典
        """
        frontend_file = frontend_file or {}

        base_payload = self.default_file_payload(entry, entry.sha256, size_bytes=size_bytes)
        fallback_name = base_payload["name"]
        sha_base64 = (
            frontend_file.get("sha256Hash")
            or frontend_file.get("sha256_hash")
            or base_payload["sha256Hash"]
        )

        mapped_file = {
            **base_payload,
            "name": frontend_file.get("name") or fallback_name,
            "sizeBytes": str(frontend_file.get("size", size_bytes)),
            "uri": frontend_file.get("uri") or fallback_name,
            "sha256Hash": sha_base64,
        }

        if frontend_file.get("displayName"):
            mapped_file["displayName"] = frontend_file["displayName"]

        if "expirationTime" in frontend_file:
            mapped_file["expirationTime"] = frontend_file["expirationTime"]
        elif entry.gemini_file_expiration:
            mapped_file["expirationTime"] = entry.gemini_file_expiration.isoformat().replace('+00:00', 'Z')

        if "downloadUri" in frontend_file:
            mapped_file["downloadUri"] = frontend_file["downloadUri"]

        return mapped_file

    def build_file_response(
        self,
        source_file: Optional[dict],
        entry: FileCacheEntry,
        size_bytes: int,
    ) -> dict:
        """根据远端返回的数据或本地缓存构造 File 响应

        Args:
            source_file: 远端返回的文件数据
            entry: 文件缓存条目
            size_bytes: 文件大小

        Returns:
            File 响应字典
        """
        try:
            if source_file:
                return File.model_validate(source_file).model_dump(by_alias=True, exclude_none=True)
        except Exception as exc:
            Logger.warning("远程文件数据无法直接验证，将使用本地映射", exc=exc)

        mapped_file_data = self.map_frontend_response_to_file_model(source_file, entry, size_bytes)
        return File.model_validate(mapped_file_data).model_dump(by_alias=True, exclude_none=True)

    def build_final_upload_response(
        self, file_data: File | dict, request: Optional[Request] = None
    ) -> JSONResponse:
        """构造带有 Google Upload 兼容头部的 JSON 响应

        Args:
            file_data: 文件数据
            request: 请求对象

        Returns:
            JSONResponse
        """
        file_obj = self.prepare_file_for_response(file_data, request)

        payload = UploadFileResponse(file=file_obj).model_dump(by_alias=True, exclude_none=True)
        return JSONResponse(
            content=payload,
            headers={
                "X-Goog-Upload-Status": "final",
                "Content-Type": "application/json",
            },
            status_code=200,
        )


# 全局单例
response_builder = ResponseBuilder()
