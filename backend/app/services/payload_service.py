"""
Payload processing service.
Responsible for inspecting, cleaning, and preparing payloads before they are sent to the frontend.
"""
from typing import Any, Optional, Dict, List
from app.core.log_utils import Logger
from app.core.mime_utils import MimeUtils
from app.core.file_manager import file_manager

class PayloadService:
    async def prepare_payload(self, command_type: str, payload: Any, request_id: str) -> Any:
        """Apply command-specific preprocessing to payload."""
        if command_type in {"generateContent", "streamGenerateContent"}:
            return await self._fix_payload_mime_types(payload, request_id)
        return payload

    async def _fix_payload_mime_types(self, payload: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        """
        修正 payload 中的 MIME 类型
        """
        if not isinstance(payload, dict) or "payload" not in payload:
            return payload

        contents = payload["payload"].get("contents", [])
        if not isinstance(contents, list):
            return payload

        fixed_contents = []
        for content in contents:
            if isinstance(content, dict):
                processed_content = await self._process_single_content(content, request_id)
                fixed_contents.append(processed_content)
            else:
                fixed_contents.append(content)

        # 始终返回重构后的 payload（即使长度相同，内容也可能被修改）
        new_payload = payload.copy()
        new_payload["payload"] = payload["payload"].copy()
        new_payload["payload"]["contents"] = fixed_contents
        return new_payload

    async def _process_single_content(self, content: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        """
        处理单个 content item，检查并修正 fileData 的 MIME 类型
        """
        # 尝试获取 fileData (支持 snake_case 和 camelCase)
        file_data = content.get("file_data") or content.get("fileData", {})
        
        # 如果没有 fileData 或 mime_type，直接返回
        if not isinstance(file_data, dict) or "mime_type" not in file_data:
            return content

        original_mime = file_data["mime_type"]
        file_name = self._resolve_file_name(file_data)

        # 检查是否需要修正
        if not MimeUtils.should_correct_mime_type(original_mime, file_name):
            return content

        # 获取修正后的 MIME
        corrected_mime = await self._determine_correct_mime(file_name, request_id)
        
        # 如果无法获取或无需修改，返回原内容
        if not corrected_mime or corrected_mime == original_mime:
            return content

        Logger.info(f"生成阶段 MIME 类型修正: {original_mime} -> {corrected_mime}",
                    file_name=file_name, request_id=request_id)

        # 构建新的 content 对象
        new_content = content.copy()
        new_file_data = file_data.copy()
        new_file_data["mime_type"] = corrected_mime
        
        # 保持原有 key 风格
        if "fileData" in content:
            new_content["fileData"] = new_file_data
        else:
            new_content["file_data"] = new_file_data
            
        return new_content

    def _resolve_file_name(self, file_data: Dict[str, Any]) -> str:
        """从 fileData 中解析文件名"""
        file_name = file_data.get("fileName", "")
        if file_name:
            return file_name

        # 尝试从 fileUri 推断
        file_uri = file_data.get("fileUri", "")
        if file_uri:
            sha256_from_uri = file_uri.split('/')[-1] if '/' in file_uri else file_uri
            cached_entry = file_manager.get_metadata_entry(sha256_from_uri)
            if cached_entry and cached_entry.original_filename:
                return cached_entry.original_filename
        
        return ""

    async def _determine_correct_mime(self, file_name: str, request_id: str) -> Optional[str]:
        """确定文件的正确 MIME 类型"""
        # 1. 尝试从缓存获取
        mime_from_cache = await self._get_file_mime_type_from_cache(file_name, request_id)
        if mime_from_cache:
            return mime_from_cache
            
        # 2. 尝试推断
        return MimeUtils.infer_mime_type(file_name)

    async def _get_file_mime_type_from_cache(self, file_name: str, request_id: str) -> Optional[str]:
        """从文件管理器缓存中查找 MIME 类型"""
        try:
            sha256 = file_manager.get_sha256_by_filename(file_name)
            if not sha256:
                return None

            entry = file_manager.get_metadata_entry(sha256)
            if not entry:
                return None

            # 检查是否有完整的文件数据包含 MIME 类型
            for data in entry.replication_map.values():
                if data.get("name") == file_name and "mimeType" in data:
                    return data.get("mimeType")
            
            return None
        except Exception as e:
            Logger.warning(f"获取文件 MIME 类型失败: {e}", file_name=file_name, request_id=request_id)
            return None

    def find_original_file_name(self, payload: Any, request_id: str) -> Optional[str]:
        """Best-effort extraction of the original file name for logging and retries."""
        original_file_name: Optional[str] = None
        try:
            if isinstance(payload, dict):
                # 策略 1: 检查 payload.contents
                if "payload" in payload:
                    original_file_name = self._search_in_contents(payload["payload"], request_id)
                
                # 策略 2: 直接检查根 payload (如果是 flat 结构)
                if not original_file_name:
                    original_file_name = self._search_in_flat_payload(payload, request_id)

            Logger.info(f"[调试] 解析出的文件名: {original_file_name}", request_id=request_id)
        except Exception as exc:
            Logger.warning(f"解析文件名时出错: {exc}", request_id=request_id)
        return original_file_name

    def _search_in_contents(self, payload_contents: Dict[str, Any], request_id: str) -> Optional[str]:
        if not isinstance(payload_contents, dict):
            return None
            
        contents = payload_contents.get("contents", [])
        if not isinstance(contents, list) or not contents:
            return None

        content = contents[0]
        if not isinstance(content, dict):
            return None

        # 检查 fileData.fileName
        for path in ["fileData.fileName", "file_data.fileName"]:
            file_name = self._get_nested_value(content, path)
            if file_name:
                Logger.info(f"[调试] 找到文件名: {file_name} (路径: {path})", request_id=request_id)
                return file_name

        # 检查 parts
        parts = content.get("parts", [])
        return self._extract_file_name_from_parts(parts, request_id)

    def _search_in_flat_payload(self, payload: Dict[str, Any], request_id: str) -> Optional[str]:
        for path in ["fileData.fileName", "file_data.fileName", "fileName"]:
            file_name = self._get_nested_value(payload, path)
            if file_name:
                Logger.info(f"[调试] 找到文件名: {file_name} (路径: {path})", request_id=request_id)
                return file_name
        return None

    def _get_nested_value(self, data: dict, path: str) -> Optional[Any]:
        """从嵌套字典中获取值"""
        keys = path.split('.')
        current = data
        try:
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
            return current
        except (KeyError, TypeError):
            return None

    def _extract_file_name_from_parts(self, parts: list, request_id: str) -> Optional[str]:
        """Extract file name from parts array in nested payload."""
        for part in parts:
            if not isinstance(part, dict):
                continue
            file_data = part.get("fileData") or part.get("file_data")
            if not isinstance(file_data, dict):
                continue
            file_uri = file_data.get("fileUri") or file_data.get("file_uri")
            if not isinstance(file_uri, str):
                continue
            
            # 尝试通过 uri 反查缓存
            full_sha256 = file_manager.get_sha256_by_filename(file_uri)
            if not full_sha256:
                continue
            cached_entry = file_manager.get_metadata_entry(full_sha256)
            if cached_entry and cached_entry.original_filename:
                Logger.info(
                    f"[调试] 从 fileUri '{file_uri}' 找到文件名: {cached_entry.original_filename}",
                    request_id=request_id,
                )
                return cached_entry.original_filename
        return None

    def update_file_uri_in_payload(
        self,
        payload: Any,
        old_file_name: str,
        new_file_uri: str,
        request_id: str,
    ) -> Any:
        """递归更新 payload 中匹配的文件 URI

        用于在文件重建后更新 payload 中的文件引用。

        Args:
            payload: 原始 payload
            old_file_name: 旧的文件名（用于匹配）
            new_file_uri: 新的文件 URI
            request_id: 请求 ID（用于日志）

        Returns:
            更新后的 payload
        """
        if not isinstance(payload, dict):
            return payload

        updated = False

        def update_file_data(file_data: dict) -> bool:
            """更新单个 fileData 对象，返回是否更新"""
            nonlocal updated
            for uri_key in ("fileUri", "file_uri"):
                current_uri = file_data.get(uri_key)
                if current_uri and self._uri_matches(current_uri, old_file_name):
                    file_data[uri_key] = new_file_uri
                    # 清理可能过时的 fileName
                    file_data.pop("fileName", None)
                    file_data.pop("file_name", None)
                    updated = True
                    Logger.debug(
                        "更新 payload 中的文件 URI",
                        request_id=request_id,
                        old_uri=current_uri,
                        new_uri=new_file_uri,
                    )
                    return True
            return False

        def traverse(obj: Any):
            """递归遍历并更新"""
            if isinstance(obj, dict):
                # 检查当前字典是否是 fileData
                if "fileUri" in obj or "file_uri" in obj:
                    update_file_data(obj)
                # 递归遍历子项
                for value in obj.values():
                    traverse(value)
            elif isinstance(obj, list):
                for item in obj:
                    traverse(item)

        # 深拷贝 payload 以避免修改原始对象
        import copy
        result = copy.deepcopy(payload)
        traverse(result)

        if updated:
            Logger.info(
                "已更新 payload 中的文件引用",
                request_id=request_id,
                old_file_name=old_file_name,
                new_uri=new_file_uri,
            )

        return result

    def _uri_matches(self, uri: str, file_name: str) -> bool:
        """检查 URI 是否匹配文件名"""
        if not uri or not file_name:
            return False
        # 直接匹配
        if uri == file_name:
            return True
        # 检查是否以文件名结尾（考虑 files/ 前缀）
        if uri.endswith(f"/{file_name}") or uri.endswith(f"files/{file_name}"):
            return True
        # 通过 sha256 匹配
        sha256_from_uri = file_manager.get_sha256_by_filename(uri)
        sha256_from_name = file_manager.get_sha256_by_filename(file_name)
        if sha256_from_uri and sha256_from_name and sha256_from_uri == sha256_from_name:
            return True
        return False


payload_service = PayloadService()