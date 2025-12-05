"""
Payload processing service.
Responsible for inspecting, cleaning, and preparing payloads before they are sent to the frontend.
"""
from typing import Any, Optional
from app.core.log_utils import Logger
from app.core.mime_utils import MimeUtils
from app.core.file_manager import file_manager

class PayloadService:
    async def _get_file_mime_type(self, file_name: str, request_id: str) -> Optional[str]:
        """
        获取文件的正确 MIME 类型

        Args:
            file_name: 文件名
            request_id: 请求ID

        Returns:
            正确的 MIME 类型，如果无法获取则返回 None
        """
        try:
            # 查找文件的 SHA256
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

            # 如果没有找到，尝试根据文件名推断
            return MimeUtils.infer_mime_type(file_name)

        except Exception as e:
            Logger.warning(f"获取文件 MIME 类型失败: {e}", file_name=file_name, request_id=request_id)
            return None

    async def _fix_payload_mime_types(self, payload: dict, request_id: str) -> dict:
        """
        修正 payload 中的 MIME 类型

        Args:
            payload: 原始 payload
            request_id: 请求ID

        Returns:
            修正后的 payload
        """
        if not isinstance(payload, dict) or "payload" not in payload:
            return payload

        contents = payload["payload"].get("contents", [])
        if not isinstance(contents, list):
            return payload

        fixed_contents = []
        for content in contents:
            if not isinstance(content, dict):
                fixed_contents.append(content)
                continue

            file_data = content.get("file_data") or content.get("fileData", {})
            if isinstance(file_data, dict) and "mime_type" in file_data:
                original_mime = file_data["mime_type"]
                file_name = file_data.get("fileName", "")

                # 如果没有 fileName，尝试从 fileUri 推断
                if not file_name:
                    file_uri = file_data.get("fileUri", "")
                    if file_uri:
                        sha256_from_uri = file_uri.split('/')[-1] if '/' in file_uri else file_uri
                        # 从文件管理器中查找原始文件名
                        cached_entry = file_manager.get_metadata_entry(sha256_from_uri)
                        if cached_entry and cached_entry.original_filename:
                            file_name = cached_entry.original_filename

                # 检查是否需要修正 MIME 类型
                if MimeUtils.should_correct_mime_type(original_mime, file_name):
                    # 尝试获取正确的 MIME 类型
                    corrected_mime = await self._get_file_mime_type(file_name, request_id)
                    if not corrected_mime:
                        # 如果无法获取，使用智能推断
                        corrected_mime = MimeUtils.infer_mime_type(file_name)

                    if corrected_mime != original_mime:
                        Logger.info(f"生成阶段 MIME 类型修正: {original_mime} -> {corrected_mime}",
                                  file_name=file_name, request_id=request_id)
                        # 创建副本以避免修改原始数据
                        new_content = content.copy()
                        new_file_data = file_data.copy()
                        new_file_data["mime_type"] = corrected_mime
                        if "fileData" in content:
                            new_content["fileData"] = new_file_data
                        else:
                            new_content["file_data"] = new_file_data
                        fixed_contents.append(new_content)
                        continue

            fixed_contents.append(content)

        # 始终返回重构后的 payload（即使长度相同，内容也可能被修改）
        new_payload = payload.copy()
        new_payload["payload"] = payload["payload"].copy()
        new_payload["payload"]["contents"] = fixed_contents
        return new_payload

    async def prepare_payload(self, command_type: str, payload: Any, request_id: str) -> Any:
        """Apply command-specific preprocessing to payload."""
        if command_type in {"generateContent", "streamGenerateContent"}:
            return await self._fix_payload_mime_types(payload, request_id)
        return payload

    def _get_nested_value(self, data: dict, path: str) -> Optional[Any]:
        """
        从嵌套字典中获取值

        Args:
            data: 要搜索的字典
            path: 点分隔的路径，如 "fileData.fileName"

        Returns:
            找到的值，如果未找到则返回 None
        """
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

    def find_original_file_name(self, payload: Any, request_id: str) -> Optional[str]:
        """Best-effort extraction of the original file name for logging and retries."""
        original_file_name: Optional[str] = None
        try:
            if isinstance(payload, dict):
                if "payload" in payload:
                    payload_contents = payload.get("payload", {})
                    if isinstance(payload_contents, dict):
                        contents = payload_contents.get("contents", [])
                        if isinstance(contents, list) and contents:
                            content = contents[0]
                            if isinstance(content, dict):
                                for path in ["fileData.fileName", "file_data.fileName"]:
                                    file_name = self._get_nested_value(content, path)
                                    if file_name:
                                        original_file_name = file_name
                                        Logger.info(
                                            f"[调试] 找到文件名: {file_name} (路径: {path})",
                                            request_id=request_id,
                                        )
                                        break

                                if not original_file_name:
                                    parts = content.get("parts", [])
                                    original_file_name = self._extract_file_name_from_parts(parts, request_id)
                                    if not original_file_name:
                                        Logger.warning(
                                            "[调试] 无法从 payload 中提取有效的文件名或 fileUri",
                                            request_id=request_id,
                                        )
                else:
                    for path in ["fileData.fileName", "file_data.fileName", "fileName"]:
                        file_name = self._get_nested_value(payload, path)
                        if file_name:
                            original_file_name = file_name
                            Logger.info(
                                f"[调试] 找到文件名: {file_name} (路径: {path})",
                                request_id=request_id,
                            )
                            break
            Logger.info(f"[调试] 解析出的文件名: {original_file_name}", request_id=request_id)
        except Exception as exc:
            Logger.warning(f"解析文件名时出错: {exc}", request_id=request_id)
        return original_file_name

payload_service = PayloadService()
