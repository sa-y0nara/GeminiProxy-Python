"""文件引用解析模块

负责解析 payload 中的文件引用并重写。
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from fastapi import HTTPException, status

from app.core.file_manager import FileCacheEntry, file_manager
from app.core.log_utils import Logger

if TYPE_CHECKING:
    pass


@dataclass
class FileReference:
    """指向 payload 中一个 fileData 节点的引用"""
    sha256: str
    entry: FileCacheEntry
    file_dict: dict
    alias: Optional[str] = None


class FileReferenceResolver:
    """文件引用解析器
    
    职责：
    1. 遍历 payload 收集文件引用
    2. 解析文件 sha256
    3. 重写文件引用
    """

    def resolve_sha_from_file_dict(
        self, file_dict: dict
    ) -> Tuple[Optional[str], Optional[str]]:
        """从 fileData 字典解析 sha256"""
        for key in ("fileUri", "file_uri", "fileName", "file_name", "fileId", "file_id"):
            value = file_dict.get(key)
            if not value or not isinstance(value, str):
                continue
            sha256 = file_manager.get_sha256_by_filename(value)
            if sha256:
                return sha256, value
        return None, None

    def extract_file_references(self, payload: Any, request_id: str) -> List[FileReference]:
        """遍历 payload，收集所有 fileData 节点"""
        references: List[FileReference] = []

        def _walk(node: Any):
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in ("fileData", "file_data") and isinstance(value, dict):
                        sha256, alias = self.resolve_sha_from_file_dict(value)
                        if not sha256:
                            Logger.warning(
                                "fileData 无法解析 sha256",
                                request_id=request_id,
                                file_data=value,
                            )
                            continue
                        entry = file_manager.get_metadata_entry(sha256)
                        if not entry:
                            raise HTTPException(
                                status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"File {value.get('fileName') or value.get('fileUri')} not found in cache.",
                            )
                        references.append(
                            FileReference(sha256=sha256, entry=entry, file_dict=value, alias=alias)
                        )
                    else:
                        _walk(value)
            elif isinstance(node, list):
                for item in node:
                    _walk(item)

        _walk(payload)
        return references

    def rewrite_file_references(
        self,
        file_refs: List[FileReference],
        client_id: str,
        request_id: str,
        alias_map: Optional[Dict[str, str]] = None,
    ) -> None:
        """将 payload 中的 fileData 替换为客户端对应的 fileUri"""
        for ref in file_refs:
            replication_data = ref.entry.replication_map.get(client_id)
            if not replication_data or replication_data.get("status") != "synced":
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Client {client_id} does not have required file {ref.sha256[:8]}",
                )

            final_file_name = replication_data.get("name")
            final_uri = replication_data.get("uri") or final_file_name
            if not final_uri:
                Logger.warning(
                    "复制数据缺少可用的 fileUri", client_id=client_id, sha256=ref.sha256
                )
                continue

            ref.file_dict["fileUri"] = final_uri
            ref.file_dict.pop("fileName", None)
            ref.file_dict.pop("file_name", None)
            ref.file_dict.pop("file_uri", None)

            if alias_map is not None:
                if final_file_name:
                    alias_map[final_file_name] = ref.sha256
                if final_uri:
                    alias_map[final_uri] = ref.sha256
                if ref.alias:
                    alias_map.setdefault(ref.alias, ref.sha256)

            Logger.debug(
                "已改写 fileData 引用",
                request_id=request_id,
                client_id=client_id,
                sha256=ref.sha256,
                file_uri=final_uri,
            )


# 全局单例
file_reference_resolver = FileReferenceResolver()
