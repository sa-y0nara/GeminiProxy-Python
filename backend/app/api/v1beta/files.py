from __future__ import annotations

"""文件 API 路由 (方案 B)

负责处理文件的上传、下载、查询和删除。
采用后端缓存策略。
"""

import uuid
from typing import Any, Generator, Optional
from app.core import manager
from app.core.config import settings
from app.core.file_manager import file_manager
from app.core.log_utils import Logger
from app.core.token_service import token_service
from app.core.utils import parse_int_safe
from app.schemas.gemini_files import (
    File,
    InitialUploadRequest,
    ListFilesPayload,
    ListFilesResponse,
)
from fastapi import APIRouter, Body, Depends, HTTPException, Path as FastAPIPath, Query, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse

# 引入 UploadService
from app.services.upload_service import upload_service
from app.services.response_builder import response_builder

# ============================================================================
# 路由器配置
# ============================================================================

router = APIRouter(tags=["Files"])
upload_router = APIRouter(tags=["Files"])

# --------------------------------------------------------------------------
# 错误消息常量
# --------------------------------------------------------------------------

_ERR_FILE_NOT_FOUND = "File not found."
_ERR_FILE_NOT_AVAILABLE = "File not available."


# --- Helper functions ---


def _build_download_response(
    entry,
    *,
    download_name: Optional[str] = None,
    include_filename: bool = True,
) -> FileResponse:
    """根据缓存条目生成 FileResponse，保持下载行为一致。"""
    resolved_name = download_name or entry.original_filename or entry.local_path.name
    response_kwargs = {"media_type": entry.mime_type}
    if include_filename:
        response_kwargs["filename"] = resolved_name
    return FileResponse(entry.local_path, **response_kwargs)


def _resolve_cached_entry_or_404(
    sha256: Optional[str],
    *,
    missing_identifier_detail: str = _ERR_FILE_NOT_FOUND,
    missing_entry_detail: str = _ERR_FILE_NOT_AVAILABLE,
):
    if not sha256:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=missing_identifier_detail)
    entry = file_manager.get_metadata_entry(sha256)
    if not entry or not entry.local_path.exists():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=missing_entry_detail)
    return entry


def _ensure_not_deleted(name: str, sha256: Optional[str]):
    """确保请求的文件未被标记为已删除（代理到 file_manager）"""
    try:
        file_manager.ensure_not_deleted(name, sha256)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=_ERR_FILE_NOT_FOUND)



async def _prepare_remote_file(
    *,
    request: Request,
    file_name: str,
    request_id: str,
    reason: str,
    deleted_log_message: Optional[str] = None,
    preferred_client_id: Optional[str] = None,
    allow_deleted: bool = False,
    require: bool = False,
) -> Optional[File]:
    """
    获取远端文件并转为响应对象。
    当 require=True 时，缺失会直接抛出 404，避免额外的样板。
    """
    remote_file, remote_sha, was_deleted = await upload_service.sync_remote_file_to_cache(
        request=request,
        file_name=file_name,
        request_id=request_id,
        reason=reason,
        preferred_client_id=preferred_client_id,
        allow_deleted=allow_deleted,
    )
    if remote_file:
        return response_builder.prepare_file_for_response(remote_file, request)

    if was_deleted and deleted_log_message:
        Logger.info(
            deleted_log_message,
            request_id=request_id,
            file_name=file_name,
            sha256=(remote_sha[:8] if remote_sha else None),
        )

    if require or was_deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=_ERR_FILE_NOT_FOUND)

    return None

# ============================================================================
# 文件上传
# ============================================================================

@upload_router.post("/files", name="files.create")
async def create_file(
    request: Request,
    body: InitialUploadRequest = Body(...),
):
    """
    初始化一个模拟的可续传上传会话，绑定到客户端以防止会话劫持。
    """
    return await upload_service.initiate_upload_session(request, body)


@router.post(
    "/files",
    name="files.metadata_create_only",
)
async def create_file_metadata_only(
    request: Request,
    body: InitialUploadRequest = Body(...),
):
    """处理 metadata-only 文件创建请求"""
    return await upload_service.create_metadata_only_file(request, body)


@router.post(
    "/files/upload/{session_id}",
    name="files.resumable_upload",
)
async def resumable_upload(
    request: Request,
    session_id: str = FastAPIPath(...),
):
    """
    接收文件内容，并触发完整的方案 B 上传/同步逻辑。
    支持自动重试机制以处理临时连接问题。
    """
    return await upload_service.handle_resumable_upload(request, session_id)


# ============================================================================
# 文件下载端点
# ============================================================================


@router.get(
    "/files/{name:path}:download",
    name="files.download",
)
async def download_file(name: str):
    """对外提供的文件内容下载端点。"""
    sha256 = file_manager.get_sha256_by_filename(name)
    entry = _resolve_cached_entry_or_404(
        sha256,
        missing_identifier_detail=_ERR_FILE_NOT_FOUND,
        missing_entry_detail=_ERR_FILE_NOT_AVAILABLE,
    )

    Logger.event("PUBLIC_DOWNLOAD", "用户下载文件", sha256=sha256[:8] if sha256 else None)
    download_name = entry.original_filename or name.split("/")[-1]
    return _build_download_response(entry, download_name=download_name)


@router.get(
    "/files/internal/{sha256}/{token}:download",
    include_in_schema=False,
)
async def internal_download_file(sha256: str, token: str):
    """供 WebSocket 客户端下载文件内容以进行上传。
    
    使用 HMAC-SHA256 签名令牌进行安全验证。
    """
    # 安全令牌验证
    if not token_service.validate_download_token(sha256, token):
        Logger.warning("内部下载令牌验证失败", sha256=sha256[:8] if sha256 else None)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid or expired download token.")
    
    entry = _resolve_cached_entry_or_404(
        sha256,
        missing_identifier_detail="File not found in cache.",
        missing_entry_detail="File not found in cache.",
    )

    Logger.event("INTERNAL_DOWNLOAD", "客户端正在下载文件", sha256=sha256[:8])
    return _build_download_response(entry, include_filename=False)


# ============================================================================
# 文件管理端点 (重构)
# ============================================================================


def _iter_synced_files() -> Generator[File, None, None]:
    """迭代所有已同步的文件，按创建时间降序
    
    提取为模块级函数以避免每次请求重新定义，提高性能和可测试性。
    """
    sorted_entries = sorted(
        file_manager.metadata_store.values(),
        key=lambda e: e.created_at,
        reverse=True,
    )
    for entry in sorted_entries:
        replica = next(
            (
                data
                for data in entry.replication_map.values()
                if data.get("status") == "synced" and data.get("name")
            ),
            None,
        )
        if not replica:
            continue
        try:
            yield File.model_validate(replica)
        except Exception as exc:
            Logger.warning(
                "复制数据不完整，跳过",
                sha256=(entry.sha256[:8] if entry.sha256 else None),
                error=str(exc),
            )


@router.get(
    "/files",
    response_model=ListFilesResponse,
    name="files.list",
)
async def list_files(request: Request, params: ListFilesPayload = Depends()):
    """从后端缓存中列出所有文件。"""
    all_valid_files = list(_iter_synced_files())

    # 实现分页
    start_index = parse_int_safe(params.page_token, default=0, label="page_token")
    if start_index < 0:
        start_index = 0

    end_index = start_index + params.page_size
    paginated_files = all_valid_files[start_index:end_index]
    prepared_files = [response_builder.prepare_file_for_response(file_obj, request) for file_obj in paginated_files]

    next_page_token = str(end_index) if end_index < len(all_valid_files) else None

    return ListFilesResponse(files=prepared_files, next_page_token=next_page_token)


@router.get(
    "/files/{name:path}",
    response_model=File,
    name="files.get",
)
async def get_file(request: Request, name: str, verify_remote: bool = Query(False, alias="verifyRemote")):
    """从后端缓存中获取指定文件的元数据，可选远程校验。"""
    request_id = str(uuid.uuid4())
    sha256 = file_manager.get_sha256_by_filename(name)
    entry = file_manager.get_metadata_entry(sha256) if sha256 else None

    if not sha256 or not entry:
        _ensure_not_deleted(name, sha256)
        return await _prepare_remote_file(
            request=request,
            file_name=name,
            request_id=request_id,
            reason="get-miss",
            deleted_log_message="请求访问已删除文件，忽略远程返回",
            require=True,
        )

    _ensure_not_deleted(name, sha256)

    if verify_remote:
        remote_file = await _prepare_remote_file(
            request=request,
            file_name=name,
            request_id=request_id,
            reason="get",
            deleted_log_message="请求访问已删除文件，忽略远程返回",
        )
        if remote_file:
            return remote_file
        Logger.warning("远程校验失败，返回本地缓存", request_id=request_id, file_name=name)

    for data in entry.replication_map.values():
        if data.get("name") == name and data.get("status") == "synced":
            try:
                return response_builder.prepare_file_for_response(data, request)
            except Exception as exc:
                Logger.warning(f"文件数据不完整，无法返回: {exc}")
                continue

    return await _prepare_remote_file(
        request=request,
        file_name=name,
        request_id=request_id,
        reason="get-refresh",
        deleted_log_message="刷新请求命中已删除文件，忽略远程返回",
        require=True,
    )


@router.delete(
    "/files/{name:path}",
    status_code=status.HTTP_200_OK,
    name="files.delete",
)
async def delete_file(request: Request, name: str):
    """
    删除文件缓存及其在所有 Gemini 客户端上的副本。
    """
    request_id = str(uuid.uuid4())
    Logger.api_request(request_id, f"删除文件请求 | {name}")

    sha256 = file_manager.get_sha256_by_filename(name)
    if not sha256:
        # 幂等删除：文件不存在时返回成功，符合 RESTful DELETE 语义
        Logger.api_response(request_id, "文件在本地未找到，视为成功")
        return JSONResponse(status_code=status.HTTP_200_OK, content={})

    entry = file_manager.get_metadata_entry(sha256)
    alias_candidates = {name}
    if entry:
        for data in entry.replication_map.values():
            if data.get("name"):
                alias_candidates.add(data["name"])
            if data.get("uri"):
                alias_candidates.add(data["uri"])
    file_manager.mark_deleted(sha256, alias_candidates)
    if not entry:
        Logger.api_response(request_id, "文件在本地未找到，视为成功")
        return JSONResponse(status_code=status.HTTP_200_OK, content={})

    # 派发后台任务去删除所有远程副本
    for client_id, data in entry.replication_map.items():
        if data.get("status") == "synced" and "name" in data:
            remote_name = data["name"]
            # 使用 manager 创建一个独立的后台删除任务
            Logger.info(f"派发远程文件删除任务", client_id=client_id, file_name=remote_name)
            manager.trigger_delete_task(client_id, remote_name)

    # 立即删除本地缓存和元数据
    file_manager.delete_entry(sha256)

    Logger.api_response(request_id, "本地文件已删除，远程删除任务已派发")
    return JSONResponse(status_code=status.HTTP_200_OK, content={})