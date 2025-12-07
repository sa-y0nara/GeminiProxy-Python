from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple, List

from app.core.config import settings
from app.core.log_utils import Logger

@dataclass
class UploadSession:
    metadata: Dict[str, Any]
    client_id: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

class SessionManager:
    """
    负责管理上传会话。
    """
    def __init__(self):
        self.upload_sessions: Dict[str, UploadSession] = {}
        Logger.event("INIT", "SessionManager 初始化")

    def start_session(self, session_id: str, client_id: str, metadata: Optional[Dict[str, Any]] = None) -> UploadSession:
        session = UploadSession(metadata=dict(metadata or {}), client_id=client_id)
        self.upload_sessions[session_id] = session
        return session

    def get_session(self, session_id: str) -> Optional[UploadSession]:
        session_data = self.upload_sessions.get(session_id)
        return self._normalize_session(session_id, session_data)

    def get_metadata(self, session_id: str) -> Dict[str, Any]:
        session = self.get_session(session_id)
        return dict(session.metadata) if session else {}
    
    def remove_session(self, session_id: str):
        self.upload_sessions.pop(session_id, None)

    def _extract_metadata_and_timestamp(self, session_data: Any) -> Tuple[Dict[str, Any], datetime]:
        metadata: Dict[str, Any] = {}
        created_at = datetime.now(timezone.utc)

        if isinstance(session_data, dict):
            metadata = dict(session_data.get("metadata") or session_data or {})
            raw_created = session_data.get("created_at")
        elif isinstance(session_data, tuple) and len(session_data) == 2:
            metadata = dict(session_data[0] or {})
            raw_created = session_data[1]
        else:
            raw_created = None

        if isinstance(raw_created, datetime):
            created_at = raw_created if raw_created.tzinfo else raw_created.replace(tzinfo=timezone.utc)

        return metadata, created_at

    def _normalize_session(self, session_id: str, session_data: Any, client_id: Optional[str] = None) -> Optional[UploadSession]:
        if isinstance(session_data, UploadSession):
            if client_id and session_data.client_id != client_id:
                Logger.warning("会话客户端不匹配，拒绝访问", session_id=session_id)
                return None
            return session_data
        if session_data is None:
            return None

        metadata, created_at = self._extract_metadata_and_timestamp(session_data)
        bound_client_id = client_id or "unknown"
        session = UploadSession(metadata=metadata, client_id=bound_client_id, created_at=created_at)
        self.upload_sessions[session_id] = session
        return session

    def cleanup_expired_sessions(self, now: datetime) -> int:
        expired_sessions = []
        session_timeout = timedelta(hours=settings.SESSION_EXPIRATION_HOURS)
        
        for session_id, session_data in list(self.upload_sessions.items()):
            session = session_data if isinstance(session_data, UploadSession) else self._normalize_session(session_id, session_data)
            if not session or (now - session.created_at) > session_timeout:
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            self.upload_sessions.pop(session_id, None)
        
        if expired_sessions:
            Logger.info(f"清理 {len(expired_sessions)} 个过期的上传会话...")
        
        return len(expired_sessions)
