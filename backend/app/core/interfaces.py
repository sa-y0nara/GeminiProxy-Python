from typing import Optional, Protocol, Any, Dict, List
from fastapi import Request

class IConnectionManager(Protocol):
    """
    Interface for ConnectionManager to decouple FileSyncService.
    """
    def get_all_clients(self) -> List[str]: ...
    
    def get_next_client(self) -> str: ...
    
    async def send_command_to_client(
        self,
        *,
        client_id: str,
        command_type: str,
        payload: Any,
        request_id: Optional[str] = None,
        is_streaming: bool = False,
    ) -> Any: ...

    async def _direct_proxy_request(
        self,
        command_type: str,
        payload: Any,
        request_id: str,
        client_id: str,
        request: Optional[Request] = None,
        is_streaming: bool = False,
    ) -> Any: ...
    
    # Optional: for binary commands if needed
    async def send_binary_command(
        self,
        *,
        client_id: str,
        command: dict[str, Any],
        binary_body: bytes,
    ) -> Any: ...
