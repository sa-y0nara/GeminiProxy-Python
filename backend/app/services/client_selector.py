"""客户端选择模块

负责选择最佳客户端进行文件操作。
"""

import random
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from fastapi import HTTPException, status

from app.core.file_manager import FileCacheEntry
from app.services.file_verification_service import file_verification_service

if TYPE_CHECKING:
    from app.core.interfaces import IConnectionManager


class ClientSelector:
    """客户端选择器
    
    职责：
    1. 扫描所有客户端
    2. 选择缺失文件最少的客户端
    3. 支持首选客户端优先
    """

    def select_best_client(
        self,
        manager: "IConnectionManager",
        required_entries: Dict[str, FileCacheEntry],
        preferred_client: str,
    ) -> Tuple[str, List[str], List[str]]:
        """扫描所有客户端，选择缺失文件最少的客户端
        
        Returns:
            (selected_client_id, missing_for_selected, missing_for_preferred)
        """
        active_clients = manager.get_all_clients()
        if not active_clients:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No frontend clients connected",
            )

        best_clients: List[str] = []
        best_missing_count: Optional[int] = None
        missing_map: Dict[str, List[str]] = {}

        for client_id in active_clients:
            missing = file_verification_service.collect_missing_for_client(
                required_entries, client_id
            )
            missing_map[client_id] = missing
            missing_count = len(missing)
            if best_missing_count is None or missing_count < best_missing_count:
                best_missing_count = missing_count
                best_clients = [client_id]
            elif missing_count == best_missing_count:
                best_clients.append(client_id)

        if not best_clients:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No frontend clients available for scheduling",
            )

        if preferred_client in best_clients:
            selected = preferred_client
        else:
            selected = random.choice(best_clients)

        return selected, missing_map.get(selected, []), missing_map.get(preferred_client, [])


# 全局单例
client_selector = ClientSelector()
