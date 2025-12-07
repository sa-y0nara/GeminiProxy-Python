import { useState, useCallback, useRef, useEffect } from 'react';
import { websocketService } from '../websocketService';

interface ReconnectionInfo {
    attempt: number;
    max: number;
}

export const useWebSocket = (addLog: (msg: string) => void) => {
    const [isConnected, setIsConnected] = useState(false);
    const [isConnecting, setIsConnecting] = useState(false);
    const [reconnectionInfo, setReconnectionInfo] = useState<ReconnectionInfo | null>(null);
    const [reconnectFailed, setReconnectFailed] = useState(false);

    const connect = useCallback((url: string, id: string) => {
        if (!url || !id) {
            addLog("Connection settings are missing. Cannot connect.");
            return;
        }
        addLog(`Initializing connection for Client ID: ${id}`);
        setIsConnecting(true);
        setReconnectFailed(false);
        setReconnectionInfo(null);

        websocketService.connect(url, id, {
            onOpen: () => {
                setIsConnected(true);
                setIsConnecting(false);
                setReconnectionInfo(null);
                setReconnectFailed(false);
            },
            onClose: () => {
                setIsConnected(false);
                setIsConnecting(false);
            },
            onError: () => { /* UI update handled by onClose */ },
            onLog: addLog,
            onReconnecting: (attempt, max) => {
                setIsConnected(false);
                setIsConnecting(false);
                setReconnectionInfo({ attempt, max });
                setReconnectFailed(false);
            },
            onReconnectSuccess: () => addLog("Successfully reconnected to the backend."),
            onReconnectFailed: () => {
                setIsConnecting(false);
                setReconnectionInfo(null);
                setReconnectFailed(true);
            },
        });
    }, [addLog]);

    const disconnect = useCallback(() => {
        websocketService.disconnect();
        setIsConnecting(false);
        setReconnectionInfo(null);
    }, []);

    return {
        isConnected,
        isConnecting,
        reconnectionInfo,
        reconnectFailed,
        connect,
        disconnect
    };
};
