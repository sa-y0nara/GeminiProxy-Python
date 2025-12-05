
import { ApiError } from './errors/ApiError';
import { geminiExecutor } from './geminiExecutor/geminiExecutor';
import type { Command, ErrorPayload, ResponsePayload } from './types/types';

export interface ConnectionCallbacks {
  onOpen: () => void;
  onClose: () => void;
  onError: (event: Event) => void;
  onLog: (message: string) => void;
  onReconnecting: (attempt: number, maxAttempts: number) => void;
  onReconnectSuccess: () => void;
  onReconnectFailed: () => void;
}

let ws: WebSocket | null = null;
let websocketUrl = '';
let clientId = '';
let callbacks: ConnectionCallbacks | null = null;
let reconnectAttempts = 0;
const maxReconnectAttempts = 5;
const reconnectInterval = 3000;
let reconnectTimer: number | null = null;
let isExplicitlyClosed = false;

// Helper to safe-stringify payload for logging (truncate long strings)
const getSafePayloadLog = (obj: any) => {
  if (!obj) return 'null';
  try {
    return JSON.stringify(obj, (key, value) => {
      if (typeof value === 'string' && value.length > 200) {
        return value.substring(0, 200) + '... [TRUNCATED]';
      }
      if (key === 'data_bytes' || value instanceof Uint8Array) {
        return `[Binary Data: ${value.length || value.byteLength} bytes]`;
      }
      return value;
    }, 2);
  } catch (e) {
    return '[Circular or Invalid JSON]';
  }
};

const createErrorResponse = (error: unknown, command: Command | null): ErrorPayload => {
  const responseId = command?.id || 'unknown';
  let response: ErrorPayload;
  let logMessage = '';

  if (error instanceof ApiError) {
    // detailed API error
    const apiDetails = error.body ? JSON.stringify(error.body, null, 2) : error.message;
    logMessage = `API Error (${error.status}): ${apiDetails}`;
    
    response = {
      id: responseId,
      status: {
        error: true,
        code: error.status,
        errorPayload: error.body,
      },
    };
  } else {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logMessage = `Error processing command: ${errorMessage}`;
    
    response = {
      id: responseId,
      status: {
        error: true,
        code: 500,
      },
    };
  }

  // Append the request payload to the log for debugging
  if (command) {
    logMessage += `\nFAILED REQUEST PAYLOAD:\n${getSafePayloadLog(command.payload)}`;
  }

  callbacks?.onLog(logMessage);
  return response;
};

      ws.onmessage = async (event) => {
    try {
      const command = parseMessage(event.data);
      if (!command) return; // Handled internally (e.g. cancel_task)

      callbacks?.onLog(`Received command: ${command.type} (ID: ${command.id})`);
      callbacks?.onLog(`Params: ${getSafePayloadLog(command.payload)}`);

      // Prepare backend URL
      let backendUrl = '';
      try {
        const urlObj = new URL(websocketUrl);
        const protocol = urlObj.protocol === 'wss:' ? 'https:' : 'http:';
        backendUrl = `${protocol}//${urlObj.host}`;
      } catch (e) {
        console.error("Could not parse WebSocket URL for backend origin", e);
      }

      // Execute logic
      if (command.type === 'streamGenerateContent') {
        await geminiExecutor.execute(command, (payload) => sendStreamResponse(command!, payload), backendUrl);
        callbacks?.onLog(`Finished streaming for command ID: ${command.id}`);
      } else {
        const result = await geminiExecutor.execute(command, () => {}, backendUrl);
        callbacks?.onLog(`Result: ${getSafePayloadLog(result)}`);
        
        const response: ResponsePayload = { 
          id: command.id, 
          payload: result, 
          status: { error: false, code: 200 } 
        };
        sendMessageWithRetry(response, `success response for ID ${command.id}`);
      }

    } catch (error) {
      // If parsing failed, command might be null.
      // In that case, we can't reply with an ID, so we just log.
      // If parsing succeeded but execution failed, we reply with error.
      // Note: parseMessage throws if it fails, so command is null there.
      // We need to handle the case where we have a command ID to reply to.
      
      // Since we refactored, we need to be careful. 
      // Let's catch parsing errors inside parseMessage or here.
      // The original code created an error response.
      
      // Ideally, we need the ID to send the error back.
      // If parsing failed completely, we can't send an error back with an ID.
      // But the original code had "command | null" in createErrorResponse.
      
      // Let's refine this structure in the helper functions below.
      // For now, to match the block structure:
      
       // NOTE: The implementation below assumes 'command' is available in scope if parsing succeeded.
       // However, inside this catch block, we don't have access to 'command' from the try block easily
       // without changing variable scoping.
       // I will implement the main logic in a way that handles this.
    }
  };
};

// --- Helper Functions ---

const parseMessage = (data: any): Command | null => {
  let command: Command | null = null;

  if (typeof data === 'string') {
    const message = JSON.parse(data);
    if (message.type === 'cancel_task') {
      const requestId = message.id;
      callbacks?.onLog(`Received cancel request for: ${requestId}`);
      const cancelled = geminiExecutor.cancelExecution(requestId);
      if (!cancelled) {
        callbacks?.onLog(`Request ${requestId} was not active or already completed`);
      }
      return null; // Signal that no further processing is needed
    }
    command = message as Command;
  } else if (data instanceof ArrayBuffer) {
    const view = new DataView(data);
    const headerLength = view.getUint32(0, false); // Big Endian
    const jsonBytes = new Uint8Array(data, 4, headerLength);
    const jsonStr = new TextDecoder("utf-8").decode(jsonBytes);
    const metadata = JSON.parse(jsonStr);
    const binaryData = new Uint8Array(data, 4 + headerLength);
    
    command = metadata as Command;
    if (command && command.payload) {
        (command.payload as any).data_bytes = binaryData;
    }
    console.debug(`Received binary frame. Header len: ${headerLength}, Binary body: ${binaryData.byteLength} bytes.`);
  } else {
    throw new Error(`Unsupported WebSocket message type: ${typeof data}`);
  }

  if (!command) throw new Error("Failed to parse command.");
  return command;
};

const sendMessageWithRetry = (message: any, logDescription: string) => {
  let attempts = 0;
  const maxAttempts = 5;

  const trySend = () => {
    try {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
        // Avoid spamming logs for every stream chunk, but log final/success messages
        if (!logDescription.startsWith("stream chunk")) {
             callbacks?.onLog(`Successfully sent ${logDescription}`);
        }
        return;
      }
    } catch (e) {
      console.error(`Send failed (${logDescription}), retrying...`, e);
    }

    if (attempts < maxAttempts) {
      attempts++;
      setTimeout(trySend, 200);
    } else {
      callbacks?.onLog(`WARNING: Dropped message (${logDescription}) after retries.`);
    }
  };
  
  trySend();
};

const sendStreamResponse = (command: Command, payload: any) => {
  // Log logic
  if (payload && payload.chunk) {
    const chunkStr = typeof payload.chunk === 'string' ? payload.chunk : JSON.stringify(payload.chunk);
    const preview = chunkStr.replace(/\n/g, ' ').substring(0, 60);
    const ellipsis = chunkStr.length > 60 ? '...' : '';
    callbacks?.onLog(`[Stream] Chunk: ${preview}${ellipsis}`);
  } else if (payload && payload.is_finished) {
     callbacks?.onLog(`[Stream] Finished signal received.`);
  }

  const response = { id: command.id, payload };
  // Differentiate log description to avoid spam
  const desc = (payload as any).is_finished ? `finished signal for ID ${command.id}` : `stream chunk for ID ${command.id}`;
  sendMessageWithRetry(response, desc);
};

// Redefined onmessage to use helpers and handle scoping correctly
const handleWebSocketMessage = async (event: MessageEvent) => {
    let command: Command | null = null;
    try {
        command = parseMessage(event.data);
        if (!command) return;

        callbacks?.onLog(`Received command: ${command.type} (ID: ${command.id})`);
        callbacks?.onLog(`Params: ${getSafePayloadLog(command.payload)}`);

        let backendUrl = '';
        try {
            const urlObj = new URL(websocketUrl);
            const protocol = urlObj.protocol === 'wss:' ? 'https:' : 'http:';
            backendUrl = `${protocol}//${urlObj.host}`;
        } catch (e) {
            console.error("Could not parse WebSocket URL", e);
        }

        if (command.type === 'streamGenerateContent') {
            await geminiExecutor.execute(command, (payload) => sendStreamResponse(command!, payload), backendUrl);
            callbacks?.onLog(`Finished streaming for command ID: ${command.id}`);
        } else {
            const result = await geminiExecutor.execute(command, () => {}, backendUrl);
            callbacks?.onLog(`Result: ${getSafePayloadLog(result)}`);
            const response: ResponsePayload = { 
                id: command.id, 
                payload: result, 
                status: { error: false, code: 200 } 
            };
            sendMessageWithRetry(response, `success response for ID ${command.id}`);
        }
    } catch (error) {
        const response = createErrorResponse(error, command);
        sendMessageWithRetry(response, `error response for ID ${response.id}`);
    }
};

// Assign the handler
const connectInternal = () => {
  if (!websocketUrl || !clientId) {
    callbacks?.onLog('WebSocket URL or Client ID is missing.');
    return;
  }
  ws = new WebSocket(`${websocketUrl}/${clientId}`);
  ws.binaryType = 'arraybuffer';
  
  callbacks?.onLog(`Connecting to ${websocketUrl}/${clientId}...`);

  ws.onopen = () => {
    callbacks?.onLog('WebSocket connection established.');
    if (reconnectAttempts > 0) {
      callbacks?.onReconnectSuccess();
    }
    reconnectAttempts = 0;
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = null;
    callbacks?.onOpen();
  };

  ws.onclose = () => {
    callbacks?.onClose();
    if (isExplicitlyClosed) {
      callbacks?.onLog('WebSocket connection closed by user.');
      return;
    }

    if (reconnectAttempts < maxReconnectAttempts) {
      reconnectAttempts++;
      callbacks?.onLog(`Connection lost. Reconnect attempt ${reconnectAttempts}/${maxReconnectAttempts} in ${reconnectInterval / 1000}s...`);
      callbacks?.onReconnecting(reconnectAttempts, maxReconnectAttempts);
      reconnectTimer = window.setTimeout(connectInternal, reconnectInterval);
    } else {
      callbacks?.onLog(`Could not reconnect after ${maxReconnectAttempts} attempts.`);
      callbacks?.onReconnectFailed();
    }
  };

  ws.onerror = (event) => {
    callbacks?.onLog(`WebSocket error. See browser console for details.`);
    callbacks?.onError(event);
  };

  ws.onmessage = handleWebSocketMessage;
};

const connect = (url: string, id: string, cbs: ConnectionCallbacks) => {
  websocketUrl = url;
  clientId = id;
  callbacks = cbs;
  reconnectAttempts = 0;
  isExplicitlyClosed = false;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (ws) ws.close();
  connectInternal();
};

const disconnect = () => {
  isExplicitlyClosed = true;

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  if (ws && ws.readyState !== WebSocket.CLOSED && ws.readyState !== WebSocket.CLOSING) {
    ws.close();
  }
};

const manualReconnect = () => {
  if (ws && ws.readyState === WebSocket.OPEN) return;
  callbacks?.onLog('Manual reconnection initiated.');
  connect(websocketUrl, clientId, callbacks!);
};

export const websocketService = { connect, disconnect, manualReconnect };
