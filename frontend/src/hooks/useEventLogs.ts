import { useState, useCallback } from 'react';

export const useEventLogs = (maxLogs = 200) => {
  const [logs, setLogs] = useState<string[]>([]);

  const addLog = useCallback((message: string) => {
    const timestamp = new Date().toLocaleTimeString('en-US', { hour12: false });
    setLogs((prevLogs) => [`[${timestamp}] ${message}`, ...prevLogs.slice(0, maxLogs - 1)]);
  }, [maxLogs]);

  const clearLogs = useCallback(() => {
    setLogs([]);
  }, []);

  return { logs, addLog, clearLogs };
};
