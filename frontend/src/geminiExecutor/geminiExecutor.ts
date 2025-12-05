import { GoogleGenAI } from "@google/genai";
import type { CreateFilePayload, DeleteFilePayload, GetFilePayload, UpdateFilePayload } from '../types/files';
import type { generateContentCommand, StreamGenerateContentCommand } from '../types/generatingContent';
import type { GetModelCommandPayload, ListModelsCommandPayload } from '../types/models';
import type { Command } from '../types/types';
import { createMetadataOnlyFile, deleteFile, getFile, initiateResumableUpload, uploadChunk } from "./files";
import { executeGenerateContent, executeStreamGenerateContent } from "./generatingContent";
import { executeGetModel, executeListModels } from './models';

export const GOOGLE_API_URL = "https://generativelanguage.googleapis.com/v1beta";
export const ai = new GoogleGenAI({ apiKey: process.env.API_KEY || "" });

// Manages active requests for cancellation
const activeRequests = new Map<string, AbortController>();

async function testGeminiConnection(): Promise<string> {
  const response = await ai.models.generateContent({
    model: 'gemini-2.5-flash',
    contents: 'hello',
  });

  if (!response || !response.text || response.text.trim() === '') {
    throw new Error("No response received from Gemini API.");
  }
  return response.text;
}

// Command type constants matching Backend protocol exactly
enum CommandType {
  // Model commands (CamelCase from Backend)
  LIST_MODELS = 'listModels',
  GET_MODEL = 'getModel',
  
  // Generation commands (CamelCase from Backend)
  GENERATE_CONTENT = 'generateContent',
  STREAM_GENERATE_CONTENT = 'streamGenerateContent',
  
  // File commands (SnakeCase from Backend)
  INITIATE_RESUMABLE_UPLOAD = 'initiate_resumable_upload',
  CREATE_FILE_METADATA = 'create_file_metadata',
  UPLOAD_CHUNK = 'upload_chunk',
  GET_FILE = 'get_file',
  DELETE_FILE = 'delete_file',
}

const COMMAND_HANDLERS: Record<string, Function> = {
  [CommandType.LIST_MODELS]: (cmd: Command) => executeListModels(cmd.payload as ListModelsCommandPayload),
  [CommandType.GET_MODEL]: (cmd: Command) => executeGetModel(cmd.payload as GetModelCommandPayload),
  [CommandType.GENERATE_CONTENT]: (cmd: Command) => executeGenerateContent(cmd as generateContentCommand, activeRequests),
  [CommandType.STREAM_GENERATE_CONTENT]: async (cmd: Command, sendResponse: (payload: unknown) => void) => 
    await executeStreamGenerateContent(cmd as StreamGenerateContentCommand, sendResponse, activeRequests),
  
  // File Handlers
  [CommandType.INITIATE_RESUMABLE_UPLOAD]: (cmd: Command) => initiateResumableUpload(cmd.payload as CreateFilePayload),
  [CommandType.CREATE_FILE_METADATA]: (cmd: Command) => createMetadataOnlyFile(cmd.payload as CreateFilePayload),
  [CommandType.UPLOAD_CHUNK]: (cmd: Command, _: unknown, backendUrl?: string) => uploadChunk(cmd.payload as UpdateFilePayload, backendUrl),
  [CommandType.GET_FILE]: (cmd: Command) => getFile(cmd.payload as GetFilePayload),
  [CommandType.DELETE_FILE]: (cmd: Command) => deleteFile(cmd.payload as DeleteFilePayload),
};

export const geminiExecutor = {
  execute: async (command: Command, sendResponse: (payload: unknown) => void, backendUrl?: string): Promise<any> => {
    const commandType = command.type as string;
    const handler = COMMAND_HANDLERS[commandType];
    
    if (!handler) {
      console.error(`Unsupported command type received: ${commandType}`);
      throw new Error(`Unsupported command type: ${command.type}`);
    }
    
    try {
      return await handler(command, sendResponse, backendUrl);
    } catch (error) {
      console.error(`Error executing command ${commandType}:`, error);
      throw error;
    }
  },
  
  // Cancel execution method
  cancelExecution: (requestId: string): boolean => {
    const controller = activeRequests.get(requestId);
    if (controller) {
      controller.abort();
      console.log(`Aborted request ${requestId}`);
      return true;
    }
    console.warn(`Request ${requestId} not found for cancellation`);
    return false;
  },
  
  testGeminiConnection,
};

// Export activeRequests for testing
export { activeRequests };