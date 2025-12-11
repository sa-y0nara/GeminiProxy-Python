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

// Command type constants to avoid string duplication
enum CommandType {
  // Model commands
  LIST_MODELS = 'listModels',
  GET_MODEL = 'getModel',
  // Generation commands
  GENERATE_CONTENT = 'generateContent',
  STREAM_GENERATE_CONTENT = 'streamGenerateContent',
  // File commands
  CREATE_FILE = 'createFile',
  INITIATE_RESUMABLE_UPLOAD = 'initiate_resumable_upload',
  CREATE_FILE_METADATA = 'create_file_metadata',
  CREATE_FILE_METADATA_ALT = 'createFileMetadata',
  UPDATE_FILE = 'updateFile',
  UPLOAD_CHUNK = 'upload_chunk',
  UPLOAD_FILE_CHUNK = 'upload_file_chunk',
  GET_FILE = 'getFile',
  GET_FILE_ALT = 'get_file',
  DELETE_FILE = 'deleteFile',
  DELETE_FILE_ALT = 'delete_file',
}

// Create alias map to reduce duplication
const COMMAND_ALIASES: Record<string, string> = {
  [CommandType.CREATE_FILE_METADATA_ALT]: CommandType.CREATE_FILE_METADATA,
  [CommandType.INITIATE_RESUMABLE_UPLOAD]: CommandType.CREATE_FILE,
  [CommandType.UPDATE_FILE]: CommandType.UPLOAD_CHUNK,
  [CommandType.UPLOAD_FILE_CHUNK]: CommandType.UPLOAD_CHUNK,
  [CommandType.GET_FILE_ALT]: CommandType.GET_FILE,
  [CommandType.DELETE_FILE_ALT]: CommandType.DELETE_FILE,
};

const COMMAND_HANDLERS: Record<string, Function> = {
  [CommandType.LIST_MODELS]: (cmd: Command) => executeListModels(cmd.payload as ListModelsCommandPayload),
  [CommandType.GET_MODEL]: (cmd: Command) => executeGetModel(cmd.payload as GetModelCommandPayload),
  [CommandType.GENERATE_CONTENT]: (cmd: Command) => executeGenerateContent(cmd as generateContentCommand, activeRequests),
  [CommandType.STREAM_GENERATE_CONTENT]: async (cmd: Command, sendResponse: (payload: unknown) => void) => 
    await executeStreamGenerateContent(cmd as StreamGenerateContentCommand, sendResponse, activeRequests),
  [CommandType.CREATE_FILE]: (cmd: Command) => initiateResumableUpload(cmd.payload as CreateFilePayload),
  [CommandType.CREATE_FILE_METADATA]: (cmd: Command) => createMetadataOnlyFile(cmd.payload as CreateFilePayload),
  [CommandType.UPLOAD_CHUNK]: (cmd: Command, _: unknown, backendUrl?: string) => uploadChunk(cmd.payload as UpdateFilePayload, backendUrl),
  [CommandType.GET_FILE]: (cmd: Command) => getFile(cmd.payload as GetFilePayload),
  [CommandType.DELETE_FILE]: (cmd: Command) => deleteFile(cmd.payload as DeleteFilePayload),
};

export const geminiExecutor = {
  execute: async (command: Command, sendResponse: (payload: unknown) => void, backendUrl?: string): Promise<any> => {
    // 解析命令类型，支持别名
    let commandType = command.type as string;
    const canonicalType = COMMAND_ALIASES[commandType] || commandType;
    
    const handler = COMMAND_HANDLERS[canonicalType];
    if (!handler) {
      throw new Error(`Unsupported command type: ${command.type}`);
    }
    return await handler(command, sendResponse, backendUrl);
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
