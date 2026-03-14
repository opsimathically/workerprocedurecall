import fs from 'node:fs';
import path from 'node:path';

export function GetLabRootDirectoryPath(): string {
  return path.resolve(__dirname);
}

export function GetLabRuntimeDirectoryPath(): string {
  return path.resolve(GetLabRootDirectoryPath(), 'runtime');
}

export function GetLabLogsDirectoryPath(): string {
  return path.resolve(GetLabRootDirectoryPath(), 'logs');
}

export function GetLabStateFilePath(): string {
  return path.resolve(GetLabRuntimeDirectoryPath(), 'lab_state.json');
}

export function EnsureLabDirectories(): void {
  fs.mkdirSync(GetLabRuntimeDirectoryPath(), {
    recursive: true
  });
  fs.mkdirSync(GetLabLogsDirectoryPath(), {
    recursive: true
  });
}

