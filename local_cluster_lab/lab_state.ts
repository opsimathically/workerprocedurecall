import fs from 'node:fs';

import { GetLabStateFilePath } from './lab_paths';
import type { lab_state_t } from './lab_types';

export function ReadLabState(): lab_state_t | null {
  const state_file_path = GetLabStateFilePath();
  if (!fs.existsSync(state_file_path)) {
    return null;
  }

  const raw_text = fs.readFileSync(state_file_path, 'utf8');
  const parsed_state = JSON.parse(raw_text) as lab_state_t;
  return parsed_state;
}

export function WriteLabState(params: { state: lab_state_t }): void {
  const state_file_path = GetLabStateFilePath();
  fs.writeFileSync(state_file_path, JSON.stringify(params.state, null, 2), 'utf8');
}

export function DeleteLabStateFile(): void {
  const state_file_path = GetLabStateFilePath();
  if (!fs.existsSync(state_file_path)) {
    return;
  }

  fs.rmSync(state_file_path);
}

