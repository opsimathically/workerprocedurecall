import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync
} from 'node:fs';
import { dirname } from 'node:path';
import type {
  cluster_admin_mutation_ack_message_i,
  cluster_admin_mutation_error_message_i,
  cluster_admin_mutation_result_message_i
} from '../clusterprotocol/ClusterProtocolTypes';

export type cluster_control_plane_mutation_response_t = {
  ack: cluster_admin_mutation_ack_message_i;
  terminal_message:
    | cluster_admin_mutation_result_message_i
    | cluster_admin_mutation_error_message_i;
};

export type cluster_control_plane_dedupe_record_t = {
  mutation_id: string;
  created_unix_ms: number;
  expires_unix_ms: number;
  status: 'in_progress' | 'completed';
  response?: cluster_control_plane_mutation_response_t;
};

export type cluster_control_plane_audit_record_t = {
  mutation_id: string;
  timestamp_unix_ms: number;
  [key: string]: unknown;
};

export type cluster_control_plane_state_snapshot_t = {
  schema_version: 1;
  updated_unix_ms: number;
  dedupe_record_by_id: Record<string, cluster_control_plane_dedupe_record_t>;
  entity_version_by_name: Record<string, string>;
  audit_record_by_mutation_id: Record<string, cluster_control_plane_audit_record_t>;
};

export type cluster_control_plane_retention_policy_t = {
  dedupe_retention_ms: number;
  audit_retention_ms?: number;
  audit_max_record_count?: number;
};

export interface cluster_control_plane_state_store_i {
  loadState(): cluster_control_plane_state_snapshot_t;
  saveState(params: {
    state: cluster_control_plane_state_snapshot_t;
  }): void;
  compactState(params: {
    state: cluster_control_plane_state_snapshot_t;
    now_unix_ms: number;
    retention_policy: cluster_control_plane_retention_policy_t;
  }): cluster_control_plane_state_snapshot_t;
}

export type cluster_control_plane_file_state_store_constructor_params_t = {
  file_path: string;
};

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function BuildEmptyStateSnapshot(params?: {
  now_unix_ms?: number;
}): cluster_control_plane_state_snapshot_t {
  return {
    schema_version: 1,
    updated_unix_ms: params?.now_unix_ms ?? Date.now(),
    dedupe_record_by_id: {},
    entity_version_by_name: {},
    audit_record_by_mutation_id: {}
  };
}

function NormalizeStateSnapshot(params: {
  state: cluster_control_plane_state_snapshot_t;
}): cluster_control_plane_state_snapshot_t {
  const { state } = params;

  if (!IsRecordObject(state)) {
    return BuildEmptyStateSnapshot();
  }

  const normalized_state: cluster_control_plane_state_snapshot_t = BuildEmptyStateSnapshot({
    now_unix_ms:
      typeof state.updated_unix_ms === 'number'
        ? state.updated_unix_ms
        : Date.now()
  });

  if (IsRecordObject(state.entity_version_by_name)) {
    for (const [entity_name, entity_version] of Object.entries(
      state.entity_version_by_name
    )) {
      if (typeof entity_version === 'string' && entity_version.length > 0) {
        normalized_state.entity_version_by_name[entity_name] = entity_version;
      }
    }
  }

  if (IsRecordObject(state.dedupe_record_by_id)) {
    for (const [mutation_id, dedupe_record] of Object.entries(state.dedupe_record_by_id)) {
      if (!IsRecordObject(dedupe_record)) {
        continue;
      }

      const status = dedupe_record.status;
      const created_unix_ms = dedupe_record.created_unix_ms;
      const expires_unix_ms = dedupe_record.expires_unix_ms;
      if (
        typeof mutation_id !== 'string' ||
        mutation_id.length === 0 ||
        typeof created_unix_ms !== 'number' ||
        typeof expires_unix_ms !== 'number' ||
        (status !== 'in_progress' && status !== 'completed')
      ) {
        continue;
      }

      normalized_state.dedupe_record_by_id[mutation_id] = {
        mutation_id,
        created_unix_ms,
        expires_unix_ms,
        status,
        response: IsRecordObject(dedupe_record.response)
          ? (structuredClone(
              dedupe_record.response
            ) as cluster_control_plane_mutation_response_t)
          : undefined
      };
    }
  }

  if (IsRecordObject(state.audit_record_by_mutation_id)) {
    for (const [mutation_id, audit_record] of Object.entries(
      state.audit_record_by_mutation_id
    )) {
      if (!IsRecordObject(audit_record)) {
        continue;
      }
      if (
        typeof audit_record.mutation_id !== 'string' ||
        typeof audit_record.timestamp_unix_ms !== 'number'
      ) {
        continue;
      }
      normalized_state.audit_record_by_mutation_id[mutation_id] = structuredClone(
        audit_record
      ) as cluster_control_plane_audit_record_t;
    }
  }

  return normalized_state;
}

function ApplyRetentionPolicy(params: {
  state: cluster_control_plane_state_snapshot_t;
  now_unix_ms: number;
  retention_policy: cluster_control_plane_retention_policy_t;
}): cluster_control_plane_state_snapshot_t {
  const { now_unix_ms, retention_policy } = params;
  const state = NormalizeStateSnapshot({
    state: params.state
  });

  for (const [mutation_id, dedupe_record] of Object.entries(state.dedupe_record_by_id)) {
    if (dedupe_record.expires_unix_ms <= now_unix_ms) {
      delete state.dedupe_record_by_id[mutation_id];
      continue;
    }

    if (
      dedupe_record.created_unix_ms + retention_policy.dedupe_retention_ms <= now_unix_ms
    ) {
      delete state.dedupe_record_by_id[mutation_id];
    }
  }

  if (typeof retention_policy.audit_retention_ms === 'number') {
    const oldest_allowed_unix_ms = now_unix_ms - retention_policy.audit_retention_ms;
    for (const [mutation_id, audit_record] of Object.entries(
      state.audit_record_by_mutation_id
    )) {
      if (audit_record.timestamp_unix_ms < oldest_allowed_unix_ms) {
        delete state.audit_record_by_mutation_id[mutation_id];
      }
    }
  }

  if (
    typeof retention_policy.audit_max_record_count === 'number' &&
    retention_policy.audit_max_record_count > 0
  ) {
    const sorted_audit_record_list = Object.values(
      state.audit_record_by_mutation_id
    ).sort((left_record, right_record): number => {
      return right_record.timestamp_unix_ms - left_record.timestamp_unix_ms;
    });

    const retained_mutation_id_set = new Set<string>();
    for (
      let index = 0;
      index < sorted_audit_record_list.length &&
      index < retention_policy.audit_max_record_count;
      index += 1
    ) {
      retained_mutation_id_set.add(sorted_audit_record_list[index].mutation_id);
    }

    for (const mutation_id of Object.keys(state.audit_record_by_mutation_id)) {
      if (!retained_mutation_id_set.has(mutation_id)) {
        delete state.audit_record_by_mutation_id[mutation_id];
      }
    }
  }

  state.updated_unix_ms = now_unix_ms;
  return state;
}

export class ClusterControlPlaneInMemoryStateStore
  implements cluster_control_plane_state_store_i
{
  private state: cluster_control_plane_state_snapshot_t = BuildEmptyStateSnapshot();

  loadState(): cluster_control_plane_state_snapshot_t {
    return structuredClone(this.state);
  }

  saveState(params: {
    state: cluster_control_plane_state_snapshot_t;
  }): void {
    this.state = NormalizeStateSnapshot({
      state: params.state
    });
  }

  compactState(params: {
    state: cluster_control_plane_state_snapshot_t;
    now_unix_ms: number;
    retention_policy: cluster_control_plane_retention_policy_t;
  }): cluster_control_plane_state_snapshot_t {
    this.state = ApplyRetentionPolicy({
      state: params.state,
      now_unix_ms: params.now_unix_ms,
      retention_policy: params.retention_policy
    });
    return structuredClone(this.state);
  }
}

export class ClusterControlPlaneFileStateStore
  implements cluster_control_plane_state_store_i
{
  private readonly file_path: string;

  constructor(params: cluster_control_plane_file_state_store_constructor_params_t) {
    const { file_path } = params;
    if (typeof file_path !== 'string' || file_path.length === 0) {
      throw new Error('file_path must be a non-empty string.');
    }
    this.file_path = file_path;
  }

  loadState(): cluster_control_plane_state_snapshot_t {
    if (!existsSync(this.file_path)) {
      return BuildEmptyStateSnapshot();
    }

    let parsed_state: unknown;
    try {
      const raw_state = readFileSync(this.file_path, 'utf8');
      parsed_state = JSON.parse(raw_state) as unknown;
    } catch (error) {
      throw new Error(
        `Failed to load control-plane state from "${this.file_path}": ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }

    return NormalizeStateSnapshot({
      state: parsed_state as cluster_control_plane_state_snapshot_t
    });
  }

  saveState(params: {
    state: cluster_control_plane_state_snapshot_t;
  }): void {
    const normalized_state = NormalizeStateSnapshot({
      state: params.state
    });
    const serialized_state = JSON.stringify(normalized_state);
    const parent_directory_path = dirname(this.file_path);
    mkdirSync(parent_directory_path, {
      recursive: true
    });

    const temp_file_path = `${this.file_path}.tmp.${process.pid}.${Date.now()}`;
    writeFileSync(temp_file_path, serialized_state, 'utf8');
    renameSync(temp_file_path, this.file_path);
  }

  compactState(params: {
    state: cluster_control_plane_state_snapshot_t;
    now_unix_ms: number;
    retention_policy: cluster_control_plane_retention_policy_t;
  }): cluster_control_plane_state_snapshot_t {
    const compacted_state = ApplyRetentionPolicy({
      state: params.state,
      now_unix_ms: params.now_unix_ms,
      retention_policy: params.retention_policy
    });

    this.saveState({
      state: compacted_state
    });

    return compacted_state;
  }
}
