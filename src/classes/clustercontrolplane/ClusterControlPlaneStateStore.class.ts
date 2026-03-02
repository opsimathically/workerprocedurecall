import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync
} from 'node:fs';
import { dirname } from 'node:path';

import type {
  cluster_control_plane_event_t,
  cluster_control_plane_gateway_record_t,
  cluster_control_plane_mutation_tracking_record_t,
  cluster_control_plane_policy_version_record_t
} from './ClusterControlPlaneProtocol';

export type cluster_control_plane_state_snapshot_t = {
  schema_version: 1;
  updated_unix_ms: number;
  service_generation: number;
  gateway_record_by_id: Record<string, cluster_control_plane_gateway_record_t>;
  policy_version_record_by_id: Record<string, cluster_control_plane_policy_version_record_t>;
  active_policy_version_id: string | null;
  mutation_tracking_record_by_id: Record<string, cluster_control_plane_mutation_tracking_record_t>;
  event_list: cluster_control_plane_event_t[];
};

export type cluster_control_plane_state_retention_policy_t = {
  event_history_max_count: number;
  mutation_history_max_count: number;
  offline_gateway_retention_ms: number;
};

export interface cluster_control_plane_state_store_i {
  loadState(): cluster_control_plane_state_snapshot_t;

  saveState(params: {
    state: cluster_control_plane_state_snapshot_t;
  }): void;

  compactState(params: {
    state: cluster_control_plane_state_snapshot_t;
    now_unix_ms: number;
    retention_policy: cluster_control_plane_state_retention_policy_t;
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
    service_generation: 0,
    gateway_record_by_id: {},
    policy_version_record_by_id: {},
    active_policy_version_id: null,
    mutation_tracking_record_by_id: {},
    event_list: []
  };
}

function NormalizeStateSnapshot(params: {
  state: unknown;
}): cluster_control_plane_state_snapshot_t {
  if (!IsRecordObject(params.state)) {
    return BuildEmptyStateSnapshot();
  }

  const state = params.state;

  const normalized_state = BuildEmptyStateSnapshot({
    now_unix_ms: typeof state.updated_unix_ms === 'number' ? state.updated_unix_ms : Date.now()
  });

  normalized_state.service_generation =
    typeof state.service_generation === 'number' && Number.isInteger(state.service_generation)
      ? state.service_generation
      : 0;

  if (IsRecordObject(state.gateway_record_by_id)) {
    for (const [gateway_id, gateway_record] of Object.entries(state.gateway_record_by_id)) {
      if (!IsRecordObject(gateway_record)) {
        continue;
      }

      if (
        typeof gateway_record.gateway_id !== 'string' ||
        gateway_record.gateway_id.length === 0 ||
        gateway_record.gateway_id !== gateway_id ||
        typeof gateway_record.last_heartbeat_unix_ms !== 'number' ||
        typeof gateway_record.lease_expires_unix_ms !== 'number'
      ) {
        continue;
      }

      normalized_state.gateway_record_by_id[gateway_id] =
        structuredClone(gateway_record) as cluster_control_plane_gateway_record_t;
    }
  }

  if (IsRecordObject(state.policy_version_record_by_id)) {
    for (const [version_id, policy_record] of Object.entries(state.policy_version_record_by_id)) {
      if (!IsRecordObject(policy_record) || !IsRecordObject(policy_record.metadata)) {
        continue;
      }

      const metadata = policy_record.metadata;
      if (
        typeof metadata.version_id !== 'string' ||
        metadata.version_id.length === 0 ||
        metadata.version_id !== version_id ||
        typeof metadata.created_unix_ms !== 'number' ||
        typeof metadata.actor_subject !== 'string' ||
        typeof metadata.checksum_sha1 !== 'string' ||
        (metadata.status !== 'staged' && metadata.status !== 'active' && metadata.status !== 'superseded')
      ) {
        continue;
      }

      normalized_state.policy_version_record_by_id[version_id] =
        structuredClone(policy_record) as cluster_control_plane_policy_version_record_t;
    }
  }

  if (
    typeof state.active_policy_version_id === 'string' &&
    state.active_policy_version_id.length > 0
  ) {
    normalized_state.active_policy_version_id = state.active_policy_version_id;
  }

  if (IsRecordObject(state.mutation_tracking_record_by_id)) {
    for (const [mutation_id, mutation_record] of Object.entries(
      state.mutation_tracking_record_by_id
    )) {
      if (!IsRecordObject(mutation_record)) {
        continue;
      }

      if (
        typeof mutation_record.mutation_id !== 'string' ||
        mutation_record.mutation_id !== mutation_id ||
        typeof mutation_record.created_unix_ms !== 'number' ||
        typeof mutation_record.updated_unix_ms !== 'number'
      ) {
        continue;
      }

      normalized_state.mutation_tracking_record_by_id[mutation_id] =
        structuredClone(mutation_record) as cluster_control_plane_mutation_tracking_record_t;
    }
  }

  if (Array.isArray(state.event_list)) {
    normalized_state.event_list = state.event_list
      .filter((event): event is cluster_control_plane_event_t => {
        return (
          IsRecordObject(event) &&
          typeof event.event_id === 'string' &&
          typeof event.event_name === 'string' &&
          typeof event.timestamp_unix_ms === 'number'
        );
      })
      .map((event) => {
        return structuredClone(event) as cluster_control_plane_event_t;
      });
  }

  return normalized_state;
}

function ApplyRetentionPolicy(params: {
  state: cluster_control_plane_state_snapshot_t;
  now_unix_ms: number;
  retention_policy: cluster_control_plane_state_retention_policy_t;
}): cluster_control_plane_state_snapshot_t {
  const state = NormalizeStateSnapshot({
    state: params.state
  });

  const mutation_record_list = Object.values(state.mutation_tracking_record_by_id).sort(
    (left_record, right_record): number => {
      return right_record.updated_unix_ms - left_record.updated_unix_ms;
    }
  );

  const retained_mutation_id_set = new Set<string>();
  for (
    let index = 0;
    index < mutation_record_list.length &&
    index < params.retention_policy.mutation_history_max_count;
    index += 1
  ) {
    retained_mutation_id_set.add(mutation_record_list[index].mutation_id);
  }

  for (const mutation_id of Object.keys(state.mutation_tracking_record_by_id)) {
    if (!retained_mutation_id_set.has(mutation_id)) {
      delete state.mutation_tracking_record_by_id[mutation_id];
    }
  }

  for (const [gateway_id, gateway_record] of Object.entries(state.gateway_record_by_id)) {
    if (gateway_record.status !== 'offline') {
      continue;
    }

    if (
      gateway_record.last_heartbeat_unix_ms +
        params.retention_policy.offline_gateway_retention_ms <=
      params.now_unix_ms
    ) {
      delete state.gateway_record_by_id[gateway_id];
    }
  }

  state.event_list = state.event_list.slice(
    Math.max(0, state.event_list.length - params.retention_policy.event_history_max_count)
  );

  state.updated_unix_ms = params.now_unix_ms;
  return state;
}

export class ClusterControlPlaneInMemoryStateStore implements cluster_control_plane_state_store_i {
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
    retention_policy: cluster_control_plane_state_retention_policy_t;
  }): cluster_control_plane_state_snapshot_t {
    this.state = ApplyRetentionPolicy({
      state: params.state,
      now_unix_ms: params.now_unix_ms,
      retention_policy: params.retention_policy
    });

    return structuredClone(this.state);
  }
}

export class ClusterControlPlaneFileStateStore implements cluster_control_plane_state_store_i {
  private readonly file_path: string;

  constructor(params: cluster_control_plane_file_state_store_constructor_params_t) {
    if (typeof params.file_path !== 'string' || params.file_path.length === 0) {
      throw new Error('file_path must be a non-empty string.');
    }

    this.file_path = params.file_path;
  }

  loadState(): cluster_control_plane_state_snapshot_t {
    if (!existsSync(this.file_path)) {
      return BuildEmptyStateSnapshot();
    }

    let parsed_state: unknown;
    try {
      parsed_state = JSON.parse(readFileSync(this.file_path, 'utf8')) as unknown;
    } catch (error) {
      throw new Error(
        `Failed to load control-plane state from "${this.file_path}": ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }

    return NormalizeStateSnapshot({
      state: parsed_state
    });
  }

  saveState(params: {
    state: cluster_control_plane_state_snapshot_t;
  }): void {
    const normalized_state = NormalizeStateSnapshot({
      state: params.state
    });

    const parent_directory_path = dirname(this.file_path);
    mkdirSync(parent_directory_path, {
      recursive: true
    });

    const serialized_state = JSON.stringify(normalized_state);
    const temp_file_path = `${this.file_path}.tmp.${process.pid}.${Date.now()}`;
    writeFileSync(temp_file_path, serialized_state, 'utf8');
    renameSync(temp_file_path, this.file_path);
  }

  compactState(params: {
    state: cluster_control_plane_state_snapshot_t;
    now_unix_ms: number;
    retention_policy: cluster_control_plane_state_retention_policy_t;
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
