import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync
} from 'node:fs';
import { dirname } from 'node:path';

import type {
  cluster_service_discovery_event_t,
  cluster_service_discovery_node_record_t
} from './ClusterServiceDiscoveryStore.class';
import type {
  cluster_service_discovery_consensus_role_t,
  cluster_service_discovery_write_request_message_t
} from './ClusterServiceDiscoveryProtocol';

export type cluster_service_discovery_daemon_metadata_t = {
  daemon_id: string;
  current_term: number;
  voted_for_daemon_id: string | null;
  leader_id: string | null;
  role: cluster_service_discovery_consensus_role_t;
  commit_index: number;
  last_applied_index: number;
  last_log_index: number;
  updated_unix_ms: number;
};

export type cluster_service_discovery_daemon_committed_operation_log_entry_t = {
  index: number;
  term: number;
  request_id: string;
  committed_unix_ms: number;
  request_message: cluster_service_discovery_write_request_message_t;
};

export type cluster_service_discovery_daemon_state_snapshot_t = {
  schema_version: 1;
  daemon_metadata: cluster_service_discovery_daemon_metadata_t;
  node_record_list: cluster_service_discovery_node_record_t[];
  committed_operation_log: cluster_service_discovery_daemon_committed_operation_log_entry_t[];
  discovery_event_log: cluster_service_discovery_event_t[];
};

export type cluster_service_discovery_daemon_state_retention_policy_t = {
  max_operation_log_count: number;
  max_discovery_event_count: number;
};

export interface cluster_service_discovery_daemon_state_store_i {
  loadState(): cluster_service_discovery_daemon_state_snapshot_t;

  saveState(params: {
    state: cluster_service_discovery_daemon_state_snapshot_t;
  }): void;

  compactState(params: {
    state: cluster_service_discovery_daemon_state_snapshot_t;
    retention_policy: cluster_service_discovery_daemon_state_retention_policy_t;
  }): cluster_service_discovery_daemon_state_snapshot_t;
}

export type cluster_service_discovery_daemon_file_state_store_constructor_params_t = {
  file_path: string;
};

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function NormalizeNodeRecordList(params: {
  node_record_list: unknown;
}): cluster_service_discovery_node_record_t[] {
  if (!Array.isArray(params.node_record_list)) {
    return [];
  }

  const node_record_list: cluster_service_discovery_node_record_t[] = [];

  for (const node_record of params.node_record_list) {
    if (!IsRecordObject(node_record)) {
      continue;
    }

    const node_identity = node_record.node_identity;
    const status = node_record.status;
    const metrics = node_record.metrics;

    if (
      !IsRecordObject(node_identity) ||
      typeof node_identity.node_id !== 'string' ||
      node_identity.node_id.length === 0 ||
      !IsRecordObject(metrics) ||
      typeof status !== 'string'
    ) {
      continue;
    }

    node_record_list.push(structuredClone(node_record) as cluster_service_discovery_node_record_t);
  }

  return node_record_list;
}

function BuildDefaultDaemonMetadata(params?: {
  daemon_id?: string;
}): cluster_service_discovery_daemon_metadata_t {
  return {
    daemon_id: params?.daemon_id ?? 'discovery_daemon',
    current_term: 0,
    voted_for_daemon_id: null,
    leader_id: null,
    role: 'follower',
    commit_index: 0,
    last_applied_index: 0,
    last_log_index: 0,
    updated_unix_ms: Date.now()
  };
}

function NormalizeDaemonMetadata(params: {
  daemon_metadata: unknown;
}): cluster_service_discovery_daemon_metadata_t {
  if (!IsRecordObject(params.daemon_metadata)) {
    return BuildDefaultDaemonMetadata();
  }

  const daemon_metadata = params.daemon_metadata;

  return {
    daemon_id:
      typeof daemon_metadata.daemon_id === 'string' && daemon_metadata.daemon_id.length > 0
        ? daemon_metadata.daemon_id
        : 'discovery_daemon',
    current_term:
      typeof daemon_metadata.current_term === 'number' &&
      Number.isInteger(daemon_metadata.current_term) &&
      daemon_metadata.current_term >= 0
        ? daemon_metadata.current_term
        : 0,
    voted_for_daemon_id:
      typeof daemon_metadata.voted_for_daemon_id === 'string'
        ? daemon_metadata.voted_for_daemon_id
        : null,
    leader_id: typeof daemon_metadata.leader_id === 'string' ? daemon_metadata.leader_id : null,
    role:
      daemon_metadata.role === 'leader' ||
      daemon_metadata.role === 'candidate' ||
      daemon_metadata.role === 'follower'
        ? daemon_metadata.role
        : 'follower',
    commit_index:
      typeof daemon_metadata.commit_index === 'number' &&
      Number.isInteger(daemon_metadata.commit_index) &&
      daemon_metadata.commit_index >= 0
        ? daemon_metadata.commit_index
        : 0,
    last_applied_index:
      typeof daemon_metadata.last_applied_index === 'number' &&
      Number.isInteger(daemon_metadata.last_applied_index) &&
      daemon_metadata.last_applied_index >= 0
        ? daemon_metadata.last_applied_index
        : 0,
    last_log_index:
      typeof daemon_metadata.last_log_index === 'number' &&
      Number.isInteger(daemon_metadata.last_log_index) &&
      daemon_metadata.last_log_index >= 0
        ? daemon_metadata.last_log_index
        : 0,
    updated_unix_ms:
      typeof daemon_metadata.updated_unix_ms === 'number' &&
      Number.isFinite(daemon_metadata.updated_unix_ms)
        ? daemon_metadata.updated_unix_ms
        : Date.now()
  };
}

function NormalizeOperationLog(params: {
  operation_log: unknown;
}): cluster_service_discovery_daemon_committed_operation_log_entry_t[] {
  if (!Array.isArray(params.operation_log)) {
    return [];
  }

  const operation_log: cluster_service_discovery_daemon_committed_operation_log_entry_t[] = [];

  for (const operation_entry of params.operation_log) {
    if (!IsRecordObject(operation_entry)) {
      continue;
    }

    if (
      !Number.isInteger(operation_entry.index) ||
      !Number.isInteger(operation_entry.term) ||
      typeof operation_entry.request_id !== 'string' ||
      operation_entry.request_id.length === 0 ||
      !Number.isFinite(operation_entry.committed_unix_ms) ||
      !IsRecordObject(operation_entry.request_message)
    ) {
      continue;
    }

    const request_message = operation_entry.request_message;
    if (
      typeof request_message.message_type !== 'string' ||
      !request_message.message_type.startsWith('cluster_service_discovery_')
    ) {
      continue;
    }

    operation_log.push({
      index: operation_entry.index as number,
      term: operation_entry.term as number,
      request_id: operation_entry.request_id,
      committed_unix_ms: operation_entry.committed_unix_ms as number,
      request_message: structuredClone(
        request_message
      ) as unknown as cluster_service_discovery_write_request_message_t
    });
  }

  operation_log.sort((left_operation, right_operation): number => {
    return left_operation.index - right_operation.index;
  });

  return operation_log;
}

function NormalizeDiscoveryEventLog(params: {
  discovery_event_log: unknown;
}): cluster_service_discovery_event_t[] {
  if (!Array.isArray(params.discovery_event_log)) {
    return [];
  }

  const event_list: cluster_service_discovery_event_t[] = [];

  for (const event_entry of params.discovery_event_log) {
    if (!IsRecordObject(event_entry)) {
      continue;
    }

    if (
      typeof event_entry.event_id !== 'string' ||
      typeof event_entry.event_name !== 'string' ||
      typeof event_entry.node_id !== 'string' ||
      !Number.isFinite(event_entry.timestamp_unix_ms)
    ) {
      continue;
    }

    event_list.push(structuredClone(event_entry) as cluster_service_discovery_event_t);
  }

  event_list.sort((left_event, right_event): number => {
    return left_event.timestamp_unix_ms - right_event.timestamp_unix_ms;
  });

  return event_list;
}

function BuildEmptyStateSnapshot(params?: {
  daemon_id?: string;
}): cluster_service_discovery_daemon_state_snapshot_t {
  return {
    schema_version: 1,
    daemon_metadata: BuildDefaultDaemonMetadata({
      daemon_id: params?.daemon_id
    }),
    node_record_list: [],
    committed_operation_log: [],
    discovery_event_log: []
  };
}

function NormalizeStateSnapshot(params: {
  state: unknown;
  fallback_daemon_id?: string;
}): cluster_service_discovery_daemon_state_snapshot_t {
  if (!IsRecordObject(params.state)) {
    return BuildEmptyStateSnapshot({
      daemon_id: params.fallback_daemon_id
    });
  }

  const normalized_state: cluster_service_discovery_daemon_state_snapshot_t = {
    schema_version: 1,
    daemon_metadata: NormalizeDaemonMetadata({
      daemon_metadata: params.state.daemon_metadata
    }),
    node_record_list: NormalizeNodeRecordList({
      node_record_list: params.state.node_record_list
    }),
    committed_operation_log: NormalizeOperationLog({
      operation_log: params.state.committed_operation_log
    }),
    discovery_event_log: NormalizeDiscoveryEventLog({
      discovery_event_log: params.state.discovery_event_log
    })
  };

  if (params.fallback_daemon_id && normalized_state.daemon_metadata.daemon_id.length === 0) {
    normalized_state.daemon_metadata.daemon_id = params.fallback_daemon_id;
  }

  if (normalized_state.daemon_metadata.last_log_index < normalized_state.committed_operation_log.length) {
    normalized_state.daemon_metadata.last_log_index =
      normalized_state.committed_operation_log[normalized_state.committed_operation_log.length - 1]
        ?.index ?? 0;
  }

  normalized_state.daemon_metadata.commit_index = Math.min(
    normalized_state.daemon_metadata.commit_index,
    normalized_state.daemon_metadata.last_log_index
  );

  normalized_state.daemon_metadata.last_applied_index = Math.min(
    normalized_state.daemon_metadata.last_applied_index,
    normalized_state.daemon_metadata.commit_index
  );

  return normalized_state;
}

function ApplyRetentionPolicy(params: {
  state: cluster_service_discovery_daemon_state_snapshot_t;
  retention_policy: cluster_service_discovery_daemon_state_retention_policy_t;
}): cluster_service_discovery_daemon_state_snapshot_t {
  const normalized_state = NormalizeStateSnapshot({
    state: params.state
  });

  if (params.retention_policy.max_operation_log_count > 0) {
    normalized_state.committed_operation_log = normalized_state.committed_operation_log.slice(
      Math.max(
        0,
        normalized_state.committed_operation_log.length - params.retention_policy.max_operation_log_count
      )
    );
  }

  if (params.retention_policy.max_discovery_event_count > 0) {
    normalized_state.discovery_event_log = normalized_state.discovery_event_log.slice(
      Math.max(
        0,
        normalized_state.discovery_event_log.length - params.retention_policy.max_discovery_event_count
      )
    );
  }

  return normalized_state;
}

export class ClusterServiceDiscoveryDaemonInMemoryStateStore
  implements cluster_service_discovery_daemon_state_store_i
{
  private state: cluster_service_discovery_daemon_state_snapshot_t;

  constructor(params: { daemon_id?: string } = {}) {
    this.state = BuildEmptyStateSnapshot({
      daemon_id: params.daemon_id
    });
  }

  loadState(): cluster_service_discovery_daemon_state_snapshot_t {
    return structuredClone(this.state);
  }

  saveState(params: {
    state: cluster_service_discovery_daemon_state_snapshot_t;
  }): void {
    this.state = NormalizeStateSnapshot({
      state: params.state,
      fallback_daemon_id: this.state.daemon_metadata.daemon_id
    });
  }

  compactState(params: {
    state: cluster_service_discovery_daemon_state_snapshot_t;
    retention_policy: cluster_service_discovery_daemon_state_retention_policy_t;
  }): cluster_service_discovery_daemon_state_snapshot_t {
    this.state = ApplyRetentionPolicy({
      state: params.state,
      retention_policy: params.retention_policy
    });

    return structuredClone(this.state);
  }
}

export class ClusterServiceDiscoveryDaemonFileStateStore
  implements cluster_service_discovery_daemon_state_store_i
{
  private readonly file_path: string;
  private readonly fallback_daemon_id: string | undefined;

  constructor(params: cluster_service_discovery_daemon_file_state_store_constructor_params_t & {
    daemon_id?: string;
  }) {
    if (typeof params.file_path !== 'string' || params.file_path.length === 0) {
      throw new Error('file_path must be a non-empty string.');
    }

    this.file_path = params.file_path;
    this.fallback_daemon_id = params.daemon_id;
  }

  loadState(): cluster_service_discovery_daemon_state_snapshot_t {
    if (!existsSync(this.file_path)) {
      return BuildEmptyStateSnapshot({
        daemon_id: this.fallback_daemon_id
      });
    }

    let parsed_state: unknown;

    try {
      parsed_state = JSON.parse(readFileSync(this.file_path, 'utf8')) as unknown;
    } catch (error) {
      throw new Error(
        `Failed to load discovery daemon state from "${this.file_path}": ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }

    return NormalizeStateSnapshot({
      state: parsed_state,
      fallback_daemon_id: this.fallback_daemon_id
    });
  }

  saveState(params: {
    state: cluster_service_discovery_daemon_state_snapshot_t;
  }): void {
    const normalized_state = NormalizeStateSnapshot({
      state: params.state,
      fallback_daemon_id: this.fallback_daemon_id
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
    state: cluster_service_discovery_daemon_state_snapshot_t;
    retention_policy: cluster_service_discovery_daemon_state_retention_policy_t;
  }): cluster_service_discovery_daemon_state_snapshot_t {
    const compacted_state = ApplyRetentionPolicy({
      state: params.state,
      retention_policy: params.retention_policy
    });

    this.saveState({
      state: compacted_state
    });

    return compacted_state;
  }
}
