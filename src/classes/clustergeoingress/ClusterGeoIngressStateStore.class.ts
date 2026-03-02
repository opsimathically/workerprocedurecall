import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync
} from 'node:fs';
import { dirname } from 'node:path';

import type {
  cluster_geo_ingress_event_t,
  cluster_geo_ingress_global_routing_policy_snapshot_t,
  cluster_geo_ingress_instance_record_t,
  cluster_geo_ingress_policy_version_record_t,
  cluster_geo_ingress_region_record_t
} from './ClusterGeoIngressProtocol';

export type cluster_geo_ingress_state_snapshot_t = {
  schema_version: 1;
  updated_unix_ms: number;
  service_generation: number;
  region_record_by_id: Record<string, cluster_geo_ingress_region_record_t>;
  ingress_instance_record_by_id: Record<string, cluster_geo_ingress_instance_record_t>;
  policy_version_record_by_id: Record<string, cluster_geo_ingress_policy_version_record_t>;
  active_policy_version_id: string | null;
  active_policy_snapshot: cluster_geo_ingress_global_routing_policy_snapshot_t;
  event_list: cluster_geo_ingress_event_t[];
  region_failover_state: {
    active_region_id_list: string[];
    degraded_region_id_list: string[];
    failover_reason: string | null;
    last_transition_unix_ms: number | null;
  };
};

export type cluster_geo_ingress_state_retention_policy_t = {
  event_history_max_count: number;
  offline_ingress_retention_ms: number;
};

export interface cluster_geo_ingress_state_store_i {
  loadState(): cluster_geo_ingress_state_snapshot_t;

  saveState(params: {
    state: cluster_geo_ingress_state_snapshot_t;
  }): void;

  compactState(params: {
    state: cluster_geo_ingress_state_snapshot_t;
    now_unix_ms: number;
    retention_policy: cluster_geo_ingress_state_retention_policy_t;
  }): cluster_geo_ingress_state_snapshot_t;
}

export type cluster_geo_ingress_file_state_store_constructor_params_t = {
  file_path: string;
};

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function BuildDefaultPolicySnapshot(): cluster_geo_ingress_global_routing_policy_snapshot_t {
  return {
    default_mode: 'latency_aware',
    retry_within_region_first: true,
    max_cross_region_attempts: 2,
    stickiness_scope: 'none'
  };
}

function BuildEmptyStateSnapshot(params?: {
  now_unix_ms?: number;
}): cluster_geo_ingress_state_snapshot_t {
  return {
    schema_version: 1,
    updated_unix_ms: params?.now_unix_ms ?? Date.now(),
    service_generation: 0,
    region_record_by_id: {},
    ingress_instance_record_by_id: {},
    policy_version_record_by_id: {},
    active_policy_version_id: null,
    active_policy_snapshot: BuildDefaultPolicySnapshot(),
    event_list: [],
    region_failover_state: {
      active_region_id_list: [],
      degraded_region_id_list: [],
      failover_reason: null,
      last_transition_unix_ms: null
    }
  };
}

function NormalizeStateSnapshot(params: {
  state: unknown;
}): cluster_geo_ingress_state_snapshot_t {
  if (!IsRecordObject(params.state)) {
    return BuildEmptyStateSnapshot();
  }

  const state = params.state;

  const normalized_state = BuildEmptyStateSnapshot({
    now_unix_ms:
      typeof state.updated_unix_ms === 'number' ? state.updated_unix_ms : Date.now()
  });

  normalized_state.service_generation =
    typeof state.service_generation === 'number' && Number.isInteger(state.service_generation)
      ? state.service_generation
      : 0;

  if (IsRecordObject(state.region_record_by_id)) {
    for (const [region_id, region_record] of Object.entries(state.region_record_by_id)) {
      if (!IsRecordObject(region_record)) {
        continue;
      }

      if (
        typeof region_record.region_id !== 'string' ||
        region_record.region_id.length === 0 ||
        region_record.region_id !== region_id ||
        typeof region_record.updated_unix_ms !== 'number'
      ) {
        continue;
      }

      normalized_state.region_record_by_id[region_id] =
        structuredClone(region_record) as cluster_geo_ingress_region_record_t;
    }
  }

  if (IsRecordObject(state.ingress_instance_record_by_id)) {
    for (const [ingress_id, ingress_record] of Object.entries(
      state.ingress_instance_record_by_id
    )) {
      if (!IsRecordObject(ingress_record)) {
        continue;
      }

      if (
        typeof ingress_record.ingress_id !== 'string' ||
        ingress_record.ingress_id.length === 0 ||
        ingress_record.ingress_id !== ingress_id ||
        typeof ingress_record.region_id !== 'string' ||
        typeof ingress_record.last_heartbeat_unix_ms !== 'number' ||
        typeof ingress_record.lease_expires_unix_ms !== 'number'
      ) {
        continue;
      }

      normalized_state.ingress_instance_record_by_id[ingress_id] =
        structuredClone(ingress_record) as cluster_geo_ingress_instance_record_t;
    }
  }

  if (IsRecordObject(state.policy_version_record_by_id)) {
    for (const [version_id, version_record] of Object.entries(
      state.policy_version_record_by_id
    )) {
      if (!IsRecordObject(version_record)) {
        continue;
      }

      if (
        typeof version_record.version_id !== 'string' ||
        version_record.version_id.length === 0 ||
        version_record.version_id !== version_id ||
        typeof version_record.created_unix_ms !== 'number' ||
        typeof version_record.actor_subject !== 'string' ||
        typeof version_record.checksum_sha1 !== 'string'
      ) {
        continue;
      }

      normalized_state.policy_version_record_by_id[version_id] =
        structuredClone(version_record) as cluster_geo_ingress_policy_version_record_t;
    }
  }

  if (
    typeof state.active_policy_version_id === 'string' &&
    state.active_policy_version_id.length > 0
  ) {
    normalized_state.active_policy_version_id = state.active_policy_version_id;
  }

  if (IsRecordObject(state.active_policy_snapshot)) {
    normalized_state.active_policy_snapshot =
      structuredClone(state.active_policy_snapshot) as cluster_geo_ingress_global_routing_policy_snapshot_t;
  }

  if (Array.isArray(state.event_list)) {
    normalized_state.event_list = state.event_list
      .filter((event): event is cluster_geo_ingress_event_t => {
        return (
          IsRecordObject(event) &&
          typeof event.event_id === 'string' &&
          typeof event.event_name === 'string' &&
          typeof event.timestamp_unix_ms === 'number'
        );
      })
      .map((event) => {
        return structuredClone(event) as cluster_geo_ingress_event_t;
      });
  }

  if (IsRecordObject(state.region_failover_state)) {
    const region_failover_state = state.region_failover_state;
    normalized_state.region_failover_state = {
      active_region_id_list: Array.isArray(region_failover_state.active_region_id_list)
        ? region_failover_state.active_region_id_list.filter(
            (region_id): region_id is string => {
              return typeof region_id === 'string' && region_id.length > 0;
            }
          )
        : [],
      degraded_region_id_list: Array.isArray(region_failover_state.degraded_region_id_list)
        ? region_failover_state.degraded_region_id_list.filter(
            (region_id): region_id is string => {
              return typeof region_id === 'string' && region_id.length > 0;
            }
          )
        : [],
      failover_reason:
        typeof region_failover_state.failover_reason === 'string'
          ? region_failover_state.failover_reason
          : null,
      last_transition_unix_ms:
        typeof region_failover_state.last_transition_unix_ms === 'number'
          ? region_failover_state.last_transition_unix_ms
          : null
    };
  }

  return normalized_state;
}

function ApplyRetentionPolicy(params: {
  state: cluster_geo_ingress_state_snapshot_t;
  now_unix_ms: number;
  retention_policy: cluster_geo_ingress_state_retention_policy_t;
}): cluster_geo_ingress_state_snapshot_t {
  const state = NormalizeStateSnapshot({
    state: params.state
  });

  for (const [ingress_id, ingress_record] of Object.entries(
    state.ingress_instance_record_by_id
  )) {
    if (ingress_record.health_status !== 'offline') {
      continue;
    }

    if (
      ingress_record.last_heartbeat_unix_ms +
        params.retention_policy.offline_ingress_retention_ms <=
      params.now_unix_ms
    ) {
      delete state.ingress_instance_record_by_id[ingress_id];
    }
  }

  state.event_list = state.event_list.slice(
    Math.max(0, state.event_list.length - params.retention_policy.event_history_max_count)
  );

  state.updated_unix_ms = params.now_unix_ms;
  return state;
}

export class ClusterGeoIngressInMemoryStateStore implements cluster_geo_ingress_state_store_i {
  private state: cluster_geo_ingress_state_snapshot_t = BuildEmptyStateSnapshot();

  loadState(): cluster_geo_ingress_state_snapshot_t {
    return structuredClone(this.state);
  }

  saveState(params: {
    state: cluster_geo_ingress_state_snapshot_t;
  }): void {
    this.state = NormalizeStateSnapshot({
      state: params.state
    });
  }

  compactState(params: {
    state: cluster_geo_ingress_state_snapshot_t;
    now_unix_ms: number;
    retention_policy: cluster_geo_ingress_state_retention_policy_t;
  }): cluster_geo_ingress_state_snapshot_t {
    this.state = ApplyRetentionPolicy({
      state: params.state,
      now_unix_ms: params.now_unix_ms,
      retention_policy: params.retention_policy
    });

    return structuredClone(this.state);
  }
}

export class ClusterGeoIngressFileStateStore implements cluster_geo_ingress_state_store_i {
  private readonly file_path: string;

  constructor(params: cluster_geo_ingress_file_state_store_constructor_params_t) {
    if (typeof params.file_path !== 'string' || params.file_path.length === 0) {
      throw new Error('file_path must be a non-empty string.');
    }

    this.file_path = params.file_path;
  }

  loadState(): cluster_geo_ingress_state_snapshot_t {
    if (!existsSync(this.file_path)) {
      return BuildEmptyStateSnapshot();
    }

    let parsed_state: unknown;
    try {
      parsed_state = JSON.parse(readFileSync(this.file_path, 'utf8')) as unknown;
    } catch (error) {
      throw new Error(
        `Failed to load geo ingress state from "${this.file_path}": ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }

    return NormalizeStateSnapshot({
      state: parsed_state
    });
  }

  saveState(params: {
    state: cluster_geo_ingress_state_snapshot_t;
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
    state: cluster_geo_ingress_state_snapshot_t;
    now_unix_ms: number;
    retention_policy: cluster_geo_ingress_state_retention_policy_t;
  }): cluster_geo_ingress_state_snapshot_t {
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
