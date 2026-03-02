export type cluster_service_discovery_node_status_t =
  | 'ready'
  | 'degraded'
  | 'restarting'
  | 'stopped'
  | 'expired';

export type cluster_service_discovery_node_metrics_t = {
  inflight_calls: number;
  pending_calls: number;
  success_rate_1m: number;
  timeout_rate_1m: number;
  ewma_latency_ms: number;
};

export type cluster_service_discovery_node_capability_t = {
  function_name: string;
  function_hash_sha1: string;
  installed: boolean;
};

export type cluster_service_discovery_node_identity_t = {
  node_id: string;
  address: {
    host: string;
    port: number;
    request_path: string;
    tls_mode: 'disabled' | 'required' | 'terminated_upstream';
  };
  labels?: Record<string, string>;
  zones?: string[];
  node_agent_semver?: string;
  runtime_semver?: string;
};

export type cluster_service_discovery_node_record_t = {
  node_identity: cluster_service_discovery_node_identity_t;
  status: cluster_service_discovery_node_status_t;
  metrics: cluster_service_discovery_node_metrics_t;
  capability_list: cluster_service_discovery_node_capability_t[];
  created_unix_ms: number;
  updated_unix_ms: number;
  last_heartbeat_unix_ms: number;
  lease_expires_unix_ms: number;
};

export type cluster_service_discovery_event_name_t =
  | 'node_registered'
  | 'node_heartbeat_updated'
  | 'node_capability_updated'
  | 'node_expired'
  | 'node_removed';

export type cluster_service_discovery_event_t = {
  event_id: string;
  event_name: cluster_service_discovery_event_name_t;
  timestamp_unix_ms: number;
  node_id: string;
  details?: Record<string, unknown>;
};

export type cluster_service_discovery_metrics_t = {
  node_registered_total: number;
  node_heartbeat_updated_total: number;
  node_capability_updated_total: number;
  node_expired_total: number;
  node_removed_total: number;
  node_count: number;
  active_node_count: number;
  expired_node_count: number;
  status_count_by_state: Record<cluster_service_discovery_node_status_t, number>;
  max_heartbeat_lag_ms: number;
  average_heartbeat_lag_ms: number;
  last_expiration_sweep_unix_ms: number | null;
  update_error_total: number;
};

export type cluster_service_discovery_config_t = {
  default_lease_ttl_ms: number;
  expiration_check_interval_ms: number;
  event_history_max_count: number;
};

export interface cluster_service_discovery_store_i {
  setConfig(params: {
    config: Partial<cluster_service_discovery_config_t>;
  }): void;

  getConfig(): cluster_service_discovery_config_t;

  upsertNodeRegistration(params: {
    node_identity: cluster_service_discovery_node_identity_t;
    status: Exclude<cluster_service_discovery_node_status_t, 'expired'>;
    metrics?: Partial<cluster_service_discovery_node_metrics_t>;
    capability_list?: cluster_service_discovery_node_capability_t[];
    lease_ttl_ms?: number;
    now_unix_ms?: number;
  }): cluster_service_discovery_node_record_t;

  updateNodeHeartbeat(params: {
    node_id: string;
    status: Exclude<cluster_service_discovery_node_status_t, 'expired'>;
    metrics: Partial<cluster_service_discovery_node_metrics_t>;
    lease_ttl_ms?: number;
    now_unix_ms?: number;
  }): cluster_service_discovery_node_record_t;

  updateNodeCapabilities(params: {
    node_id: string;
    capability_list: cluster_service_discovery_node_capability_t[];
    now_unix_ms?: number;
  }): cluster_service_discovery_node_record_t;

  removeNode(params: {
    node_id: string;
    now_unix_ms?: number;
    reason?: string;
  }): boolean;

  getNodeById(params: {
    node_id: string;
    include_expired?: boolean;
  }): cluster_service_discovery_node_record_t | null;

  listNodes(params?: {
    include_expired?: boolean;
  }): cluster_service_discovery_node_record_t[];

  queryNodes(params?: {
    include_expired?: boolean;
    status_list?: cluster_service_discovery_node_status_t[];
    label_match?: Record<string, string>;
    zone?: string;
    capability_function_name?: string;
    capability_function_hash_sha1?: string;
  }): cluster_service_discovery_node_record_t[];

  expireStaleNodes(params?: {
    now_unix_ms?: number;
  }): {
    expired_node_id_list: string[];
  };

  startExpirationLoop(): void;

  stopExpirationLoop(): void;

  getMetrics(): cluster_service_discovery_metrics_t;

  getRecentEvents(params?: {
    limit?: number;
    event_name?: cluster_service_discovery_event_name_t;
    node_id?: string;
  }): cluster_service_discovery_event_t[];

  onDiscoveryEvent(params: {
    listener: (event: cluster_service_discovery_event_t) => void;
  }): number;

  offDiscoveryEvent(params: {
    listener_id: number;
  }): void;
}

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function NormalizeFiniteNumber(params: {
  value: unknown;
  fallback_value: number;
}): number {
  const { value, fallback_value } = params;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return fallback_value;
  }

  return value;
}

function NormalizeNodeMetrics(params: {
  metrics?: Partial<cluster_service_discovery_node_metrics_t>;
}): cluster_service_discovery_node_metrics_t {
  const metrics = params.metrics ?? {};

  return {
    inflight_calls: Math.max(
      0,
      NormalizeFiniteNumber({ value: metrics.inflight_calls, fallback_value: 0 })
    ),
    pending_calls: Math.max(
      0,
      NormalizeFiniteNumber({ value: metrics.pending_calls, fallback_value: 0 })
    ),
    success_rate_1m: Math.max(
      0,
      Math.min(1, NormalizeFiniteNumber({ value: metrics.success_rate_1m, fallback_value: 1 }))
    ),
    timeout_rate_1m: Math.max(
      0,
      Math.min(1, NormalizeFiniteNumber({ value: metrics.timeout_rate_1m, fallback_value: 0 }))
    ),
    ewma_latency_ms: Math.max(
      0,
      NormalizeFiniteNumber({ value: metrics.ewma_latency_ms, fallback_value: 0 })
    )
  };
}

function ValidatePositiveInteger(params: { value: number; label: string }): void {
  if (!Number.isInteger(params.value) || params.value <= 0) {
    throw new Error(`${params.label} must be a positive integer.`);
  }
}

function ValidateNodeIdentity(params: {
  node_identity: cluster_service_discovery_node_identity_t;
}): void {
  const { node_identity } = params;

  if (!IsRecordObject(node_identity)) {
    throw new Error('node_identity must be an object.');
  }

  if (typeof node_identity.node_id !== 'string' || node_identity.node_id.length === 0) {
    throw new Error('node_identity.node_id must be a non-empty string.');
  }

  const address = node_identity.address;
  if (!IsRecordObject(address)) {
    throw new Error('node_identity.address must be an object.');
  }

  if (typeof address.host !== 'string' || address.host.length === 0) {
    throw new Error('node_identity.address.host must be a non-empty string.');
  }

  if (!Number.isInteger(address.port) || address.port < 0 || address.port > 65535) {
    throw new Error('node_identity.address.port must be an integer between 0 and 65535.');
  }

  if (
    typeof address.request_path !== 'string' ||
    address.request_path.length === 0 ||
    !address.request_path.startsWith('/')
  ) {
    throw new Error(
      'node_identity.address.request_path must be a non-empty path string starting with "/".'
    );
  }

  if (
    address.tls_mode !== 'disabled' &&
    address.tls_mode !== 'required' &&
    address.tls_mode !== 'terminated_upstream'
  ) {
    throw new Error(
      'node_identity.address.tls_mode must be one of disabled|required|terminated_upstream.'
    );
  }
}

function NormalizeCapabilityList(params: {
  capability_list?: cluster_service_discovery_node_capability_t[];
}): cluster_service_discovery_node_capability_t[] {
  if (!Array.isArray(params.capability_list)) {
    return [];
  }

  return params.capability_list
    .filter((capability): capability is cluster_service_discovery_node_capability_t => {
      return (
        IsRecordObject(capability) &&
        typeof capability.function_name === 'string' &&
        capability.function_name.length > 0 &&
        typeof capability.function_hash_sha1 === 'string' &&
        capability.function_hash_sha1.length > 0 &&
        typeof capability.installed === 'boolean'
      );
    })
    .map((capability) => {
      return {
        function_name: capability.function_name,
        function_hash_sha1: capability.function_hash_sha1,
        installed: capability.installed
      };
    });
}

function MatchLabelSet(params: {
  node_labels: Record<string, string> | undefined;
  label_match: Record<string, string> | undefined;
}): boolean {
  const { node_labels, label_match } = params;
  if (!label_match || Object.keys(label_match).length === 0) {
    return true;
  }

  if (!node_labels) {
    return false;
  }

  for (const [label_key, label_value] of Object.entries(label_match)) {
    if (node_labels[label_key] !== label_value) {
      return false;
    }
  }

  return true;
}

function MatchZone(params: {
  node_zones: string[] | undefined;
  zone: string | undefined;
}): boolean {
  const { node_zones, zone } = params;
  if (typeof zone !== 'string' || zone.length === 0) {
    return true;
  }

  if (!Array.isArray(node_zones)) {
    return false;
  }

  return node_zones.includes(zone);
}

function MatchCapability(params: {
  capability_list: cluster_service_discovery_node_capability_t[];
  capability_function_name: string | undefined;
  capability_function_hash_sha1: string | undefined;
}): boolean {
  const { capability_list, capability_function_name, capability_function_hash_sha1 } =
    params;

  if (
    typeof capability_function_name !== 'string' ||
    capability_function_name.length === 0
  ) {
    return true;
  }

  for (const capability of capability_list) {
    if (!capability.installed) {
      continue;
    }

    if (capability.function_name !== capability_function_name) {
      continue;
    }

    if (
      typeof capability_function_hash_sha1 === 'string' &&
      capability_function_hash_sha1.length > 0 &&
      capability.function_hash_sha1 !== capability_function_hash_sha1
    ) {
      continue;
    }

    return true;
  }

  return false;
}

export class ClusterInMemoryServiceDiscoveryStore
  implements cluster_service_discovery_store_i
{
  private discovery_config: cluster_service_discovery_config_t = {
    default_lease_ttl_ms: 15_000,
    expiration_check_interval_ms: 1_000,
    event_history_max_count: 2_000
  };

  private node_record_by_id = new Map<string, cluster_service_discovery_node_record_t>();
  private recent_event_history: cluster_service_discovery_event_t[] = [];
  private event_listener_by_id = new Map<
    number,
    (event: cluster_service_discovery_event_t) => void
  >();

  private next_listener_id = 1;
  private next_event_id = 1;

  private expiration_interval_handle: NodeJS.Timeout | null = null;
  private expiration_loop_reference_count = 0;

  private node_registered_total = 0;
  private node_heartbeat_updated_total = 0;
  private node_capability_updated_total = 0;
  private node_expired_total = 0;
  private node_removed_total = 0;
  private update_error_total = 0;
  private last_expiration_sweep_unix_ms: number | null = null;

  setConfig(params: { config: Partial<cluster_service_discovery_config_t> }): void {
    const { config } = params;

    if (typeof config.default_lease_ttl_ms === 'number') {
      ValidatePositiveInteger({
        value: config.default_lease_ttl_ms,
        label: 'default_lease_ttl_ms'
      });
      this.discovery_config.default_lease_ttl_ms = config.default_lease_ttl_ms;
    }

    if (typeof config.expiration_check_interval_ms === 'number') {
      ValidatePositiveInteger({
        value: config.expiration_check_interval_ms,
        label: 'expiration_check_interval_ms'
      });
      this.discovery_config.expiration_check_interval_ms =
        config.expiration_check_interval_ms;

      if (this.expiration_interval_handle) {
        this.stopExpirationInterval();
        this.startExpirationInterval();
      }
    }

    if (typeof config.event_history_max_count === 'number') {
      ValidatePositiveInteger({
        value: config.event_history_max_count,
        label: 'event_history_max_count'
      });
      this.discovery_config.event_history_max_count = config.event_history_max_count;
      this.truncateEventHistory();
    }
  }

  getConfig(): cluster_service_discovery_config_t {
    return {
      ...this.discovery_config
    };
  }

  upsertNodeRegistration(params: {
    node_identity: cluster_service_discovery_node_identity_t;
    status: Exclude<cluster_service_discovery_node_status_t, 'expired'>;
    metrics?: Partial<cluster_service_discovery_node_metrics_t>;
    capability_list?: cluster_service_discovery_node_capability_t[];
    lease_ttl_ms?: number;
    now_unix_ms?: number;
  }): cluster_service_discovery_node_record_t {
    const {
      node_identity,
      status,
      metrics,
      capability_list,
      lease_ttl_ms,
      now_unix_ms = Date.now()
    } = params;

    ValidateNodeIdentity({ node_identity });

    const effective_lease_ttl_ms =
      typeof lease_ttl_ms === 'number' ? lease_ttl_ms : this.discovery_config.default_lease_ttl_ms;
    ValidatePositiveInteger({
      value: effective_lease_ttl_ms,
      label: 'lease_ttl_ms'
    });

    const existing_record = this.node_record_by_id.get(node_identity.node_id);
    const normalized_metrics = NormalizeNodeMetrics({ metrics });
    const normalized_capability_list = NormalizeCapabilityList({ capability_list });

    const next_record: cluster_service_discovery_node_record_t = {
      node_identity: {
        node_id: node_identity.node_id,
        address: {
          host: node_identity.address.host,
          port: node_identity.address.port,
          request_path: node_identity.address.request_path,
          tls_mode: node_identity.address.tls_mode
        },
        labels: node_identity.labels ? { ...node_identity.labels } : undefined,
        zones: Array.isArray(node_identity.zones) ? [...node_identity.zones] : undefined,
        node_agent_semver: node_identity.node_agent_semver,
        runtime_semver: node_identity.runtime_semver
      },
      status,
      metrics: normalized_metrics,
      capability_list:
        normalized_capability_list.length > 0
          ? normalized_capability_list
          : existing_record?.capability_list ?? [],
      created_unix_ms: existing_record?.created_unix_ms ?? now_unix_ms,
      updated_unix_ms: now_unix_ms,
      last_heartbeat_unix_ms: now_unix_ms,
      lease_expires_unix_ms: now_unix_ms + effective_lease_ttl_ms
    };

    this.node_record_by_id.set(node_identity.node_id, next_record);

    this.node_registered_total += 1;
    this.emitDiscoveryEvent({
      event_name: 'node_registered',
      node_id: node_identity.node_id,
      timestamp_unix_ms: now_unix_ms,
      details: {
        status,
        lease_expires_unix_ms: next_record.lease_expires_unix_ms,
        updated_existing_node: Boolean(existing_record)
      }
    });

    return structuredClone(next_record);
  }

  updateNodeHeartbeat(params: {
    node_id: string;
    status: Exclude<cluster_service_discovery_node_status_t, 'expired'>;
    metrics: Partial<cluster_service_discovery_node_metrics_t>;
    lease_ttl_ms?: number;
    now_unix_ms?: number;
  }): cluster_service_discovery_node_record_t {
    const {
      node_id,
      status,
      metrics,
      lease_ttl_ms,
      now_unix_ms = Date.now()
    } = params;

    const existing_record = this.node_record_by_id.get(node_id);
    if (!existing_record) {
      this.update_error_total += 1;
      throw new Error(`Cannot update heartbeat. Node "${node_id}" is not registered.`);
    }

    const effective_lease_ttl_ms =
      typeof lease_ttl_ms === 'number' ? lease_ttl_ms : this.discovery_config.default_lease_ttl_ms;
    ValidatePositiveInteger({
      value: effective_lease_ttl_ms,
      label: 'lease_ttl_ms'
    });

    const normalized_metrics = NormalizeNodeMetrics({
      metrics: {
        ...existing_record.metrics,
        ...metrics
      }
    });

    const next_record: cluster_service_discovery_node_record_t = {
      ...existing_record,
      status,
      metrics: normalized_metrics,
      updated_unix_ms: now_unix_ms,
      last_heartbeat_unix_ms: now_unix_ms,
      lease_expires_unix_ms: now_unix_ms + effective_lease_ttl_ms
    };

    this.node_record_by_id.set(node_id, next_record);
    this.node_heartbeat_updated_total += 1;

    this.emitDiscoveryEvent({
      event_name: 'node_heartbeat_updated',
      node_id,
      timestamp_unix_ms: now_unix_ms,
      details: {
        status,
        lease_expires_unix_ms: next_record.lease_expires_unix_ms
      }
    });

    return structuredClone(next_record);
  }

  updateNodeCapabilities(params: {
    node_id: string;
    capability_list: cluster_service_discovery_node_capability_t[];
    now_unix_ms?: number;
  }): cluster_service_discovery_node_record_t {
    const { node_id, capability_list, now_unix_ms = Date.now() } = params;

    const existing_record = this.node_record_by_id.get(node_id);
    if (!existing_record) {
      this.update_error_total += 1;
      throw new Error(`Cannot update capabilities. Node "${node_id}" is not registered.`);
    }

    const normalized_capability_list = NormalizeCapabilityList({ capability_list });

    const next_record: cluster_service_discovery_node_record_t = {
      ...existing_record,
      capability_list: normalized_capability_list,
      updated_unix_ms: now_unix_ms
    };

    this.node_record_by_id.set(node_id, next_record);
    this.node_capability_updated_total += 1;

    this.emitDiscoveryEvent({
      event_name: 'node_capability_updated',
      node_id,
      timestamp_unix_ms: now_unix_ms,
      details: {
        capability_count: normalized_capability_list.length
      }
    });

    return structuredClone(next_record);
  }

  removeNode(params: {
    node_id: string;
    now_unix_ms?: number;
    reason?: string;
  }): boolean {
    const { node_id, now_unix_ms = Date.now(), reason } = params;

    const existed = this.node_record_by_id.delete(node_id);
    if (!existed) {
      return false;
    }

    this.node_removed_total += 1;
    this.emitDiscoveryEvent({
      event_name: 'node_removed',
      node_id,
      timestamp_unix_ms: now_unix_ms,
      details: {
        reason
      }
    });

    return true;
  }

  getNodeById(params: {
    node_id: string;
    include_expired?: boolean;
  }): cluster_service_discovery_node_record_t | null {
    const { node_id, include_expired = false } = params;
    const node_record = this.node_record_by_id.get(node_id);
    if (!node_record) {
      return null;
    }

    if (!include_expired && node_record.status === 'expired') {
      return null;
    }

    return structuredClone(node_record);
  }

  listNodes(params: {
    include_expired?: boolean;
  } = {}): cluster_service_discovery_node_record_t[] {
    const include_expired = params.include_expired === true;

    return Array.from(this.node_record_by_id.values())
      .filter((node_record): boolean => {
        if (include_expired) {
          return true;
        }

        return node_record.status !== 'expired';
      })
      .sort((left_node, right_node): number => {
        return left_node.node_identity.node_id.localeCompare(
          right_node.node_identity.node_id
        );
      })
      .map((node_record): cluster_service_discovery_node_record_t => {
        return structuredClone(node_record);
      });
  }

  queryNodes(params: {
    include_expired?: boolean;
    status_list?: cluster_service_discovery_node_status_t[];
    label_match?: Record<string, string>;
    zone?: string;
    capability_function_name?: string;
    capability_function_hash_sha1?: string;
  } = {}): cluster_service_discovery_node_record_t[] {
    const include_expired = params.include_expired === true;
    const status_set =
      Array.isArray(params.status_list) && params.status_list.length > 0
        ? new Set<cluster_service_discovery_node_status_t>(params.status_list)
        : null;

    return Array.from(this.node_record_by_id.values())
      .filter((node_record): boolean => {
        if (!include_expired && node_record.status === 'expired') {
          return false;
        }

        if (status_set && !status_set.has(node_record.status)) {
          return false;
        }

        if (
          !MatchLabelSet({
            node_labels: node_record.node_identity.labels,
            label_match: params.label_match
          })
        ) {
          return false;
        }

        if (
          !MatchZone({
            node_zones: node_record.node_identity.zones,
            zone: params.zone
          })
        ) {
          return false;
        }

        if (
          !MatchCapability({
            capability_list: node_record.capability_list,
            capability_function_name: params.capability_function_name,
            capability_function_hash_sha1: params.capability_function_hash_sha1
          })
        ) {
          return false;
        }

        return true;
      })
      .sort((left_node, right_node): number => {
        return left_node.node_identity.node_id.localeCompare(
          right_node.node_identity.node_id
        );
      })
      .map((node_record): cluster_service_discovery_node_record_t => {
        return structuredClone(node_record);
      });
  }

  expireStaleNodes(params: { now_unix_ms?: number } = {}): {
    expired_node_id_list: string[];
  } {
    const now_unix_ms = params.now_unix_ms ?? Date.now();
    const expired_node_id_list: string[] = [];

    for (const [node_id, node_record] of this.node_record_by_id.entries()) {
      if (
        node_record.status !== 'expired' &&
        node_record.lease_expires_unix_ms <= now_unix_ms
      ) {
        node_record.status = 'expired';
        node_record.updated_unix_ms = now_unix_ms;
        expired_node_id_list.push(node_id);

        this.node_expired_total += 1;
        this.emitDiscoveryEvent({
          event_name: 'node_expired',
          node_id,
          timestamp_unix_ms: now_unix_ms,
          details: {
            lease_expires_unix_ms: node_record.lease_expires_unix_ms,
            last_heartbeat_unix_ms: node_record.last_heartbeat_unix_ms
          }
        });
      }
    }

    this.last_expiration_sweep_unix_ms = now_unix_ms;

    return {
      expired_node_id_list
    };
  }

  startExpirationLoop(): void {
    this.expiration_loop_reference_count += 1;

    if (this.expiration_interval_handle) {
      return;
    }

    this.startExpirationInterval();
  }

  stopExpirationLoop(): void {
    this.expiration_loop_reference_count = Math.max(
      0,
      this.expiration_loop_reference_count - 1
    );

    if (
      this.expiration_loop_reference_count === 0 &&
      this.expiration_interval_handle
    ) {
      this.stopExpirationInterval();
    }
  }

  getMetrics(): cluster_service_discovery_metrics_t {
    const status_count_by_state: Record<cluster_service_discovery_node_status_t, number> = {
      ready: 0,
      degraded: 0,
      restarting: 0,
      stopped: 0,
      expired: 0
    };

    for (const node_record of this.node_record_by_id.values()) {
      status_count_by_state[node_record.status] += 1;
    }

    const node_count = this.node_record_by_id.size;
    const expired_node_count = status_count_by_state.expired;
    const now_unix_ms = Date.now();
    let max_heartbeat_lag_ms = 0;
    let heartbeat_lag_sum_ms = 0;

    for (const node_record of this.node_record_by_id.values()) {
      const node_heartbeat_lag_ms = Math.max(
        0,
        now_unix_ms - node_record.last_heartbeat_unix_ms
      );
      heartbeat_lag_sum_ms += node_heartbeat_lag_ms;
      max_heartbeat_lag_ms = Math.max(max_heartbeat_lag_ms, node_heartbeat_lag_ms);
    }

    return {
      node_registered_total: this.node_registered_total,
      node_heartbeat_updated_total: this.node_heartbeat_updated_total,
      node_capability_updated_total: this.node_capability_updated_total,
      node_expired_total: this.node_expired_total,
      node_removed_total: this.node_removed_total,
      node_count,
      active_node_count: Math.max(0, node_count - expired_node_count),
      expired_node_count,
      status_count_by_state,
      max_heartbeat_lag_ms,
      average_heartbeat_lag_ms:
        node_count > 0 ? heartbeat_lag_sum_ms / node_count : 0,
      last_expiration_sweep_unix_ms: this.last_expiration_sweep_unix_ms,
      update_error_total: this.update_error_total
    };
  }

  getRecentEvents(params: {
    limit?: number;
    event_name?: cluster_service_discovery_event_name_t;
    node_id?: string;
  } = {}): cluster_service_discovery_event_t[] {
    let event_list = [...this.recent_event_history];

    if (typeof params.event_name === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.event_name === params.event_name;
      });
    }

    if (typeof params.node_id === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.node_id === params.node_id;
      });
    }

    if (typeof params.limit === 'number' && params.limit >= 0) {
      event_list = event_list.slice(Math.max(0, event_list.length - params.limit));
    }

    return event_list.map((event): cluster_service_discovery_event_t => {
      return {
        ...event,
        details: event.details ? { ...event.details } : undefined
      };
    });
  }

  onDiscoveryEvent(params: {
    listener: (event: cluster_service_discovery_event_t) => void;
  }): number {
    const { listener } = params;

    const listener_id = this.next_listener_id;
    this.next_listener_id += 1;

    this.event_listener_by_id.set(listener_id, listener);
    return listener_id;
  }

  offDiscoveryEvent(params: { listener_id: number }): void {
    this.event_listener_by_id.delete(params.listener_id);
  }

  private emitDiscoveryEvent(params: {
    event_name: cluster_service_discovery_event_name_t;
    node_id: string;
    timestamp_unix_ms: number;
    details?: Record<string, unknown>;
  }): void {
    const event: cluster_service_discovery_event_t = {
      event_id: `discovery_event_${this.next_event_id}`,
      event_name: params.event_name,
      timestamp_unix_ms: params.timestamp_unix_ms,
      node_id: params.node_id,
      details: params.details ? { ...params.details } : undefined
    };

    this.next_event_id += 1;

    this.recent_event_history.push(event);
    this.truncateEventHistory();

    for (const listener of this.event_listener_by_id.values()) {
      try {
        listener(event);
      } catch {
        // Listener failures must not interrupt discovery flow.
      }
    }
  }

  private truncateEventHistory(): void {
    if (this.recent_event_history.length <= this.discovery_config.event_history_max_count) {
      return;
    }

    this.recent_event_history.splice(
      0,
      this.recent_event_history.length - this.discovery_config.event_history_max_count
    );
  }

  private startExpirationInterval(): void {
    this.expiration_interval_handle = setInterval((): void => {
      try {
        this.expireStaleNodes();
      } catch {
        this.update_error_total += 1;
      }
    }, this.discovery_config.expiration_check_interval_ms);

    this.expiration_interval_handle.unref();
  }

  private stopExpirationInterval(): void {
    if (!this.expiration_interval_handle) {
      return;
    }

    clearInterval(this.expiration_interval_handle);
    this.expiration_interval_handle = null;
  }
}
