import { connect, type ClientHttp2Session } from 'node:http2';
import {
  BuildHttpsAuthority,
  BuildTlsClientConnectOptions,
  ValidateTlsClientConfig,
  type cluster_tls_client_config_t
} from '../clustertransport/ClusterTlsSecurity.class';

import {
  ClusterInMemoryServiceDiscoveryStore,
  type cluster_service_discovery_config_t,
  type cluster_service_discovery_event_name_t,
  type cluster_service_discovery_event_t,
  type cluster_service_discovery_metrics_t,
  type cluster_service_discovery_node_capability_t,
  type cluster_service_discovery_node_identity_t,
  type cluster_service_discovery_node_metrics_t,
  type cluster_service_discovery_node_record_t,
  type cluster_service_discovery_node_status_t,
  type cluster_service_discovery_store_i
} from './ClusterServiceDiscoveryStore.class';
import type {
  cluster_service_discovery_protocol_error_t,
  cluster_service_discovery_protocol_request_message_type_t,
  cluster_service_discovery_redirect_endpoint_t,
  cluster_service_discovery_request_message_t,
  cluster_service_discovery_response_error_message_i,
  cluster_service_discovery_response_success_message_i
} from './ClusterServiceDiscoveryProtocol';
import {
  ParseClusterServiceDiscoveryResponseErrorMessage,
  ParseClusterServiceDiscoveryResponseSuccessMessage
} from './ClusterServiceDiscoveryProtocolValidators';

export type cluster_remote_service_discovery_auth_headers_provider_t = () =>
  | Record<string, string>
  | Promise<Record<string, string>>;

export type cluster_remote_service_discovery_endpoint_t = {
  daemon_id?: string;
  host: string;
  port: number;
  request_path?: string;
  tls_mode?: 'required';
};

export type cluster_remote_service_discovery_store_adapter_constructor_params_t = {
  host?: string;
  port?: number;
  request_path?: string;
  tls_mode?: 'required';
  endpoint_list?: cluster_remote_service_discovery_endpoint_t[];
  request_timeout_ms?: number;
  synchronization_interval_ms?: number;
  retry_base_delay_ms?: number;
  retry_max_delay_ms?: number;
  endpoint_cooldown_ms?: number;
  max_request_attempts?: number;
  auth_headers?: Record<string, string>;
  auth_headers_provider?: cluster_remote_service_discovery_auth_headers_provider_t;
  prefer_leader_reads?: boolean;
  transport_security?: cluster_tls_client_config_t;
};

type cluster_remote_discovery_pending_operation_t = {
  operation_id: number;
  request_message: cluster_service_discovery_request_message_t;
  retry_attempt: number;
  next_attempt_unix_ms: number;
};

type cluster_remote_service_discovery_adapter_metrics_t = {
  daemon_request_total: number;
  daemon_request_success_total: number;
  daemon_request_failed_total: number;
  daemon_request_retry_total: number;
  daemon_sync_total: number;
  daemon_sync_failed_total: number;
  last_daemon_sync_unix_ms: number | null;
  daemon_redirect_total: number;
  endpoint_failure_total: number;
};

type cluster_remote_discovery_endpoint_state_t = {
  endpoint_id: string;
  daemon_id: string | null;
  host: string;
  port: number;
  request_path: string;
  tls_mode: 'required';
  failure_count: number;
  last_failure_unix_ms: number | null;
  cooldown_until_unix_ms: number;
  last_success_unix_ms: number | null;
};

function BuildRequestPath(params: { request_path: string | undefined }): string {
  if (typeof params.request_path !== 'string' || params.request_path.length === 0) {
    return '/wpc/cluster/discovery';
  }

  if (params.request_path.startsWith('/')) {
    return params.request_path;
  }

  return `/${params.request_path}`;
}

function BuildTransportAuthority(params: {
  host: string;
  port: number;
  tls_mode: 'required';
}): string {
  if (params.tls_mode !== 'required') {
    throw new Error('Only tls_mode="required" is supported.');
  }

  return BuildHttpsAuthority({
    host: params.host,
    port: params.port
  });
}

function NormalizeHeaders(params: {
  headers: Record<string, string> | undefined;
}): Record<string, string> {
  const headers = params.headers ?? {};
  const normalized_headers: Record<string, string> = {};

  for (const [header_name, header_value] of Object.entries(headers)) {
    if (typeof header_name !== 'string' || header_name.length === 0) {
      continue;
    }

    if (typeof header_value !== 'string') {
      continue;
    }

    normalized_headers[header_name.toLowerCase()] = header_value;
  }

  return normalized_headers;
}

function BuildRequestId(params: {
  operation_id: number;
  operation_message_type: cluster_service_discovery_protocol_request_message_type_t;
}): string {
  return `discovery_adapter_${params.operation_message_type}_${params.operation_id}`;
}

function BuildRetryDelayMs(params: {
  retry_attempt: number;
  retry_base_delay_ms: number;
  retry_max_delay_ms: number;
}): number {
  const exponent = Math.max(0, params.retry_attempt - 1);
  const delay_ms = params.retry_base_delay_ms * Math.pow(2, exponent);
  return Math.min(params.retry_max_delay_ms, delay_ms);
}

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export class ClusterRemoteServiceDiscoveryStoreAdapter
  implements cluster_service_discovery_store_i
{
  private readonly local_cache_store = new ClusterInMemoryServiceDiscoveryStore();

  private request_timeout_ms: number;
  private synchronization_interval_ms: number;
  private retry_base_delay_ms: number;
  private retry_max_delay_ms: number;
  private endpoint_cooldown_ms: number;
  private max_request_attempts: number;
  private prefer_leader_reads: boolean;

  private readonly static_auth_headers: Record<string, string>;
  private readonly auth_headers_provider:
    | cluster_remote_service_discovery_auth_headers_provider_t
    | undefined;
  private readonly transport_security: cluster_tls_client_config_t;

  private endpoint_state_list: cluster_remote_discovery_endpoint_state_t[] = [];
  private known_leader_daemon_id: string | null = null;

  private synchronization_interval_handle: NodeJS.Timeout | null = null;
  private started_reference_count = 0;
  private operation_flush_in_progress = false;
  private synchronization_in_progress = false;

  private next_operation_id = 1;
  private pending_operation_queue: cluster_remote_discovery_pending_operation_t[] = [];

  private adapter_metrics: cluster_remote_service_discovery_adapter_metrics_t = {
    daemon_request_total: 0,
    daemon_request_success_total: 0,
    daemon_request_failed_total: 0,
    daemon_request_retry_total: 0,
    daemon_sync_total: 0,
    daemon_sync_failed_total: 0,
    last_daemon_sync_unix_ms: null,
    daemon_redirect_total: 0,
    endpoint_failure_total: 0
  };

  constructor(params: cluster_remote_service_discovery_store_adapter_constructor_params_t) {
    this.request_timeout_ms = params.request_timeout_ms ?? 3_000;
    this.synchronization_interval_ms = params.synchronization_interval_ms ?? 1_000;
    this.retry_base_delay_ms = params.retry_base_delay_ms ?? 100;
    this.retry_max_delay_ms = params.retry_max_delay_ms ?? 2_000;
    this.endpoint_cooldown_ms = params.endpoint_cooldown_ms ?? 2_000;
    this.max_request_attempts = params.max_request_attempts ?? 4;
    this.prefer_leader_reads = params.prefer_leader_reads !== false;

    this.static_auth_headers = NormalizeHeaders({
      headers: params.auth_headers
    });
    this.auth_headers_provider = params.auth_headers_provider;
    this.transport_security = ValidateTlsClientConfig({
      tls_client_config: params.transport_security,
      field_prefix: 'cluster_remote_service_discovery.transport_security'
    });

    const endpoint_list_from_params = Array.isArray(params.endpoint_list)
      ? params.endpoint_list
      : [];

    if (endpoint_list_from_params.length > 0) {
      this.setEndpointList({
        endpoint_list: endpoint_list_from_params
      });
    } else if (typeof params.host === 'string' && typeof params.port === 'number') {
      this.setEndpointList({
        endpoint_list: [
          {
            daemon_id: 'default',
            host: params.host,
            port: params.port,
            request_path: params.request_path,
            tls_mode: params.tls_mode
          }
        ]
      });
    } else {
      throw new Error('ClusterRemoteServiceDiscoveryStoreAdapter requires endpoint_list or host+port.');
    }
  }

  setEndpointList(params: {
    endpoint_list: cluster_remote_service_discovery_endpoint_t[];
  }): void {
    const next_endpoint_state_list: cluster_remote_discovery_endpoint_state_t[] = [];

    for (let endpoint_index = 0; endpoint_index < params.endpoint_list.length; endpoint_index += 1) {
      const endpoint = params.endpoint_list[endpoint_index];
      const request_path = BuildRequestPath({
        request_path: endpoint.request_path
      });

      const endpoint_id = `${endpoint.host}:${endpoint.port}${request_path}`;
      const existing_endpoint_state = this.endpoint_state_list.find((state): boolean => {
        return state.endpoint_id === endpoint_id;
      });

      next_endpoint_state_list.push({
        endpoint_id,
        daemon_id: endpoint.daemon_id ?? null,
        host: endpoint.host,
        port: endpoint.port,
        request_path,
        tls_mode: endpoint.tls_mode ?? 'required',
        failure_count: existing_endpoint_state?.failure_count ?? 0,
        last_failure_unix_ms: existing_endpoint_state?.last_failure_unix_ms ?? null,
        cooldown_until_unix_ms: existing_endpoint_state?.cooldown_until_unix_ms ?? 0,
        last_success_unix_ms: existing_endpoint_state?.last_success_unix_ms ?? null
      });
    }

    this.endpoint_state_list = next_endpoint_state_list;
  }

  setConfig(params: { config: Partial<cluster_service_discovery_config_t> }): void {
    this.local_cache_store.setConfig({
      config: params.config
    });

    if (typeof params.config.expiration_check_interval_ms === 'number') {
      this.synchronization_interval_ms = params.config.expiration_check_interval_ms;
      if (this.synchronization_interval_handle) {
        clearInterval(this.synchronization_interval_handle);
        this.startSynchronizationLoop();
      }
    }
  }

  getConfig(): cluster_service_discovery_config_t {
    return this.local_cache_store.getConfig();
  }

  upsertNodeRegistration(params: {
    node_identity: cluster_service_discovery_node_identity_t;
    status: Exclude<cluster_service_discovery_node_status_t, 'expired'>;
    metrics?: Partial<cluster_service_discovery_node_metrics_t>;
    capability_list?: cluster_service_discovery_node_capability_t[];
    lease_ttl_ms?: number;
    now_unix_ms?: number;
  }): cluster_service_discovery_node_record_t {
    const node_record = this.local_cache_store.upsertNodeRegistration(params);

    this.enqueueRequestMessage({
      request_message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_register_node',
        timestamp_unix_ms: params.now_unix_ms ?? Date.now(),
        request_id: BuildRequestId({
          operation_id: this.nextOperationId(),
          operation_message_type: 'cluster_service_discovery_register_node'
        }),
        node_identity: params.node_identity,
        status: params.status,
        metrics: params.metrics,
        capability_list: params.capability_list,
        lease_ttl_ms: params.lease_ttl_ms
      }
    });

    return node_record;
  }

  updateNodeHeartbeat(params: {
    node_id: string;
    status: Exclude<cluster_service_discovery_node_status_t, 'expired'>;
    metrics: Partial<cluster_service_discovery_node_metrics_t>;
    lease_ttl_ms?: number;
    now_unix_ms?: number;
  }): cluster_service_discovery_node_record_t {
    const node_record = this.local_cache_store.updateNodeHeartbeat(params);

    this.enqueueRequestMessage({
      request_message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_heartbeat_node',
        timestamp_unix_ms: params.now_unix_ms ?? Date.now(),
        request_id: BuildRequestId({
          operation_id: this.nextOperationId(),
          operation_message_type: 'cluster_service_discovery_heartbeat_node'
        }),
        node_id: params.node_id,
        status: params.status,
        metrics: params.metrics,
        lease_ttl_ms: params.lease_ttl_ms
      }
    });

    return node_record;
  }

  updateNodeCapabilities(params: {
    node_id: string;
    capability_list: cluster_service_discovery_node_capability_t[];
    now_unix_ms?: number;
  }): cluster_service_discovery_node_record_t {
    const node_record = this.local_cache_store.updateNodeCapabilities(params);

    this.enqueueRequestMessage({
      request_message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_update_capability',
        timestamp_unix_ms: params.now_unix_ms ?? Date.now(),
        request_id: BuildRequestId({
          operation_id: this.nextOperationId(),
          operation_message_type: 'cluster_service_discovery_update_capability'
        }),
        node_id: params.node_id,
        capability_list: params.capability_list
      }
    });

    return node_record;
  }

  removeNode(params: {
    node_id: string;
    now_unix_ms?: number;
    reason?: string;
  }): boolean {
    const removed = this.local_cache_store.removeNode(params);

    this.enqueueRequestMessage({
      request_message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_remove_node',
        timestamp_unix_ms: params.now_unix_ms ?? Date.now(),
        request_id: BuildRequestId({
          operation_id: this.nextOperationId(),
          operation_message_type: 'cluster_service_discovery_remove_node'
        }),
        node_id: params.node_id,
        reason: params.reason
      }
    });

    return removed;
  }

  getNodeById(params: {
    node_id: string;
    include_expired?: boolean;
  }): cluster_service_discovery_node_record_t | null {
    return this.local_cache_store.getNodeById(params);
  }

  listNodes(params: {
    include_expired?: boolean;
  } = {}): cluster_service_discovery_node_record_t[] {
    return this.local_cache_store.listNodes(params);
  }

  queryNodes(params: {
    include_expired?: boolean;
    status_list?: cluster_service_discovery_node_status_t[];
    label_match?: Record<string, string>;
    zone?: string;
    capability_function_name?: string;
    capability_function_hash_sha1?: string;
  } = {}): cluster_service_discovery_node_record_t[] {
    return this.local_cache_store.queryNodes(params);
  }

  expireStaleNodes(params: {
    now_unix_ms?: number;
  } = {}): {
    expired_node_id_list: string[];
  } {
    return this.local_cache_store.expireStaleNodes(params);
  }

  startExpirationLoop(): void {
    this.started_reference_count += 1;
    if (this.synchronization_interval_handle) {
      return;
    }

    this.startSynchronizationLoop();
  }

  stopExpirationLoop(): void {
    this.started_reference_count = Math.max(0, this.started_reference_count - 1);

    if (this.started_reference_count > 0) {
      return;
    }

    if (this.synchronization_interval_handle) {
      clearInterval(this.synchronization_interval_handle);
      this.synchronization_interval_handle = null;
    }
  }

  getMetrics(): cluster_service_discovery_metrics_t {
    const base_metrics = this.local_cache_store.getMetrics();

    return {
      ...base_metrics,
      update_error_total:
        base_metrics.update_error_total +
        this.adapter_metrics.daemon_request_failed_total +
        this.adapter_metrics.daemon_sync_failed_total
    };
  }

  getRecentEvents(params: {
    limit?: number;
    event_name?: cluster_service_discovery_event_name_t;
    node_id?: string;
  } = {}): cluster_service_discovery_event_t[] {
    return this.local_cache_store.getRecentEvents(params);
  }

  onDiscoveryEvent(params: {
    listener: (event: cluster_service_discovery_event_t) => void;
  }): number {
    return this.local_cache_store.onDiscoveryEvent(params);
  }

  offDiscoveryEvent(params: { listener_id: number }): void {
    this.local_cache_store.offDiscoveryEvent(params);
  }

  getAdapterMetrics(): cluster_remote_service_discovery_adapter_metrics_t {
    return {
      ...this.adapter_metrics
    };
  }

  private startSynchronizationLoop(): void {
    this.synchronization_interval_handle = setInterval((): void => {
      void this.flushPendingOperations();
      void this.synchronizeFromRemote();
    }, this.synchronization_interval_ms);

    this.synchronization_interval_handle.unref();

    void this.flushPendingOperations();
    void this.synchronizeFromRemote();
  }

  private nextOperationId(): number {
    const operation_id = this.next_operation_id;
    this.next_operation_id += 1;
    return operation_id;
  }

  private enqueueRequestMessage(params: {
    request_message: cluster_service_discovery_request_message_t;
  }): void {
    this.pending_operation_queue.push({
      operation_id: this.nextOperationId(),
      request_message: params.request_message,
      retry_attempt: 0,
      next_attempt_unix_ms: Date.now()
    });

    void this.flushPendingOperations();
  }

  private async flushPendingOperations(): Promise<void> {
    if (this.operation_flush_in_progress) {
      return;
    }

    this.operation_flush_in_progress = true;

    try {
      while (this.pending_operation_queue.length > 0) {
        const pending_operation = this.pending_operation_queue[0];

        if (pending_operation.next_attempt_unix_ms > Date.now()) {
          break;
        }

        try {
          await this.sendRequestMessage({
            request_message: pending_operation.request_message,
            force_leader: true
          });
          this.pending_operation_queue.shift();
        } catch (error) {
          pending_operation.retry_attempt += 1;
          pending_operation.next_attempt_unix_ms =
            Date.now() +
            BuildRetryDelayMs({
              retry_attempt: pending_operation.retry_attempt,
              retry_base_delay_ms: this.retry_base_delay_ms,
              retry_max_delay_ms: this.retry_max_delay_ms
            });
          this.adapter_metrics.daemon_request_retry_total += 1;

          const retryable =
            (error as Error & { retryable?: boolean }).retryable !== false;
          if (!retryable) {
            this.pending_operation_queue.shift();
          }

          break;
        }
      }
    } finally {
      this.operation_flush_in_progress = false;
    }
  }

  private async synchronizeFromRemote(): Promise<void> {
    if (this.synchronization_in_progress) {
      return;
    }

    this.synchronization_in_progress = true;

    try {
      const list_message: cluster_service_discovery_request_message_t = {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_list_nodes',
        timestamp_unix_ms: Date.now(),
        request_id: BuildRequestId({
          operation_id: this.nextOperationId(),
          operation_message_type: 'cluster_service_discovery_list_nodes'
        }),
        include_expired: false,
        consistency_mode: this.prefer_leader_reads ? 'leader_linearizable' : 'stale_ok'
      };

      const response_message = await this.sendRequestMessage({
        request_message: list_message,
        count_as_write_request: false,
        force_leader: this.prefer_leader_reads
      });

      const node_list = Array.isArray((response_message.data as { node_list?: unknown }).node_list)
        ? ((response_message.data as { node_list?: unknown }).node_list as cluster_service_discovery_node_record_t[])
        : [];

      this.replaceLocalCacheWithNodeList({
        node_list
      });

      this.adapter_metrics.daemon_sync_total += 1;
      this.adapter_metrics.last_daemon_sync_unix_ms = Date.now();
    } catch {
      this.adapter_metrics.daemon_sync_failed_total += 1;
    } finally {
      this.synchronization_in_progress = false;
    }
  }

  private replaceLocalCacheWithNodeList(params: {
    node_list: cluster_service_discovery_node_record_t[];
  }): void {
    const existing_node_id_set = new Set(
      this.local_cache_store
        .listNodes({
          include_expired: true
        })
        .map((node_record): string => node_record.node_identity.node_id)
    );

    const next_node_id_set = new Set<string>();

    for (const node_record of params.node_list) {
      if (!node_record || typeof node_record !== 'object') {
        continue;
      }

      if (node_record.status === 'expired') {
        continue;
      }

      next_node_id_set.add(node_record.node_identity.node_id);

      const lease_ttl_ms = Math.max(1, node_record.lease_expires_unix_ms - Date.now());

      this.local_cache_store.upsertNodeRegistration({
        node_identity: node_record.node_identity,
        status: node_record.status,
        metrics: node_record.metrics,
        capability_list: node_record.capability_list,
        lease_ttl_ms,
        now_unix_ms: node_record.last_heartbeat_unix_ms
      });
    }

    for (const existing_node_id of existing_node_id_set) {
      if (next_node_id_set.has(existing_node_id)) {
        continue;
      }

      this.local_cache_store.removeNode({
        node_id: existing_node_id,
        reason: 'remote_synchronization_prune'
      });
    }
  }

  private markEndpointFailure(params: {
    endpoint_state: cluster_remote_discovery_endpoint_state_t;
  }): void {
    params.endpoint_state.failure_count += 1;
    params.endpoint_state.last_failure_unix_ms = Date.now();
    params.endpoint_state.cooldown_until_unix_ms =
      Date.now() + this.endpoint_cooldown_ms;

    this.adapter_metrics.endpoint_failure_total += 1;
  }

  private markEndpointSuccess(params: {
    endpoint_state: cluster_remote_discovery_endpoint_state_t;
    response_message: cluster_service_discovery_response_success_message_i;
  }): void {
    params.endpoint_state.failure_count = 0;
    params.endpoint_state.cooldown_until_unix_ms = 0;
    params.endpoint_state.last_success_unix_ms = Date.now();

    const leader_id = params.response_message.ha_metadata?.leader_id;
    if (typeof leader_id === 'string' && leader_id.length > 0) {
      this.known_leader_daemon_id = leader_id;
    }

    if (
      typeof params.response_message.ha_metadata?.daemon_id === 'string' &&
      params.endpoint_state.daemon_id === null
    ) {
      params.endpoint_state.daemon_id = params.response_message.ha_metadata.daemon_id;
    }
  }

  private addOrUpdateRedirectEndpoint(params: {
    redirect_endpoint: cluster_service_discovery_redirect_endpoint_t;
  }): void {
    const redirect_endpoint = params.redirect_endpoint;
    const endpoint_id = `${redirect_endpoint.host}:${redirect_endpoint.port}${redirect_endpoint.request_path}`;

    const existing_endpoint_state = this.endpoint_state_list.find((endpoint_state): boolean => {
      return endpoint_state.endpoint_id === endpoint_id;
    });

    if (existing_endpoint_state) {
      existing_endpoint_state.daemon_id = redirect_endpoint.daemon_id ?? existing_endpoint_state.daemon_id;
      existing_endpoint_state.tls_mode = redirect_endpoint.tls_mode;
      existing_endpoint_state.request_path = redirect_endpoint.request_path;
      return;
    }

    this.endpoint_state_list.push({
      endpoint_id,
      daemon_id: redirect_endpoint.daemon_id ?? null,
      host: redirect_endpoint.host,
      port: redirect_endpoint.port,
      request_path: redirect_endpoint.request_path,
      tls_mode: redirect_endpoint.tls_mode,
      failure_count: 0,
      last_failure_unix_ms: null,
      cooldown_until_unix_ms: 0,
      last_success_unix_ms: null
    });
  }

  private selectCandidateEndpointList(params: {
    force_leader: boolean;
  }): cluster_remote_discovery_endpoint_state_t[] {
    const now_unix_ms = Date.now();
    const cooled_down_endpoint_list = this.endpoint_state_list.filter((endpoint_state): boolean => {
      return endpoint_state.cooldown_until_unix_ms <= now_unix_ms;
    });

    const all_endpoint_list = cooled_down_endpoint_list.length > 0
      ? cooled_down_endpoint_list
      : this.endpoint_state_list;

    const prioritized_endpoint_list: cluster_remote_discovery_endpoint_state_t[] = [];

    if (params.force_leader && this.known_leader_daemon_id) {
      const known_leader_endpoint = all_endpoint_list.find((endpoint_state): boolean => {
        return endpoint_state.daemon_id === this.known_leader_daemon_id;
      });

      if (known_leader_endpoint) {
        prioritized_endpoint_list.push(known_leader_endpoint);
      }
    }

    for (const endpoint_state of all_endpoint_list) {
      if (prioritized_endpoint_list.includes(endpoint_state)) {
        continue;
      }

      prioritized_endpoint_list.push(endpoint_state);
    }

    return prioritized_endpoint_list;
  }

  private async sendRequestMessage(params: {
    request_message: cluster_service_discovery_request_message_t;
    count_as_write_request?: boolean;
    force_leader?: boolean;
  }): Promise<cluster_service_discovery_response_success_message_i> {
    const count_as_write_request = params.count_as_write_request ?? true;
    const force_leader = params.force_leader ?? count_as_write_request;

    if (count_as_write_request) {
      this.adapter_metrics.daemon_request_total += 1;
    }

    let last_error: Error | null = null;

    const max_attempt_count = Math.max(
      1,
      Math.min(this.max_request_attempts, this.endpoint_state_list.length * 2)
    );

    for (let attempt_index = 0; attempt_index < max_attempt_count; attempt_index += 1) {
      const candidate_endpoint_list = this.selectCandidateEndpointList({
        force_leader
      });

      if (candidate_endpoint_list.length === 0) {
        break;
      }

      const endpoint_state =
        candidate_endpoint_list[attempt_index % candidate_endpoint_list.length];

      try {
        const response_message = await this.sendRequestMessageToEndpoint({
          endpoint_state,
          request_message: params.request_message
        });

        this.markEndpointSuccess({
          endpoint_state,
          response_message
        });

        if (count_as_write_request) {
          this.adapter_metrics.daemon_request_success_total += 1;
        }

        return response_message;
      } catch (error) {
        this.markEndpointFailure({
          endpoint_state
        });

        const typed_error = error as Error & {
          retryable?: boolean;
          code?: string;
          details?: Record<string, unknown>;
        };

        if (typed_error.code === 'DISCOVERY_NOT_LEADER') {
          this.adapter_metrics.daemon_redirect_total += 1;

          if (IsRecordObject(typed_error.details?.redirect_endpoint)) {
            const redirect_endpoint = typed_error.details
              ?.redirect_endpoint as cluster_service_discovery_redirect_endpoint_t;

            if (
              typeof redirect_endpoint.host === 'string' &&
              typeof redirect_endpoint.port === 'number' &&
              typeof redirect_endpoint.request_path === 'string' &&
              redirect_endpoint.tls_mode === 'required'
            ) {
              this.addOrUpdateRedirectEndpoint({
                redirect_endpoint
              });

              if (typeof redirect_endpoint.daemon_id === 'string') {
                this.known_leader_daemon_id = redirect_endpoint.daemon_id;
              }
            }
          }
        }

        last_error = typed_error;
        const retryable = typed_error.retryable !== false;

        if (!retryable) {
          break;
        }
      }
    }

    if (count_as_write_request) {
      this.adapter_metrics.daemon_request_failed_total += 1;
    }

    throw (
      last_error ??
      new Error('Discovery daemon request failed because no endpoint was available.')
    );
  }

  private async sendRequestMessageToEndpoint(params: {
    endpoint_state: cluster_remote_discovery_endpoint_state_t;
    request_message: cluster_service_discovery_request_message_t;
  }): Promise<cluster_service_discovery_response_success_message_i> {
    let client_session: ClientHttp2Session | null = null;

    try {
      client_session = connect(
        BuildTransportAuthority({
          host: params.endpoint_state.host,
          port: params.endpoint_state.port,
          tls_mode: params.endpoint_state.tls_mode
        }),
        BuildTlsClientConnectOptions({
          tls_client_config: this.transport_security
        })
      );

      client_session.on('error', (): void => {
        // request stream reports operation failures
      });

      const headers: Record<string, string> = {
        ':method': 'POST',
        ':path': params.endpoint_state.request_path,
        'content-type': 'application/json',
        ...this.static_auth_headers
      };

      if (this.auth_headers_provider) {
        const dynamic_headers = await this.auth_headers_provider();
        Object.assign(headers, NormalizeHeaders({ headers: dynamic_headers }));
      }

      const raw_response = await new Promise<string>((resolve, reject): void => {
        const request_stream = client_session?.request(headers);
        if (!request_stream) {
          reject(new Error('Failed to create discovery request stream.'));
          return;
        }

        const chunk_list: Buffer[] = [];

        const timeout_handle = setTimeout((): void => {
          try {
            request_stream.close();
          } catch {
            // ignore stream close errors
          }
          reject(new Error('Discovery daemon request timed out.'));
        }, this.request_timeout_ms);

        request_stream.on('data', (chunk): void => {
          if (typeof chunk === 'string') {
            chunk_list.push(Buffer.from(chunk));
            return;
          }

          chunk_list.push(chunk as Buffer);
        });

        request_stream.on('end', (): void => {
          clearTimeout(timeout_handle);
          resolve(Buffer.concat(chunk_list).toString('utf8'));
        });

        request_stream.on('error', (error): void => {
          clearTimeout(timeout_handle);
          reject(error);
        });

        request_stream.end(JSON.stringify(params.request_message));
      });

      const parsed_json = JSON.parse(raw_response) as unknown;

      const success_result = ParseClusterServiceDiscoveryResponseSuccessMessage({
        message: parsed_json
      });
      if (success_result.ok) {
        return success_result.value;
      }

      const error_result = ParseClusterServiceDiscoveryResponseErrorMessage({
        message: parsed_json
      });
      if (!error_result.ok) {
        throw new Error(
          `Failed to parse discovery daemon response: ${error_result.error.message}`
        );
      }

      const response_error = error_result.value as cluster_service_discovery_response_error_message_i;
      const response_details = IsRecordObject(response_error.error.details)
        ? response_error.error.details
        : undefined;

      const error = new Error(response_error.error.message) as Error & {
        retryable?: boolean;
        code?: cluster_service_discovery_protocol_error_t['code'];
        details?: Record<string, unknown>;
      };
      error.retryable = response_error.error.retryable;
      error.code = response_error.error.code;
      error.details = response_details;

      throw error;
    } finally {
      try {
        client_session?.close();
      } catch {
        // ignore close errors
      }
    }
  }
}
