import { connect, type ClientHttp2Session } from 'node:http2';

import type {
  cluster_call_request_message_i,
  cluster_call_response_error_message_i,
  cluster_call_response_success_message_i
} from '../clusterprotocol/ClusterProtocolTypes';
import {
  ParseClusterCallAckMessage,
  ParseClusterCallResponseErrorMessage,
  ParseClusterCallResponseSuccessMessage
} from '../clusterprotocol/ClusterProtocolValidators';
import {
  ClusterInMemoryServiceDiscoveryStore,
  type cluster_service_discovery_config_t,
  type cluster_service_discovery_node_identity_t,
  type cluster_service_discovery_node_status_t,
  type cluster_service_discovery_store_i,
  type cluster_service_discovery_metrics_t,
  type cluster_service_discovery_event_t
} from '../clusterservicediscovery/ClusterServiceDiscoveryStore.class';
import {
  ClusterRemoteServiceDiscoveryStoreAdapter,
  type cluster_remote_service_discovery_auth_headers_provider_t,
  type cluster_remote_service_discovery_endpoint_t
} from '../clusterservicediscovery/ClusterRemoteServiceDiscoveryStoreAdapter.class';
import {
  ClusterControlPlaneGatewayAdapter,
  type cluster_control_plane_gateway_adapter_auth_headers_provider_t,
  type cluster_control_plane_gateway_adapter_endpoint_t,
  type cluster_control_plane_gateway_adapter_metrics_t,
  type cluster_control_plane_gateway_adapter_snapshot_t
} from '../clustercontrolplane/ClusterControlPlaneGatewayAdapter.class';
import type {
  cluster_control_plane_gateway_record_t,
  cluster_control_plane_policy_snapshot_t,
  cluster_control_plane_policy_version_record_t,
  cluster_control_plane_topology_snapshot_t
} from '../clustercontrolplane/ClusterControlPlaneProtocol';
import type {
  WorkerProcedureCall,
  cluster_call_node_health_state_t,
  cluster_call_node_metrics_t,
  cluster_operations_observability_snapshot_t,
  handle_cluster_call_response_t
} from '../workerprocedurecall/WorkerProcedureCall.class';
import {
  ClusterHttp2Transport,
  type cluster_http2_authenticate_request_t,
  type cluster_http2_transport_security_config_t,
  type cluster_http2_transport_metrics_t,
  type cluster_http2_transport_event_name_t,
  type cluster_http2_transport_event_t,
  type cluster_http2_session_snapshot_t,
  type cluster_http2_transport_address_t,
  type cluster_http2_transport_event_listener_t
} from './ClusterHttp2Transport.class';
import type { cluster_tls_client_config_t } from './ClusterTlsSecurity.class';
import type {
  cluster_transport_jwt_key_t,
  cluster_transport_token_validation_config_t
} from './ClusterTransportAuth.class';

export type cluster_node_agent_discovery_request_headers_provider_t = () =>
  | Record<string, string>
  | Promise<Record<string, string>>;

export type cluster_node_agent_discovery_external_daemon_config_t = {
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
  transport_security?: cluster_tls_client_config_t;
};

export type cluster_node_agent_discovery_config_t = {
  enabled?: boolean;
  service_discovery_store?: cluster_service_discovery_store_i;
  external_daemon?: cluster_node_agent_discovery_external_daemon_config_t;
  heartbeat_interval_ms?: number;
  lease_ttl_ms?: number;
  expiration_check_interval_ms?: number;
  sync_interval_ms?: number;
  labels?: Record<string, string>;
  zones?: string[];
  node_agent_semver?: string;
  runtime_semver?: string;
  request_headers_provider?: cluster_node_agent_discovery_request_headers_provider_t;
};

export type cluster_node_agent_discovery_metrics_t = {
  register_attempt_total: number;
  register_success_total: number;
  heartbeat_update_total: number;
  capability_update_total: number;
  sync_total: number;
  sync_error_total: number;
  remote_node_registered_total: number;
  remote_node_removed_total: number;
  inter_node_dispatch_total: number;
  inter_node_dispatch_error_total: number;
  last_sync_unix_ms: number | null;
};

export type cluster_node_agent_discovery_snapshot_t = {
  enabled: boolean;
  node_id: string;
  heartbeat_interval_ms: number;
  lease_ttl_ms: number;
  sync_interval_ms: number;
  known_remote_node_count: number;
  known_remote_node_id_list: string[];
  metrics: cluster_node_agent_discovery_metrics_t;
  store_metrics: cluster_service_discovery_metrics_t | null;
};

export type cluster_node_agent_control_plane_config_t = {
  enabled?: boolean;
  gateway_id?: string;
  endpoint?: cluster_control_plane_gateway_adapter_endpoint_t;
  endpoint_list?: cluster_control_plane_gateway_adapter_endpoint_t[];
  sync_interval_ms?: number;
  heartbeat_interval_ms?: number;
  request_timeout_ms?: number;
  retry_base_delay_ms?: number;
  retry_max_delay_ms?: number;
  endpoint_cooldown_ms?: number;
  max_request_attempts?: number;
  auth_headers?: Record<string, string>;
  auth_headers_provider?: cluster_control_plane_gateway_adapter_auth_headers_provider_t;
  use_topology_for_routing?: boolean;
  transport_security?: cluster_tls_client_config_t;
};

export type cluster_node_agent_control_plane_metrics_t = {
  register_attempt_total: number;
  register_success_total: number;
  heartbeat_total: number;
  sync_total: number;
  sync_error_total: number;
  policy_apply_total: number;
  policy_apply_error_total: number;
  topology_apply_total: number;
  topology_apply_error_total: number;
  stale_mode_entry_total: number;
  stale_mode_exit_total: number;
  last_sync_unix_ms: number | null;
};

export type cluster_node_agent_control_plane_snapshot_t = {
  enabled: boolean;
  gateway_id: string;
  sync_interval_ms: number;
  heartbeat_interval_ms: number;
  use_topology_for_routing: boolean;
  applied_policy_version_id: string | null;
  known_policy_version_id: string | null;
  known_service_generation: number | null;
  stale_since_unix_ms: number | null;
  last_error_message: string | null;
  known_remote_gateway_count: number;
  known_remote_gateway_id_list: string[];
  metrics: cluster_node_agent_control_plane_metrics_t;
  adapter_metrics: cluster_control_plane_gateway_adapter_metrics_t | null;
  adapter_snapshot: cluster_control_plane_gateway_adapter_snapshot_t | null;
};

export type cluster_node_agent_constructor_params_t = {
  workerprocedurecall: WorkerProcedureCall;
  node_id: string;
  worker_start_count?: number;
  transport?: {
    host?: string;
    port?: number;
    request_path?: string;
    authenticate_request?: cluster_http2_authenticate_request_t;
    security?: cluster_http2_transport_security_config_t;
  };
  discovery?: cluster_node_agent_discovery_config_t;
  control_plane?: cluster_node_agent_control_plane_config_t;
};

function BuildDefaultNodeMetrics(): cluster_call_node_metrics_t {
  return {
    inflight_calls: 0,
    pending_calls: 0,
    success_rate_1m: 1,
    timeout_rate_1m: 0,
    ewma_latency_ms: 0
  };
}

function BuildNodeHealthStateFromDiscoveryStatus(params: {
  status: cluster_service_discovery_node_status_t;
}): cluster_call_node_health_state_t {
  const { status } = params;

  if (status === 'ready') {
    return 'ready';
  }

  if (status === 'degraded') {
    return 'degraded';
  }

  if (status === 'restarting') {
    return 'restarting';
  }

  return 'stopped';
}

function BuildDiscoveryStatusFromWorkerState(params: {
  worker_health_summary: cluster_operations_observability_snapshot_t['worker_health_summary'];
}): Exclude<cluster_service_discovery_node_status_t, 'expired'> {
  const { worker_health_summary } = params;

  if (
    worker_health_summary.lifecycle_state === 'stopped' ||
    worker_health_summary.lifecycle_state === 'stopping'
  ) {
    return 'stopped';
  }

  if (worker_health_summary.worker_count === 0) {
    return 'degraded';
  }

  if (worker_health_summary.worker_health_count_by_state.restarting > 0) {
    return 'restarting';
  }

  if (
    worker_health_summary.worker_health_count_by_state.ready > 0 &&
    worker_health_summary.pending_call_count === 0
  ) {
    return 'ready';
  }

  if (worker_health_summary.worker_health_count_by_state.ready > 0) {
    return 'degraded';
  }

  return 'stopped';
}

function BuildGatewayStatusFromWorkerState(params: {
  worker_health_summary: cluster_operations_observability_snapshot_t['worker_health_summary'];
}): 'active' | 'degraded' | 'draining' {
  const discovery_status = BuildDiscoveryStatusFromWorkerState({
    worker_health_summary: params.worker_health_summary
  });

  if (discovery_status === 'ready') {
    return 'active';
  }

  return 'degraded';
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

function BuildControlPlaneGatewayAddressFromLocalTransport(params: {
  address: cluster_http2_transport_address_t;
}): cluster_control_plane_gateway_record_t['address'] {
  return {
    host: params.address.host,
    port: params.address.port,
    request_path: params.address.request_path,
    tls_mode: params.address.tls_mode
  };
}

function BuildControlPlanePolicyApplyResult(params: {
  policy_snapshot: cluster_control_plane_policy_snapshot_t;
}): {
  apply_routing_succeeded: boolean;
  apply_auth_succeeded: boolean;
} {
  const has_routing_policy =
    typeof params.policy_snapshot.routing_policy === 'object' &&
    params.policy_snapshot.routing_policy !== null;

  const has_authorization_policy_list = Array.isArray(
    params.policy_snapshot.authorization_policy_list
  );

  return {
    apply_routing_succeeded: has_routing_policy,
    apply_auth_succeeded: has_authorization_policy_list
  };
}

export class ClusterNodeAgent {
  private readonly workerprocedurecall: WorkerProcedureCall;
  private readonly worker_start_count?: number;
  private readonly cluster_http2_transport: ClusterHttp2Transport;
  private readonly node_id: string;
  private readonly gateway_id: string;

  private started_workers_via_agent = false;

  private readonly discovery_enabled: boolean;
  private readonly service_discovery_store: cluster_service_discovery_store_i | null;
  private readonly discovery_heartbeat_interval_ms: number;
  private readonly discovery_lease_ttl_ms: number;
  private readonly discovery_sync_interval_ms: number;
  private readonly discovery_labels: Record<string, string> | undefined;
  private readonly discovery_zones: string[] | undefined;
  private readonly discovery_node_agent_semver: string | undefined;
  private readonly discovery_runtime_semver: string | undefined;
  private readonly discovery_request_headers_provider:
    | cluster_node_agent_discovery_request_headers_provider_t
    | undefined;

  private discovery_heartbeat_interval_handle: NodeJS.Timeout | null = null;
  private discovery_sync_interval_handle: NodeJS.Timeout | null = null;
  private discovery_listener_id: number | null = null;
  private discovery_sync_in_progress = false;
  private discovery_sync_pending = false;
  private started_discovery_expiration_loop = false;

  private discovery_known_remote_node_id_set = new Set<string>();
  private discovery_remote_node_identity_by_id = new Map<
    string,
    cluster_service_discovery_node_identity_t
  >();

  private readonly control_plane_enabled: boolean;
  private readonly control_plane_gateway_adapter: ClusterControlPlaneGatewayAdapter | null;
  private readonly control_plane_sync_interval_ms: number;
  private readonly control_plane_heartbeat_interval_ms: number;
  private readonly control_plane_use_topology_for_routing: boolean;

  private control_plane_heartbeat_interval_handle: NodeJS.Timeout | null = null;
  private control_plane_sync_interval_handle: NodeJS.Timeout | null = null;
  private control_plane_sync_in_progress = false;
  private control_plane_sync_pending = false;
  private control_plane_known_service_generation: number | null = null;
  private control_plane_known_policy_version_id: string | null = null;
  private control_plane_applied_policy_version_id: string | null = null;
  private control_plane_stale_since_unix_ms: number | null = null;
  private control_plane_last_error_message: string | null = null;
  private control_plane_known_remote_gateway_id_set = new Set<string>();
  private control_plane_remote_gateway_record_by_id = new Map<
    string,
    cluster_control_plane_gateway_record_t
  >();

  private local_transport_address: cluster_http2_transport_address_t | null = null;

  private discovery_metrics: cluster_node_agent_discovery_metrics_t = {
    register_attempt_total: 0,
    register_success_total: 0,
    heartbeat_update_total: 0,
    capability_update_total: 0,
    sync_total: 0,
    sync_error_total: 0,
    remote_node_registered_total: 0,
    remote_node_removed_total: 0,
    inter_node_dispatch_total: 0,
    inter_node_dispatch_error_total: 0,
    last_sync_unix_ms: null
  };

  private control_plane_metrics: cluster_node_agent_control_plane_metrics_t = {
    register_attempt_total: 0,
    register_success_total: 0,
    heartbeat_total: 0,
    sync_total: 0,
    sync_error_total: 0,
    policy_apply_total: 0,
    policy_apply_error_total: 0,
    topology_apply_total: 0,
    topology_apply_error_total: 0,
    stale_mode_entry_total: 0,
    stale_mode_exit_total: 0,
    last_sync_unix_ms: null
  };

  constructor(params: cluster_node_agent_constructor_params_t) {
    const {
      workerprocedurecall,
      node_id,
      worker_start_count,
      transport,
      discovery,
      control_plane
    } = params;

    this.workerprocedurecall = workerprocedurecall;
    this.worker_start_count = worker_start_count;
    this.node_id = node_id;
    this.gateway_id = control_plane?.gateway_id ?? node_id;

    this.cluster_http2_transport = new ClusterHttp2Transport({
      workerprocedurecall,
      node_id,
      host: transport?.host,
      port: transport?.port,
      request_path: transport?.request_path,
      authenticate_request: transport?.authenticate_request,
      security: transport?.security
    });

    const external_discovery_daemon_config = discovery?.external_daemon;

    this.discovery_enabled = discovery?.enabled !== false;
    if (!this.discovery_enabled) {
      this.service_discovery_store = null;
    } else if (discovery?.service_discovery_store) {
      this.service_discovery_store = discovery.service_discovery_store;
    } else if (external_discovery_daemon_config) {
      this.service_discovery_store = new ClusterRemoteServiceDiscoveryStoreAdapter({
        host: external_discovery_daemon_config.host,
        port: external_discovery_daemon_config.port,
        request_path: external_discovery_daemon_config.request_path,
        tls_mode: external_discovery_daemon_config.tls_mode,
        endpoint_list: external_discovery_daemon_config.endpoint_list,
        request_timeout_ms: external_discovery_daemon_config.request_timeout_ms,
        synchronization_interval_ms:
          external_discovery_daemon_config.synchronization_interval_ms,
        retry_base_delay_ms: external_discovery_daemon_config.retry_base_delay_ms,
        retry_max_delay_ms: external_discovery_daemon_config.retry_max_delay_ms,
        endpoint_cooldown_ms: external_discovery_daemon_config.endpoint_cooldown_ms,
        max_request_attempts: external_discovery_daemon_config.max_request_attempts,
        auth_headers: external_discovery_daemon_config.auth_headers,
        auth_headers_provider: external_discovery_daemon_config.auth_headers_provider,
        transport_security:
          external_discovery_daemon_config.transport_security ??
          this.cluster_http2_transport.getTlsClientConfig()
      });
    } else {
      this.service_discovery_store = new ClusterInMemoryServiceDiscoveryStore();
    }

    this.discovery_heartbeat_interval_ms = discovery?.heartbeat_interval_ms ?? 2_000;
    this.discovery_lease_ttl_ms =
      discovery?.lease_ttl_ms ?? Math.max(5_000, this.discovery_heartbeat_interval_ms * 3);
    this.discovery_sync_interval_ms =
      discovery?.sync_interval_ms ?? this.discovery_heartbeat_interval_ms;

    if (this.service_discovery_store) {
      const discovery_store_config_update: Partial<cluster_service_discovery_config_t> = {
        default_lease_ttl_ms: this.discovery_lease_ttl_ms
      };

      if (typeof discovery?.expiration_check_interval_ms === 'number') {
        discovery_store_config_update.expiration_check_interval_ms =
          discovery.expiration_check_interval_ms;
      }

      this.service_discovery_store.setConfig({
        config: discovery_store_config_update
      });
    }

    this.discovery_labels = discovery?.labels ? { ...discovery.labels } : undefined;
    this.discovery_zones = Array.isArray(discovery?.zones)
      ? [...(discovery?.zones ?? [])]
      : undefined;
    this.discovery_node_agent_semver = discovery?.node_agent_semver;
    this.discovery_runtime_semver = discovery?.runtime_semver;
    this.discovery_request_headers_provider = discovery?.request_headers_provider;

    this.control_plane_enabled = control_plane?.enabled === true;
    this.control_plane_sync_interval_ms = control_plane?.sync_interval_ms ?? 2_000;
    this.control_plane_heartbeat_interval_ms =
      control_plane?.heartbeat_interval_ms ?? this.control_plane_sync_interval_ms;
    this.control_plane_use_topology_for_routing =
      control_plane?.use_topology_for_routing ?? !this.discovery_enabled;

    if (!this.control_plane_enabled) {
      this.control_plane_gateway_adapter = null;
    } else {
      this.control_plane_gateway_adapter = new ClusterControlPlaneGatewayAdapter({
        endpoint: control_plane?.endpoint,
        endpoint_list: control_plane?.endpoint_list,
        request_timeout_ms: control_plane?.request_timeout_ms,
        retry_base_delay_ms: control_plane?.retry_base_delay_ms,
        retry_max_delay_ms: control_plane?.retry_max_delay_ms,
        endpoint_cooldown_ms: control_plane?.endpoint_cooldown_ms,
        max_request_attempts: control_plane?.max_request_attempts,
        auth_headers: control_plane?.auth_headers,
        auth_headers_provider: control_plane?.auth_headers_provider,
        transport_security:
          control_plane?.transport_security ??
          this.cluster_http2_transport.getTlsClientConfig()
      });
    }
  }

  async start(): Promise<cluster_http2_transport_address_t> {
    if (typeof this.worker_start_count === 'number') {
      await this.workerprocedurecall.startWorkers({
        count: this.worker_start_count
      });
      this.started_workers_via_agent = true;
    }

    const address = await this.cluster_http2_transport.start();
    this.local_transport_address = address;

    if (this.service_discovery_store) {
      this.service_discovery_store.startExpirationLoop();
      this.started_discovery_expiration_loop = true;
      await this.registerLocalNodeInDiscovery({ address });
      await this.publishLocalCapabilitiesToDiscovery();
      this.startDiscoveryLifecycleLoops();
      await this.syncDiscoveredNodesToRouting();
    }

    if (this.control_plane_gateway_adapter) {
      await this.registerLocalGatewayInControlPlane();
      this.startControlPlaneLifecycleLoops();
      await this.syncControlPlaneState();
    }

    return address;
  }

  async stop(): Promise<void> {
    this.stopDiscoveryLifecycleLoops();
    this.stopControlPlaneLifecycleLoops();

    if (this.service_discovery_store) {
      this.service_discovery_store.removeNode({
        node_id: this.node_id,
        reason: 'node_agent_stop'
      });

      for (const remote_node_id of this.discovery_known_remote_node_id_set) {
        this.workerprocedurecall.unregisterClusterCallNode({
          node_id: remote_node_id
        });
      }

      this.discovery_known_remote_node_id_set.clear();
      this.discovery_remote_node_identity_by_id.clear();

      if (this.started_discovery_expiration_loop) {
        this.service_discovery_store.stopExpirationLoop();
        this.started_discovery_expiration_loop = false;
      }
    }

    if (this.control_plane_gateway_adapter) {
      try {
        await this.control_plane_gateway_adapter.deregisterGateway({
          gateway_id: this.gateway_id,
          reason: 'cluster_node_agent_stop'
        });
      } catch {
        // Ignore control-plane deregistration failures during shutdown.
      }

      for (const remote_gateway_id of this.control_plane_known_remote_gateway_id_set) {
        this.workerprocedurecall.unregisterClusterCallNode({
          node_id: remote_gateway_id
        });
      }

      this.control_plane_known_remote_gateway_id_set.clear();
      this.control_plane_remote_gateway_record_by_id.clear();
    }

    await this.cluster_http2_transport.stop();

    if (this.started_workers_via_agent) {
      await this.workerprocedurecall.stopWorkers();
      this.started_workers_via_agent = false;
    }
  }

  getAddress(): cluster_http2_transport_address_t {
    return this.cluster_http2_transport.getAddress();
  }

  getSessionSnapshot(): cluster_http2_session_snapshot_t[] {
    return this.cluster_http2_transport.getSessionSnapshot();
  }

  getTransportMetrics(): cluster_http2_transport_metrics_t {
    return this.cluster_http2_transport.getTransportMetrics();
  }

  getRecentTransportEvents(params?: {
    limit?: number;
    event_name?: cluster_http2_transport_event_name_t;
    request_id?: string;
    session_id?: string;
  }): cluster_http2_transport_event_t[] {
    return this.cluster_http2_transport.getRecentTransportEvents({
      limit: params?.limit,
      event_name: params?.event_name,
      request_id: params?.request_id,
      session_id: params?.session_id
    });
  }

  getOperationsObservabilitySnapshot(): {
    captured_at_unix_ms: number;
    workerprocedurecall: cluster_operations_observability_snapshot_t;
    transport: cluster_http2_transport_metrics_t;
    session_count: number;
  } {
    return {
      captured_at_unix_ms: Date.now(),
      workerprocedurecall: this.workerprocedurecall.getClusterOperationsObservabilitySnapshot(),
      transport: this.cluster_http2_transport.getTransportMetrics(),
      session_count: this.cluster_http2_transport.getSessionSnapshot().length
    };
  }

  getDiscoverySnapshot(): cluster_node_agent_discovery_snapshot_t {
    return {
      enabled: this.service_discovery_store !== null,
      node_id: this.node_id,
      heartbeat_interval_ms: this.discovery_heartbeat_interval_ms,
      lease_ttl_ms: this.discovery_lease_ttl_ms,
      sync_interval_ms: this.discovery_sync_interval_ms,
      known_remote_node_count: this.discovery_known_remote_node_id_set.size,
      known_remote_node_id_list: [...this.discovery_known_remote_node_id_set].sort(),
      metrics: {
        ...this.discovery_metrics
      },
      store_metrics: this.service_discovery_store
        ? this.service_discovery_store.getMetrics()
        : null
    };
  }

  getDiscoveryEvents(params?: {
    limit?: number;
    event_name?: cluster_service_discovery_event_t['event_name'];
    node_id?: string;
  }): cluster_service_discovery_event_t[] {
    if (!this.service_discovery_store) {
      return [];
    }

    return this.service_discovery_store.getRecentEvents({
      limit: params?.limit,
      event_name: params?.event_name,
      node_id: params?.node_id
    });
  }

  getControlPlaneSnapshot(): cluster_node_agent_control_plane_snapshot_t {
    return {
      enabled: this.control_plane_gateway_adapter !== null,
      gateway_id: this.gateway_id,
      sync_interval_ms: this.control_plane_sync_interval_ms,
      heartbeat_interval_ms: this.control_plane_heartbeat_interval_ms,
      use_topology_for_routing: this.control_plane_use_topology_for_routing,
      applied_policy_version_id: this.control_plane_applied_policy_version_id,
      known_policy_version_id: this.control_plane_known_policy_version_id,
      known_service_generation: this.control_plane_known_service_generation,
      stale_since_unix_ms: this.control_plane_stale_since_unix_ms,
      last_error_message: this.control_plane_last_error_message,
      known_remote_gateway_count: this.control_plane_known_remote_gateway_id_set.size,
      known_remote_gateway_id_list: [...this.control_plane_known_remote_gateway_id_set].sort(),
      metrics: {
        ...this.control_plane_metrics
      },
      adapter_metrics: this.control_plane_gateway_adapter
        ? this.control_plane_gateway_adapter.getMetrics()
        : null,
      adapter_snapshot: this.control_plane_gateway_adapter
        ? this.control_plane_gateway_adapter.getSnapshot()
        : null
    };
  }

  onTransportEvent(params: {
    listener: cluster_http2_transport_event_listener_t;
  }): number {
    return this.cluster_http2_transport.onTransportEvent({
      listener: params.listener
    });
  }

  offTransportEvent(params: { listener_id: number }): void {
    this.cluster_http2_transport.offTransportEvent({
      listener_id: params.listener_id
    });
  }

  configureSecurity(params: {
    security_config: cluster_http2_transport_security_config_t;
  }): void {
    this.cluster_http2_transport.configureSecurity({
      security_config: params.security_config
    });
  }

  setTokenValidationConfig(params: {
    token_validation_config: cluster_transport_token_validation_config_t;
  }): void {
    this.cluster_http2_transport.setTokenValidationConfig({
      token_validation_config: params.token_validation_config
    });
  }

  setTokenVerificationKeySet(params: {
    key_by_kid: Record<string, cluster_transport_jwt_key_t>;
  }): void {
    this.cluster_http2_transport.setTokenVerificationKeySet({
      key_by_kid: params.key_by_kid
    });
  }

  upsertTokenVerificationKey(params: { key: cluster_transport_jwt_key_t }): void {
    this.cluster_http2_transport.upsertTokenVerificationKey({
      key: params.key
    });
  }

  private async registerLocalGatewayInControlPlane(): Promise<void> {
    if (!this.control_plane_gateway_adapter) {
      return;
    }

    if (!this.local_transport_address) {
      throw new Error('Cannot register gateway in control-plane before transport start.');
    }

    const local_observability = this.workerprocedurecall.getClusterOperationsObservabilitySnapshot();
    const gateway_status = BuildGatewayStatusFromWorkerState({
      worker_health_summary: local_observability.worker_health_summary
    });

    this.control_plane_metrics.register_attempt_total += 1;

    await this.control_plane_gateway_adapter.registerGateway({
      gateway_id: this.gateway_id,
      node_reference: {
        node_id: this.node_id,
        health_summary: {
          worker_count: local_observability.worker_health_summary.worker_count,
          pending_call_count: local_observability.worker_health_summary.pending_call_count,
          lifecycle_state: local_observability.worker_health_summary.lifecycle_state
        }
      },
      address: BuildControlPlaneGatewayAddressFromLocalTransport({
        address: this.local_transport_address
      }),
      status: gateway_status,
      gateway_version: 'phase17',
      lease_ttl_ms: Math.max(
        5_000,
        this.control_plane_heartbeat_interval_ms * 3
      )
    });

    this.control_plane_metrics.register_success_total += 1;
    this.markControlPlaneHealthy();
  }

  private startControlPlaneLifecycleLoops(): void {
    if (!this.control_plane_gateway_adapter) {
      return;
    }

    if (!this.control_plane_heartbeat_interval_handle) {
      this.control_plane_heartbeat_interval_handle = setInterval((): void => {
        void this.publishLocalHeartbeatToControlPlane();
      }, this.control_plane_heartbeat_interval_ms);
      this.control_plane_heartbeat_interval_handle.unref();
    }

    if (!this.control_plane_sync_interval_handle) {
      this.control_plane_sync_interval_handle = setInterval((): void => {
        this.scheduleControlPlaneSync();
      }, this.control_plane_sync_interval_ms);
      this.control_plane_sync_interval_handle.unref();
    }
  }

  private stopControlPlaneLifecycleLoops(): void {
    if (this.control_plane_heartbeat_interval_handle) {
      clearInterval(this.control_plane_heartbeat_interval_handle);
      this.control_plane_heartbeat_interval_handle = null;
    }

    if (this.control_plane_sync_interval_handle) {
      clearInterval(this.control_plane_sync_interval_handle);
      this.control_plane_sync_interval_handle = null;
    }
  }

  private async publishLocalHeartbeatToControlPlane(): Promise<void> {
    if (!this.control_plane_gateway_adapter) {
      return;
    }

    const local_observability = this.workerprocedurecall.getClusterOperationsObservabilitySnapshot();
    const gateway_status = BuildGatewayStatusFromWorkerState({
      worker_health_summary: local_observability.worker_health_summary
    });

    try {
      await this.control_plane_gateway_adapter.heartbeatGateway({
        gateway_id: this.gateway_id,
        node_reference: {
          node_id: this.node_id,
          health_summary: {
            worker_count: local_observability.worker_health_summary.worker_count,
            pending_call_count: local_observability.worker_health_summary.pending_call_count,
            lifecycle_state: local_observability.worker_health_summary.lifecycle_state
          }
        },
        status: gateway_status,
        gateway_version: 'phase17',
        lease_ttl_ms: Math.max(5_000, this.control_plane_heartbeat_interval_ms * 3)
      });

      this.control_plane_metrics.heartbeat_total += 1;
      this.markControlPlaneHealthy();
    } catch (error) {
      this.control_plane_metrics.sync_error_total += 1;
      this.markControlPlaneStale({
        error
      });
    }
  }

  private scheduleControlPlaneSync(): void {
    if (this.control_plane_sync_in_progress) {
      this.control_plane_sync_pending = true;
      return;
    }

    this.control_plane_sync_in_progress = true;
    void this.syncControlPlaneState()
      .catch((error): void => {
        this.control_plane_metrics.sync_error_total += 1;
        this.markControlPlaneStale({
          error
        });
      })
      .finally((): void => {
        this.control_plane_sync_in_progress = false;
        if (this.control_plane_sync_pending) {
          this.control_plane_sync_pending = false;
          this.scheduleControlPlaneSync();
        }
      });
  }

  private async syncControlPlaneState(): Promise<void> {
    if (!this.control_plane_gateway_adapter) {
      return;
    }

    const subscribe_response = await this.control_plane_gateway_adapter.subscribeUpdates({
      gateway_id: this.gateway_id,
      known_policy_version_id: this.control_plane_known_policy_version_id ?? undefined,
      known_service_generation: this.control_plane_known_service_generation ?? undefined,
      include_topology: this.control_plane_use_topology_for_routing
    });

    this.control_plane_known_service_generation = subscribe_response.service_generation;
    this.control_plane_known_policy_version_id = subscribe_response.active_policy_version_id;

    if (subscribe_response.active_policy_version) {
      await this.applyControlPlanePolicySnapshot({
        active_policy_version: subscribe_response.active_policy_version
      });
    }

    if (this.control_plane_use_topology_for_routing && subscribe_response.topology_snapshot) {
      try {
        await this.applyControlPlaneTopologySnapshot({
          topology_snapshot: subscribe_response.topology_snapshot
        });
      } catch (error) {
        this.control_plane_metrics.topology_apply_error_total += 1;
        throw error;
      }
    }

    this.control_plane_metrics.sync_total += 1;
    this.control_plane_metrics.last_sync_unix_ms = Date.now();
    this.markControlPlaneHealthy();
  }

  private async applyControlPlanePolicySnapshot(params: {
    active_policy_version: cluster_control_plane_policy_version_record_t;
  }): Promise<void> {
    const policy_version = params.active_policy_version;
    const policy_version_id = policy_version.metadata.version_id;

    if (policy_version_id === this.control_plane_applied_policy_version_id) {
      return;
    }

    let apply_error: Error | null = null;
    this.control_plane_metrics.policy_apply_total += 1;

    try {
      const policy_snapshot = policy_version.snapshot;
      const apply_result = BuildControlPlanePolicyApplyResult({
        policy_snapshot
      });

      if (apply_result.apply_routing_succeeded) {
        this.workerprocedurecall.setClusterCallRoutingPolicy({
          policy: {
            ...(policy_snapshot.routing_policy ?? {})
          }
        });
      }

      if (apply_result.apply_auth_succeeded) {
        this.workerprocedurecall.setClusterAuthorizationPolicyList({
          policy_list: [...(policy_snapshot.authorization_policy_list ?? [])]
        });
      }

      this.control_plane_applied_policy_version_id = policy_version_id;
    } catch (error) {
      apply_error = error instanceof Error ? error : new Error(String(error));
      this.control_plane_metrics.policy_apply_error_total += 1;
    }

    if (this.control_plane_gateway_adapter) {
      try {
        await this.control_plane_gateway_adapter.announceConfigVersion({
          gateway_id: this.gateway_id,
          policy_version_id: policy_version_id
        });
      } catch {
        // Ignore announce errors and rely on next sync.
      }

      await this.control_plane_gateway_adapter.acknowledgeConfigVersion({
        gateway_id: this.gateway_id,
        policy_version_id: policy_version_id,
        applied: apply_error === null,
        details: apply_error
          ? {
              error_message: apply_error.message
            }
          : {
              applied_at_unix_ms: Date.now()
            }
      });
    }

    if (apply_error) {
      throw apply_error;
    }
  }

  private async applyControlPlaneTopologySnapshot(params: {
    topology_snapshot: cluster_control_plane_topology_snapshot_t;
  }): Promise<void> {
    const { topology_snapshot } = params;
    const next_remote_gateway_id_set = new Set<string>();

    for (const gateway_record of topology_snapshot.gateway_list) {
      if (gateway_record.gateway_id === this.gateway_id) {
        continue;
      }

      if (gateway_record.status === 'offline') {
        continue;
      }

      const remote_node_id = gateway_record.node_reference.node_id;
      if (remote_node_id === this.node_id) {
        continue;
      }

      next_remote_gateway_id_set.add(remote_node_id);
      this.control_plane_remote_gateway_record_by_id.set(
        remote_node_id,
        structuredClone(gateway_record)
      );

      const health_state =
        gateway_record.status === 'active'
          ? 'ready'
          : gateway_record.status === 'draining'
            ? 'degraded'
            : 'degraded';

      this.workerprocedurecall.registerClusterCallNode({
        node: {
          node_id: remote_node_id
        },
        initial_health_state: health_state,
        initial_metrics: BuildDefaultNodeMetrics(),
        capability_list: [],
        last_health_update_unix_ms: gateway_record.last_heartbeat_unix_ms,
        call_executor: async (executor_params): Promise<handle_cluster_call_response_t> => {
          return await this.forwardClusterCallToControlPlaneGatewayNode({
            request: executor_params.request,
            target_node_id: executor_params.node.node_id
          });
        }
      });
    }

    for (const known_remote_node_id of this.control_plane_known_remote_gateway_id_set) {
      if (next_remote_gateway_id_set.has(known_remote_node_id)) {
        continue;
      }

      this.workerprocedurecall.unregisterClusterCallNode({
        node_id: known_remote_node_id
      });
      this.control_plane_remote_gateway_record_by_id.delete(known_remote_node_id);
    }

    this.control_plane_known_remote_gateway_id_set = next_remote_gateway_id_set;
    this.control_plane_metrics.topology_apply_total += 1;
  }

  private markControlPlaneStale(params: {
    error: unknown;
  }): void {
    if (this.control_plane_stale_since_unix_ms === null) {
      this.control_plane_stale_since_unix_ms = Date.now();
      this.control_plane_metrics.stale_mode_entry_total += 1;
    }

    this.control_plane_last_error_message =
      params.error instanceof Error ? params.error.message : String(params.error);
  }

  private markControlPlaneHealthy(): void {
    if (this.control_plane_stale_since_unix_ms !== null) {
      this.control_plane_metrics.stale_mode_exit_total += 1;
    }

    this.control_plane_stale_since_unix_ms = null;
    this.control_plane_last_error_message = null;
  }

  private async registerLocalNodeInDiscovery(params: {
    address: cluster_http2_transport_address_t;
  }): Promise<void> {
    if (!this.service_discovery_store) {
      return;
    }

    const local_observability = this.workerprocedurecall.getClusterOperationsObservabilitySnapshot();
    const status = BuildDiscoveryStatusFromWorkerState({
      worker_health_summary: local_observability.worker_health_summary
    });

    this.discovery_metrics.register_attempt_total += 1;

    this.service_discovery_store.upsertNodeRegistration({
      node_identity: {
        node_id: this.node_id,
        address: {
          host: params.address.host,
          port: params.address.port,
          request_path: params.address.request_path,
          tls_mode: params.address.tls_mode
        },
        labels: this.discovery_labels ? { ...this.discovery_labels } : undefined,
        zones: this.discovery_zones ? [...this.discovery_zones] : undefined,
        node_agent_semver: this.discovery_node_agent_semver,
        runtime_semver: this.discovery_runtime_semver
      },
      status,
      metrics: BuildDefaultNodeMetrics(),
      lease_ttl_ms: this.discovery_lease_ttl_ms
    });

    this.discovery_metrics.register_success_total += 1;
  }

  private startDiscoveryLifecycleLoops(): void {
    if (!this.service_discovery_store) {
      return;
    }

    if (this.discovery_listener_id === null) {
      this.discovery_listener_id = this.service_discovery_store.onDiscoveryEvent({
        listener: (): void => {
          this.scheduleDiscoverySync();
        }
      });
    }

    if (!this.discovery_heartbeat_interval_handle) {
      this.discovery_heartbeat_interval_handle = setInterval((): void => {
        void this.publishLocalHeartbeatToDiscovery();
      }, this.discovery_heartbeat_interval_ms);
      this.discovery_heartbeat_interval_handle.unref();
    }

    if (!this.discovery_sync_interval_handle) {
      this.discovery_sync_interval_handle = setInterval((): void => {
        void this.publishLocalCapabilitiesToDiscovery();
        this.scheduleDiscoverySync();
      }, this.discovery_sync_interval_ms);
      this.discovery_sync_interval_handle.unref();
    }
  }

  private stopDiscoveryLifecycleLoops(): void {
    if (this.discovery_heartbeat_interval_handle) {
      clearInterval(this.discovery_heartbeat_interval_handle);
      this.discovery_heartbeat_interval_handle = null;
    }

    if (this.discovery_sync_interval_handle) {
      clearInterval(this.discovery_sync_interval_handle);
      this.discovery_sync_interval_handle = null;
    }

    if (this.service_discovery_store && this.discovery_listener_id !== null) {
      this.service_discovery_store.offDiscoveryEvent({
        listener_id: this.discovery_listener_id
      });
      this.discovery_listener_id = null;
    }
  }

  private async publishLocalHeartbeatToDiscovery(): Promise<void> {
    if (!this.service_discovery_store) {
      return;
    }

    const local_observability = this.workerprocedurecall.getClusterOperationsObservabilitySnapshot();
    const status = BuildDiscoveryStatusFromWorkerState({
      worker_health_summary: local_observability.worker_health_summary
    });

    const metrics: cluster_call_node_metrics_t = {
      inflight_calls: local_observability.worker_health_summary.pending_call_count,
      pending_calls: local_observability.worker_health_summary.pending_call_count,
      success_rate_1m: 1,
      timeout_rate_1m: 0,
      ewma_latency_ms: local_observability.worker_health_summary.pending_call_count
    };

    try {
      this.service_discovery_store.updateNodeHeartbeat({
        node_id: this.node_id,
        status,
        metrics,
        lease_ttl_ms: this.discovery_lease_ttl_ms
      });
      this.discovery_metrics.heartbeat_update_total += 1;
    } catch {
      this.discovery_metrics.sync_error_total += 1;
    }
  }

  private async publishLocalCapabilitiesToDiscovery(): Promise<void> {
    if (!this.service_discovery_store) {
      return;
    }

    try {
      const remote_function_list = await this.workerprocedurecall.getRemoteFunctions();
      const capability_list = remote_function_list.map((remote_function) => {
        return {
          function_name: remote_function.name,
          function_hash_sha1: remote_function.function_hash_sha1,
          installed: true
        };
      });

      this.service_discovery_store.updateNodeCapabilities({
        node_id: this.node_id,
        capability_list
      });

      this.discovery_metrics.capability_update_total += 1;
    } catch {
      this.discovery_metrics.sync_error_total += 1;
    }
  }

  private scheduleDiscoverySync(): void {
    if (this.discovery_sync_in_progress) {
      this.discovery_sync_pending = true;
      return;
    }

    this.discovery_sync_in_progress = true;
    void this.syncDiscoveredNodesToRouting()
      .catch((): void => {
        this.discovery_metrics.sync_error_total += 1;
      })
      .finally((): void => {
        this.discovery_sync_in_progress = false;
        if (this.discovery_sync_pending) {
          this.discovery_sync_pending = false;
          this.scheduleDiscoverySync();
        }
      });
  }

  private async syncDiscoveredNodesToRouting(): Promise<void> {
    if (!this.service_discovery_store) {
      return;
    }

    const discovered_node_list = this.service_discovery_store.listNodes({
      include_expired: false
    });

    const next_remote_node_id_set = new Set<string>();

    for (const discovered_node of discovered_node_list) {
      const node_id = discovered_node.node_identity.node_id;
      if (node_id === this.node_id) {
        continue;
      }

      next_remote_node_id_set.add(node_id);
      this.discovery_remote_node_identity_by_id.set(node_id, structuredClone(discovered_node.node_identity));

      const was_known = this.discovery_known_remote_node_id_set.has(node_id);

      this.workerprocedurecall.registerClusterCallNode({
        node: {
          node_id,
          labels: discovered_node.node_identity.labels
            ? { ...discovered_node.node_identity.labels }
            : undefined,
          zones: Array.isArray(discovered_node.node_identity.zones)
            ? [...discovered_node.node_identity.zones]
            : undefined,
          node_agent_semver: discovered_node.node_identity.node_agent_semver,
          runtime_semver: discovered_node.node_identity.runtime_semver
        },
        initial_health_state: BuildNodeHealthStateFromDiscoveryStatus({
          status: discovered_node.status
        }),
        initial_metrics: {
          ...discovered_node.metrics
        },
        capability_list: discovered_node.capability_list.map((capability) => {
          return {
            function_name: capability.function_name,
            function_hash_sha1: capability.function_hash_sha1,
            installed: capability.installed
          };
        }),
        last_health_update_unix_ms: discovered_node.last_heartbeat_unix_ms,
        call_executor: async (params): Promise<handle_cluster_call_response_t> => {
          return await this.forwardClusterCallToDiscoveredNode({
            request: params.request,
            target_node_id: params.node.node_id
          });
        }
      });

      if (!was_known) {
        this.discovery_metrics.remote_node_registered_total += 1;
      }
    }

    for (const known_remote_node_id of this.discovery_known_remote_node_id_set) {
      if (next_remote_node_id_set.has(known_remote_node_id)) {
        continue;
      }

      this.workerprocedurecall.unregisterClusterCallNode({
        node_id: known_remote_node_id
      });
      this.discovery_remote_node_identity_by_id.delete(known_remote_node_id);
      this.discovery_metrics.remote_node_removed_total += 1;
    }

    this.discovery_known_remote_node_id_set = next_remote_node_id_set;
    this.discovery_metrics.sync_total += 1;
    this.discovery_metrics.last_sync_unix_ms = Date.now();
  }

  private async forwardClusterCallToDiscoveredNode(params: {
    request: cluster_call_request_message_i;
    target_node_id: string;
  }): Promise<handle_cluster_call_response_t> {
    const { request, target_node_id } = params;

    const target_node_identity = this.discovery_remote_node_identity_by_id.get(target_node_id);
    if (!target_node_identity) {
      this.discovery_metrics.inter_node_dispatch_error_total += 1;
      throw new Error(`Target node "${target_node_id}" is not present in discovery identity cache.`);
    }

    this.discovery_metrics.inter_node_dispatch_total += 1;

    try {
      return await this.forwardClusterCallToRemoteAddress({
        request,
        target_address: {
          host: target_node_identity.address.host,
          port: target_node_identity.address.port,
          request_path: target_node_identity.address.request_path,
          tls_mode: target_node_identity.address.tls_mode
        }
      });
    } catch (error) {
      this.discovery_metrics.inter_node_dispatch_error_total += 1;
      throw error;
    }
  }

  private async forwardClusterCallToControlPlaneGatewayNode(params: {
    request: cluster_call_request_message_i;
    target_node_id: string;
  }): Promise<handle_cluster_call_response_t> {
    const { request, target_node_id } = params;

    const target_gateway_record =
      this.control_plane_remote_gateway_record_by_id.get(target_node_id);
    if (!target_gateway_record) {
      this.control_plane_metrics.topology_apply_error_total += 1;
      throw new Error(
        `Target node "${target_node_id}" is not present in control-plane topology cache.`
      );
    }

    this.discovery_metrics.inter_node_dispatch_total += 1;

    try {
      return await this.forwardClusterCallToRemoteAddress({
        request,
        target_address: {
          host: target_gateway_record.address.host,
          port: target_gateway_record.address.port,
          request_path: target_gateway_record.address.request_path,
          tls_mode: target_gateway_record.address.tls_mode
        }
      });
    } catch (error) {
      this.discovery_metrics.inter_node_dispatch_error_total += 1;
      throw error;
    }
  }

  private async forwardClusterCallToRemoteAddress(params: {
    request: cluster_call_request_message_i;
    target_address: {
      host: string;
      port: number;
      request_path: string;
      tls_mode: 'required';
    };
  }): Promise<handle_cluster_call_response_t> {
    const { request, target_address } = params;

    if (target_address.tls_mode !== 'required') {
      throw new Error('Only tls_mode="required" is supported for inter-node dispatch.');
    }

    const authority = `https://${target_address.host}:${target_address.port}`;

    const client_session = connect(
      authority,
      this.cluster_http2_transport.getTlsClientConnectOptions()
    );
    client_session.on('error', (): void => {
      // request_stream handles dispatch errors; this prevents uncaught session errors
      // from surfacing when remote nodes disappear during failover windows.
    });

    try {
      const headers: Record<string, string> = {
        ':method': 'POST',
        ':path': target_address.request_path,
        'content-type': 'application/json'
      };

      if (this.discovery_request_headers_provider) {
        const provided_headers = await this.discovery_request_headers_provider();
        Object.assign(headers, NormalizeHeaders({ headers: provided_headers }));
      }

      const response_text = await new Promise<string>((resolve, reject): void => {
        const request_stream = client_session.request(headers);
        const chunk_list: Buffer[] = [];

        const timeout_ms = Math.max(100, request.deadline_unix_ms - Date.now());
        const timeout_handle = setTimeout((): void => {
          try {
            request_stream.close();
          } catch {
            // Ignore request stream close errors.
          }
          reject(new Error(`Inter-node call timed out after ${timeout_ms}ms.`));
        }, timeout_ms);

        request_stream.on('response', (): void => {
          // no-op
        });

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

        request_stream.end(JSON.stringify(request));
      });

      const response_record = JSON.parse(response_text) as Record<string, unknown>;
      if (typeof response_record !== 'object' || response_record === null) {
        throw new Error('Inter-node response payload is not an object.');
      }

      const ack_result = ParseClusterCallAckMessage({
        message: response_record.ack
      });
      if (!ack_result.ok) {
        throw new Error(`Inter-node ACK parse failed: ${ack_result.error.message}`);
      }

      const success_result = ParseClusterCallResponseSuccessMessage({
        message: response_record.terminal_message
      });
      if (success_result.ok) {
        return {
          ack: ack_result.value,
          terminal_message: success_result.value
        };
      }

      const error_result = ParseClusterCallResponseErrorMessage({
        message: response_record.terminal_message
      });
      if (!error_result.ok) {
        throw new Error(`Inter-node terminal response parse failed: ${error_result.error.message}`);
      }

      const terminal_message = error_result.value as cluster_call_response_error_message_i;
      return {
        ack: ack_result.value,
        terminal_message: terminal_message as
          | cluster_call_response_success_message_i
          | cluster_call_response_error_message_i
      };
    } finally {
      try {
        client_session.close();
      } catch {
        // Ignore session close errors.
      }
    }
  }
}
