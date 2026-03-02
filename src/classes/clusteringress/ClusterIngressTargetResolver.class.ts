import {
  ClusterRemoteServiceDiscoveryStoreAdapter,
  type cluster_remote_service_discovery_auth_headers_provider_t,
  type cluster_remote_service_discovery_endpoint_t
} from '../clusterservicediscovery/ClusterRemoteServiceDiscoveryStoreAdapter.class';
import {
  ClusterGeoIngressAdapter,
  type cluster_geo_ingress_adapter_auth_headers_provider_t,
  type cluster_geo_ingress_adapter_endpoint_t
} from '../clustergeoingress/ClusterGeoIngressAdapter.class';
import type {
  cluster_geo_ingress_global_routing_policy_snapshot_t,
  cluster_geo_ingress_instance_record_t,
  cluster_geo_ingress_topology_snapshot_t
} from '../clustergeoingress/ClusterGeoIngressProtocol';
import type {
  cluster_service_discovery_node_record_t,
  cluster_service_discovery_store_i
} from '../clusterservicediscovery/ClusterServiceDiscoveryStore.class';
import {
  ClusterControlPlaneGatewayAdapter,
  type cluster_control_plane_gateway_adapter_auth_headers_provider_t,
  type cluster_control_plane_gateway_adapter_endpoint_t
} from '../clustercontrolplane/ClusterControlPlaneGatewayAdapter.class';
import type { cluster_control_plane_gateway_record_t } from '../clustercontrolplane/ClusterControlPlaneProtocol';
import type { cluster_call_request_message_i } from '../clusterprotocol/ClusterProtocolTypes';
import type {
  ingress_balancer_target_record_t,
  ingress_balancer_target_snapshot_t
} from './ClusterIngressBalancerProtocol';

export type cluster_ingress_static_target_t = {
  target_id: string;
  node_id?: string;
  endpoint: {
    host: string;
    port: number;
    request_path: string;
    tls_mode: 'disabled' | 'required' | 'terminated_upstream';
  };
  zones?: string[];
  labels?: Record<string, string>;
  capability_hash_by_function_name?: Record<string, string>;
  health_state?: 'ready' | 'degraded' | 'restarting' | 'stopped';
};

export type cluster_ingress_target_resolver_constructor_params_t = {
  control_plane?: {
    gateway_adapter?: ClusterControlPlaneGatewayAdapter;
    endpoint?: cluster_control_plane_gateway_adapter_endpoint_t;
    endpoint_list?: cluster_control_plane_gateway_adapter_endpoint_t[];
    auth_headers?: Record<string, string>;
    auth_headers_provider?: cluster_control_plane_gateway_adapter_auth_headers_provider_t;
    request_timeout_ms?: number;
    retry_base_delay_ms?: number;
    retry_max_delay_ms?: number;
    endpoint_cooldown_ms?: number;
    max_request_attempts?: number;
  };
  discovery?: {
    service_discovery_store?: cluster_service_discovery_store_i;
    endpoint?: cluster_remote_service_discovery_endpoint_t;
    endpoint_list?: cluster_remote_service_discovery_endpoint_t[];
    auth_headers?: Record<string, string>;
    auth_headers_provider?: cluster_remote_service_discovery_auth_headers_provider_t;
    request_timeout_ms?: number;
    synchronization_interval_ms?: number;
    retry_base_delay_ms?: number;
    retry_max_delay_ms?: number;
    endpoint_cooldown_ms?: number;
    max_request_attempts?: number;
  };
  geo_control_plane?: {
    adapter?: ClusterGeoIngressAdapter;
    endpoint?: cluster_geo_ingress_adapter_endpoint_t;
    endpoint_list?: cluster_geo_ingress_adapter_endpoint_t[];
    auth_headers?: Record<string, string>;
    auth_headers_provider?: cluster_geo_ingress_adapter_auth_headers_provider_t;
    request_timeout_ms?: number;
    retry_base_delay_ms?: number;
    retry_max_delay_ms?: number;
    endpoint_cooldown_ms?: number;
    max_request_attempts?: number;
    enabled?: boolean;
    mode?: 'global' | 'regional';
    region_id?: string;
  };
  static_target_list?: cluster_ingress_static_target_t[];
  refresh_interval_ms?: number;
  stale_snapshot_max_age_ms?: number;
};

function BuildEmptySnapshot(params?: {
  now_unix_ms?: number;
}): ingress_balancer_target_snapshot_t {
  return {
    refreshed_unix_ms: params?.now_unix_ms ?? 0,
    stale_since_unix_ms: null,
    stale: false,
    source_success_by_name: {
      control_plane: false,
      discovery: false,
      static: false,
      geo_control_plane: false
    },
    target_list: []
  };
}

function HealthStateFromControlPlaneGateway(params: {
  gateway_record: cluster_control_plane_gateway_record_t;
}): ingress_balancer_target_record_t['health_state'] {
  const status = params.gateway_record.status;
  if (status === 'active') {
    return 'ready';
  }

  if (status === 'degraded' || status === 'draining') {
    return 'degraded';
  }

  return 'stopped';
}

function HealthStateFromDiscoveryRecord(params: {
  node_record: cluster_service_discovery_node_record_t;
}): ingress_balancer_target_record_t['health_state'] {
  const status = params.node_record.status;
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

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function ToStringArray(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }

  const string_list = value.filter((item): item is string => {
    return typeof item === 'string' && item.length > 0;
  });

  return string_list.length > 0 ? string_list : undefined;
}

function ToRecordStringArrayMap(value: unknown): Record<string, string> {
  if (!IsRecordObject(value)) {
    return {};
  }

  const mapped_record: Record<string, string> = {};
  for (const [key, mapped_value] of Object.entries(value)) {
    if (typeof mapped_value === 'string' && mapped_value.length > 0) {
      mapped_record[key] = mapped_value;
    }
  }

  return mapped_record;
}

export class ClusterIngressTargetResolver {
  private readonly geo_ingress_adapter: ClusterGeoIngressAdapter | null;
  private readonly geo_mode: 'global' | 'regional';
  private readonly geo_region_id: string | null;
  private readonly control_plane_gateway_adapter: ClusterControlPlaneGatewayAdapter | null;
  private readonly service_discovery_store: cluster_service_discovery_store_i | null;
  private readonly static_target_list: cluster_ingress_static_target_t[];
  private readonly created_service_discovery_store_internally: boolean;

  private readonly refresh_interval_ms: number;
  private readonly stale_snapshot_max_age_ms: number;
  private geo_control_stale_since_unix_ms: number | null = null;
  private geo_last_topology_snapshot: cluster_geo_ingress_topology_snapshot_t | null = null;
  private geo_last_routing_policy_snapshot:
    | cluster_geo_ingress_global_routing_policy_snapshot_t
    | null = null;

  private target_snapshot: ingress_balancer_target_snapshot_t = BuildEmptySnapshot({
    now_unix_ms: Date.now()
  });

  private refresh_timer_handle: NodeJS.Timeout | null = null;
  private refresh_in_progress = false;

  constructor(params: cluster_ingress_target_resolver_constructor_params_t = {}) {
    this.refresh_interval_ms = params.refresh_interval_ms ?? 1_000;
    this.stale_snapshot_max_age_ms = params.stale_snapshot_max_age_ms ?? 10_000;

    this.geo_mode = params.geo_control_plane?.mode ?? 'regional';
    this.geo_region_id =
      typeof params.geo_control_plane?.region_id === 'string' &&
      params.geo_control_plane.region_id.length > 0
        ? params.geo_control_plane.region_id
        : null;

    if (params.geo_control_plane?.adapter) {
      this.geo_ingress_adapter = params.geo_control_plane.adapter;
    } else if (
      params.geo_control_plane?.enabled !== false &&
      (params.geo_control_plane?.endpoint ||
        (params.geo_control_plane?.endpoint_list &&
          params.geo_control_plane.endpoint_list.length > 0))
    ) {
      this.geo_ingress_adapter = new ClusterGeoIngressAdapter({
        endpoint: params.geo_control_plane.endpoint,
        endpoint_list: params.geo_control_plane.endpoint_list,
        auth_headers: params.geo_control_plane.auth_headers,
        auth_headers_provider: params.geo_control_plane.auth_headers_provider,
        request_timeout_ms: params.geo_control_plane.request_timeout_ms,
        retry_base_delay_ms: params.geo_control_plane.retry_base_delay_ms,
        retry_max_delay_ms: params.geo_control_plane.retry_max_delay_ms,
        endpoint_cooldown_ms: params.geo_control_plane.endpoint_cooldown_ms,
        max_request_attempts: params.geo_control_plane.max_request_attempts
      });
    } else {
      this.geo_ingress_adapter = null;
    }

    if (params.control_plane?.gateway_adapter) {
      this.control_plane_gateway_adapter = params.control_plane.gateway_adapter;
    } else if (
      params.control_plane?.endpoint ||
      (params.control_plane?.endpoint_list &&
        params.control_plane.endpoint_list.length > 0)
    ) {
      this.control_plane_gateway_adapter = new ClusterControlPlaneGatewayAdapter({
        endpoint: params.control_plane.endpoint,
        endpoint_list: params.control_plane.endpoint_list,
        request_timeout_ms: params.control_plane.request_timeout_ms,
        retry_base_delay_ms: params.control_plane.retry_base_delay_ms,
        retry_max_delay_ms: params.control_plane.retry_max_delay_ms,
        endpoint_cooldown_ms: params.control_plane.endpoint_cooldown_ms,
        max_request_attempts: params.control_plane.max_request_attempts,
        auth_headers: params.control_plane.auth_headers,
        auth_headers_provider: params.control_plane.auth_headers_provider
      });
    } else {
      this.control_plane_gateway_adapter = null;
    }

    this.created_service_discovery_store_internally = Boolean(
      !params.discovery?.service_discovery_store &&
        (params.discovery?.endpoint ||
          (params.discovery?.endpoint_list &&
            params.discovery.endpoint_list.length > 0))
    );

    if (params.discovery?.service_discovery_store) {
      this.service_discovery_store = params.discovery.service_discovery_store;
    } else if (this.created_service_discovery_store_internally) {
      this.service_discovery_store = new ClusterRemoteServiceDiscoveryStoreAdapter({
        endpoint_list: params.discovery?.endpoint_list,
        host: params.discovery?.endpoint?.host,
        port: params.discovery?.endpoint?.port,
        request_path: params.discovery?.endpoint?.request_path,
        tls_mode: params.discovery?.endpoint?.tls_mode,
        auth_headers: params.discovery?.auth_headers,
        auth_headers_provider: params.discovery?.auth_headers_provider,
        request_timeout_ms: params.discovery?.request_timeout_ms,
        synchronization_interval_ms: params.discovery?.synchronization_interval_ms,
        retry_base_delay_ms: params.discovery?.retry_base_delay_ms,
        retry_max_delay_ms: params.discovery?.retry_max_delay_ms,
        endpoint_cooldown_ms: params.discovery?.endpoint_cooldown_ms,
        max_request_attempts: params.discovery?.max_request_attempts
      });
    } else {
      this.service_discovery_store = null;
    }

    this.static_target_list = [...(params.static_target_list ?? [])];
  }

  start(): void {
    if (this.created_service_discovery_store_internally && this.service_discovery_store) {
      this.service_discovery_store.startExpirationLoop();
    }

    if (this.refresh_timer_handle) {
      return;
    }

    this.refresh_timer_handle = setInterval((): void => {
      void this.refreshTargetSnapshot().catch((): void => {
        // periodic refresh failures are represented by stale snapshot state.
      });
    }, this.refresh_interval_ms);
    this.refresh_timer_handle.unref();
  }

  stop(): void {
    if (this.refresh_timer_handle) {
      clearInterval(this.refresh_timer_handle);
      this.refresh_timer_handle = null;
    }

    if (this.created_service_discovery_store_internally && this.service_discovery_store) {
      this.service_discovery_store.stopExpirationLoop();
    }
  }

  getSnapshot(): ingress_balancer_target_snapshot_t {
    return {
      refreshed_unix_ms: this.target_snapshot.refreshed_unix_ms,
      stale_since_unix_ms: this.target_snapshot.stale_since_unix_ms,
      stale: this.target_snapshot.stale,
      source_success_by_name: {
        ...this.target_snapshot.source_success_by_name
      },
      target_list: this.target_snapshot.target_list.map((target) => {
        return {
          ...target,
          endpoint: {
            ...target.endpoint
          },
          capability_hash_by_function_name: {
            ...target.capability_hash_by_function_name
          },
          zones: target.zones ? [...target.zones] : undefined,
          labels: target.labels ? { ...target.labels } : undefined,
          tenant_allow_list: target.tenant_allow_list
            ? [...target.tenant_allow_list]
            : undefined,
          environment_allow_list: target.environment_allow_list
            ? [...target.environment_allow_list]
            : undefined,
          metadata: target.metadata ? { ...target.metadata } : undefined
        };
      })
    };
  }

  async getEligibleTargetsForRequest(params: {
    request: cluster_call_request_message_i;
    now_unix_ms?: number;
  }): Promise<{
    candidate_list: ingress_balancer_target_record_t[];
    stale: boolean;
    stale_since_unix_ms: number | null;
    geo_routing_context?: {
      enabled: boolean;
      selected_region_id: string | null;
      ordered_region_id_list: string[];
      region_selection_reason: string;
      max_cross_region_attempts: number;
      retry_within_region_first: boolean;
      stale_control: boolean;
      stale_control_since_unix_ms: number | null;
    };
  }> {
    const now_unix_ms = params.now_unix_ms ?? Date.now();

    if (
      this.target_snapshot.refreshed_unix_ms === 0 ||
      now_unix_ms - this.target_snapshot.refreshed_unix_ms >= this.refresh_interval_ms
    ) {
      try {
        await this.refreshTargetSnapshot();
      } catch {
        // handled by stale snapshot fallback below.
      }
    }

    const snapshot_age_ms = now_unix_ms - this.target_snapshot.refreshed_unix_ms;
    if (
      this.target_snapshot.target_list.length === 0 ||
      snapshot_age_ms > this.stale_snapshot_max_age_ms
    ) {
      throw new Error('INGRESS_NO_TARGET');
    }

    let filtered_candidate_list = this.filterTargetsForRequest({
      target_list: this.target_snapshot.target_list,
      request: params.request
    });

    let geo_routing_context:
      | {
          enabled: boolean;
          selected_region_id: string | null;
          ordered_region_id_list: string[];
          region_selection_reason: string;
          max_cross_region_attempts: number;
          retry_within_region_first: boolean;
          stale_control: boolean;
          stale_control_since_unix_ms: number | null;
        }
      | undefined;

    if (
      this.geo_mode === 'global' &&
      (this.geo_last_routing_policy_snapshot || this.geo_last_topology_snapshot)
    ) {
      const geo_selection_result = this.applyGeoPolicyAndRegionSelection({
        request: params.request,
        candidate_list: filtered_candidate_list,
        now_unix_ms
      });

      filtered_candidate_list = geo_selection_result.candidate_list;
      geo_routing_context = {
        enabled: true,
        selected_region_id: geo_selection_result.selected_region_id,
        ordered_region_id_list: geo_selection_result.ordered_region_id_list,
        region_selection_reason: geo_selection_result.region_selection_reason,
        max_cross_region_attempts: geo_selection_result.max_cross_region_attempts,
        retry_within_region_first: geo_selection_result.retry_within_region_first,
        stale_control: geo_selection_result.stale_control,
        stale_control_since_unix_ms:
          geo_selection_result.stale_control_since_unix_ms
      };
    }

    return {
      candidate_list: filtered_candidate_list,
      stale: this.target_snapshot.stale,
      stale_since_unix_ms: this.target_snapshot.stale_since_unix_ms,
      geo_routing_context
    };
  }

  async refreshTargetSnapshot(): Promise<ingress_balancer_target_snapshot_t> {
    if (this.refresh_in_progress) {
      return this.getSnapshot();
    }

    this.refresh_in_progress = true;

    const now_unix_ms = Date.now();
    const source_success_by_name: ingress_balancer_target_snapshot_t['source_success_by_name'] =
      {
        control_plane: false,
        discovery: false,
        static: false,
        geo_control_plane: false
      };

    try {
      const merged_target_by_id = new Map<string, ingress_balancer_target_record_t>();

      if (this.static_target_list.length > 0) {
        for (const static_target of this.static_target_list) {
          merged_target_by_id.set(static_target.target_id, {
            target_id: static_target.target_id,
            gateway_id: static_target.target_id,
            node_id: static_target.node_id ?? static_target.target_id,
            endpoint: {
              ...static_target.endpoint
            },
            source: 'static',
            health_state: static_target.health_state ?? 'ready',
            inflight_calls: 0,
            pending_calls: 0,
            success_rate_1m: 1,
            timeout_rate_1m: 0,
            ewma_latency_ms: 0,
            capability_hash_by_function_name: {
              ...(static_target.capability_hash_by_function_name ?? {})
            },
            zones: static_target.zones ? [...static_target.zones] : undefined,
            labels: static_target.labels ? { ...static_target.labels } : undefined,
            updated_unix_ms: now_unix_ms
          });
        }

        source_success_by_name.static = true;
      }

      if (this.service_discovery_store) {
        try {
          const discovery_node_list = this.service_discovery_store.listNodes({
            include_expired: false
          });

          for (const node_record of discovery_node_list) {
            if (node_record.status === 'expired') {
              continue;
            }

            merged_target_by_id.set(node_record.node_identity.node_id, {
              target_id: node_record.node_identity.node_id,
              gateway_id: node_record.node_identity.node_id,
              node_id: node_record.node_identity.node_id,
              endpoint: {
                host: node_record.node_identity.address.host,
                port: node_record.node_identity.address.port,
                request_path: node_record.node_identity.address.request_path,
                tls_mode: node_record.node_identity.address.tls_mode
              },
              source: 'discovery',
              health_state: HealthStateFromDiscoveryRecord({
                node_record
              }),
              inflight_calls: node_record.metrics.inflight_calls,
              pending_calls: node_record.metrics.pending_calls,
              success_rate_1m: node_record.metrics.success_rate_1m,
              timeout_rate_1m: node_record.metrics.timeout_rate_1m,
              ewma_latency_ms: node_record.metrics.ewma_latency_ms,
              capability_hash_by_function_name: Object.fromEntries(
                node_record.capability_list
                  .filter((capability): boolean => {
                    return capability.installed;
                  })
                  .map((capability): [string, string] => {
                    return [capability.function_name, capability.function_hash_sha1];
                  })
              ),
              zones: node_record.node_identity.zones
                ? [...node_record.node_identity.zones]
                : undefined,
              labels: node_record.node_identity.labels
                ? { ...node_record.node_identity.labels }
                : undefined,
              updated_unix_ms: node_record.last_heartbeat_unix_ms
            });
          }

          source_success_by_name.discovery = true;
        } catch {
          source_success_by_name.discovery = false;
        }
      }

      if (this.control_plane_gateway_adapter) {
        try {
          const topology_response =
            await this.control_plane_gateway_adapter.getTopologySnapshot();
          const policy_response = await this.control_plane_gateway_adapter.getPolicySnapshot();

          for (const gateway_record of topology_response.topology_snapshot.gateway_list) {
            const node_id = gateway_record.node_reference.node_id;
            const existing_target = merged_target_by_id.get(node_id);

            const metadata = IsRecordObject(gateway_record.metadata)
              ? gateway_record.metadata
              : {};

            const capability_hash_by_function_name = IsRecordObject(
              metadata.capability_hash_by_function_name
            )
              ? ToRecordStringArrayMap(metadata.capability_hash_by_function_name)
              : existing_target?.capability_hash_by_function_name ?? {};

            merged_target_by_id.set(node_id, {
              target_id: node_id,
              gateway_id: gateway_record.gateway_id,
              node_id,
              endpoint: {
                host: gateway_record.address.host,
                port: gateway_record.address.port,
                request_path: gateway_record.address.request_path,
                tls_mode: gateway_record.address.tls_mode
              },
              source: 'control_plane',
              health_state: HealthStateFromControlPlaneGateway({
                gateway_record
              }),
              inflight_calls: existing_target?.inflight_calls ?? 0,
              pending_calls:
                gateway_record.node_reference.health_summary?.pending_call_count ??
                existing_target?.pending_calls ??
                0,
              success_rate_1m: existing_target?.success_rate_1m ?? 1,
              timeout_rate_1m: existing_target?.timeout_rate_1m ?? 0,
              ewma_latency_ms: existing_target?.ewma_latency_ms ?? 0,
              capability_hash_by_function_name,
              zones:
                ToStringArray(metadata.zones) ??
                existing_target?.zones ??
                undefined,
              labels:
                (IsRecordObject(metadata.labels)
                  ? (metadata.labels as Record<string, string>)
                  : existing_target?.labels) ?? undefined,
              tenant_allow_list:
                ToStringArray(metadata.tenant_allow_list) ??
                existing_target?.tenant_allow_list,
              environment_allow_list:
                ToStringArray(metadata.environment_allow_list) ??
                existing_target?.environment_allow_list,
              policy_version_id:
                gateway_record.applied_policy_version_id ??
                policy_response.active_policy_version_id,
              updated_unix_ms: gateway_record.last_heartbeat_unix_ms,
              metadata
            });
          }

          source_success_by_name.control_plane = true;
        } catch {
          source_success_by_name.control_plane = false;
        }
      }

      if (this.geo_ingress_adapter && this.geo_mode === 'global') {
        try {
          const [topology_response, policy_response] = await Promise.all([
            this.geo_ingress_adapter.getGlobalTopologySnapshot(),
            this.geo_ingress_adapter.getGlobalRoutingPolicy()
          ]);

          this.geo_last_topology_snapshot = structuredClone(
            topology_response.topology_snapshot
          );
          this.geo_last_routing_policy_snapshot = policy_response.active_policy_version_record
            ? structuredClone(policy_response.active_policy_version_record.snapshot)
            : this.geo_last_routing_policy_snapshot;
          this.geo_control_stale_since_unix_ms = null;
          source_success_by_name.geo_control_plane = true;

          const region_record_by_id = new Map(
            topology_response.topology_snapshot.region_record_list.map((region_record) => {
              return [region_record.region_id, region_record];
            })
          );

          for (const ingress_record of topology_response.topology_snapshot
            .ingress_instance_record_list) {
            const ingress_role =
              typeof ingress_record.metadata?.ingress_role === 'string'
                ? ingress_record.metadata.ingress_role
                : 'regional';

            if (ingress_role !== 'regional') {
              continue;
            }

            const region_record = region_record_by_id.get(ingress_record.region_id);

            merged_target_by_id.set(ingress_record.ingress_id, {
              target_id: ingress_record.ingress_id,
              gateway_id: ingress_record.ingress_id,
              node_id: ingress_record.ingress_id,
              endpoint: {
                host: ingress_record.endpoint.host,
                port: ingress_record.endpoint.port,
                request_path: ingress_record.endpoint.request_path,
                tls_mode: ingress_record.endpoint.tls_mode
              },
              source: 'geo_control_plane',
              health_state:
                ingress_record.health_status === 'ready' ||
                ingress_record.health_status === 'overloaded'
                  ? 'ready'
                  : ingress_record.health_status === 'degraded'
                    ? 'degraded'
                    : 'stopped',
              inflight_calls: ingress_record.inflight_calls,
              pending_calls: ingress_record.pending_calls,
              success_rate_1m: ingress_record.success_rate_1m,
              timeout_rate_1m: ingress_record.timeout_rate_1m,
              ewma_latency_ms: ingress_record.ewma_latency_ms,
              capability_hash_by_function_name: IsRecordObject(
                ingress_record.metadata?.capability_hash_by_function_name
              )
                ? ToRecordStringArrayMap(
                    ingress_record.metadata?.capability_hash_by_function_name
                  )
                : {},
              zones: ToStringArray(ingress_record.metadata?.zones),
              labels: IsRecordObject(ingress_record.metadata?.labels)
                ? (ingress_record.metadata?.labels as Record<string, string>)
                : undefined,
              tenant_allow_list: ToStringArray(
                ingress_record.metadata?.tenant_allow_list
              ),
              environment_allow_list: ToStringArray(
                ingress_record.metadata?.environment_allow_list
              ),
              policy_version_id: ingress_record.policy_version_id,
              updated_unix_ms: ingress_record.last_heartbeat_unix_ms,
              metadata: IsRecordObject(ingress_record.metadata)
                ? { ...ingress_record.metadata }
                : undefined,
              region_id: ingress_record.region_id,
              region_status: region_record?.status,
              region_priority: region_record?.priority,
              region_capacity_score: region_record?.capacity_score,
              region_latency_ewma_ms: region_record?.latency_ewma_ms
            });
          }

        } catch {
          source_success_by_name.geo_control_plane = false;
          if (!this.geo_control_stale_since_unix_ms) {
            this.geo_control_stale_since_unix_ms = now_unix_ms;
          }
        }
      }

      const target_list = [...merged_target_by_id.values()].sort(
        (left_target, right_target): number => {
          return left_target.target_id.localeCompare(right_target.target_id);
        }
      );

      if (target_list.length === 0) {
        throw new Error('No target records were resolved.');
      }

      this.target_snapshot = {
        refreshed_unix_ms: now_unix_ms,
        stale_since_unix_ms: null,
        stale: false,
        source_success_by_name,
        target_list
      };

      return this.getSnapshot();
    } catch {
      if (this.target_snapshot.target_list.length > 0) {
        if (!this.target_snapshot.stale_since_unix_ms) {
          this.target_snapshot.stale_since_unix_ms = now_unix_ms;
        }

        this.target_snapshot.stale = true;
        return this.getSnapshot();
      }

      throw new Error('No targets available from resolver sources.');
    } finally {
      this.refresh_in_progress = false;
    }
  }

  private applyGeoPolicyAndRegionSelection(params: {
    request: cluster_call_request_message_i;
    candidate_list: ingress_balancer_target_record_t[];
    now_unix_ms: number;
  }): {
    candidate_list: ingress_balancer_target_record_t[];
    selected_region_id: string | null;
    ordered_region_id_list: string[];
    region_selection_reason: string;
    max_cross_region_attempts: number;
    retry_within_region_first: boolean;
    stale_control: boolean;
    stale_control_since_unix_ms: number | null;
  } {
    const policy_snapshot = this.geo_last_routing_policy_snapshot ?? {
      default_mode: 'latency_aware',
      retry_within_region_first: true,
      max_cross_region_attempts: 2,
      stickiness_scope: 'none'
    };

    const topology_snapshot = this.geo_last_topology_snapshot;
    const stale_control =
      this.geo_control_stale_since_unix_ms !== null &&
      this.geo_control_stale_since_unix_ms <= params.now_unix_ms;

    const tenant_id = params.request.caller_identity.tenant_id;
    const request_environment =
      typeof params.request.metadata?.environment === 'string'
        ? params.request.metadata.environment
        : undefined;

    let filtered_candidate_list = params.candidate_list.filter((candidate): boolean => {
      if (!candidate.region_id) {
        return false;
      }

      const region_status = candidate.region_status ?? 'active';
      if (region_status === 'offline' || region_status === 'draining') {
        return false;
      }

      const allow_region_list = policy_snapshot.region_allow_by_tenant?.[tenant_id];
      if (Array.isArray(allow_region_list) && allow_region_list.length > 0) {
        if (!allow_region_list.includes(candidate.region_id)) {
          return false;
        }
      }

      const deny_region_list = policy_snapshot.region_deny_by_tenant?.[tenant_id];
      if (Array.isArray(deny_region_list) && deny_region_list.includes(candidate.region_id)) {
        return false;
      }

      if (typeof request_environment === 'string') {
        const environment_region_allow_list =
          policy_snapshot.environment_region_allow_list?.[request_environment];
        if (
          Array.isArray(environment_region_allow_list) &&
          environment_region_allow_list.length > 0 &&
          !environment_region_allow_list.includes(candidate.region_id)
        ) {
          return false;
        }
      }

      return true;
    });

    if (filtered_candidate_list.length === 0) {
      return {
        candidate_list: [],
        selected_region_id: null,
        ordered_region_id_list: [],
        region_selection_reason: 'no_eligible_geo_region',
        max_cross_region_attempts: policy_snapshot.max_cross_region_attempts,
        retry_within_region_first: policy_snapshot.retry_within_region_first,
        stale_control,
        stale_control_since_unix_ms: this.geo_control_stale_since_unix_ms
      };
    }

    const candidate_list_by_region_id = new Map<string, ingress_balancer_target_record_t[]>();
    for (const candidate of filtered_candidate_list) {
      const region_id = candidate.region_id as string;
      const region_candidate_list = candidate_list_by_region_id.get(region_id) ?? [];
      region_candidate_list.push(candidate);
      candidate_list_by_region_id.set(region_id, region_candidate_list);
    }

    const region_id_list = [...candidate_list_by_region_id.keys()].sort();
    const region_metadata_by_id = new Map(
      topology_snapshot?.region_record_list.map((region_record) => {
        return [region_record.region_id, region_record];
      }) ?? []
    );

    const requested_region_id =
      typeof params.request.metadata?.target_region_id === 'string'
        ? (params.request.metadata.target_region_id as string)
        : undefined;

    let selected_region_id: string | null = null;
    let region_selection_reason = 'geo_default_region_selected';

    if (typeof requested_region_id === 'string') {
      if (candidate_list_by_region_id.has(requested_region_id)) {
        selected_region_id = requested_region_id;
        region_selection_reason = 'geo_explicit_region_selected';
      } else {
        region_selection_reason = 'geo_explicit_region_not_found';
      }
    }

    if (!selected_region_id) {
      if (policy_snapshot.default_mode === 'capacity_aware') {
        selected_region_id = [...region_id_list].sort((left_region_id, right_region_id): number => {
          const left_region = region_metadata_by_id.get(left_region_id);
          const right_region = region_metadata_by_id.get(right_region_id);

          const left_capacity_score = left_region?.capacity_score ?? 0;
          const right_capacity_score = right_region?.capacity_score ?? 0;
          if (left_capacity_score !== right_capacity_score) {
            return right_capacity_score - left_capacity_score;
          }

          const left_pending_calls = (
            candidate_list_by_region_id.get(left_region_id) ?? []
          ).reduce((total, candidate): number => {
            return total + candidate.pending_calls + candidate.inflight_calls;
          }, 0);
          const right_pending_calls = (
            candidate_list_by_region_id.get(right_region_id) ?? []
          ).reduce((total, candidate): number => {
            return total + candidate.pending_calls + candidate.inflight_calls;
          }, 0);

          if (left_pending_calls !== right_pending_calls) {
            return left_pending_calls - right_pending_calls;
          }

          return left_region_id.localeCompare(right_region_id);
        })[0];
        region_selection_reason = 'geo_capacity_aware_selected';
      } else if (policy_snapshot.default_mode === 'affinity') {
        const affinity_key =
          String(params.request.routing_hint.affinity_key ?? params.request.caller_identity.subject);
        const affinity_hash = affinity_key
          .split('')
          .reduce((hash_value, character): number => {
            return (hash_value * 31 + character.charCodeAt(0)) >>> 0;
          }, 7);
        selected_region_id = region_id_list[affinity_hash % region_id_list.length] ?? null;
        region_selection_reason = 'geo_affinity_selected';
      } else {
        selected_region_id = [...region_id_list].sort((left_region_id, right_region_id): number => {
          const left_region = region_metadata_by_id.get(left_region_id);
          const right_region = region_metadata_by_id.get(right_region_id);

          const left_latency = left_region?.latency_ewma_ms ?? Number.POSITIVE_INFINITY;
          const right_latency = right_region?.latency_ewma_ms ?? Number.POSITIVE_INFINITY;
          if (left_latency !== right_latency) {
            return left_latency - right_latency;
          }

          const left_priority = left_region?.priority ?? Number.MAX_SAFE_INTEGER;
          const right_priority = right_region?.priority ?? Number.MAX_SAFE_INTEGER;
          if (left_priority !== right_priority) {
            return left_priority - right_priority;
          }

          return left_region_id.localeCompare(right_region_id);
        })[0];
        region_selection_reason = 'geo_latency_aware_selected';
      }
    }

    const region_priority_list = Array.isArray(policy_snapshot.region_priority_list)
      ? policy_snapshot.region_priority_list.filter((region_id): region_id is string => {
          return typeof region_id === 'string' && region_id.length > 0;
        })
      : [];

    let ordered_region_id_list = [...region_id_list].sort((left_region_id, right_region_id): number => {
      const left_priority_index = region_priority_list.indexOf(left_region_id);
      const right_priority_index = region_priority_list.indexOf(right_region_id);

      if (left_priority_index !== -1 || right_priority_index !== -1) {
        const normalized_left_priority =
          left_priority_index === -1 ? Number.MAX_SAFE_INTEGER : left_priority_index;
        const normalized_right_priority =
          right_priority_index === -1 ? Number.MAX_SAFE_INTEGER : right_priority_index;

        if (normalized_left_priority !== normalized_right_priority) {
          return normalized_left_priority - normalized_right_priority;
        }
      }

      return left_region_id.localeCompare(right_region_id);
    });

    if (selected_region_id && ordered_region_id_list.includes(selected_region_id)) {
      ordered_region_id_list = [
        selected_region_id,
        ...ordered_region_id_list.filter((region_id): boolean => {
          return region_id !== selected_region_id;
        })
      ];
    }

    if (policy_snapshot.retry_within_region_first !== false && selected_region_id) {
      ordered_region_id_list = [
        selected_region_id,
        ...ordered_region_id_list.filter((region_id): boolean => {
          return region_id !== selected_region_id;
        })
      ];
    }

    const ordered_candidate_list: ingress_balancer_target_record_t[] = [];
    for (const region_id of ordered_region_id_list) {
      const region_candidate_list = candidate_list_by_region_id.get(region_id) ?? [];
      region_candidate_list.sort((left_candidate, right_candidate): number => {
        const left_load = left_candidate.pending_calls + left_candidate.inflight_calls;
        const right_load = right_candidate.pending_calls + right_candidate.inflight_calls;
        if (left_load !== right_load) {
          return left_load - right_load;
        }

        return left_candidate.target_id.localeCompare(right_candidate.target_id);
      });

      ordered_candidate_list.push(...region_candidate_list);
    }

    return {
      candidate_list: ordered_candidate_list,
      selected_region_id,
      ordered_region_id_list,
      region_selection_reason,
      max_cross_region_attempts: Math.max(
        0,
        policy_snapshot.max_cross_region_attempts ?? 2
      ),
      retry_within_region_first: policy_snapshot.retry_within_region_first !== false,
      stale_control,
      stale_control_since_unix_ms: this.geo_control_stale_since_unix_ms
    };
  }

  private filterTargetsForRequest(params: {
    target_list: ingress_balancer_target_record_t[];
    request: cluster_call_request_message_i;
  }): ingress_balancer_target_record_t[] {
    const tenant_id = params.request.caller_identity.tenant_id;
    const request_environment =
      typeof params.request.metadata?.environment === 'string'
        ? params.request.metadata.environment
        : undefined;
    const request_zone = params.request.routing_hint.zone;

    let filtered_target_list = params.target_list.filter((target): boolean => {
      if (
        target.health_state === 'stopped' ||
        target.health_state === 'restarting'
      ) {
        return false;
      }

      if (
        Array.isArray(target.tenant_allow_list) &&
        target.tenant_allow_list.length > 0 &&
        !target.tenant_allow_list.includes(tenant_id)
      ) {
        return false;
      }

      if (
        typeof request_environment === 'string' &&
        Array.isArray(target.environment_allow_list) &&
        target.environment_allow_list.length > 0 &&
        !target.environment_allow_list.includes(request_environment)
      ) {
        return false;
      }

      return true;
    });

    filtered_target_list = filtered_target_list.filter((target): boolean => {
      const known_function_hash =
        target.capability_hash_by_function_name[params.request.function_name];

      if (
        Object.keys(target.capability_hash_by_function_name).length > 0 &&
        typeof known_function_hash !== 'string'
      ) {
        return false;
      }

      if (
        typeof params.request.function_hash_sha1 === 'string' &&
        typeof known_function_hash === 'string' &&
        params.request.function_hash_sha1 !== known_function_hash
      ) {
        return false;
      }

      return true;
    });

    if (typeof request_zone === 'string' && request_zone.length > 0) {
      filtered_target_list = filtered_target_list.filter((target): boolean => {
        if (!target.zones || target.zones.length === 0) {
          return false;
        }

        return target.zones.includes(request_zone);
      });
    }

    return filtered_target_list.sort((left_target, right_target): number => {
      return left_target.target_id.localeCompare(right_target.target_id);
    });
  }
}
