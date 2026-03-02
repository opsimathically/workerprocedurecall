import {
  createServer,
  type Http2Server,
  type Http2ServerRequest,
  type Http2ServerResponse,
  type IncomingHttpHeaders
} from 'node:http2';

import type { cluster_call_request_message_i } from '../clusterprotocol/ClusterProtocolTypes';
import {
  ClusterGeoIngressAdapter,
  type cluster_geo_ingress_adapter_auth_headers_provider_t,
  type cluster_geo_ingress_adapter_endpoint_t
} from '../clustergeoingress/ClusterGeoIngressAdapter.class';
import {
  ClusterIngressForwarder,
  type cluster_ingress_forwarder_auth_headers_provider_t
} from './ClusterIngressForwarder.class';
import {
  ClusterIngressRoutingEngine,
  type cluster_ingress_routing_mode_t
} from './ClusterIngressRoutingEngine.class';
import {
  ClusterIngressTargetResolver,
  type cluster_ingress_static_target_t,
  type cluster_ingress_target_resolver_constructor_params_t
} from './ClusterIngressTargetResolver.class';
import {
  BuildIngressBalancerError,
  ParseIngressCallRequestMessage
} from './ClusterIngressBalancerProtocolValidators';
import type {
  ingress_balancer_event_t,
  ingress_balancer_metrics_t,
  ingress_balancer_target_record_t,
  ingress_balancer_target_snapshot_t
} from './ClusterIngressBalancerProtocol';

export type cluster_ingress_balancer_identity_t = {
  subject: string;
  tenant_id: string;
  scopes: string[];
  environment?: string;
  signed_claims?: string;
};

export type cluster_ingress_balancer_auth_result_t =
  | {
      ok: true;
      identity: cluster_ingress_balancer_identity_t;
    }
  | {
      ok: false;
      message: string;
      details?: Record<string, unknown>;
    };

export type cluster_ingress_balancer_authenticate_request_t = (params: {
  headers: IncomingHttpHeaders;
  request: cluster_call_request_message_i;
}) =>
  | cluster_ingress_balancer_auth_result_t
  | Promise<cluster_ingress_balancer_auth_result_t>;

export type cluster_ingress_balancer_authorize_call_result_t =
  | {
      ok: true;
    }
  | {
      ok: false;
      message: string;
      details?: Record<string, unknown>;
    };

export type cluster_ingress_balancer_authorize_call_request_t = (params: {
  request: cluster_call_request_message_i;
  identity?: cluster_ingress_balancer_identity_t;
}) =>
  | cluster_ingress_balancer_authorize_call_result_t
  | Promise<cluster_ingress_balancer_authorize_call_result_t>;

export type cluster_ingress_balancer_service_constructor_params_t = {
  enabled?: boolean;
  ingress_id?: string;
  host?: string;
  port?: number;
  request_path?: string;
  routing_mode?: cluster_ingress_routing_mode_t;
  max_attempts?: number;
  request_timeout_ms?: number;
  stale_snapshot_max_age_ms?: number;
  target_refresh_interval_ms?: number;
  sticky_ttl_ms?: number;
  target_resolver?: ClusterIngressTargetResolver;
  target_resolver_config?: cluster_ingress_target_resolver_constructor_params_t;
  static_target_list?: cluster_ingress_static_target_t[];
  authenticate_request?: cluster_ingress_balancer_authenticate_request_t;
  authorize_call_request?: cluster_ingress_balancer_authorize_call_request_t;
  forward_request_headers?: Record<string, string>;
  forward_request_headers_provider?: cluster_ingress_forwarder_auth_headers_provider_t;
  geo_ingress?: {
    enabled?: boolean;
    role?: 'global' | 'regional';
    region_id?: string;
    endpoint?: cluster_geo_ingress_adapter_endpoint_t;
    endpoint_list?: cluster_geo_ingress_adapter_endpoint_t[];
    auth_headers?: Record<string, string>;
    auth_headers_provider?: cluster_geo_ingress_adapter_auth_headers_provider_t;
    request_timeout_ms?: number;
    retry_base_delay_ms?: number;
    retry_max_delay_ms?: number;
    endpoint_cooldown_ms?: number;
    max_request_attempts?: number;
    sync_interval_ms?: number;
    lease_ttl_ms?: number;
    stale_snapshot_max_age_ms?: number;
    max_cross_region_attempts?: number;
    region_priority?: number;
    region_capacity_score?: number;
    region_latency_slo_ms?: number;
    region_latency_ewma_ms?: number;
  };
};

export type cluster_ingress_balancer_address_t = {
  host: string;
  port: number;
  request_path: string;
};

type ingress_target_dispatch_stats_t = {
  success_total: number;
  failure_total: number;
  retry_total: number;
  last_error_message: string | null;
  last_latency_ms: number | null;
};

type cluster_ingress_geo_routing_context_t = {
  enabled: boolean;
  selected_region_id: string | null;
  ordered_region_id_list: string[];
  region_selection_reason: string;
  max_cross_region_attempts: number;
  retry_within_region_first: boolean;
  stale_control: boolean;
  stale_control_since_unix_ms: number | null;
};

function BuildRequestPath(params: { request_path: string | undefined }): string {
  if (typeof params.request_path !== 'string' || params.request_path.length === 0) {
    return '/wpc/cluster/ingress';
  }

  if (params.request_path.startsWith('/')) {
    return params.request_path;
  }

  return `/${params.request_path}`;
}

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

async function ReadRequestBody(params: {
  request: Http2ServerRequest;
}): Promise<string> {
  return await new Promise<string>((resolve, reject): void => {
    const chunk_list: Buffer[] = [];

    params.request.on('data', (chunk): void => {
      if (typeof chunk === 'string') {
        chunk_list.push(Buffer.from(chunk));
        return;
      }

      chunk_list.push(chunk as Buffer);
    });

    params.request.on('end', (): void => {
      resolve(Buffer.concat(chunk_list).toString('utf8'));
    });

    params.request.on('error', (error): void => {
      reject(error);
    });
  });
}

function BuildDefaultMetrics(): ingress_balancer_metrics_t {
  return {
    ingress_requests_total: 0,
    ingress_success_total: 0,
    ingress_failure_total: 0,
    ingress_retry_total: 0,
    ingress_failover_total: 0,
    ingress_no_target_total: 0,
    ingress_dispatch_latency_ms: {
      count: 0,
      min: 0,
      max: 0,
      avg: 0,
      total: 0
    },
    ingress_target_selection_count_by_reason: {},
    geo_ingress_requests_total: 0,
    geo_ingress_success_total: 0,
    geo_ingress_failure_total: 0,
    geo_ingress_region_failover_total: 0,
    geo_ingress_region_selection_count_by_reason: {},
    geo_ingress_control_stale_total: 0,
    geo_ingress_dispatch_latency_ms: {
      count: 0,
      min: 0,
      max: 0,
      avg: 0,
      total: 0
    }
  };
}

function BuildAuthorizationFromIdentity(params: {
  request: cluster_call_request_message_i;
  identity: cluster_ingress_balancer_identity_t;
}): cluster_call_request_message_i {
  return {
    ...params.request,
    caller_identity: {
      subject: params.identity.subject,
      tenant_id: params.identity.tenant_id,
      scopes: [...params.identity.scopes],
      signed_claims: params.identity.signed_claims ?? params.request.caller_identity.signed_claims
    }
  };
}

function HasCallScope(params: {
  scope_list: string[];
  function_name: string;
}): boolean {
  if (params.scope_list.includes('rpc.call:*')) {
    return true;
  }

  if (params.scope_list.includes(`rpc.call:function:${params.function_name}`)) {
    return true;
  }

  return false;
}

export class ClusterIngressBalancerService {
  private readonly enabled: boolean;
  private readonly ingress_id: string;
  private readonly request_path: string;

  private host: string;
  private port: number;
  private readonly routing_mode: cluster_ingress_routing_mode_t;
  private readonly max_attempts: number;

  private readonly target_resolver: ClusterIngressTargetResolver;
  private readonly routing_engine: ClusterIngressRoutingEngine;
  private readonly forwarder: ClusterIngressForwarder;

  private readonly authenticate_request:
    | cluster_ingress_balancer_authenticate_request_t
    | undefined;
  private readonly authorize_call_request:
    | cluster_ingress_balancer_authorize_call_request_t
    | undefined;
  private readonly geo_ingress_enabled: boolean;
  private readonly geo_ingress_role: 'global' | 'regional';
  private readonly geo_ingress_region_id: string;
  private readonly geo_ingress_sync_interval_ms: number;
  private readonly geo_ingress_lease_ttl_ms: number;
  private readonly geo_ingress_stale_snapshot_max_age_ms: number;
  private readonly geo_ingress_max_cross_region_attempts: number | null;
  private readonly geo_ingress_region_priority: number | undefined;
  private readonly geo_ingress_region_capacity_score: number | undefined;
  private readonly geo_ingress_region_latency_slo_ms: number | undefined;
  private readonly geo_ingress_region_latency_ewma_ms: number | undefined;
  private readonly geo_ingress_adapter: ClusterGeoIngressAdapter | null;
  private geo_ingress_last_sync_error_message: string | null = null;
  private geo_ingress_sync_interval_handle: NodeJS.Timeout | null = null;

  private http2_server: Http2Server | null = null;

  private metrics: ingress_balancer_metrics_t = BuildDefaultMetrics();
  private recent_event_list: ingress_balancer_event_t[] = [];
  private max_event_history_count = 5_000;
  private next_event_index = 1;

  private target_dispatch_stats_by_target_id = new Map<
    string,
    ingress_target_dispatch_stats_t
  >();

  constructor(params: cluster_ingress_balancer_service_constructor_params_t = {}) {
    this.enabled = params.enabled !== false;
    this.ingress_id = params.ingress_id ?? `ingress_${process.pid}`;
    this.host = params.host ?? '127.0.0.1';
    this.port = params.port ?? 0;
    this.request_path = BuildRequestPath({
      request_path: params.request_path
    });
    this.routing_mode = params.routing_mode ?? 'least_loaded';
    this.max_attempts = params.max_attempts ?? 3;

    this.geo_ingress_enabled = params.geo_ingress?.enabled === true;
    this.geo_ingress_role = params.geo_ingress?.role ?? 'global';
    this.geo_ingress_region_id =
      typeof params.geo_ingress?.region_id === 'string' &&
      params.geo_ingress.region_id.length > 0
        ? params.geo_ingress.region_id
        : 'region_default';
    this.geo_ingress_sync_interval_ms = params.geo_ingress?.sync_interval_ms ?? 1_000;
    this.geo_ingress_lease_ttl_ms =
      params.geo_ingress?.lease_ttl_ms ??
      Math.max(5_000, this.geo_ingress_sync_interval_ms * 3);
    this.geo_ingress_stale_snapshot_max_age_ms =
      params.geo_ingress?.stale_snapshot_max_age_ms ?? 15_000;
    this.geo_ingress_max_cross_region_attempts =
      typeof params.geo_ingress?.max_cross_region_attempts === 'number'
        ? Math.max(0, params.geo_ingress.max_cross_region_attempts)
        : null;
    this.geo_ingress_region_priority =
      typeof params.geo_ingress?.region_priority === 'number'
        ? params.geo_ingress.region_priority
        : undefined;
    this.geo_ingress_region_capacity_score =
      typeof params.geo_ingress?.region_capacity_score === 'number'
        ? params.geo_ingress.region_capacity_score
        : undefined;
    this.geo_ingress_region_latency_slo_ms =
      typeof params.geo_ingress?.region_latency_slo_ms === 'number'
        ? params.geo_ingress.region_latency_slo_ms
        : undefined;
    this.geo_ingress_region_latency_ewma_ms =
      typeof params.geo_ingress?.region_latency_ewma_ms === 'number'
        ? params.geo_ingress.region_latency_ewma_ms
        : undefined;

    if (params.geo_ingress?.enabled === true && params.geo_ingress?.endpoint_list) {
      this.geo_ingress_adapter = new ClusterGeoIngressAdapter({
        endpoint: params.geo_ingress.endpoint,
        endpoint_list: params.geo_ingress.endpoint_list,
        auth_headers: params.geo_ingress.auth_headers,
        auth_headers_provider: params.geo_ingress.auth_headers_provider,
        request_timeout_ms: params.geo_ingress.request_timeout_ms,
        retry_base_delay_ms: params.geo_ingress.retry_base_delay_ms,
        retry_max_delay_ms: params.geo_ingress.retry_max_delay_ms,
        endpoint_cooldown_ms: params.geo_ingress.endpoint_cooldown_ms,
        max_request_attempts: params.geo_ingress.max_request_attempts
      });
    } else if (
      params.geo_ingress?.enabled === true &&
      params.geo_ingress?.endpoint
    ) {
      this.geo_ingress_adapter = new ClusterGeoIngressAdapter({
        endpoint: params.geo_ingress.endpoint,
        endpoint_list: params.geo_ingress.endpoint_list,
        auth_headers: params.geo_ingress.auth_headers,
        auth_headers_provider: params.geo_ingress.auth_headers_provider,
        request_timeout_ms: params.geo_ingress.request_timeout_ms,
        retry_base_delay_ms: params.geo_ingress.retry_base_delay_ms,
        retry_max_delay_ms: params.geo_ingress.retry_max_delay_ms,
        endpoint_cooldown_ms: params.geo_ingress.endpoint_cooldown_ms,
        max_request_attempts: params.geo_ingress.max_request_attempts
      });
    } else {
      this.geo_ingress_adapter = null;
    }

    const resolved_target_resolver_config: cluster_ingress_target_resolver_constructor_params_t = {
      ...(params.target_resolver_config ?? {})
    };

    if (this.geo_ingress_enabled) {
      resolved_target_resolver_config.geo_control_plane = {
        ...(params.target_resolver_config?.geo_control_plane ?? {}),
        enabled: params.target_resolver_config?.geo_control_plane?.enabled ?? true,
        mode:
          params.target_resolver_config?.geo_control_plane?.mode ??
          (this.geo_ingress_role === 'global' ? 'global' : 'regional'),
        region_id:
          params.target_resolver_config?.geo_control_plane?.region_id ??
          this.geo_ingress_region_id,
        adapter:
          params.target_resolver_config?.geo_control_plane?.adapter ??
          this.geo_ingress_adapter ??
          undefined,
        endpoint:
          params.target_resolver_config?.geo_control_plane?.endpoint ??
          params.geo_ingress?.endpoint,
        endpoint_list:
          params.target_resolver_config?.geo_control_plane?.endpoint_list ??
          params.geo_ingress?.endpoint_list,
        auth_headers:
          params.target_resolver_config?.geo_control_plane?.auth_headers ??
          params.geo_ingress?.auth_headers,
        auth_headers_provider:
          params.target_resolver_config?.geo_control_plane
            ?.auth_headers_provider ??
          params.geo_ingress?.auth_headers_provider,
        request_timeout_ms:
          params.target_resolver_config?.geo_control_plane?.request_timeout_ms ??
          params.geo_ingress?.request_timeout_ms,
        retry_base_delay_ms:
          params.target_resolver_config?.geo_control_plane
            ?.retry_base_delay_ms ??
          params.geo_ingress?.retry_base_delay_ms,
        retry_max_delay_ms:
          params.target_resolver_config?.geo_control_plane?.retry_max_delay_ms ??
          params.geo_ingress?.retry_max_delay_ms,
        endpoint_cooldown_ms:
          params.target_resolver_config?.geo_control_plane
            ?.endpoint_cooldown_ms ??
          params.geo_ingress?.endpoint_cooldown_ms,
        max_request_attempts:
          params.target_resolver_config?.geo_control_plane?.max_request_attempts ??
          params.geo_ingress?.max_request_attempts
      };
    }

    this.target_resolver =
      params.target_resolver ??
      new ClusterIngressTargetResolver({
        ...resolved_target_resolver_config,
        static_target_list:
          params.target_resolver_config?.static_target_list ??
          params.static_target_list,
        refresh_interval_ms:
          params.target_resolver_config?.refresh_interval_ms ??
          params.target_refresh_interval_ms,
        stale_snapshot_max_age_ms:
          params.target_resolver_config?.stale_snapshot_max_age_ms ??
          params.stale_snapshot_max_age_ms
      });

    this.routing_engine = new ClusterIngressRoutingEngine({
      default_mode: this.routing_mode,
      sticky_ttl_ms: params.sticky_ttl_ms
    });

    this.forwarder = new ClusterIngressForwarder({
      request_timeout_ms: params.request_timeout_ms,
      static_request_headers: params.forward_request_headers,
      auth_headers_provider: params.forward_request_headers_provider
    });

    this.authenticate_request = params.authenticate_request;
    this.authorize_call_request = params.authorize_call_request;
  }

  async start(params?: {
    host?: string;
    port?: number;
  }): Promise<cluster_ingress_balancer_address_t> {
    if (!this.enabled) {
      throw new Error('ClusterIngressBalancerService is disabled.');
    }

    if (this.http2_server) {
      return this.getAddress();
    }

    this.host = params?.host ?? this.host;
    this.port = params?.port ?? this.port;

    this.target_resolver.start();
    await this.target_resolver.refreshTargetSnapshot().catch((): void => {
      // service can start before initial target convergence.
    });

    this.http2_server = createServer();
    this.http2_server.on('request', (request, response): void => {
      void this.handleHttpRequest({ request, response });
    });

    await new Promise<void>((resolve, reject): void => {
      this.http2_server?.once('error', reject);
      this.http2_server?.listen(this.port, this.host, (): void => {
        this.http2_server?.off('error', reject);
        resolve();
      });
    });

    const address = this.http2_server.address();
    if (!address || typeof address === 'string') {
      throw new Error('Failed to resolve ingress service address.');
    }

    this.host = address.address;
    this.port = address.port;

    if (this.geo_ingress_enabled) {
      await this.registerIngressInGeoControlPlane().catch((error): void => {
        this.geo_ingress_last_sync_error_message =
          error instanceof Error ? error.message : String(error);
      });
      this.startGeoIngressLifecycleLoop();
    }

    return this.getAddress();
  }

  async stop(): Promise<void> {
    this.stopGeoIngressLifecycleLoop();
    this.target_resolver.stop();

    if (this.geo_ingress_enabled) {
      await this.deregisterIngressFromGeoControlPlane().catch((): void => {
        // ignore geo deregistration failures during shutdown.
      });
    }

    if (!this.http2_server) {
      return;
    }

    const server_to_close = this.http2_server;
    this.http2_server = null;

    await new Promise<void>((resolve, reject): void => {
      server_to_close.close((error): void => {
        if (error) {
          if (
            error instanceof Error &&
            'code' in error &&
            (error as Error & { code?: string }).code === 'ERR_SERVER_NOT_RUNNING'
          ) {
            resolve();
            return;
          }

          reject(error);
          return;
        }
        resolve();
      });
    });
  }

  getAddress(): cluster_ingress_balancer_address_t {
    return {
      host: this.host,
      port: this.port,
      request_path: this.request_path
    };
  }

  getMetrics(): ingress_balancer_metrics_t {
    return {
      ...this.metrics,
      ingress_dispatch_latency_ms: {
        ...this.metrics.ingress_dispatch_latency_ms
      },
      ingress_target_selection_count_by_reason: {
        ...this.metrics.ingress_target_selection_count_by_reason
      },
      geo_ingress_region_selection_count_by_reason: {
        ...this.metrics.geo_ingress_region_selection_count_by_reason
      },
      geo_ingress_dispatch_latency_ms: {
        ...this.metrics.geo_ingress_dispatch_latency_ms
      }
    };
  }

  getRecentEvents(params: {
    limit?: number;
    event_name?: ingress_balancer_event_t['event_name'];
    request_id?: string;
  } = {}): ingress_balancer_event_t[] {
    let event_list = [...this.recent_event_list];

    if (typeof params.event_name === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.event_name === params.event_name;
      });
    }

    if (typeof params.request_id === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.request_id === params.request_id;
      });
    }

    if (typeof params.limit === 'number' && params.limit >= 0) {
      event_list = event_list.slice(Math.max(0, event_list.length - params.limit));
    }

    return event_list.map((event): ingress_balancer_event_t => {
      return {
        ...event,
        details: event.details ? { ...event.details } : undefined
      };
    });
  }

  getSnapshot(): {
    ingress_id: string;
    routing_mode: cluster_ingress_routing_mode_t;
    max_attempts: number;
    geo_ingress: {
      enabled: boolean;
      role: 'global' | 'regional';
      region_id: string;
      stale_snapshot_max_age_ms: number;
      last_sync_error_message: string | null;
    };
    metrics: ingress_balancer_metrics_t;
    target_snapshot: ingress_balancer_target_snapshot_t;
    target_dispatch_stats_by_target_id: Record<string, ingress_target_dispatch_stats_t>;
  } {
    const target_dispatch_stats_by_target_id: Record<string, ingress_target_dispatch_stats_t> = {};

    for (const [target_id, stats] of this.target_dispatch_stats_by_target_id.entries()) {
      target_dispatch_stats_by_target_id[target_id] = { ...stats };
    }

    return {
      ingress_id: this.ingress_id,
      routing_mode: this.routing_mode,
      max_attempts: this.max_attempts,
      geo_ingress: {
        enabled: this.geo_ingress_enabled,
        role: this.geo_ingress_role,
        region_id: this.geo_ingress_region_id,
        stale_snapshot_max_age_ms: this.geo_ingress_stale_snapshot_max_age_ms,
        last_sync_error_message: this.geo_ingress_last_sync_error_message
      },
      metrics: this.getMetrics(),
      target_snapshot: this.target_resolver.getSnapshot(),
      target_dispatch_stats_by_target_id
    };
  }

  private async handleHttpRequest(params: {
    request: Http2ServerRequest;
    response: Http2ServerResponse;
  }): Promise<void> {
    const started_unix_ms = Date.now();

    if (params.request.method !== 'POST') {
      this.writeWireResponse({
        response: params.response,
        payload: {
          ack: {
            protocol_version: 1,
            message_type: 'cluster_call_ack',
            timestamp_unix_ms: Date.now(),
            request_id: 'unknown_request',
            attempt_index: 1,
            node_id: this.ingress_id,
            accepted: false,
            queue_position: 0,
            estimated_start_delay_ms: 0
          },
          terminal_message: {
            protocol_version: 1,
            message_type: 'cluster_call_response_error',
            timestamp_unix_ms: Date.now(),
            request_id: 'unknown_request',
            attempt_index: 1,
            node_id: this.ingress_id,
            error: BuildIngressBalancerError({
              code: 'INGRESS_VALIDATION_FAILED',
              message: 'Ingress accepts only POST requests.',
              retryable: false,
              unknown_outcome: false,
              details: {
                method: params.request.method
              }
            }),
            timing: {
              gateway_received_unix_ms: started_unix_ms,
              last_attempt_started_unix_ms: started_unix_ms
            }
          }
        }
      });
      return;
    }

    if (params.request.url !== this.request_path) {
      this.writeWireResponse({
        response: params.response,
        payload: {
          ack: {
            protocol_version: 1,
            message_type: 'cluster_call_ack',
            timestamp_unix_ms: Date.now(),
            request_id: 'unknown_request',
            attempt_index: 1,
            node_id: this.ingress_id,
            accepted: false,
            queue_position: 0,
            estimated_start_delay_ms: 0
          },
          terminal_message: {
            protocol_version: 1,
            message_type: 'cluster_call_response_error',
            timestamp_unix_ms: Date.now(),
            request_id: 'unknown_request',
            attempt_index: 1,
            node_id: this.ingress_id,
            error: BuildIngressBalancerError({
              code: 'INGRESS_VALIDATION_FAILED',
              message: 'Ingress request path mismatch.',
              retryable: false,
              unknown_outcome: false,
              details: {
                path: params.request.url
              }
            }),
            timing: {
              gateway_received_unix_ms: started_unix_ms,
              last_attempt_started_unix_ms: started_unix_ms
            }
          }
        }
      });
      return;
    }

    this.metrics.ingress_requests_total += 1;

    let call_request: cluster_call_request_message_i | null = null;

    try {
      const request_body = await ReadRequestBody({
        request: params.request
      });
      const parsed_json = JSON.parse(request_body) as unknown;
      const parse_result = ParseIngressCallRequestMessage({
        message: parsed_json
      });

      if (!parse_result.ok) {
        this.recordRequestFailure({
          reason: parse_result.error.code
        });
        this.writeWireResponse({
          response: params.response,
          payload: {
            ack: {
              protocol_version: 1,
              message_type: 'cluster_call_ack',
              timestamp_unix_ms: Date.now(),
              request_id:
                IsRecordObject(parsed_json) && typeof parsed_json.request_id === 'string'
                  ? parsed_json.request_id
                  : 'unknown_request',
              attempt_index: 1,
              node_id: this.ingress_id,
              accepted: false,
              queue_position: 0,
              estimated_start_delay_ms: 0
            },
            terminal_message: {
              protocol_version: 1,
              message_type: 'cluster_call_response_error',
              timestamp_unix_ms: Date.now(),
              request_id:
                IsRecordObject(parsed_json) && typeof parsed_json.request_id === 'string'
                  ? parsed_json.request_id
                  : 'unknown_request',
              attempt_index: 1,
              node_id: this.ingress_id,
              error: parse_result.error,
              timing: {
                gateway_received_unix_ms: started_unix_ms,
                last_attempt_started_unix_ms: started_unix_ms
              }
            }
          }
        });
        return;
      }

      call_request = parse_result.value;
      this.recordEvent({
        event_name: 'ingress_request_received',
        request_id: call_request.request_id,
        trace_id: call_request.trace_id,
        details: {
          function_name: call_request.function_name
        }
      });

      let effective_request = call_request;
      let authenticated_identity: cluster_ingress_balancer_identity_t | undefined;

      if (this.authenticate_request) {
        const auth_result = await this.authenticate_request({
          headers: params.request.headers,
          request: call_request
        });

        if (!auth_result.ok) {
          const wire_response = ClusterIngressForwarder.buildIngressNoTargetResponse({
            request: call_request,
            error_code: 'INGRESS_AUTH_FAILED',
            message: auth_result.message,
            details: auth_result.details
          });

          this.recordRequestFailure({
            reason: 'INGRESS_AUTH_FAILED'
          });
          this.writeWireResponse({
            response: params.response,
            payload: wire_response
          });
          return;
        }

        authenticated_identity = auth_result.identity;
        effective_request = BuildAuthorizationFromIdentity({
          request: call_request,
          identity: authenticated_identity
        });
      }

      if (authenticated_identity) {
        if (
          authenticated_identity.tenant_id !== effective_request.caller_identity.tenant_id ||
          !HasCallScope({
            scope_list: authenticated_identity.scopes,
            function_name: effective_request.function_name
          })
        ) {
          const wire_response = ClusterIngressForwarder.buildIngressNoTargetResponse({
            request: effective_request,
            error_code: 'INGRESS_FORBIDDEN',
            message: 'Ingress authorization denied for this call request.'
          });

          this.recordRequestFailure({
            reason: 'INGRESS_FORBIDDEN'
          });
          this.writeWireResponse({
            response: params.response,
            payload: wire_response
          });
          return;
        }
      }

      if (this.authorize_call_request) {
        const authz_result = await this.authorize_call_request({
          request: effective_request,
          identity: authenticated_identity
        });

        if (!authz_result.ok) {
          const wire_response = ClusterIngressForwarder.buildIngressNoTargetResponse({
            request: effective_request,
            error_code: 'INGRESS_FORBIDDEN',
            message: authz_result.message,
            details: authz_result.details
          });

          this.recordRequestFailure({
            reason: 'INGRESS_FORBIDDEN'
          });
          this.writeWireResponse({
            response: params.response,
            payload: wire_response
          });
          return;
        }
      }

      const resolver_result = await this.target_resolver.getEligibleTargetsForRequest({
        request: effective_request
      });
      const geo_routing_context =
        resolver_result.geo_routing_context as cluster_ingress_geo_routing_context_t | undefined;

      if (geo_routing_context?.enabled) {
        this.metrics.geo_ingress_requests_total += 1;
        this.metrics.geo_ingress_region_selection_count_by_reason[
          geo_routing_context.region_selection_reason
        ] =
          (this.metrics.geo_ingress_region_selection_count_by_reason[
            geo_routing_context.region_selection_reason
          ] ?? 0) + 1;

        this.recordEvent({
          event_name: 'geo_ingress_region_selected',
          request_id: effective_request.request_id,
          trace_id: effective_request.trace_id,
          details: {
            selected_region_id: geo_routing_context.selected_region_id,
            ordered_region_id_list: geo_routing_context.ordered_region_id_list,
            region_selection_reason: geo_routing_context.region_selection_reason
          }
        });

        if (geo_routing_context.stale_control) {
          this.metrics.geo_ingress_control_stale_total += 1;

          const stale_since_unix_ms =
            geo_routing_context.stale_control_since_unix_ms ?? Date.now();
          const stale_duration_ms = Date.now() - stale_since_unix_ms;
          this.recordEvent({
            event_name: 'geo_ingress_region_degraded',
            request_id: effective_request.request_id,
            trace_id: effective_request.trace_id,
            details: {
              stale_since_unix_ms,
              stale_duration_ms
            }
          });

          if (stale_duration_ms > this.geo_ingress_stale_snapshot_max_age_ms) {
            const wire_response = ClusterIngressForwarder.buildIngressNoTargetResponse({
              request: effective_request,
              error_code: 'INGRESS_NO_TARGET',
              message: 'Geo ingress control snapshot is stale beyond allowed age.',
              details: {
                stale_since_unix_ms,
                stale_duration_ms,
                stale_snapshot_max_age_ms:
                  this.geo_ingress_stale_snapshot_max_age_ms
              }
            });

            this.metrics.geo_ingress_failure_total += 1;
            this.recordRequestFailure({
              reason: 'INGRESS_NO_TARGET'
            });
            this.writeWireResponse({
              response: params.response,
              payload: wire_response
            });
            return;
          }
        }
      }

      if (resolver_result.candidate_list.length === 0) {
        const wire_response = ClusterIngressForwarder.buildIngressNoTargetResponse({
          request: effective_request,
          error_code: 'INGRESS_NO_TARGET',
          message: 'No eligible ingress targets were available after filtering.'
        });
        this.recordRequestFailure({
          reason: 'INGRESS_NO_TARGET'
        });
        this.writeWireResponse({
          response: params.response,
          payload: wire_response
        });
        return;
      }

      const dispatch_plan = this.routing_engine.buildDispatchPlan({
        request: effective_request,
        candidate_list: resolver_result.candidate_list
      });

      this.metrics.ingress_target_selection_count_by_reason[dispatch_plan.routing_decision.reason] =
        (this.metrics.ingress_target_selection_count_by_reason[
          dispatch_plan.routing_decision.reason
        ] ?? 0) + 1;

      this.recordEvent({
        event_name: 'ingress_target_selected',
        request_id: effective_request.request_id,
        trace_id: effective_request.trace_id,
        target_id: dispatch_plan.routing_decision.selected_target_id ?? undefined,
        details: {
          mode: dispatch_plan.routing_decision.mode,
          reason: dispatch_plan.routing_decision.reason,
          candidate_target_id_list: dispatch_plan.routing_decision.candidate_target_id_list,
          stale_snapshot: resolver_result.stale
        }
      });

      if (dispatch_plan.ordered_candidate_list.length === 0) {
        const wire_response = ClusterIngressForwarder.buildIngressNoTargetResponse({
          request: effective_request,
          error_code: 'INGRESS_FORBIDDEN',
          message: 'Explicit target node was not eligible for dispatch.'
        });

        this.recordRequestFailure({
          reason: 'INGRESS_FORBIDDEN'
        });
        this.writeWireResponse({
          response: params.response,
          payload: wire_response
        });
        return;
      }

      let ordered_candidate_list_for_dispatch = dispatch_plan.ordered_candidate_list;
      let max_attempts_for_dispatch = this.max_attempts;
      if (geo_routing_context?.enabled) {
        const geo_dispatch_plan = this.buildGeoAwareDispatchCandidateList({
          ordered_candidate_list: dispatch_plan.ordered_candidate_list,
          geo_routing_context
        });
        ordered_candidate_list_for_dispatch = geo_dispatch_plan.ordered_candidate_list;
        max_attempts_for_dispatch = Math.max(
          1,
          Math.min(this.max_attempts, geo_dispatch_plan.max_attempts)
        );
      }

      if (ordered_candidate_list_for_dispatch.length === 0) {
        const wire_response = ClusterIngressForwarder.buildIngressNoTargetResponse({
          request: effective_request,
          error_code: 'INGRESS_NO_TARGET',
          message: 'Geo routing produced no dispatchable regional targets.'
        });

        if (geo_routing_context?.enabled) {
          this.metrics.geo_ingress_failure_total += 1;
        }
        this.recordRequestFailure({
          reason: 'INGRESS_NO_TARGET'
        });
        this.writeWireResponse({
          response: params.response,
          payload: wire_response
        });
        return;
      }

      const forwarding_result = await this.forwarder.dispatchWithFailover({
        request: effective_request,
        ordered_candidate_list: ordered_candidate_list_for_dispatch,
        max_attempts: max_attempts_for_dispatch
      });

      for (const attempt of forwarding_result.forwarding_attempt_list) {
        this.updateTargetDispatchStats({
          target_id: attempt.target_id,
          success: attempt.outcome === 'success',
          retry: attempt.outcome === 'retryable_error' || attempt.outcome === 'transport_error',
          latency_ms: attempt.duration_ms,
          error_message: attempt.error?.message
        });
      }

      if (forwarding_result.forwarding_attempt_list.length > 1) {
        this.metrics.ingress_failover_total += 1;
      }

      const retry_attempt_count = Math.max(
        0,
        forwarding_result.forwarding_attempt_list.length - 1
      );
      this.metrics.ingress_retry_total += retry_attempt_count;

      const last_attempt =
        forwarding_result.forwarding_attempt_list[
          forwarding_result.forwarding_attempt_list.length - 1
        ];

      this.recordDispatchLatency({
        latency_ms: last_attempt?.duration_ms ?? Date.now() - started_unix_ms
      });
      if (geo_routing_context?.enabled) {
        this.recordGeoDispatchLatency({
          latency_ms: last_attempt?.duration_ms ?? Date.now() - started_unix_ms
        });
      }

      if (
        forwarding_result.call_response.terminal_message.message_type ===
        'cluster_call_response_success'
      ) {
        this.metrics.ingress_success_total += 1;
        if (geo_routing_context?.enabled) {
          this.metrics.geo_ingress_success_total += 1;
        }
      } else {
        this.recordRequestFailure({
          reason: forwarding_result.call_response.terminal_message.error.code
        });
        if (geo_routing_context?.enabled) {
          this.metrics.geo_ingress_failure_total += 1;
        }
      }

      if (geo_routing_context?.enabled) {
        const geo_failover_summary = this.summarizeGeoFailover({
          forwarding_attempt_list: forwarding_result.forwarding_attempt_list,
          ordered_candidate_list_for_dispatch
        });

        if (geo_failover_summary.cross_region_failover_occurred) {
          this.metrics.geo_ingress_region_failover_total += 1;
          this.recordEvent({
            event_name: 'geo_ingress_failover_attempted',
            request_id: effective_request.request_id,
            trace_id: effective_request.trace_id,
            details: {
              region_transition_list: geo_failover_summary.region_transition_list
            }
          });
        }

        if (
          geo_failover_summary.cross_region_failover_occurred &&
          forwarding_result.call_response.terminal_message.message_type ===
            'cluster_call_response_success'
        ) {
          this.recordEvent({
            event_name: 'geo_ingress_region_restored',
            request_id: effective_request.request_id,
            trace_id: effective_request.trace_id,
            target_id: last_attempt?.target_id,
            details: {
              selected_region_id: geo_routing_context.selected_region_id,
              final_region_id: geo_failover_summary.final_region_id
            }
          });
        }
      }

      this.recordEvent({
        event_name:
          forwarding_result.call_response.terminal_message.message_type ===
          'cluster_call_response_success'
            ? 'ingress_request_completed'
            : 'ingress_dispatch_failed',
        request_id: effective_request.request_id,
        trace_id: effective_request.trace_id,
        target_id: last_attempt?.target_id,
        details: {
          forwarding_attempt_count: forwarding_result.forwarding_attempt_list.length,
          terminal_message_type:
            forwarding_result.call_response.terminal_message.message_type,
          terminal_error_code:
            forwarding_result.call_response.terminal_message.message_type ===
            'cluster_call_response_error'
              ? forwarding_result.call_response.terminal_message.error.code
              : undefined
        }
      });

      if (retry_attempt_count > 0) {
        this.recordEvent({
          event_name: 'ingress_failover_attempted',
          request_id: effective_request.request_id,
          trace_id: effective_request.trace_id,
          target_id: last_attempt?.target_id,
          details: {
            retry_attempt_count
          }
        });
      }

      this.writeWireResponse({
        response: params.response,
        payload: forwarding_result.call_response
      });
    } catch (error) {
      const request_id = call_request?.request_id ?? 'unknown_request';
      const request_timestamp = call_request?.timestamp_unix_ms ?? started_unix_ms;
      const error_message = error instanceof Error ? error.message : String(error);
      const ingress_error_code =
        error_message === 'INGRESS_NO_TARGET'
          ? 'INGRESS_NO_TARGET'
          : 'INGRESS_INTERNAL';

      this.recordRequestFailure({
        reason: ingress_error_code
      });
      this.recordEvent({
        event_name: 'ingress_dispatch_failed',
        request_id,
        trace_id: call_request?.trace_id,
        details: {
          message: error_message,
          ingress_error_code
        }
      });

      this.writeWireResponse({
        response: params.response,
        payload: {
          ack: {
            protocol_version: 1,
            message_type: 'cluster_call_ack',
            timestamp_unix_ms: Date.now(),
            request_id,
            attempt_index: call_request?.attempt_index ?? 1,
            node_id: this.ingress_id,
            accepted: false,
            queue_position: 0,
            estimated_start_delay_ms: 0
          },
          terminal_message: {
            protocol_version: 1,
            message_type: 'cluster_call_response_error',
            timestamp_unix_ms: Date.now(),
            request_id,
            attempt_index: call_request?.attempt_index ?? 1,
            node_id: this.ingress_id,
            error: BuildIngressBalancerError({
              code: ingress_error_code,
              message:
                ingress_error_code === 'INGRESS_NO_TARGET'
                  ? 'Ingress had no eligible target nodes.'
                  : `Ingress internal error: ${error_message}`,
              retryable: ingress_error_code !== 'INGRESS_INTERNAL',
              unknown_outcome: ingress_error_code === 'INGRESS_INTERNAL',
              details: {
                original_error: error_message
              }
            }),
            timing: {
              gateway_received_unix_ms: request_timestamp,
              last_attempt_started_unix_ms: Date.now()
            }
          }
        }
      });
    }
  }

  private startGeoIngressLifecycleLoop(): void {
    if (!this.geo_ingress_enabled || !this.geo_ingress_adapter) {
      return;
    }

    if (this.geo_ingress_sync_interval_handle) {
      return;
    }

    this.geo_ingress_sync_interval_handle = setInterval((): void => {
      void this.publishGeoIngressHeartbeat();
    }, this.geo_ingress_sync_interval_ms);
    this.geo_ingress_sync_interval_handle.unref();
  }

  private stopGeoIngressLifecycleLoop(): void {
    if (this.geo_ingress_sync_interval_handle) {
      clearInterval(this.geo_ingress_sync_interval_handle);
      this.geo_ingress_sync_interval_handle = null;
    }
  }

  private async registerIngressInGeoControlPlane(): Promise<void> {
    if (!this.geo_ingress_enabled || !this.geo_ingress_adapter) {
      return;
    }

    const address = this.getAddress();
    await this.geo_ingress_adapter.registerIngress({
      ingress_id: this.ingress_id,
      region_id: this.geo_ingress_region_id,
      endpoint: {
        host: address.host,
        port: address.port,
        request_path: address.request_path,
        tls_mode: 'disabled'
      },
      health_status: this.resolveGeoIngressHealthStatus(),
      ingress_version: 'phase19',
      lease_ttl_ms: this.geo_ingress_lease_ttl_ms,
      region_metadata: {
        status: 'active',
        priority: this.geo_ingress_region_priority,
        capacity_score: this.geo_ingress_region_capacity_score,
        latency_slo_ms: this.geo_ingress_region_latency_slo_ms,
        latency_ewma_ms: this.geo_ingress_region_latency_ewma_ms
      },
      metrics_summary: this.buildGeoIngressMetricsSummary(),
      metadata: {
        ingress_role: this.geo_ingress_role
      }
    });

    if (this.geo_ingress_last_sync_error_message) {
      this.recordEvent({
        event_name: 'geo_ingress_region_restored',
        details: {
          region_id: this.geo_ingress_region_id,
          reason: 'geo_registration_recovered'
        }
      });
    }

    this.geo_ingress_last_sync_error_message = null;
  }

  private async publishGeoIngressHeartbeat(): Promise<void> {
    if (!this.geo_ingress_enabled || !this.geo_ingress_adapter) {
      return;
    }

    try {
      await this.geo_ingress_adapter.heartbeatIngress({
        ingress_id: this.ingress_id,
        region_id: this.geo_ingress_region_id,
        health_status: this.resolveGeoIngressHealthStatus(),
        ingress_version: 'phase19',
        lease_ttl_ms: this.geo_ingress_lease_ttl_ms,
        region_metadata: {
          status: 'active',
          priority: this.geo_ingress_region_priority,
          capacity_score: this.geo_ingress_region_capacity_score,
          latency_slo_ms: this.geo_ingress_region_latency_slo_ms,
          latency_ewma_ms: this.geo_ingress_region_latency_ewma_ms
        },
        metrics_summary: this.buildGeoIngressMetricsSummary(),
        metadata: {
          ingress_role: this.geo_ingress_role
        }
      });

      if (this.geo_ingress_last_sync_error_message) {
        this.recordEvent({
          event_name: 'geo_ingress_region_restored',
          details: {
            region_id: this.geo_ingress_region_id,
            reason: 'geo_heartbeat_recovered'
          }
        });
      }
      this.geo_ingress_last_sync_error_message = null;
    } catch (error) {
      this.geo_ingress_last_sync_error_message =
        error instanceof Error ? error.message : String(error);
      this.recordEvent({
        event_name: 'geo_ingress_region_degraded',
        details: {
          region_id: this.geo_ingress_region_id,
          message: this.geo_ingress_last_sync_error_message
        }
      });
    }
  }

  private async deregisterIngressFromGeoControlPlane(): Promise<void> {
    if (!this.geo_ingress_enabled || !this.geo_ingress_adapter) {
      return;
    }

    await this.geo_ingress_adapter.deregisterIngress({
      ingress_id: this.ingress_id,
      reason: 'ingress_service_stop'
    });
  }

  private resolveGeoIngressHealthStatus(): 'ready' | 'overloaded' | 'degraded' {
    const recent_failure_rate =
      this.metrics.ingress_requests_total === 0
        ? 0
        : this.metrics.ingress_failure_total / this.metrics.ingress_requests_total;

    if (recent_failure_rate >= 0.5) {
      return 'degraded';
    }

    if (this.metrics.ingress_retry_total > this.metrics.ingress_success_total + 5) {
      return 'overloaded';
    }

    return 'ready';
  }

  private buildGeoIngressMetricsSummary(): {
    inflight_calls: number;
    pending_calls: number;
    success_rate_1m: number;
    timeout_rate_1m: number;
    ewma_latency_ms: number;
  } {
    const ingress_request_total = Math.max(1, this.metrics.ingress_requests_total);
    const success_rate_1m = this.metrics.ingress_success_total / ingress_request_total;
    const timeout_rate_1m = this.metrics.ingress_failure_total / ingress_request_total;

    return {
      inflight_calls: 0,
      pending_calls: 0,
      success_rate_1m,
      timeout_rate_1m,
      ewma_latency_ms: this.metrics.ingress_dispatch_latency_ms.avg
    };
  }

  private buildGeoAwareDispatchCandidateList(params: {
    ordered_candidate_list: ingress_balancer_target_record_t[];
    geo_routing_context: cluster_ingress_geo_routing_context_t;
  }): {
    ordered_candidate_list: ingress_balancer_target_record_t[];
    max_attempts: number;
  } {
    const unique_region_id_list: string[] = [];
    const candidate_list_by_region_id = new Map<string, ingress_balancer_target_record_t[]>();

    for (const candidate of params.ordered_candidate_list) {
      const region_id = candidate.region_id;
      if (typeof region_id !== 'string' || region_id.length === 0) {
        continue;
      }

      if (!candidate_list_by_region_id.has(region_id)) {
        unique_region_id_list.push(region_id);
        candidate_list_by_region_id.set(region_id, []);
      }
      candidate_list_by_region_id.get(region_id)?.push(candidate);
    }

    if (unique_region_id_list.length === 0) {
      return {
        ordered_candidate_list: [],
        max_attempts: 0
      };
    }

    const selected_region_id =
      params.geo_routing_context.selected_region_id ?? unique_region_id_list[0];
    const configured_cross_region_attempts =
      this.geo_ingress_max_cross_region_attempts ??
      params.geo_routing_context.max_cross_region_attempts;

    const candidate_region_order = [
      selected_region_id,
      ...params.geo_routing_context.ordered_region_id_list.filter((region_id): boolean => {
        return region_id !== selected_region_id;
      }),
      ...unique_region_id_list.filter((region_id): boolean => {
        return (
          region_id !== selected_region_id &&
          !params.geo_routing_context.ordered_region_id_list.includes(region_id)
        );
      })
    ].filter((region_id, index, list): boolean => {
      return (
        typeof region_id === 'string' &&
        region_id.length > 0 &&
        list.indexOf(region_id) === index &&
        candidate_list_by_region_id.has(region_id)
      );
    });

    const allowed_region_id_list = candidate_region_order.slice(
      0,
      Math.max(1, 1 + configured_cross_region_attempts)
    );

    const bounded_candidate_list: ingress_balancer_target_record_t[] = [];
    if (params.geo_routing_context.retry_within_region_first) {
      for (const region_id of allowed_region_id_list) {
        const region_candidate_list = candidate_list_by_region_id.get(region_id) ?? [];
        bounded_candidate_list.push(...region_candidate_list);
      }
    } else {
      const mutable_region_candidate_list_by_id = new Map(
        allowed_region_id_list.map((region_id) => {
          return [region_id, [...(candidate_list_by_region_id.get(region_id) ?? [])]];
        })
      );

      let appended_candidate = true;
      while (appended_candidate) {
        appended_candidate = false;
        for (const region_id of allowed_region_id_list) {
          const region_candidate_list =
            mutable_region_candidate_list_by_id.get(region_id) ?? [];
          const next_candidate = region_candidate_list.shift();
          if (next_candidate) {
            bounded_candidate_list.push(next_candidate);
            appended_candidate = true;
          }
          mutable_region_candidate_list_by_id.set(region_id, region_candidate_list);
        }
      }
    }

    return {
      ordered_candidate_list: bounded_candidate_list,
      max_attempts: bounded_candidate_list.length
    };
  }

  private summarizeGeoFailover(params: {
    forwarding_attempt_list: {
      target_id: string;
    }[];
    ordered_candidate_list_for_dispatch: ingress_balancer_target_record_t[];
  }): {
    cross_region_failover_occurred: boolean;
    region_transition_list: string[];
    final_region_id: string | null;
  } {
    const region_id_by_target_id = new Map<string, string>();
    for (const candidate of params.ordered_candidate_list_for_dispatch) {
      if (candidate.region_id) {
        region_id_by_target_id.set(candidate.target_id, candidate.region_id);
      }
    }

    const region_transition_list: string[] = [];
    for (const attempt of params.forwarding_attempt_list) {
      const region_id = region_id_by_target_id.get(attempt.target_id);
      if (!region_id) {
        continue;
      }

      if (
        region_transition_list.length === 0 ||
        region_transition_list[region_transition_list.length - 1] !== region_id
      ) {
        region_transition_list.push(region_id);
      }
    }

    return {
      cross_region_failover_occurred: region_transition_list.length > 1,
      region_transition_list,
      final_region_id:
        region_transition_list.length > 0
          ? region_transition_list[region_transition_list.length - 1]
          : null
    };
  }

  private updateTargetDispatchStats(params: {
    target_id: string;
    success: boolean;
    retry: boolean;
    latency_ms: number;
    error_message?: string;
  }): void {
    const current_stats = this.target_dispatch_stats_by_target_id.get(params.target_id) ?? {
      success_total: 0,
      failure_total: 0,
      retry_total: 0,
      last_error_message: null,
      last_latency_ms: null
    };

    if (params.success) {
      current_stats.success_total += 1;
    } else {
      current_stats.failure_total += 1;
    }

    if (params.retry) {
      current_stats.retry_total += 1;
    }

    current_stats.last_error_message = params.error_message ?? null;
    current_stats.last_latency_ms = params.latency_ms;

    this.target_dispatch_stats_by_target_id.set(params.target_id, current_stats);
  }

  private recordDispatchLatency(params: { latency_ms: number }): void {
    const latency_metrics = this.metrics.ingress_dispatch_latency_ms;
    latency_metrics.count += 1;
    latency_metrics.total += params.latency_ms;
    latency_metrics.avg = latency_metrics.total / latency_metrics.count;

    if (latency_metrics.count === 1) {
      latency_metrics.min = params.latency_ms;
      latency_metrics.max = params.latency_ms;
    } else {
      latency_metrics.min = Math.min(latency_metrics.min, params.latency_ms);
      latency_metrics.max = Math.max(latency_metrics.max, params.latency_ms);
    }
  }

  private recordGeoDispatchLatency(params: { latency_ms: number }): void {
    const latency_metrics = this.metrics.geo_ingress_dispatch_latency_ms;
    latency_metrics.count += 1;
    latency_metrics.total += params.latency_ms;
    latency_metrics.avg = latency_metrics.total / latency_metrics.count;

    if (latency_metrics.count === 1) {
      latency_metrics.min = params.latency_ms;
      latency_metrics.max = params.latency_ms;
    } else {
      latency_metrics.min = Math.min(latency_metrics.min, params.latency_ms);
      latency_metrics.max = Math.max(latency_metrics.max, params.latency_ms);
    }
  }

  private recordRequestFailure(params: {
    reason: string;
  }): void {
    this.metrics.ingress_failure_total += 1;
    if (params.reason === 'INGRESS_NO_TARGET') {
      this.metrics.ingress_no_target_total += 1;
    }
  }

  private recordEvent(params: {
    event_name: ingress_balancer_event_t['event_name'];
    request_id?: string;
    trace_id?: string;
    target_id?: string;
    details?: Record<string, unknown>;
  }): void {
    this.recent_event_list.push({
      event_id: `ingress_event_${Date.now()}_${this.next_event_index++}`,
      timestamp_unix_ms: Date.now(),
      event_name: params.event_name,
      request_id: params.request_id,
      trace_id: params.trace_id,
      target_id: params.target_id,
      details: params.details ? { ...params.details } : undefined
    });

    if (this.recent_event_list.length > this.max_event_history_count) {
      this.recent_event_list.splice(
        0,
        this.recent_event_list.length - this.max_event_history_count
      );
    }
  }

  private writeWireResponse(params: {
    response: Http2ServerResponse;
    payload: Record<string, unknown>;
  }): void {
    if (params.response.writableEnded) {
      return;
    }

    params.response.statusCode = 200;
    params.response.setHeader('content-type', 'application/json; charset=utf-8');
    params.response.end(JSON.stringify(params.payload));
  }
}
