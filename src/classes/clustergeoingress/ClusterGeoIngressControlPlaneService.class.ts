import {
  createServer,
  type Http2Server,
  type Http2ServerRequest,
  type Http2ServerResponse,
  type IncomingHttpHeaders
} from 'node:http2';
import { createHash } from 'node:crypto';

import type {
  cluster_geo_ingress_event_t,
  cluster_geo_ingress_global_routing_policy_snapshot_t,
  cluster_geo_ingress_instance_record_t,
  cluster_geo_ingress_metrics_t,
  cluster_geo_ingress_policy_version_record_t,
  cluster_geo_ingress_protocol_error_t,
  cluster_geo_ingress_protocol_request_message_type_t,
  cluster_geo_ingress_region_record_t,
  cluster_geo_ingress_request_message_t,
  cluster_geo_ingress_response_error_message_i,
  cluster_geo_ingress_response_success_message_i,
  cluster_geo_ingress_topology_snapshot_t
} from './ClusterGeoIngressProtocol';
import {
  BuildClusterGeoIngressProtocolError,
  ParseClusterGeoIngressRequestMessage
} from './ClusterGeoIngressProtocolValidators';
import {
  ClusterGeoIngressInMemoryStateStore,
  type cluster_geo_ingress_state_retention_policy_t,
  type cluster_geo_ingress_state_snapshot_t,
  type cluster_geo_ingress_state_store_i
} from './ClusterGeoIngressStateStore.class';

export type cluster_geo_ingress_control_plane_identity_t = {
  subject: string;
  scopes: string[];
};

export type cluster_geo_ingress_control_plane_auth_result_t =
  | {
      ok: true;
      identity: cluster_geo_ingress_control_plane_identity_t;
    }
  | {
      ok: false;
      message: string;
      details?: Record<string, unknown>;
    };

export type cluster_geo_ingress_control_plane_authenticate_request_t = (params: {
  headers: IncomingHttpHeaders;
  request_message: cluster_geo_ingress_request_message_t;
}) =>
  | cluster_geo_ingress_control_plane_auth_result_t
  | Promise<cluster_geo_ingress_control_plane_auth_result_t>;

export type cluster_geo_ingress_control_plane_service_constructor_params_t = {
  host?: string;
  port?: number;
  request_path?: string;
  state_store?: cluster_geo_ingress_state_store_i;
  authenticate_request?: cluster_geo_ingress_control_plane_authenticate_request_t;
  ingress_default_lease_ttl_ms?: number;
  ingress_expiration_check_interval_ms?: number;
  retention_policy?: Partial<cluster_geo_ingress_state_retention_policy_t>;
};

export type cluster_geo_ingress_control_plane_address_t = {
  host: string;
  port: number;
  request_path: string;
};

type geo_ingress_protocol_error_exception_t = {
  kind: 'protocol_error';
  error: cluster_geo_ingress_protocol_error_t;
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

function BuildDefaultRetentionPolicy(): cluster_geo_ingress_state_retention_policy_t {
  return {
    event_history_max_count: 5_000,
    offline_ingress_retention_ms: 60_000
  };
}

function BuildRequestPath(params: { request_path: string | undefined }): string {
  if (typeof params.request_path !== 'string' || params.request_path.length === 0) {
    return '/wpc/cluster/geo-ingress';
  }

  if (params.request_path.startsWith('/')) {
    return params.request_path;
  }

  return `/${params.request_path}`;
}

function ThrowProtocolError(params: {
  error: cluster_geo_ingress_protocol_error_t;
}): never {
  throw {
    kind: 'protocol_error',
    error: params.error
  } as geo_ingress_protocol_error_exception_t;
}

function IsProtocolErrorException(
  value: unknown
): value is geo_ingress_protocol_error_exception_t {
  return (
    IsRecordObject(value) &&
    value.kind === 'protocol_error' &&
    IsRecordObject(value.error)
  );
}

async function ReadRequestBody(params: {
  request: Http2ServerRequest;
}): Promise<string> {
  const chunk_list: Buffer[] = [];

  return await new Promise<string>((resolve, reject): void => {
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

function HasScope(params: {
  scope_list: string[];
  required_scope: string;
}): boolean {
  if (params.scope_list.includes('rpc.admin.geo_ingress:*')) {
    return true;
  }

  if (params.scope_list.includes(params.required_scope)) {
    return true;
  }

  if (params.required_scope.startsWith('rpc.read.geo_ingress:')) {
    if (params.scope_list.includes('rpc.read.geo_ingress:*')) {
      return true;
    }

    if (params.scope_list.includes('rpc.read.cluster:*')) {
      return true;
    }
  }

  if (params.required_scope === 'rpc.read.debug:*') {
    if (params.scope_list.includes('rpc.read.debug:*')) {
      return true;
    }
  }

  return false;
}

export class ClusterGeoIngressControlPlaneService {
  private readonly request_path: string;
  private readonly state_store: cluster_geo_ingress_state_store_i;
  private readonly authenticate_request:
    | cluster_geo_ingress_control_plane_authenticate_request_t
    | undefined;

  private host: string;
  private port: number;

  private http2_server: Http2Server | null = null;
  private ingress_expiration_interval_handle: NodeJS.Timeout | null = null;

  private ingress_default_lease_ttl_ms: number;
  private ingress_expiration_check_interval_ms: number;
  private retention_policy: cluster_geo_ingress_state_retention_policy_t;

  private started_unix_ms: number | null = null;

  private region_record_by_id = new Map<string, cluster_geo_ingress_region_record_t>();
  private ingress_instance_record_by_id = new Map<string, cluster_geo_ingress_instance_record_t>();
  private policy_version_record_by_id = new Map<string, cluster_geo_ingress_policy_version_record_t>();
  private active_policy_version_id: string | null = null;
  private active_policy_snapshot: cluster_geo_ingress_global_routing_policy_snapshot_t =
    BuildDefaultPolicySnapshot();

  private event_list: cluster_geo_ingress_event_t[] = [];
  private service_generation = 0;
  private next_event_index = 1;
  private next_policy_version_index = 1;

  private region_failover_state: {
    active_region_id_list: string[];
    degraded_region_id_list: string[];
    failover_reason: string | null;
    last_transition_unix_ms: number | null;
  } = {
    active_region_id_list: [],
    degraded_region_id_list: [],
    failover_reason: null,
    last_transition_unix_ms: null
  };

  private metrics: cluster_geo_ingress_metrics_t = {
    geo_ingress_requests_total: 0,
    geo_ingress_success_total: 0,
    geo_ingress_failure_total: 0,
    geo_ingress_region_failover_total: 0,
    geo_ingress_region_selection_count_by_reason: {},
    geo_ingress_control_stale_total: 0,
    geo_ingress_dispatch_latency_ms: {
      count: 0,
      total: 0,
      min: 0,
      max: 0,
      avg: 0
    },
    geo_ingress_request_failure_count_by_reason: {}
  };

  constructor(params: cluster_geo_ingress_control_plane_service_constructor_params_t = {}) {
    this.host = params.host ?? '127.0.0.1';
    this.port = params.port ?? 0;
    this.request_path = BuildRequestPath({
      request_path: params.request_path
    });

    this.state_store = params.state_store ?? new ClusterGeoIngressInMemoryStateStore();
    this.authenticate_request = params.authenticate_request;

    this.ingress_default_lease_ttl_ms = params.ingress_default_lease_ttl_ms ?? 15_000;
    this.ingress_expiration_check_interval_ms =
      params.ingress_expiration_check_interval_ms ?? 1_000;

    this.retention_policy = {
      ...BuildDefaultRetentionPolicy(),
      ...(params.retention_policy ?? {})
    };

    this.restoreStateFromStore();
  }

  async start(params: {
    host?: string;
    port?: number;
  } = {}): Promise<cluster_geo_ingress_control_plane_address_t> {
    if (this.http2_server) {
      return this.getAddress();
    }

    this.host = params.host ?? this.host;
    this.port = params.port ?? this.port;

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
      throw new Error('Failed to resolve geo ingress control-plane service address.');
    }

    this.host = address.address;
    this.port = address.port;
    this.started_unix_ms = Date.now();

    this.startIngressExpirationLoop();

    return this.getAddress();
  }

  async stop(): Promise<void> {
    this.stopIngressExpirationLoop();

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

    this.persistState();
  }

  getAddress(): cluster_geo_ingress_control_plane_address_t {
    return {
      host: this.host,
      port: this.port,
      request_path: this.request_path
    };
  }

  getMetrics(): cluster_geo_ingress_metrics_t {
    return {
      ...this.metrics,
      geo_ingress_region_selection_count_by_reason: {
        ...this.metrics.geo_ingress_region_selection_count_by_reason
      },
      geo_ingress_request_failure_count_by_reason: {
        ...this.metrics.geo_ingress_request_failure_count_by_reason
      },
      geo_ingress_dispatch_latency_ms: {
        ...this.metrics.geo_ingress_dispatch_latency_ms
      }
    };
  }

  getRecentEvents(params: {
    limit?: number;
  } = {}): cluster_geo_ingress_event_t[] {
    let event_list = [...this.event_list];

    if (typeof params.limit === 'number' && params.limit >= 0) {
      event_list = event_list.slice(Math.max(0, event_list.length - params.limit));
    }

    return event_list.map((event): cluster_geo_ingress_event_t => {
      return {
        ...event,
        details: event.details ? { ...event.details } : undefined
      };
    });
  }

  getTopologySnapshot(): cluster_geo_ingress_topology_snapshot_t {
    this.expireStaleIngressInstances({
      now_unix_ms: Date.now()
    });

    return this.buildTopologySnapshot({
      now_unix_ms: Date.now()
    });
  }

  getServiceStatusSnapshot(): {
    captured_at_unix_ms: number;
    service_generation: number;
    active_policy_version_id: string | null;
    region_count: number;
    ingress_instance_count: number;
    active_ingress_instance_count: number;
  } {
    this.expireStaleIngressInstances({
      now_unix_ms: Date.now()
    });

    const active_ingress_instance_count = [...this.ingress_instance_record_by_id.values()].filter(
      (record): boolean => {
        return record.health_status !== 'offline';
      }
    ).length;

    return {
      captured_at_unix_ms: Date.now(),
      service_generation: this.service_generation,
      active_policy_version_id: this.active_policy_version_id,
      region_count: this.region_record_by_id.size,
      ingress_instance_count: this.ingress_instance_record_by_id.size,
      active_ingress_instance_count
    };
  }

  async publishGlobalRoutingPolicy(params: {
    actor_subject: string;
    policy_snapshot: cluster_geo_ingress_global_routing_policy_snapshot_t;
    expected_active_policy_version_id?: string;
    activate_immediately?: boolean;
    requested_version_id?: string;
  }): Promise<cluster_geo_ingress_policy_version_record_t> {
    const request_message: cluster_geo_ingress_request_message_t = {
      protocol_version: 1,
      message_type: 'cluster_geo_ingress_publish_global_routing_policy',
      timestamp_unix_ms: Date.now(),
      request_id: `geo_ingress_publish_${Date.now()}`,
      actor_subject: params.actor_subject,
      policy_snapshot: params.policy_snapshot,
      expected_active_policy_version_id: params.expected_active_policy_version_id,
      activate_immediately: params.activate_immediately,
      requested_version_id: params.requested_version_id
    };

    const response_message = await this.handleGeoIngressRequest({
      request_message,
      authenticated_identity: {
        subject: params.actor_subject,
        scopes: ['rpc.admin.geo_ingress:*']
      }
    });

    const response_data = response_message.data as {
      policy_version_record: cluster_geo_ingress_policy_version_record_t;
    };

    return response_data.policy_version_record;
  }

  private startIngressExpirationLoop(): void {
    if (this.ingress_expiration_interval_handle) {
      return;
    }

    this.ingress_expiration_interval_handle = setInterval((): void => {
      this.expireStaleIngressInstances({
        now_unix_ms: Date.now()
      });
    }, this.ingress_expiration_check_interval_ms);

    this.ingress_expiration_interval_handle.unref();
  }

  private stopIngressExpirationLoop(): void {
    if (this.ingress_expiration_interval_handle) {
      clearInterval(this.ingress_expiration_interval_handle);
      this.ingress_expiration_interval_handle = null;
    }
  }

  private async handleHttpRequest(params: {
    request: Http2ServerRequest;
    response: Http2ServerResponse;
  }): Promise<void> {
    const { request, response } = params;

    if (request.method !== 'POST') {
      this.writeErrorResponse({
        response,
        request_id: 'unknown_request',
        error: BuildClusterGeoIngressProtocolError({
          code: 'GEO_INGRESS_VALIDATION_FAILED',
          message: 'Geo ingress control-plane accepts only POST requests.',
          retryable: false,
          details: {
            reason: 'invalid_method',
            method: request.method
          }
        })
      });
      return;
    }

    if (request.url !== this.request_path) {
      this.writeErrorResponse({
        response,
        request_id: 'unknown_request',
        error: BuildClusterGeoIngressProtocolError({
          code: 'GEO_INGRESS_VALIDATION_FAILED',
          message: 'Geo ingress request path mismatch.',
          retryable: false,
          details: {
            reason: 'invalid_path',
            path: request.url
          }
        })
      });
      return;
    }

    this.metrics.geo_ingress_requests_total += 1;

    let parsed_message: cluster_geo_ingress_request_message_t | null = null;

    try {
      const raw_body = await ReadRequestBody({
        request
      });

      const parsed_json = JSON.parse(raw_body) as unknown;
      const parse_result = ParseClusterGeoIngressRequestMessage({
        message: parsed_json
      });

      if (!parse_result.ok) {
        this.incrementFailureMetric({
          reason: 'validation_failed'
        });

        this.writeErrorResponse({
          response,
          request_id: 'unknown_request',
          error: parse_result.error
        });
        return;
      }

      parsed_message = parse_result.value;

      let authenticated_identity: cluster_geo_ingress_control_plane_identity_t | undefined;
      if (this.authenticate_request) {
        const auth_result = await this.authenticate_request({
          headers: request.headers,
          request_message: parsed_message
        });

        if (!auth_result.ok) {
          this.incrementFailureMetric({
            reason: 'auth_failed'
          });

          this.writeErrorResponse({
            response,
            request_id: parsed_message.request_id,
            trace_id: parsed_message.trace_id,
            operation_message_type: parsed_message.message_type,
            error: BuildClusterGeoIngressProtocolError({
              code: 'GEO_INGRESS_AUTH_FAILED',
              message: auth_result.message,
              retryable: false,
              details: auth_result.details
            })
          });
          return;
        }

        authenticated_identity = auth_result.identity;
      }

      const success_message = await this.handleGeoIngressRequest({
        request_message: parsed_message,
        authenticated_identity
      });

      this.metrics.geo_ingress_success_total += 1;
      this.writeSuccessResponse({
        response,
        success_message
      });
    } catch (error) {
      this.metrics.geo_ingress_failure_total += 1;

      if (IsProtocolErrorException(error)) {
        this.incrementFailureMetric({
          reason: 'state_failed'
        });

        this.writeErrorResponse({
          response,
          request_id: parsed_message?.request_id ?? 'unknown_request',
          trace_id: parsed_message?.trace_id,
          operation_message_type: parsed_message?.message_type,
          error: error.error
        });
        return;
      }

      this.incrementFailureMetric({
        reason: 'internal_failed'
      });

      this.writeErrorResponse({
        response,
        request_id: parsed_message?.request_id ?? 'unknown_request',
        trace_id: parsed_message?.trace_id,
        operation_message_type: parsed_message?.message_type,
        error: BuildClusterGeoIngressProtocolError({
          code: 'GEO_INGRESS_INTERNAL',
          message: 'Geo ingress control-plane request handling failed.',
          retryable: true,
          details: {
            message: error instanceof Error ? error.message : String(error)
          }
        })
      });
    }
  }

  private async handleGeoIngressRequest(params: {
    request_message: cluster_geo_ingress_request_message_t;
    authenticated_identity?: cluster_geo_ingress_control_plane_identity_t;
  }): Promise<cluster_geo_ingress_response_success_message_i> {
    const { request_message, authenticated_identity } = params;

    this.authorizeRequest({
      request_message,
      identity: authenticated_identity
    });

    this.expireStaleIngressInstances({
      now_unix_ms: request_message.timestamp_unix_ms
    });

    if (request_message.message_type === 'cluster_geo_ingress_ingress_register') {
      const existing_record = this.ingress_instance_record_by_id.get(request_message.ingress_id);

      const now_unix_ms = request_message.timestamp_unix_ms;
      const lease_ttl_ms = request_message.lease_ttl_ms ?? this.ingress_default_lease_ttl_ms;

      const region_record = this.upsertRegionRecord({
        region_id: request_message.region_id,
        now_unix_ms,
        region_metadata: request_message.region_metadata
      });

      const next_ingress_record: cluster_geo_ingress_instance_record_t = {
        ingress_id: request_message.ingress_id,
        region_id: request_message.region_id,
        endpoint: structuredClone(request_message.endpoint),
        health_status: request_message.health_status,
        inflight_calls: Number(request_message.metrics_summary?.inflight_calls ?? 0),
        pending_calls: Number(request_message.metrics_summary?.pending_calls ?? 0),
        success_rate_1m: Number(request_message.metrics_summary?.success_rate_1m ?? 1),
        timeout_rate_1m: Number(request_message.metrics_summary?.timeout_rate_1m ?? 0),
        ewma_latency_ms: Number(
          request_message.metrics_summary?.ewma_latency_ms ??
            request_message.region_metadata?.latency_ewma_ms ??
            region_record.latency_ewma_ms
        ),
        ingress_version: request_message.ingress_version,
        policy_version_id: request_message.policy_version_id ?? null,
        lease_expires_unix_ms: now_unix_ms + lease_ttl_ms,
        last_heartbeat_unix_ms: now_unix_ms,
        registered_unix_ms: existing_record?.registered_unix_ms ?? now_unix_ms,
        metadata: request_message.metadata
          ? structuredClone(request_message.metadata)
          : existing_record?.metadata
      };

      this.ingress_instance_record_by_id.set(request_message.ingress_id, next_ingress_record);

      if (!existing_record) {
        this.recordEvent({
          event_name: 'geo_ingress_region_restored',
          ingress_id: request_message.ingress_id,
          region_id: request_message.region_id,
          details: {
            reason: 'ingress_registered'
          }
        });
      }

      this.recalculateRegionHealthState({
        now_unix_ms
      });

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          ingress_instance_record: structuredClone(next_ingress_record),
          topology_snapshot: this.buildTopologySnapshot({
            now_unix_ms
          })
        }
      });
    }

    if (request_message.message_type === 'cluster_geo_ingress_ingress_heartbeat') {
      const ingress_record = this.ingress_instance_record_by_id.get(request_message.ingress_id);
      if (!ingress_record) {
        ThrowProtocolError({
          error: BuildClusterGeoIngressProtocolError({
            code: 'GEO_INGRESS_CONFLICT',
            message: `Ingress "${request_message.ingress_id}" is not registered.`,
            retryable: false,
            details: {
              ingress_id: request_message.ingress_id
            }
          })
        });
      }

      const now_unix_ms = request_message.timestamp_unix_ms;
      const previous_health_status = ingress_record.health_status;
      const lease_ttl_ms = request_message.lease_ttl_ms ?? this.ingress_default_lease_ttl_ms;

      if (typeof request_message.region_id === 'string') {
        ingress_record.region_id = request_message.region_id;
      }

      ingress_record.health_status = request_message.health_status ?? ingress_record.health_status;
      ingress_record.ingress_version = request_message.ingress_version ?? ingress_record.ingress_version;
      ingress_record.policy_version_id =
        request_message.policy_version_id ?? ingress_record.policy_version_id;
      ingress_record.last_heartbeat_unix_ms = now_unix_ms;
      ingress_record.lease_expires_unix_ms = now_unix_ms + lease_ttl_ms;

      if (request_message.metrics_summary) {
        ingress_record.inflight_calls = Number(
          request_message.metrics_summary.inflight_calls ?? ingress_record.inflight_calls
        );
        ingress_record.pending_calls = Number(
          request_message.metrics_summary.pending_calls ?? ingress_record.pending_calls
        );
        ingress_record.success_rate_1m = Number(
          request_message.metrics_summary.success_rate_1m ?? ingress_record.success_rate_1m
        );
        ingress_record.timeout_rate_1m = Number(
          request_message.metrics_summary.timeout_rate_1m ?? ingress_record.timeout_rate_1m
        );
        ingress_record.ewma_latency_ms = Number(
          request_message.metrics_summary.ewma_latency_ms ?? ingress_record.ewma_latency_ms
        );
      }

      if (request_message.metadata) {
        ingress_record.metadata = structuredClone(request_message.metadata);
      }

      this.upsertRegionRecord({
        region_id: ingress_record.region_id,
        now_unix_ms,
        region_metadata: request_message.region_metadata
      });

      if (
        previous_health_status === 'offline' &&
        ingress_record.health_status !== 'offline'
      ) {
        this.recordEvent({
          event_name: 'geo_ingress_region_restored',
          ingress_id: ingress_record.ingress_id,
          region_id: ingress_record.region_id,
          details: {
            reason: 'ingress_heartbeat_restored'
          }
        });
      }

      this.recalculateRegionHealthState({
        now_unix_ms
      });

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          ingress_instance_record: structuredClone(ingress_record),
          topology_snapshot: this.buildTopologySnapshot({
            now_unix_ms
          })
        }
      });
    }

    if (request_message.message_type === 'cluster_geo_ingress_ingress_deregister') {
      const ingress_record = this.ingress_instance_record_by_id.get(request_message.ingress_id);

      if (!ingress_record) {
        return this.buildSuccessMessage({
          request_message,
          data: {
            deregistered: false,
            topology_snapshot: this.buildTopologySnapshot({
              now_unix_ms: request_message.timestamp_unix_ms
            })
          }
        });
      }

      ingress_record.health_status = 'offline';
      ingress_record.last_heartbeat_unix_ms = request_message.timestamp_unix_ms;
      ingress_record.lease_expires_unix_ms = request_message.timestamp_unix_ms;

      this.recordEvent({
        event_name: 'geo_ingress_region_degraded',
        ingress_id: request_message.ingress_id,
        region_id: ingress_record.region_id,
        details: {
          reason: request_message.reason ?? 'ingress_deregister'
        }
      });

      this.recalculateRegionHealthState({
        now_unix_ms: request_message.timestamp_unix_ms
      });

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          deregistered: true,
          topology_snapshot: this.buildTopologySnapshot({
            now_unix_ms: request_message.timestamp_unix_ms
          })
        }
      });
    }

    if (request_message.message_type === 'cluster_geo_ingress_publish_global_routing_policy') {
      if (
        typeof request_message.expected_active_policy_version_id === 'string' &&
        request_message.expected_active_policy_version_id !== this.active_policy_version_id
      ) {
        ThrowProtocolError({
          error: BuildClusterGeoIngressProtocolError({
            code: 'GEO_INGRESS_CONFLICT',
            message: 'expected_active_policy_version_id did not match current active version.',
            retryable: false,
            details: {
              expected_active_policy_version_id: request_message.expected_active_policy_version_id,
              active_policy_version_id: this.active_policy_version_id
            }
          })
        });
      }

      const policy_snapshot = structuredClone(request_message.policy_snapshot);
      const version_id =
        request_message.requested_version_id ??
        `geo_ingress_policy_${Date.now()}_${this.next_policy_version_index++}`;

      if (this.policy_version_record_by_id.has(version_id)) {
        ThrowProtocolError({
          error: BuildClusterGeoIngressProtocolError({
            code: 'GEO_INGRESS_CONFLICT',
            message: `Policy version_id "${version_id}" already exists.`,
            retryable: false,
            details: {
              version_id
            }
          })
        });
      }

      const checksum_sha1 = createHash('sha1')
        .update(JSON.stringify(policy_snapshot), 'utf8')
        .digest('hex');

      const activate_immediately = request_message.activate_immediately !== false;

      const policy_version_record: cluster_geo_ingress_policy_version_record_t = {
        version_id,
        created_unix_ms: request_message.timestamp_unix_ms,
        actor_subject: request_message.actor_subject,
        checksum_sha1,
        status: activate_immediately ? 'active' : 'staged',
        snapshot: policy_snapshot
      };

      if (activate_immediately && this.active_policy_version_id) {
        const previous_active_policy_record = this.policy_version_record_by_id.get(
          this.active_policy_version_id
        );
        if (previous_active_policy_record) {
          previous_active_policy_record.status = 'superseded';
        }
      }

      this.policy_version_record_by_id.set(version_id, policy_version_record);

      if (activate_immediately) {
        this.active_policy_version_id = version_id;
        this.active_policy_snapshot = structuredClone(policy_snapshot);
      }

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          policy_version_record: structuredClone(policy_version_record),
          active_policy_version_id: this.active_policy_version_id
        }
      });
    }

    if (request_message.message_type === 'cluster_geo_ingress_get_global_routing_policy') {
      const active_policy_version_record = this.active_policy_version_id
        ? this.policy_version_record_by_id.get(this.active_policy_version_id) ?? null
        : null;

      return this.buildSuccessMessage({
        request_message,
        data: {
          active_policy_version_id: this.active_policy_version_id,
          active_policy_version_record: active_policy_version_record
            ? structuredClone(active_policy_version_record)
            : null
        }
      });
    }

    if (request_message.message_type === 'cluster_geo_ingress_get_global_topology_snapshot') {
      return this.buildSuccessMessage({
        request_message,
        data: {
          topology_snapshot: this.buildTopologySnapshot({
            now_unix_ms: request_message.timestamp_unix_ms
          })
        }
      });
    }

    if (request_message.message_type === 'cluster_geo_ingress_report_metrics_summary') {
      const ingress_record = this.ingress_instance_record_by_id.get(request_message.ingress_id);

      if (!ingress_record) {
        ThrowProtocolError({
          error: BuildClusterGeoIngressProtocolError({
            code: 'GEO_INGRESS_CONFLICT',
            message: `Ingress "${request_message.ingress_id}" is not registered.`,
            retryable: false,
            details: {
              ingress_id: request_message.ingress_id
            }
          })
        });
      }

      ingress_record.inflight_calls = Number(
        request_message.metrics_summary.inflight_calls ?? ingress_record.inflight_calls
      );
      ingress_record.pending_calls = Number(
        request_message.metrics_summary.pending_calls ?? ingress_record.pending_calls
      );
      ingress_record.success_rate_1m = Number(
        request_message.metrics_summary.success_rate_1m ?? ingress_record.success_rate_1m
      );
      ingress_record.timeout_rate_1m = Number(
        request_message.metrics_summary.timeout_rate_1m ?? ingress_record.timeout_rate_1m
      );
      ingress_record.ewma_latency_ms = Number(
        request_message.metrics_summary.ewma_latency_ms ?? ingress_record.ewma_latency_ms
      );

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          acknowledged: true,
          ingress_instance_record: structuredClone(ingress_record)
        }
      });
    }

    if (request_message.message_type === 'cluster_geo_ingress_subscribe_updates') {
      const active_policy_version_record = this.active_policy_version_id
        ? this.policy_version_record_by_id.get(this.active_policy_version_id) ?? null
        : null;

      const changed =
        request_message.known_service_generation !== this.service_generation ||
        request_message.known_policy_version_id !== this.active_policy_version_id;

      return this.buildSuccessMessage({
        request_message,
        data: {
          changed,
          topology_snapshot:
            request_message.include_topology || changed
              ? this.buildTopologySnapshot({
                  now_unix_ms: request_message.timestamp_unix_ms
                })
              : undefined,
          active_policy_version_id: this.active_policy_version_id,
          active_policy_version_record: active_policy_version_record
            ? structuredClone(active_policy_version_record)
            : null,
          service_generation: this.service_generation
        }
      });
    }

    return this.buildSuccessMessage({
      request_message,
      data: {
        service_generation: this.service_generation,
        active_policy_version_id: this.active_policy_version_id,
        region_count: this.region_record_by_id.size,
        ingress_instance_count: this.ingress_instance_record_by_id.size,
        metrics: this.getMetrics(),
        event_list:
          typeof request_message.include_events_limit === 'number'
            ? this.getRecentEvents({
                limit: request_message.include_events_limit
              })
            : undefined
      }
    });
  }

  private authorizeRequest(params: {
    request_message: cluster_geo_ingress_request_message_t;
    identity?: cluster_geo_ingress_control_plane_identity_t;
  }): void {
    if (!params.identity) {
      return;
    }

    const required_scope = this.getRequiredScopeForRequest({
      request_message: params.request_message
    });

    if (
      typeof required_scope === 'string' &&
      !HasScope({
        scope_list: params.identity.scopes,
        required_scope
      })
    ) {
      ThrowProtocolError({
        error: BuildClusterGeoIngressProtocolError({
          code: 'GEO_INGRESS_FORBIDDEN',
          message: `Identity "${params.identity.subject}" lacks required scope "${required_scope}".`,
          retryable: false,
          details: {
            subject: params.identity.subject,
            required_scope,
            message_type: params.request_message.message_type
          }
        })
      });
    }
  }

  private getRequiredScopeForRequest(params: {
    request_message: cluster_geo_ingress_request_message_t;
  }): string | null {
    const message_type = params.request_message.message_type;

    if (message_type === 'cluster_geo_ingress_get_global_topology_snapshot') {
      return 'rpc.read.geo_ingress:*';
    }

    if (message_type === 'cluster_geo_ingress_get_global_routing_policy') {
      return 'rpc.read.geo_ingress:*';
    }

    if (message_type === 'cluster_geo_ingress_subscribe_updates') {
      return 'rpc.read.geo_ingress:*';
    }

    if (message_type === 'cluster_geo_ingress_get_service_status') {
      return 'rpc.read.debug:*';
    }

    if (message_type === 'cluster_geo_ingress_publish_global_routing_policy') {
      return 'rpc.admin.geo_ingress:policy:update';
    }

    if (
      message_type === 'cluster_geo_ingress_ingress_register' ||
      message_type === 'cluster_geo_ingress_ingress_heartbeat' ||
      message_type === 'cluster_geo_ingress_ingress_deregister' ||
      message_type === 'cluster_geo_ingress_report_metrics_summary'
    ) {
      return 'rpc.admin.geo_ingress:*';
    }

    return null;
  }

  private upsertRegionRecord(params: {
    region_id: string;
    now_unix_ms: number;
    region_metadata?: {
      priority?: number;
      latency_slo_ms?: number;
      capacity_score?: number;
      latency_ewma_ms?: number;
      status?: 'active' | 'degraded' | 'draining' | 'offline';
    };
  }): cluster_geo_ingress_region_record_t {
    const existing_region_record = this.region_record_by_id.get(params.region_id);

    const next_region_record: cluster_geo_ingress_region_record_t = {
      region_id: params.region_id,
      status: params.region_metadata?.status ?? existing_region_record?.status ?? 'active',
      priority: Number(params.region_metadata?.priority ?? existing_region_record?.priority ?? 100),
      latency_slo_ms: Number(
        params.region_metadata?.latency_slo_ms ?? existing_region_record?.latency_slo_ms ?? 120
      ),
      capacity_score: Number(
        params.region_metadata?.capacity_score ?? existing_region_record?.capacity_score ?? 1
      ),
      latency_ewma_ms: Number(
        params.region_metadata?.latency_ewma_ms ?? existing_region_record?.latency_ewma_ms ?? 120
      ),
      updated_unix_ms: params.now_unix_ms
    };

    this.region_record_by_id.set(params.region_id, next_region_record);

    return next_region_record;
  }

  private recalculateRegionHealthState(params: {
    now_unix_ms: number;
  }): void {
    const previous_failover_state = structuredClone(this.region_failover_state);

    const ingress_instance_list = [...this.ingress_instance_record_by_id.values()];
    const ingress_instance_list_by_region_id = new Map<string, cluster_geo_ingress_instance_record_t[]>();

    for (const ingress_instance_record of ingress_instance_list) {
      const region_instance_list =
        ingress_instance_list_by_region_id.get(ingress_instance_record.region_id) ?? [];
      region_instance_list.push(ingress_instance_record);
      ingress_instance_list_by_region_id.set(ingress_instance_record.region_id, region_instance_list);
    }

    for (const [region_id, region_record] of this.region_record_by_id.entries()) {
      const region_instance_list = ingress_instance_list_by_region_id.get(region_id) ?? [];
      const ready_instance_count = region_instance_list.filter((record): boolean => {
        return record.health_status === 'ready' || record.health_status === 'overloaded';
      }).length;

      if (ready_instance_count === 0 && region_instance_list.length > 0) {
        region_record.status = 'degraded';
      } else if (ready_instance_count > 0 && region_record.status !== 'draining') {
        region_record.status = 'active';
      } else if (region_instance_list.length === 0) {
        region_record.status = 'offline';
      }

      if (region_instance_list.length > 0) {
        const latency_total = region_instance_list.reduce((total, ingress_record): number => {
          return total + ingress_record.ewma_latency_ms;
        }, 0);

        region_record.latency_ewma_ms = latency_total / region_instance_list.length;
        region_record.capacity_score = Math.max(
          0.1,
          region_instance_list.reduce((total, ingress_record): number => {
            const readiness_multiplier =
              ingress_record.health_status === 'ready'
                ? 1
                : ingress_record.health_status === 'overloaded'
                  ? 0.5
                  : ingress_record.health_status === 'degraded'
                    ? 0.25
                    : 0;

            const headroom = 1 / (1 + ingress_record.inflight_calls + ingress_record.pending_calls);
            return total + readiness_multiplier * headroom;
          }, 0)
        );
      }

      region_record.updated_unix_ms = params.now_unix_ms;
    }

    const active_region_id_list = [...this.region_record_by_id.values()]
      .filter((region_record): boolean => {
        return region_record.status === 'active';
      })
      .map((region_record): string => {
        return region_record.region_id;
      })
      .sort();

    const degraded_region_id_list = [...this.region_record_by_id.values()]
      .filter((region_record): boolean => {
        return region_record.status === 'degraded';
      })
      .map((region_record): string => {
        return region_record.region_id;
      })
      .sort();

    let failover_reason: string | null = null;
    if (active_region_id_list.length === 0 && degraded_region_id_list.length > 0) {
      failover_reason = 'all_regions_degraded';
    } else if (degraded_region_id_list.length > 0) {
      failover_reason = 'partial_region_degradation';
    }

    this.region_failover_state = {
      active_region_id_list,
      degraded_region_id_list,
      failover_reason,
      last_transition_unix_ms:
        JSON.stringify(previous_failover_state) ===
        JSON.stringify({
          active_region_id_list,
          degraded_region_id_list,
          failover_reason,
          last_transition_unix_ms: previous_failover_state.last_transition_unix_ms
        })
          ? previous_failover_state.last_transition_unix_ms
          : params.now_unix_ms
    };

    if (
      previous_failover_state.active_region_id_list.length > 0 &&
      active_region_id_list.length === 0
    ) {
      this.metrics.geo_ingress_region_failover_total += 1;
    }
  }

  private buildTopologySnapshot(params: {
    now_unix_ms: number;
  }): cluster_geo_ingress_topology_snapshot_t {
    this.recalculateRegionHealthState({
      now_unix_ms: params.now_unix_ms
    });

    return {
      captured_at_unix_ms: params.now_unix_ms,
      service_generation: this.service_generation,
      active_policy_version_id: this.active_policy_version_id,
      region_record_list: [...this.region_record_by_id.values()].map((region_record) => {
        return structuredClone(region_record);
      }),
      ingress_instance_record_list: [...this.ingress_instance_record_by_id.values()].map(
        (ingress_instance_record) => {
          return structuredClone(ingress_instance_record);
        }
      ),
      region_failover_state: structuredClone(this.region_failover_state)
    };
  }

  private expireStaleIngressInstances(params: {
    now_unix_ms: number;
  }): void {
    let changed = false;

    for (const ingress_instance_record of this.ingress_instance_record_by_id.values()) {
      if (ingress_instance_record.health_status === 'offline') {
        continue;
      }

      if (ingress_instance_record.lease_expires_unix_ms > params.now_unix_ms) {
        continue;
      }

      ingress_instance_record.health_status = 'offline';
      ingress_instance_record.last_heartbeat_unix_ms = params.now_unix_ms;

      this.recordEvent({
        event_name: 'geo_ingress_region_degraded',
        ingress_id: ingress_instance_record.ingress_id,
        region_id: ingress_instance_record.region_id,
        details: {
          reason: 'lease_expired'
        }
      });

      changed = true;
    }

    if (changed) {
      this.recalculateRegionHealthState({
        now_unix_ms: params.now_unix_ms
      });

      this.bumpGeneration();
      this.persistState();
    }
  }

  private bumpGeneration(): void {
    this.service_generation += 1;
  }

  private recordEvent(params: {
    event_name: cluster_geo_ingress_event_t['event_name'];
    ingress_id?: string;
    region_id?: string;
    request_id?: string;
    trace_id?: string;
    details?: Record<string, unknown>;
  }): void {
    this.event_list.push({
      event_id: `geo_ingress_event_${Date.now()}_${this.next_event_index++}`,
      event_name: params.event_name,
      timestamp_unix_ms: Date.now(),
      ingress_id: params.ingress_id,
      region_id: params.region_id,
      request_id: params.request_id,
      trace_id: params.trace_id,
      details: params.details ? structuredClone(params.details) : undefined
    });

    if (this.event_list.length > this.retention_policy.event_history_max_count) {
      this.event_list.splice(
        0,
        this.event_list.length - this.retention_policy.event_history_max_count
      );
    }
  }

  private incrementFailureMetric(params: {
    reason: string;
  }): void {
    this.metrics.geo_ingress_failure_total += 1;
    this.metrics.geo_ingress_request_failure_count_by_reason[params.reason] =
      (this.metrics.geo_ingress_request_failure_count_by_reason[params.reason] ?? 0) + 1;
  }

  private buildSuccessMessage(params: {
    request_message: cluster_geo_ingress_request_message_t;
    data: cluster_geo_ingress_response_success_message_i['data'];
  }): cluster_geo_ingress_response_success_message_i {
    return {
      protocol_version: 1,
      message_type: 'cluster_geo_ingress_response_success',
      timestamp_unix_ms: Date.now(),
      request_id: params.request_message.request_id,
      trace_id: params.request_message.trace_id,
      operation_message_type: params.request_message.message_type,
      data: params.data
    };
  }

  private writeSuccessResponse(params: {
    response: Http2ServerResponse;
    success_message: cluster_geo_ingress_response_success_message_i;
  }): void {
    params.response.statusCode = 200;
    params.response.setHeader('content-type', 'application/json');
    params.response.end(JSON.stringify(params.success_message));
  }

  private writeErrorResponse(params: {
    response: Http2ServerResponse;
    request_id: string;
    trace_id?: string;
    operation_message_type?: cluster_geo_ingress_protocol_request_message_type_t;
    error: cluster_geo_ingress_protocol_error_t;
  }): void {
    const error_message: cluster_geo_ingress_response_error_message_i = {
      protocol_version: 1,
      message_type: 'cluster_geo_ingress_response_error',
      timestamp_unix_ms: Date.now(),
      request_id: params.request_id,
      trace_id: params.trace_id,
      operation_message_type: params.operation_message_type,
      error: params.error
    };

    params.response.statusCode = 200;
    params.response.setHeader('content-type', 'application/json');
    params.response.end(JSON.stringify(error_message));
  }

  private restoreStateFromStore(): void {
    const snapshot = this.state_store.loadState();

    this.region_record_by_id.clear();
    for (const [region_id, region_record] of Object.entries(snapshot.region_record_by_id)) {
      this.region_record_by_id.set(region_id, structuredClone(region_record));
    }

    this.ingress_instance_record_by_id.clear();
    for (const [ingress_id, ingress_record] of Object.entries(
      snapshot.ingress_instance_record_by_id
    )) {
      this.ingress_instance_record_by_id.set(ingress_id, structuredClone(ingress_record));
    }

    this.policy_version_record_by_id.clear();
    for (const [version_id, version_record] of Object.entries(
      snapshot.policy_version_record_by_id
    )) {
      this.policy_version_record_by_id.set(version_id, structuredClone(version_record));
    }

    this.active_policy_version_id = snapshot.active_policy_version_id;
    this.active_policy_snapshot = structuredClone(snapshot.active_policy_snapshot);

    this.event_list = snapshot.event_list.map((event) => {
      return structuredClone(event);
    });

    this.region_failover_state = structuredClone(snapshot.region_failover_state);

    this.service_generation = snapshot.service_generation;
  }

  private persistState(): void {
    const snapshot: cluster_geo_ingress_state_snapshot_t = {
      schema_version: 1,
      updated_unix_ms: Date.now(),
      service_generation: this.service_generation,
      region_record_by_id: {},
      ingress_instance_record_by_id: {},
      policy_version_record_by_id: {},
      active_policy_version_id: this.active_policy_version_id,
      active_policy_snapshot: structuredClone(this.active_policy_snapshot),
      event_list: this.event_list.map((event) => structuredClone(event)),
      region_failover_state: structuredClone(this.region_failover_state)
    };

    for (const [region_id, region_record] of this.region_record_by_id.entries()) {
      snapshot.region_record_by_id[region_id] = structuredClone(region_record);
    }

    for (const [ingress_id, ingress_record] of this.ingress_instance_record_by_id.entries()) {
      snapshot.ingress_instance_record_by_id[ingress_id] = structuredClone(ingress_record);
    }

    for (const [version_id, version_record] of this.policy_version_record_by_id.entries()) {
      snapshot.policy_version_record_by_id[version_id] = structuredClone(version_record);
    }

    const compacted_snapshot = this.state_store.compactState({
      state: snapshot,
      now_unix_ms: Date.now(),
      retention_policy: this.retention_policy
    });

    this.state_store.saveState({
      state: compacted_snapshot
    });
  }
}
