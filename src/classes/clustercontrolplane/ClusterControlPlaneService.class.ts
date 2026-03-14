import {
  createSecureServer,
  type Http2SecureServer,
  type Http2Server,
  type Http2ServerRequest,
  type Http2ServerResponse,
  type IncomingHttpHeaders
} from 'node:http2';
import { createHash } from 'node:crypto';
import {
  BuildTlsServerOptions,
  ValidateTlsServerConfig,
  type cluster_tls_server_config_t
} from '../clustertransport/ClusterTlsSecurity.class';

import type {
  cluster_control_plane_event_t,
  cluster_control_plane_gateway_record_t,
  cluster_control_plane_metrics_t,
  cluster_control_plane_mutation_tracking_record_t,
  cluster_control_plane_policy_snapshot_t,
  cluster_control_plane_policy_version_record_t,
  cluster_control_plane_protocol_error_t,
  cluster_control_plane_protocol_request_message_type_t,
  cluster_control_plane_request_message_t,
  cluster_control_plane_response_error_message_i,
  cluster_control_plane_response_success_message_i,
  cluster_control_plane_service_status_snapshot_t,
  cluster_control_plane_topology_snapshot_t
} from './ClusterControlPlaneProtocol';
import {
  BuildClusterControlPlaneProtocolError,
  ParseClusterControlPlaneRequestMessage
} from './ClusterControlPlaneProtocolValidators';
import {
  ClusterControlPlaneInMemoryStateStore,
  type cluster_control_plane_state_retention_policy_t,
  type cluster_control_plane_state_snapshot_t,
  type cluster_control_plane_state_store_i
} from './ClusterControlPlaneStateStore.class';

export type cluster_control_plane_service_identity_t = {
  subject: string;
  scopes: string[];
};

export type cluster_control_plane_service_auth_result_t =
  | {
      ok: true;
      identity: cluster_control_plane_service_identity_t;
    }
  | {
      ok: false;
      message: string;
      details?: Record<string, unknown>;
    };

export type cluster_control_plane_service_authenticate_request_t = (params: {
  headers: IncomingHttpHeaders;
  request_message: cluster_control_plane_request_message_t;
}) =>
  | cluster_control_plane_service_auth_result_t
  | Promise<cluster_control_plane_service_auth_result_t>;

export type cluster_control_plane_service_constructor_params_t = {
  host?: string;
  port?: number;
  request_path?: string;
  state_store?: cluster_control_plane_state_store_i;
  authenticate_request?: cluster_control_plane_service_authenticate_request_t;
  gateway_default_lease_ttl_ms?: number;
  gateway_expiration_check_interval_ms?: number;
  retention_policy?: Partial<cluster_control_plane_state_retention_policy_t>;
  security?: {
    tls?: cluster_tls_server_config_t;
  };
};

export type cluster_control_plane_service_address_t = {
  host: string;
  port: number;
  request_path: string;
};

type control_plane_protocol_error_exception_t = {
  kind: 'protocol_error';
  error: cluster_control_plane_protocol_error_t;
};

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function BuildRequestPath(params: { request_path: string | undefined }): string {
  if (typeof params.request_path !== 'string' || params.request_path.length === 0) {
    return '/wpc/cluster/control-plane';
  }

  if (params.request_path.startsWith('/')) {
    return params.request_path;
  }

  return `/${params.request_path}`;
}

function BuildDefaultRetentionPolicy(): cluster_control_plane_state_retention_policy_t {
  return {
    event_history_max_count: 5_000,
    mutation_history_max_count: 2_000,
    offline_gateway_retention_ms: 60_000
  };
}

function ThrowProtocolError(params: {
  error: cluster_control_plane_protocol_error_t;
}): never {
  throw {
    kind: 'protocol_error',
    error: params.error
  } as control_plane_protocol_error_exception_t;
}

function IsProtocolErrorException(value: unknown): value is control_plane_protocol_error_exception_t {
  return (
    IsRecordObject(value) &&
    value.kind === 'protocol_error' &&
    IsRecordObject(value.error)
  );
}

function ReadRequestBody(params: {
  request: Http2ServerRequest;
}): Promise<string> {
  const chunk_list: Buffer[] = [];

  return new Promise<string>((resolve, reject): void => {
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
  scopes: string[];
  required_scope: string;
}): boolean {
  if (params.scopes.includes('rpc.admin.control_plane:*')) {
    return true;
  }

  if (params.scopes.includes(params.required_scope)) {
    return true;
  }

  if (params.required_scope.endsWith(':read')) {
    const namespace_scope = `${params.required_scope.split(':')[0]}:*`;
    if (params.scopes.includes(namespace_scope)) {
      return true;
    }
  }

  return false;
}

export class ClusterControlPlaneService {
  private readonly request_path: string;
  private readonly state_store: cluster_control_plane_state_store_i;
  private readonly authenticate_request:
    | cluster_control_plane_service_authenticate_request_t
    | undefined;

  private host: string;
  private port: number;
  private readonly tls_server_config: cluster_tls_server_config_t;

  private http2_server: Http2SecureServer | null = null;
  private gateway_expiration_interval_handle: NodeJS.Timeout | null = null;

  private gateway_default_lease_ttl_ms: number;
  private gateway_expiration_check_interval_ms: number;
  private retention_policy: cluster_control_plane_state_retention_policy_t;

  private started_unix_ms: number | null = null;

  private gateway_record_by_id = new Map<string, cluster_control_plane_gateway_record_t>();
  private policy_version_record_by_id = new Map<string, cluster_control_plane_policy_version_record_t>();
  private mutation_tracking_record_by_id = new Map<string, cluster_control_plane_mutation_tracking_record_t>();
  private event_list: cluster_control_plane_event_t[] = [];

  private active_policy_version_id: string | null = null;
  private service_generation = 0;

  private next_policy_version_index = 1;
  private next_event_index = 1;

  private metrics: cluster_control_plane_metrics_t = {
    control_plane_gateway_registered_total: 0,
    control_plane_gateway_active_count: 0,
    control_plane_policy_version_total: 0,
    control_plane_policy_apply_failures_total: 0,
    control_plane_sync_lag_ms: 0,
    control_plane_request_failure_total: 0,
    control_plane_request_failure_count_by_reason: {}
  };

  constructor(params: cluster_control_plane_service_constructor_params_t = {}) {
    this.host = params.host ?? '127.0.0.1';
    this.port = params.port ?? 0;
    this.tls_server_config = ValidateTlsServerConfig({
      tls_server_config: params.security?.tls
    });
    this.request_path = BuildRequestPath({
      request_path: params.request_path
    });

    this.state_store = params.state_store ?? new ClusterControlPlaneInMemoryStateStore();
    this.authenticate_request = params.authenticate_request;

    this.gateway_default_lease_ttl_ms = params.gateway_default_lease_ttl_ms ?? 15_000;
    this.gateway_expiration_check_interval_ms =
      params.gateway_expiration_check_interval_ms ?? 1_000;

    this.retention_policy = {
      ...BuildDefaultRetentionPolicy(),
      ...(params.retention_policy ?? {})
    };

    this.restoreStateFromStore();
  }

  async start(params: {
    host?: string;
    port?: number;
  } = {}): Promise<cluster_control_plane_service_address_t> {
    if (this.http2_server) {
      return this.getAddress();
    }

    this.host = params.host ?? this.host;
    this.port = params.port ?? this.port;

    this.http2_server = createSecureServer(
      BuildTlsServerOptions({
        tls_server_config: this.tls_server_config
      })
    );
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
      throw new Error('Failed to resolve control-plane service address.');
    }

    this.host = address.address;
    this.port = address.port;
    this.started_unix_ms = Date.now();

    this.startGatewayExpirationLoop();

    return this.getAddress();
  }

  async stop(): Promise<void> {
    this.stopGatewayExpirationLoop();

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

  getAddress(): cluster_control_plane_service_address_t {
    return {
      host: this.host,
      port: this.port,
      request_path: this.request_path
    };
  }

  getMetrics(): cluster_control_plane_metrics_t {
    this.updateDerivedMetrics();

    return {
      ...this.metrics,
      control_plane_request_failure_count_by_reason: {
        ...this.metrics.control_plane_request_failure_count_by_reason
      }
    };
  }

  getRecentEvents(params: {
    limit?: number;
  } = {}): cluster_control_plane_event_t[] {
    let event_list = [...this.event_list];

    if (typeof params.limit === 'number' && params.limit >= 0) {
      event_list = event_list.slice(Math.max(0, event_list.length - params.limit));
    }

    return event_list.map((event): cluster_control_plane_event_t => {
      return {
        ...event,
        details: event.details ? { ...event.details } : undefined
      };
    });
  }

  getServiceStatusSnapshot(): cluster_control_plane_service_status_snapshot_t {
    this.expireStaleGateways({
      now_unix_ms: Date.now()
    });

    return {
      captured_at_unix_ms: Date.now(),
      service_generation: this.service_generation,
      active_gateway_count: [...this.gateway_record_by_id.values()].filter((gateway_record): boolean => {
        return gateway_record.status !== 'offline';
      }).length,
      gateway_count: this.gateway_record_by_id.size,
      active_policy_version_id: this.active_policy_version_id,
      policy_version_count: this.policy_version_record_by_id.size,
      mutation_tracking_count: this.mutation_tracking_record_by_id.size
    };
  }

  async publishPolicySnapshot(params: {
    actor_subject: string;
    policy_snapshot: cluster_control_plane_policy_snapshot_t;
    expected_active_policy_version_id?: string;
    activate_immediately?: boolean;
    requested_version_id?: string;
  }): Promise<cluster_control_plane_policy_version_record_t> {
    const request_message: cluster_control_plane_request_message_t = {
      protocol_version: 1,
      message_type: 'cluster_control_plane_update_policy_snapshot',
      timestamp_unix_ms: Date.now(),
      request_id: `control_plane_publish_${Date.now()}`,
      actor_subject: params.actor_subject,
      policy_snapshot: params.policy_snapshot,
      expected_active_policy_version_id: params.expected_active_policy_version_id,
      activate_immediately: params.activate_immediately,
      requested_version_id: params.requested_version_id
    };

    const response_message = await this.handleControlPlaneRequest({
      request_message,
      authenticated_identity: {
        subject: params.actor_subject,
        scopes: ['rpc.admin.control_plane:*']
      }
    });

    const response_data = response_message.data as {
      policy_version: cluster_control_plane_policy_version_record_t;
    };

    return response_data.policy_version;
  }

  private startGatewayExpirationLoop(): void {
    if (this.gateway_expiration_interval_handle) {
      return;
    }

    this.gateway_expiration_interval_handle = setInterval((): void => {
      this.expireStaleGateways({
        now_unix_ms: Date.now()
      });
    }, this.gateway_expiration_check_interval_ms);

    this.gateway_expiration_interval_handle.unref();
  }

  private stopGatewayExpirationLoop(): void {
    if (this.gateway_expiration_interval_handle) {
      clearInterval(this.gateway_expiration_interval_handle);
      this.gateway_expiration_interval_handle = null;
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
        error: BuildClusterControlPlaneProtocolError({
          code: 'CONTROL_PLANE_VALIDATION_FAILED',
          message: 'Control-plane service accepts only POST requests.',
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
        error: BuildClusterControlPlaneProtocolError({
          code: 'CONTROL_PLANE_VALIDATION_FAILED',
          message: 'Control-plane request path mismatch.',
          retryable: false,
          details: {
            reason: 'invalid_path',
            path: request.url
          }
        })
      });
      return;
    }

    let parsed_message: cluster_control_plane_request_message_t | null = null;

    try {
      const raw_body = await ReadRequestBody({
        request
      });

      const parsed_json = JSON.parse(raw_body) as unknown;
      const parse_result = ParseClusterControlPlaneRequestMessage({
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

      let authenticated_identity: cluster_control_plane_service_identity_t | undefined;
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
            error: BuildClusterControlPlaneProtocolError({
              code: 'CONTROL_PLANE_AUTH_FAILED',
              message: auth_result.message,
              retryable: false,
              details: auth_result.details
            })
          });
          return;
        }

        authenticated_identity = auth_result.identity;
      }

      const success_message = await this.handleControlPlaneRequest({
        request_message: parsed_message,
        authenticated_identity
      });

      this.writeSuccessResponse({
        response,
        success_message
      });
    } catch (error) {
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
        error: BuildClusterControlPlaneProtocolError({
          code: 'CONTROL_PLANE_INTERNAL',
          message: 'Control-plane request handling failed.',
          retryable: true,
          details: {
            message: error instanceof Error ? error.message : String(error)
          }
        })
      });
    }
  }

  private async handleControlPlaneRequest(params: {
    request_message: cluster_control_plane_request_message_t;
    authenticated_identity?: cluster_control_plane_service_identity_t;
  }): Promise<cluster_control_plane_response_success_message_i> {
    const { request_message, authenticated_identity } = params;

    this.authorizeRequest({
      request_message,
      identity: authenticated_identity
    });

    this.expireStaleGateways({
      now_unix_ms: request_message.timestamp_unix_ms
    });

    if (request_message.message_type === 'cluster_control_plane_gateway_register') {
      const existing_record = this.gateway_record_by_id.get(request_message.gateway_id);

      const now_unix_ms = request_message.timestamp_unix_ms;
      const lease_ttl_ms = request_message.lease_ttl_ms ?? this.gateway_default_lease_ttl_ms;

      const next_gateway_record: cluster_control_plane_gateway_record_t = {
        gateway_id: request_message.gateway_id,
        node_reference: structuredClone(request_message.node_reference),
        address: structuredClone(request_message.address),
        status: request_message.status,
        gateway_version: request_message.gateway_version,
        registered_unix_ms: existing_record?.registered_unix_ms ?? now_unix_ms,
        last_heartbeat_unix_ms: now_unix_ms,
        lease_expires_unix_ms: now_unix_ms + lease_ttl_ms,
        applied_policy_version_id: existing_record?.applied_policy_version_id,
        pending_policy_version_id: existing_record?.pending_policy_version_id,
        metadata: request_message.metadata ? structuredClone(request_message.metadata) : undefined
      };

      this.gateway_record_by_id.set(request_message.gateway_id, next_gateway_record);

      if (!existing_record) {
        this.metrics.control_plane_gateway_registered_total += 1;
        this.recordEvent({
          event_name: 'control_plane_gateway_registered',
          gateway_id: request_message.gateway_id,
          details: {
            gateway_version: request_message.gateway_version
          }
        });
      } else if (existing_record.status === 'offline') {
        this.recordEvent({
          event_name: 'control_plane_sync_restored',
          gateway_id: request_message.gateway_id,
          details: {
            gateway_version: request_message.gateway_version
          }
        });
      }

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          gateway_record: structuredClone(next_gateway_record),
          topology_snapshot: this.buildTopologySnapshot({
            now_unix_ms
          })
        }
      });
    }

    if (request_message.message_type === 'cluster_control_plane_gateway_heartbeat') {
      const gateway_record = this.gateway_record_by_id.get(request_message.gateway_id);

      if (!gateway_record) {
        ThrowProtocolError({
          error: BuildClusterControlPlaneProtocolError({
            code: 'CONTROL_PLANE_CONFLICT',
            message: `Gateway "${request_message.gateway_id}" is not registered.`,
            retryable: false,
            details: {
              gateway_id: request_message.gateway_id
            }
          })
        });
      }

      const now_unix_ms = request_message.timestamp_unix_ms;
      const lease_ttl_ms = request_message.lease_ttl_ms ?? this.gateway_default_lease_ttl_ms;
      const previous_status = gateway_record.status;

      gateway_record.last_heartbeat_unix_ms = now_unix_ms;
      gateway_record.lease_expires_unix_ms = now_unix_ms + lease_ttl_ms;
      gateway_record.status = request_message.status ?? gateway_record.status;
      gateway_record.gateway_version = request_message.gateway_version ?? gateway_record.gateway_version;
      gateway_record.node_reference = request_message.node_reference
        ? structuredClone(request_message.node_reference)
        : gateway_record.node_reference;
      gateway_record.metadata = request_message.metadata
        ? structuredClone(request_message.metadata)
        : gateway_record.metadata;

      if (previous_status === 'offline' && gateway_record.status !== 'offline') {
        this.recordEvent({
          event_name: 'control_plane_sync_restored',
          gateway_id: gateway_record.gateway_id,
          details: {
            previous_status,
            next_status: gateway_record.status
          }
        });
      }

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          gateway_record: structuredClone(gateway_record),
          topology_snapshot: this.buildTopologySnapshot({
            now_unix_ms
          })
        }
      });
    }

    if (request_message.message_type === 'cluster_control_plane_gateway_deregister') {
      const gateway_record = this.gateway_record_by_id.get(request_message.gateway_id);

      if (!gateway_record) {
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

      gateway_record.status = 'offline';
      gateway_record.last_heartbeat_unix_ms = request_message.timestamp_unix_ms;
      gateway_record.lease_expires_unix_ms = request_message.timestamp_unix_ms;

      this.recordEvent({
        event_name: 'control_plane_gateway_expired',
        gateway_id: request_message.gateway_id,
        details: {
          reason: request_message.reason ?? 'gateway_deregister'
        }
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

    if (request_message.message_type === 'cluster_control_plane_get_topology_snapshot') {
      return this.buildSuccessMessage({
        request_message,
        data: {
          topology_snapshot: this.buildTopologySnapshot({
            now_unix_ms: request_message.timestamp_unix_ms
          })
        }
      });
    }

    if (request_message.message_type === 'cluster_control_plane_get_policy_snapshot') {
      const active_policy_version = this.active_policy_version_id
        ? this.policy_version_record_by_id.get(this.active_policy_version_id) ?? null
        : null;

      return this.buildSuccessMessage({
        request_message,
        data: {
          active_policy_version_id: this.active_policy_version_id,
          active_policy_version: active_policy_version
            ? structuredClone(active_policy_version)
            : null
        }
      });
    }

    if (request_message.message_type === 'cluster_control_plane_update_policy_snapshot') {
      if (
        typeof request_message.expected_active_policy_version_id === 'string' &&
        request_message.expected_active_policy_version_id !== this.active_policy_version_id
      ) {
        ThrowProtocolError({
          error: BuildClusterControlPlaneProtocolError({
            code: 'CONTROL_PLANE_CONFLICT',
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
        `cp_policy_${Date.now()}_${this.next_policy_version_index++}`;

      if (this.policy_version_record_by_id.has(version_id)) {
        ThrowProtocolError({
          error: BuildClusterControlPlaneProtocolError({
            code: 'CONTROL_PLANE_CONFLICT',
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

      const policy_version_record: cluster_control_plane_policy_version_record_t = {
        metadata: {
          version_id,
          created_unix_ms: request_message.timestamp_unix_ms,
          actor_subject: request_message.actor_subject,
          checksum_sha1,
          status: activate_immediately ? 'active' : 'staged'
        },
        snapshot: policy_snapshot
      };

      if (activate_immediately && this.active_policy_version_id) {
        const previous_active_policy = this.policy_version_record_by_id.get(
          this.active_policy_version_id
        );
        if (previous_active_policy) {
          previous_active_policy.metadata.status = 'superseded';
        }
      }

      this.policy_version_record_by_id.set(version_id, policy_version_record);
      if (activate_immediately) {
        this.active_policy_version_id = version_id;
      }

      this.metrics.control_plane_policy_version_total += 1;

      this.recordEvent({
        event_name: 'control_plane_policy_published',
        policy_version_id: version_id,
        details: {
          activate_immediately,
          actor_subject: request_message.actor_subject
        }
      });

      if (activate_immediately) {
        this.recordEvent({
          event_name: 'control_plane_policy_applied',
          policy_version_id: version_id,
          details: {
            actor_subject: request_message.actor_subject
          }
        });
      }

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          policy_version: structuredClone(policy_version_record),
          active_policy_version_id: this.active_policy_version_id
        }
      });
    }

    if (request_message.message_type === 'cluster_control_plane_subscribe_updates') {
      const active_policy_version = this.active_policy_version_id
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
          active_policy_version: active_policy_version
            ? structuredClone(active_policy_version)
            : null,
          service_generation: this.service_generation
        }
      });
    }

    if (request_message.message_type === 'cluster_control_plane_config_version_announce') {
      const gateway_record = this.gateway_record_by_id.get(request_message.gateway_id);
      if (!gateway_record) {
        ThrowProtocolError({
          error: BuildClusterControlPlaneProtocolError({
            code: 'CONTROL_PLANE_CONFLICT',
            message: `Gateway "${request_message.gateway_id}" is not registered.`,
            retryable: false,
            details: {
              gateway_id: request_message.gateway_id
            }
          })
        });
      }

      gateway_record.pending_policy_version_id = request_message.policy_version_id;

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          acknowledged: true,
          gateway_record: structuredClone(gateway_record)
        }
      });
    }

    if (request_message.message_type === 'cluster_control_plane_config_version_ack') {
      const gateway_record = this.gateway_record_by_id.get(request_message.gateway_id);
      if (!gateway_record) {
        ThrowProtocolError({
          error: BuildClusterControlPlaneProtocolError({
            code: 'CONTROL_PLANE_CONFLICT',
            message: `Gateway "${request_message.gateway_id}" is not registered.`,
            retryable: false,
            details: {
              gateway_id: request_message.gateway_id
            }
          })
        });
      }

      if (request_message.applied) {
        gateway_record.applied_policy_version_id = request_message.policy_version_id;
        gateway_record.pending_policy_version_id = undefined;

        this.recordEvent({
          event_name: 'control_plane_policy_applied',
          gateway_id: request_message.gateway_id,
          policy_version_id: request_message.policy_version_id,
          details: request_message.details
        });
      } else {
        this.metrics.control_plane_policy_apply_failures_total += 1;

        this.recordEvent({
          event_name: 'control_plane_policy_apply_failed',
          gateway_id: request_message.gateway_id,
          policy_version_id: request_message.policy_version_id,
          details: request_message.details
        });
      }

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          acknowledged: true,
          gateway_record: structuredClone(gateway_record)
        }
      });
    }

    if (request_message.message_type === 'cluster_control_plane_get_service_status') {
      return this.buildSuccessMessage({
        request_message,
        data: {
          service_status: this.getServiceStatusSnapshot(),
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

    if (request_message.message_type === 'cluster_control_plane_mutation_intent_update') {
      const now_unix_ms = request_message.timestamp_unix_ms;
      const existing_record = this.mutation_tracking_record_by_id.get(request_message.mutation_id);

      const mutation_tracking_record: cluster_control_plane_mutation_tracking_record_t = {
        mutation_id: request_message.mutation_id,
        status: request_message.status,
        created_unix_ms: existing_record?.created_unix_ms ?? now_unix_ms,
        updated_unix_ms: now_unix_ms,
        dispatch_intent: request_message.dispatch_intent
          ? structuredClone(request_message.dispatch_intent)
          : existing_record?.dispatch_intent,
        gateway_progress_by_id: existing_record
          ? structuredClone(existing_record.gateway_progress_by_id)
          : {}
      };

      if (request_message.gateway_id) {
        mutation_tracking_record.gateway_progress_by_id[request_message.gateway_id] = {
          gateway_id: request_message.gateway_id,
          status: request_message.status,
          updated_unix_ms: now_unix_ms,
          details: request_message.details ? structuredClone(request_message.details) : undefined
        };
      }

      this.mutation_tracking_record_by_id.set(
        request_message.mutation_id,
        mutation_tracking_record
      );

      this.bumpGeneration();
      this.persistState();

      return this.buildSuccessMessage({
        request_message,
        data: {
          mutation_tracking_record: structuredClone(mutation_tracking_record)
        }
      });
    }

    const mutation_tracking_record = this.mutation_tracking_record_by_id.get(
      request_message.mutation_id
    );

    return this.buildSuccessMessage({
      request_message,
      data: {
        mutation_tracking_record: mutation_tracking_record
          ? structuredClone(mutation_tracking_record)
          : null
      }
    });
  }

  private authorizeRequest(params: {
    request_message: cluster_control_plane_request_message_t;
    identity?: cluster_control_plane_service_identity_t;
  }): void {
    if (!params.identity) {
      return;
    }

    const { request_message, identity } = params;

    const required_scope = this.getRequiredScopeForRequest({
      request_message
    });

    if (
      typeof required_scope === 'string' &&
      !HasScope({
        scopes: identity.scopes,
        required_scope
      })
    ) {
      ThrowProtocolError({
        error: BuildClusterControlPlaneProtocolError({
          code: 'CONTROL_PLANE_FORBIDDEN',
          message: `Identity "${identity.subject}" lacks required scope "${required_scope}".`,
          retryable: false,
          details: {
            subject: identity.subject,
            required_scope,
            message_type: request_message.message_type
          }
        })
      });
    }
  }

  private getRequiredScopeForRequest(params: {
    request_message: cluster_control_plane_request_message_t;
  }): string | null {
    const { request_message } = params;

    if (request_message.message_type === 'cluster_control_plane_get_topology_snapshot') {
      return 'rpc.read.cluster:*';
    }

    if (request_message.message_type === 'cluster_control_plane_get_policy_snapshot') {
      return 'rpc.read.cluster:*';
    }

    if (request_message.message_type === 'cluster_control_plane_subscribe_updates') {
      return 'rpc.read.cluster:*';
    }

    if (request_message.message_type === 'cluster_control_plane_get_mutation_status') {
      return 'rpc.read.cluster:*';
    }

    if (request_message.message_type === 'cluster_control_plane_get_service_status') {
      return 'rpc.read.debug:*';
    }

    if (request_message.message_type === 'cluster_control_plane_update_policy_snapshot') {
      return 'rpc.admin.control_plane:policy:update';
    }

    if (
      request_message.message_type === 'cluster_control_plane_gateway_register' ||
      request_message.message_type === 'cluster_control_plane_gateway_heartbeat' ||
      request_message.message_type === 'cluster_control_plane_gateway_deregister' ||
      request_message.message_type === 'cluster_control_plane_config_version_announce' ||
      request_message.message_type === 'cluster_control_plane_config_version_ack' ||
      request_message.message_type === 'cluster_control_plane_mutation_intent_update'
    ) {
      return 'rpc.admin.control_plane:*';
    }

    return null;
  }

  private buildSuccessMessage(params: {
    request_message: cluster_control_plane_request_message_t;
    data: cluster_control_plane_response_success_message_i['data'];
  }): cluster_control_plane_response_success_message_i {
    return {
      protocol_version: 1,
      message_type: 'cluster_control_plane_response_success',
      timestamp_unix_ms: Date.now(),
      request_id: params.request_message.request_id,
      trace_id: params.request_message.trace_id,
      operation_message_type: params.request_message.message_type,
      data: params.data
    };
  }

  private writeSuccessResponse(params: {
    response: Http2ServerResponse;
    success_message: cluster_control_plane_response_success_message_i;
  }): void {
    params.response.statusCode = 200;
    params.response.setHeader('content-type', 'application/json');
    params.response.end(JSON.stringify(params.success_message));
  }

  private writeErrorResponse(params: {
    response: Http2ServerResponse;
    request_id: string;
    trace_id?: string;
    operation_message_type?: cluster_control_plane_protocol_request_message_type_t;
    error: cluster_control_plane_protocol_error_t;
  }): void {
    const error_message: cluster_control_plane_response_error_message_i = {
      protocol_version: 1,
      message_type: 'cluster_control_plane_response_error',
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

  private buildTopologySnapshot(params: {
    now_unix_ms: number;
  }): cluster_control_plane_topology_snapshot_t {
    return {
      captured_at_unix_ms: params.now_unix_ms,
      gateway_list: [...this.gateway_record_by_id.values()].map((gateway_record) => {
        return structuredClone(gateway_record);
      }),
      active_policy_version_id: this.active_policy_version_id,
      service_generation: this.service_generation
    };
  }

  private expireStaleGateways(params: {
    now_unix_ms: number;
  }): void {
    let changed = false;

    for (const gateway_record of this.gateway_record_by_id.values()) {
      if (gateway_record.status === 'offline') {
        continue;
      }

      if (gateway_record.lease_expires_unix_ms > params.now_unix_ms) {
        continue;
      }

      gateway_record.status = 'offline';
      gateway_record.last_heartbeat_unix_ms = params.now_unix_ms;

      this.recordEvent({
        event_name: 'control_plane_gateway_expired',
        gateway_id: gateway_record.gateway_id,
        details: {
          reason: 'lease_expired'
        }
      });

      this.recordEvent({
        event_name: 'control_plane_sync_degraded',
        gateway_id: gateway_record.gateway_id,
        details: {
          reason: 'lease_expired'
        }
      });

      changed = true;
    }

    if (changed) {
      this.bumpGeneration();
      this.persistState();
    }

    this.updateDerivedMetrics();
  }

  private updateDerivedMetrics(): void {
    const active_gateway_record_list = [...this.gateway_record_by_id.values()].filter(
      (gateway_record): boolean => {
        return gateway_record.status !== 'offline';
      }
    );

    this.metrics.control_plane_gateway_active_count = active_gateway_record_list.length;

    if (active_gateway_record_list.length === 0) {
      this.metrics.control_plane_sync_lag_ms = 0;
      return;
    }

    let max_lag_ms = 0;
    const now_unix_ms = Date.now();

    for (const gateway_record of active_gateway_record_list) {
      const lag_ms = Math.max(0, now_unix_ms - gateway_record.last_heartbeat_unix_ms);
      if (lag_ms > max_lag_ms) {
        max_lag_ms = lag_ms;
      }
    }

    this.metrics.control_plane_sync_lag_ms = max_lag_ms;
  }

  private bumpGeneration(): void {
    this.service_generation += 1;
  }

  private recordEvent(params: {
    event_name: cluster_control_plane_event_t['event_name'];
    gateway_id?: string;
    policy_version_id?: string;
    mutation_id?: string;
    details?: Record<string, unknown>;
  }): void {
    this.event_list.push({
      event_id: `cp_event_${Date.now()}_${this.next_event_index++}`,
      event_name: params.event_name,
      timestamp_unix_ms: Date.now(),
      gateway_id: params.gateway_id,
      policy_version_id: params.policy_version_id,
      mutation_id: params.mutation_id,
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
    this.metrics.control_plane_request_failure_total += 1;
    this.metrics.control_plane_request_failure_count_by_reason[params.reason] =
      (this.metrics.control_plane_request_failure_count_by_reason[params.reason] ?? 0) +
      1;
  }

  private restoreStateFromStore(): void {
    const snapshot = this.state_store.loadState();

    this.gateway_record_by_id.clear();
    for (const [gateway_id, gateway_record] of Object.entries(snapshot.gateway_record_by_id)) {
      this.gateway_record_by_id.set(gateway_id, structuredClone(gateway_record));
    }

    this.policy_version_record_by_id.clear();
    for (const [version_id, policy_record] of Object.entries(
      snapshot.policy_version_record_by_id
    )) {
      this.policy_version_record_by_id.set(version_id, structuredClone(policy_record));
    }

    this.active_policy_version_id = snapshot.active_policy_version_id;

    this.mutation_tracking_record_by_id.clear();
    for (const [mutation_id, mutation_record] of Object.entries(
      snapshot.mutation_tracking_record_by_id
    )) {
      this.mutation_tracking_record_by_id.set(mutation_id, structuredClone(mutation_record));
    }

    this.event_list = snapshot.event_list.map((event) => {
      return structuredClone(event);
    });

    this.service_generation = snapshot.service_generation;

    this.metrics.control_plane_policy_version_total = this.policy_version_record_by_id.size;
    this.updateDerivedMetrics();
  }

  private persistState(): void {
    const snapshot: cluster_control_plane_state_snapshot_t = {
      schema_version: 1,
      updated_unix_ms: Date.now(),
      service_generation: this.service_generation,
      gateway_record_by_id: {},
      policy_version_record_by_id: {},
      active_policy_version_id: this.active_policy_version_id,
      mutation_tracking_record_by_id: {},
      event_list: this.event_list.map((event) => structuredClone(event))
    };

    for (const [gateway_id, gateway_record] of this.gateway_record_by_id.entries()) {
      snapshot.gateway_record_by_id[gateway_id] = structuredClone(gateway_record);
    }

    for (const [version_id, policy_record] of this.policy_version_record_by_id.entries()) {
      snapshot.policy_version_record_by_id[version_id] = structuredClone(policy_record);
    }

    for (const [mutation_id, mutation_record] of this.mutation_tracking_record_by_id.entries()) {
      snapshot.mutation_tracking_record_by_id[mutation_id] = structuredClone(mutation_record);
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
