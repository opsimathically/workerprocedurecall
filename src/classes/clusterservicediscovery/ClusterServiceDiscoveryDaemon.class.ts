import {
  createSecureServer,
  type Http2SecureServer,
  type Http2Server,
  type Http2ServerRequest,
  type Http2ServerResponse,
  type IncomingHttpHeaders,
  connect,
  type ClientHttp2Session
} from 'node:http2';
import {
  BuildHttpsAuthority,
  BuildTlsClientConnectOptions,
  BuildTlsServerOptions,
  ValidateTlsClientConfig,
  ValidateTlsServerConfig,
  type cluster_tls_client_config_t,
  type cluster_tls_server_config_t
} from '../clustertransport/ClusterTlsSecurity.class';

import {
  ClusterInMemoryServiceDiscoveryStore,
  type cluster_service_discovery_event_t,
  type cluster_service_discovery_store_i,
  type cluster_service_discovery_metrics_t,
  type cluster_service_discovery_node_record_t
} from './ClusterServiceDiscoveryStore.class';
import type {
  cluster_service_discovery_consensus_append_entries_request_message_i,
  cluster_service_discovery_consensus_log_entry_t,
  cluster_service_discovery_consensus_request_vote_request_message_i,
  cluster_service_discovery_consensus_role_t,
  cluster_service_discovery_protocol_error_t,
  cluster_service_discovery_protocol_request_message_type_t,
  cluster_service_discovery_redirect_endpoint_t,
  cluster_service_discovery_request_message_t,
  cluster_service_discovery_response_error_message_i,
  cluster_service_discovery_response_success_message_i,
  cluster_service_discovery_write_request_message_t
} from './ClusterServiceDiscoveryProtocol';
import {
  BuildClusterServiceDiscoveryProtocolError,
  ParseClusterServiceDiscoveryRequestMessage,
  ParseClusterServiceDiscoveryResponseErrorMessage,
  ParseClusterServiceDiscoveryResponseSuccessMessage
} from './ClusterServiceDiscoveryProtocolValidators';
import {
  ClusterServiceDiscoveryDaemonInMemoryStateStore,
  type cluster_service_discovery_daemon_committed_operation_log_entry_t,
  type cluster_service_discovery_daemon_state_retention_policy_t,
  type cluster_service_discovery_daemon_state_snapshot_t,
  type cluster_service_discovery_daemon_state_store_i
} from './ClusterServiceDiscoveryDaemonStateStore.class';

export type cluster_service_discovery_daemon_identity_t = {
  subject: string;
  scopes: string[];
};

export type cluster_service_discovery_daemon_auth_result_t =
  | {
      ok: true;
      identity: cluster_service_discovery_daemon_identity_t;
    }
  | {
      ok: false;
      message: string;
      details?: Record<string, unknown>;
    };

export type cluster_service_discovery_daemon_authenticate_request_t = (params: {
  headers: IncomingHttpHeaders;
  request_message: cluster_service_discovery_request_message_t;
}) =>
  | cluster_service_discovery_daemon_auth_result_t
  | Promise<cluster_service_discovery_daemon_auth_result_t>;

export type cluster_service_discovery_daemon_peer_endpoint_t = {
  daemon_id: string;
  host: string;
  port: number;
  request_path?: string;
  tls_mode?: 'required';
};

export type cluster_service_discovery_daemon_ha_config_t = {
  enabled?: boolean;
  daemon_id: string;
  peer_endpoint_list?: cluster_service_discovery_daemon_peer_endpoint_t[];
  election_timeout_min_ms?: number;
  election_timeout_max_ms?: number;
  leader_heartbeat_interval_ms?: number;
  replication_timeout_ms?: number;
  state_store?: cluster_service_discovery_daemon_state_store_i;
  state_retention_policy?: Partial<cluster_service_discovery_daemon_state_retention_policy_t>;
  allow_follower_stale_reads?: boolean;
};

export type cluster_service_discovery_daemon_constructor_params_t = {
  host?: string;
  port?: number;
  request_path?: string;
  service_discovery_store?: cluster_service_discovery_store_i;
  authenticate_request?: cluster_service_discovery_daemon_authenticate_request_t;
  ha?: cluster_service_discovery_daemon_ha_config_t;
  security?: {
    tls?: cluster_tls_server_config_t;
  };
  transport_security?: cluster_tls_client_config_t;
};

export type cluster_service_discovery_daemon_address_t = {
  host: string;
  port: number;
  request_path: string;
};

export type cluster_service_discovery_daemon_event_name_t =
  | 'daemon_started'
  | 'daemon_stopped'
  | 'request_received'
  | 'request_succeeded'
  | 'request_failed'
  | 'auth_failed'
  | 'discovery_leader_elected'
  | 'discovery_leader_lost'
  | 'discovery_quorum_degraded'
  | 'discovery_quorum_restored'
  | 'discovery_replication_failed';

export type cluster_service_discovery_daemon_event_t = {
  event_name: cluster_service_discovery_daemon_event_name_t;
  timestamp_unix_ms: number;
  request_id?: string;
  trace_id?: string;
  node_id?: string;
  details?: Record<string, unknown>;
};

export type cluster_service_discovery_daemon_ha_metrics_t = {
  leader_election_total: number;
  leader_term: number;
  quorum_size: number;
  quorum_write_failure_total: number;
  quorum_write_success_total: number;
  replication_timeout_total: number;
  redirect_total: number;
  role: cluster_service_discovery_consensus_role_t;
  leader_id: string | null;
  commit_index: number;
  applied_index: number;
  replication_lag_by_peer: Record<string, number>;
};

export type cluster_service_discovery_daemon_metrics_t = {
  request_total: number;
  request_success_total: number;
  request_failed_total: number;
  validation_failed_total: number;
  auth_failed_total: number;
  state_failed_total: number;
  internal_failed_total: number;
  request_failed_count_by_reason: Record<string, number>;
  started_unix_ms: number | null;
  active: boolean;
  ha: cluster_service_discovery_daemon_ha_metrics_t;
};

type cluster_service_discovery_protocol_error_exception_t = {
  kind: 'protocol_error';
  error: cluster_service_discovery_protocol_error_t;
};

function IsProtocolErrorException(
  value: unknown
): value is cluster_service_discovery_protocol_error_exception_t {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as { kind?: string }).kind === 'protocol_error' &&
    typeof (value as { error?: unknown }).error === 'object'
  );
}

function ThrowProtocolError(params: {
  error: cluster_service_discovery_protocol_error_t;
}): never {
  throw {
    kind: 'protocol_error',
    error: params.error
  } as cluster_service_discovery_protocol_error_exception_t;
}

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

function BuildDeterministicHash(params: {
  value: string;
}): number {
  let hash = 2166136261;

  for (const character of params.value) {
    hash ^= character.charCodeAt(0);
    hash = Math.imul(hash, 16777619);
  }

  return Math.abs(hash >>> 0);
}

function BuildElectionTimeoutMs(params: {
  daemon_id: string;
  term: number;
  min_timeout_ms: number;
  max_timeout_ms: number;
}): number {
  const range = Math.max(1, params.max_timeout_ms - params.min_timeout_ms + 1);
  const offset =
    BuildDeterministicHash({
      value: `${params.daemon_id}:${params.term}`
    }) % range;

  return params.min_timeout_ms + offset;
}

function IsWriteRequestMessage(params: {
  request_message: cluster_service_discovery_request_message_t;
}): params is {
  request_message: cluster_service_discovery_write_request_message_t;
} {
  return (
    params.request_message.message_type === 'cluster_service_discovery_register_node' ||
    params.request_message.message_type === 'cluster_service_discovery_heartbeat_node' ||
    params.request_message.message_type === 'cluster_service_discovery_update_capability' ||
    params.request_message.message_type === 'cluster_service_discovery_remove_node' ||
    params.request_message.message_type === 'cluster_service_discovery_expire_stale_nodes'
  );
}

function IsReadRequestMessage(params: {
  request_message: cluster_service_discovery_request_message_t;
}): boolean {
  return (
    params.request_message.message_type === 'cluster_service_discovery_list_nodes' ||
    params.request_message.message_type === 'cluster_service_discovery_query_nodes' ||
    params.request_message.message_type === 'cluster_service_discovery_get_metrics' ||
    params.request_message.message_type === 'cluster_service_discovery_get_events'
  );
}

function ReadRequestBody(params: { request: Http2ServerRequest }): Promise<string> {
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

function BuildDefaultStateRetentionPolicy(): cluster_service_discovery_daemon_state_retention_policy_t {
  return {
    max_operation_log_count: 10_000,
    max_discovery_event_count: 5_000
  };
}

export class ClusterServiceDiscoveryDaemon {
  private readonly service_discovery_store: cluster_service_discovery_store_i;
  private readonly authenticate_request:
    | cluster_service_discovery_daemon_authenticate_request_t
    | undefined;
  private readonly request_path: string;

  private host: string;
  private port: number;
  private readonly tls_server_config: cluster_tls_server_config_t;
  private readonly tls_client_config: cluster_tls_client_config_t;
  private http2_server: Http2SecureServer | null = null;

  private started_unix_ms: number | null = null;
  private started_discovery_expiration_loop = false;

  private readonly daemon_event_list: cluster_service_discovery_daemon_event_t[] = [];
  private max_daemon_event_count = 2_000;

  private daemon_metrics: cluster_service_discovery_daemon_metrics_t = {
    request_total: 0,
    request_success_total: 0,
    request_failed_total: 0,
    validation_failed_total: 0,
    auth_failed_total: 0,
    state_failed_total: 0,
    internal_failed_total: 0,
    request_failed_count_by_reason: {},
    started_unix_ms: null,
    active: false,
    ha: {
      leader_election_total: 0,
      leader_term: 0,
      quorum_size: 1,
      quorum_write_failure_total: 0,
      quorum_write_success_total: 0,
      replication_timeout_total: 0,
      redirect_total: 0,
      role: 'follower',
      leader_id: null,
      commit_index: 0,
      applied_index: 0,
      replication_lag_by_peer: {}
    }
  };

  private readonly ha_enabled: boolean;
  private readonly daemon_id: string;
  private readonly allow_follower_stale_reads: boolean;
  private readonly ha_state_store: cluster_service_discovery_daemon_state_store_i;
  private readonly state_retention_policy: cluster_service_discovery_daemon_state_retention_policy_t;

  private readonly peer_endpoint_by_daemon_id = new Map<
    string,
    cluster_service_discovery_daemon_peer_endpoint_t
  >();

  private leader_id: string | null = null;
  private role: cluster_service_discovery_consensus_role_t = 'follower';
  private current_term = 0;
  private voted_for_daemon_id: string | null = null;

  private commit_index = 0;
  private applied_index = 0;

  private operation_log: cluster_service_discovery_consensus_log_entry_t[] = [];
  private applied_result_by_request_id = new Map<
    string,
    cluster_service_discovery_response_success_message_i['data']
  >();
  private lease_revision_by_node_id = new Map<string, number>();

  private next_index_by_peer_id = new Map<string, number>();
  private match_index_by_peer_id = new Map<string, number>();

  private election_timeout_min_ms = 500;
  private election_timeout_max_ms = 900;
  private leader_heartbeat_interval_ms = 150;
  private replication_timeout_ms = 2_000;

  private consensus_tick_interval_handle: NodeJS.Timeout | null = null;
  private leader_heartbeat_interval_handle: NodeJS.Timeout | null = null;
  private next_election_deadline_unix_ms = 0;
  private last_leader_heartbeat_unix_ms = 0;
  private election_in_progress = false;

  constructor(params: cluster_service_discovery_daemon_constructor_params_t = {}) {
    this.host = params.host ?? '127.0.0.1';
    this.port = params.port ?? 0;
    this.tls_server_config = ValidateTlsServerConfig({
      tls_server_config: params.security?.tls
    });
    this.tls_client_config = ValidateTlsClientConfig({
      tls_client_config: params.transport_security,
      field_prefix: 'cluster_service_discovery_daemon.transport_security'
    });
    this.request_path = BuildRequestPath({
      request_path: params.request_path
    });

    this.service_discovery_store =
      params.service_discovery_store ?? new ClusterInMemoryServiceDiscoveryStore();
    this.authenticate_request = params.authenticate_request;

    const ha_config = params.ha;
    this.ha_enabled = ha_config?.enabled === true;
    this.daemon_id = ha_config?.daemon_id ?? `discovery_daemon_${process.pid}`;
    this.allow_follower_stale_reads = ha_config?.allow_follower_stale_reads === true;

    this.ha_state_store =
      ha_config?.state_store ??
      new ClusterServiceDiscoveryDaemonInMemoryStateStore({
        daemon_id: this.daemon_id
      });

    this.state_retention_policy = {
      ...BuildDefaultStateRetentionPolicy(),
      ...(ha_config?.state_retention_policy ?? {})
    };

    this.election_timeout_min_ms = ha_config?.election_timeout_min_ms ?? this.election_timeout_min_ms;
    this.election_timeout_max_ms = Math.max(
      this.election_timeout_min_ms,
      ha_config?.election_timeout_max_ms ?? this.election_timeout_max_ms
    );
    this.leader_heartbeat_interval_ms =
      ha_config?.leader_heartbeat_interval_ms ?? this.leader_heartbeat_interval_ms;
    this.replication_timeout_ms =
      ha_config?.replication_timeout_ms ?? this.replication_timeout_ms;

    for (const peer_endpoint of ha_config?.peer_endpoint_list ?? []) {
      if (peer_endpoint.daemon_id === this.daemon_id) {
        continue;
      }

      this.peer_endpoint_by_daemon_id.set(peer_endpoint.daemon_id, {
        daemon_id: peer_endpoint.daemon_id,
        host: peer_endpoint.host,
        port: peer_endpoint.port,
        request_path: peer_endpoint.request_path,
        tls_mode: peer_endpoint.tls_mode ?? 'required'
      });
    }

    this.hydrateFromStateStore();
    this.recomputeQuorumSizeMetric();
  }

  setHaPeerEndpointList(params: {
    peer_endpoint_list: cluster_service_discovery_daemon_peer_endpoint_t[];
  }): void {
    this.peer_endpoint_by_daemon_id.clear();

    for (const peer_endpoint of params.peer_endpoint_list) {
      if (peer_endpoint.daemon_id === this.daemon_id) {
        continue;
      }

      this.peer_endpoint_by_daemon_id.set(peer_endpoint.daemon_id, {
        daemon_id: peer_endpoint.daemon_id,
        host: peer_endpoint.host,
        port: peer_endpoint.port,
        request_path: peer_endpoint.request_path,
        tls_mode: peer_endpoint.tls_mode ?? 'required'
      });
    }

    this.recomputeQuorumSizeMetric();
  }

  async start(params: {
    host?: string;
    port?: number;
  } = {}): Promise<cluster_service_discovery_daemon_address_t> {
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
      throw new Error('Failed to resolve discovery daemon listen address.');
    }

    this.host = address.address;
    this.port = address.port;

    this.service_discovery_store.startExpirationLoop();
    this.started_discovery_expiration_loop = true;

    this.started_unix_ms = Date.now();
    this.daemon_metrics.started_unix_ms = this.started_unix_ms;
    this.daemon_metrics.active = true;

    if (this.ha_enabled) {
      this.startConsensusLoop();
    }

    this.recordDaemonEvent({
      event_name: 'daemon_started',
      details: {
        host: this.host,
        port: this.port,
        request_path: this.request_path,
        ha_enabled: this.ha_enabled,
        daemon_id: this.daemon_id
      }
    });

    return this.getAddress();
  }

  async stop(): Promise<void> {
    this.stopConsensusLoop();

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

    if (this.started_discovery_expiration_loop) {
      this.service_discovery_store.stopExpirationLoop();
      this.started_discovery_expiration_loop = false;
    }

    this.daemon_metrics.active = false;
    this.persistStateSnapshot();

    this.recordDaemonEvent({
      event_name: 'daemon_stopped'
    });
  }

  getAddress(): cluster_service_discovery_daemon_address_t {
    return {
      host: this.host,
      port: this.port,
      request_path: this.request_path
    };
  }

  getMetrics(): cluster_service_discovery_daemon_metrics_t {
    return {
      ...this.daemon_metrics,
      request_failed_count_by_reason: {
        ...this.daemon_metrics.request_failed_count_by_reason
      },
      ha: {
        ...this.daemon_metrics.ha,
        replication_lag_by_peer: {
          ...this.daemon_metrics.ha.replication_lag_by_peer
        }
      }
    };
  }

  getRecentEvents(params: {
    limit?: number;
    event_name?: cluster_service_discovery_daemon_event_name_t;
    request_id?: string;
  } = {}): cluster_service_discovery_daemon_event_t[] {
    let event_list = [...this.daemon_event_list];

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

    return event_list.map((event): cluster_service_discovery_daemon_event_t => {
      return {
        ...event,
        details: event.details ? { ...event.details } : undefined
      };
    });
  }

  getSnapshot(): {
    captured_at_unix_ms: number;
    address: cluster_service_discovery_daemon_address_t;
    daemon_metrics: cluster_service_discovery_daemon_metrics_t;
    discovery_metrics: cluster_service_discovery_metrics_t;
    recent_daemon_events: cluster_service_discovery_daemon_event_t[];
    recent_discovery_events: cluster_service_discovery_event_t[];
    ha_status: cluster_service_discovery_response_success_message_i['data'];
  } {
    return {
      captured_at_unix_ms: Date.now(),
      address: this.getAddress(),
      daemon_metrics: this.getMetrics(),
      discovery_metrics: this.service_discovery_store.getMetrics(),
      recent_daemon_events: this.getRecentEvents({
        limit: 100
      }),
      recent_discovery_events: this.service_discovery_store.getRecentEvents({
        limit: 100
      }),
      ha_status: {
        ha_status: this.buildHaStatus()
      }
    };
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
        error: BuildClusterServiceDiscoveryProtocolError({
          code: 'DISCOVERY_VALIDATION_FAILED',
          message: 'Discovery daemon only accepts POST requests.',
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
        error: BuildClusterServiceDiscoveryProtocolError({
          code: 'DISCOVERY_VALIDATION_FAILED',
          message: 'Discovery daemon request path mismatch.',
          retryable: false,
          details: {
            reason: 'invalid_path',
            path: request.url
          }
        })
      });
      return;
    }

    let parsed_message: cluster_service_discovery_request_message_t | null = null;

    try {
      const raw_body = await ReadRequestBody({
        request
      });

      const parsed_json = JSON.parse(raw_body) as unknown;
      const parse_result = ParseClusterServiceDiscoveryRequestMessage({
        message: parsed_json
      });

      if (!parse_result.ok) {
        this.incrementFailureMetric({
          reason: 'validation_failed',
          error_code: parse_result.error.code
        });

        this.writeErrorResponse({
          response,
          request_id: 'unknown_request',
          error: {
            ...parse_result.error,
            code: 'DISCOVERY_VALIDATION_FAILED'
          }
        });
        return;
      }

      parsed_message = parse_result.value;
      this.daemon_metrics.request_total += 1;

      this.recordDaemonEvent({
        event_name: 'request_received',
        request_id: parsed_message.request_id,
        trace_id: parsed_message.trace_id,
        node_id:
          'node_id' in parsed_message && typeof parsed_message.node_id === 'string'
            ? parsed_message.node_id
            : 'node_identity' in parsed_message
              ? parsed_message.node_identity.node_id
              : undefined,
        details: {
          message_type: parsed_message.message_type,
          term: this.current_term,
          role: this.role
        }
      });

      if (this.authenticate_request) {
        const auth_result = await this.authenticate_request({
          headers: request.headers,
          request_message: parsed_message
        });

        if (!auth_result.ok) {
          this.incrementFailureMetric({
            reason: 'auth_failed',
            error_code: 'DISCOVERY_AUTH_FAILED'
          });

          this.recordDaemonEvent({
            event_name: 'auth_failed',
            request_id: parsed_message.request_id,
            trace_id: parsed_message.trace_id,
            details: auth_result.details
          });

          this.writeErrorResponse({
            response,
            request_id: parsed_message.request_id,
            trace_id: parsed_message.trace_id,
            operation_message_type: parsed_message.message_type,
            error: BuildClusterServiceDiscoveryProtocolError({
              code: 'DISCOVERY_AUTH_FAILED',
              message: auth_result.message,
              retryable: false,
              details: auth_result.details
            })
          });
          return;
        }
      }

      const success_message = await this.handleDiscoveryRequest({
        request_message: parsed_message
      });

      this.daemon_metrics.request_success_total += 1;
      this.recordDaemonEvent({
        event_name: 'request_succeeded',
        request_id: parsed_message.request_id,
        trace_id: parsed_message.trace_id,
        details: {
          message_type: parsed_message.message_type
        }
      });

      this.writeSuccessResponse({
        response,
        success_message
      });
    } catch (error) {
      if (IsProtocolErrorException(error)) {
        const request_id = parsed_message?.request_id ?? 'unknown_request';

        this.incrementFailureMetric({
          reason: 'state_failed',
          error_code: error.error.code
        });

        this.recordDaemonEvent({
          event_name: 'request_failed',
          request_id,
          trace_id: parsed_message?.trace_id,
          details: {
            code: error.error.code,
            message: error.error.message,
            operation_message_type: parsed_message?.message_type
          }
        });

        this.writeErrorResponse({
          response,
          request_id,
          trace_id: parsed_message?.trace_id,
          operation_message_type: parsed_message?.message_type,
          error: error.error
        });

        return;
      }

      const request_id = parsed_message?.request_id ?? 'unknown_request';

      this.incrementFailureMetric({
        reason: 'internal_failed',
        error_code: 'DISCOVERY_INTERNAL'
      });

      this.recordDaemonEvent({
        event_name: 'request_failed',
        request_id,
        trace_id: parsed_message?.trace_id,
        details: {
          message: error instanceof Error ? error.message : String(error)
        }
      });

      this.writeErrorResponse({
        response,
        request_id,
        trace_id: parsed_message?.trace_id,
        operation_message_type: parsed_message?.message_type,
        error: BuildClusterServiceDiscoveryProtocolError({
          code: 'DISCOVERY_INTERNAL',
          message: 'Discovery daemon request handling failed.',
          retryable: true,
          details: {
            message: error instanceof Error ? error.message : String(error)
          }
        })
      });
    }
  }

  private async handleDiscoveryRequest(params: {
    request_message: cluster_service_discovery_request_message_t;
  }): Promise<cluster_service_discovery_response_success_message_i> {
    const { request_message } = params;
    const request_message_type = request_message.message_type;

    if (request_message.message_type === 'cluster_service_discovery_consensus_request_vote') {
      return this.handleConsensusRequestVote({
        request_message
      });
    }

    if (request_message.message_type === 'cluster_service_discovery_consensus_append_entries') {
      return await this.handleConsensusAppendEntries({
        request_message
      });
    }

    if (request_message.message_type === 'cluster_service_discovery_get_ha_status') {
      return this.buildSuccessMessage({
        request_message,
        data: {
          ha_status: this.buildHaStatus()
        }
      });
    }

    const is_read_request =
      request_message.message_type === 'cluster_service_discovery_list_nodes' ||
      request_message.message_type === 'cluster_service_discovery_query_nodes' ||
      request_message.message_type === 'cluster_service_discovery_get_metrics' ||
      request_message.message_type === 'cluster_service_discovery_get_events';

    if (is_read_request) {
      const read_request_message = request_message as Extract<
        cluster_service_discovery_request_message_t,
        {
          message_type:
            | 'cluster_service_discovery_list_nodes'
            | 'cluster_service_discovery_query_nodes'
            | 'cluster_service_discovery_get_metrics'
            | 'cluster_service_discovery_get_events';
        }
      >;

      const consistency_mode =
        'consistency_mode' in read_request_message
          ? read_request_message.consistency_mode
          : undefined;

      if (
        this.ha_enabled &&
        consistency_mode !== 'stale_ok' &&
        !this.isLeader()
      ) {
        this.throwNotLeaderProtocolError({
          retryable: true
        });
      }

      return this.handleDiscoveryReadRequest({ request_message: read_request_message });
    }

    const is_write_request =
      request_message.message_type === 'cluster_service_discovery_register_node' ||
      request_message.message_type === 'cluster_service_discovery_heartbeat_node' ||
      request_message.message_type === 'cluster_service_discovery_update_capability' ||
      request_message.message_type === 'cluster_service_discovery_remove_node' ||
      request_message.message_type === 'cluster_service_discovery_expire_stale_nodes';

    if (!is_write_request) {
      ThrowProtocolError({
        error: BuildClusterServiceDiscoveryProtocolError({
          code: 'DISCOVERY_VALIDATION_FAILED',
          message: `Unsupported discovery request type "${request_message_type}".`,
          retryable: false,
          details: {
            message_type: request_message_type
          }
        })
      });
    }

    const write_request_message = request_message as cluster_service_discovery_write_request_message_t;

    if (!this.ha_enabled) {
      const write_data = this.applyWriteRequestMessage({
        request_message: write_request_message
      });

      return this.buildSuccessMessage({
        request_message,
        data: write_data
      });
    }

    if (!this.isLeader()) {
      this.daemon_metrics.ha.redirect_total += 1;
      this.throwNotLeaderProtocolError({
        retryable: true
      });
    }

    if (this.applied_result_by_request_id.has(request_message.request_id)) {
      return this.buildSuccessMessage({
        request_message,
        data: this.applied_result_by_request_id.get(request_message.request_id) as cluster_service_discovery_response_success_message_i['data']
      });
    }

    const next_index = this.operation_log.length + 1;
    const log_entry: cluster_service_discovery_consensus_log_entry_t = {
      index: next_index,
      term: this.current_term,
      request_id: request_message.request_id,
      request_message: write_request_message
    };

    this.operation_log.push(log_entry);
    this.persistStateSnapshot();

    const quorum_committed = await this.replicateUntilQuorumCommitted({
      target_index: next_index,
      request_id: request_message.request_id
    });

    if (!quorum_committed) {
      this.daemon_metrics.ha.quorum_write_failure_total += 1;
      ThrowProtocolError({
        error: BuildClusterServiceDiscoveryProtocolError({
          code: 'DISCOVERY_QUORUM_UNAVAILABLE',
          message: 'Failed to commit discovery write to quorum.',
          retryable: true,
          details: {
            request_id: request_message.request_id,
            target_index: next_index,
            term: this.current_term,
            leader_id: this.leader_id
          }
        })
      });
    }

    this.daemon_metrics.ha.quorum_write_success_total += 1;

    const applied_data = this.applied_result_by_request_id.get(request_message.request_id);
    if (!applied_data) {
      ThrowProtocolError({
        error: BuildClusterServiceDiscoveryProtocolError({
          code: 'DISCOVERY_INTERNAL',
          message: 'Committed write did not produce applied response.',
          retryable: true,
          details: {
            request_id: request_message.request_id,
            commit_index: this.commit_index,
            applied_index: this.applied_index
          }
        })
      });
    }

    return this.buildSuccessMessage({
      request_message,
      data: applied_data
    });
  }

  private handleDiscoveryReadRequest(params: {
    request_message: Extract<
      cluster_service_discovery_request_message_t,
      {
        message_type:
          | 'cluster_service_discovery_list_nodes'
          | 'cluster_service_discovery_query_nodes'
          | 'cluster_service_discovery_get_metrics'
          | 'cluster_service_discovery_get_events';
      }
    >;
  }): cluster_service_discovery_response_success_message_i {
    const { request_message } = params;

    if (request_message.message_type === 'cluster_service_discovery_list_nodes') {
      const node_list = this.service_discovery_store.listNodes({
        include_expired: request_message.include_expired
      });

      return this.buildSuccessMessage({
        request_message,
        data: {
          node_list
        }
      });
    }

    if (request_message.message_type === 'cluster_service_discovery_query_nodes') {
      const node_list = this.service_discovery_store.queryNodes({
        include_expired: request_message.include_expired,
        status_list: request_message.status_list,
        label_match: request_message.label_match,
        zone: request_message.zone,
        capability_function_name: request_message.capability_function_name,
        capability_function_hash_sha1: request_message.capability_function_hash_sha1
      });

      return this.buildSuccessMessage({
        request_message,
        data: {
          node_list
        }
      });
    }

    if (request_message.message_type === 'cluster_service_discovery_get_metrics') {
      return this.buildSuccessMessage({
        request_message,
        data: {
          metrics: this.service_discovery_store.getMetrics()
        }
      });
    }

    return this.buildSuccessMessage({
      request_message,
      data: {
        event_list: this.service_discovery_store.getRecentEvents({
          limit: request_message.limit,
          event_name: request_message.event_name,
          node_id: request_message.node_id
        })
      }
    });
  }

  private applyWriteRequestMessage(params: {
    request_message: cluster_service_discovery_write_request_message_t;
  }): cluster_service_discovery_response_success_message_i['data'] {
    const { request_message } = params;

    if (request_message.message_type === 'cluster_service_discovery_register_node') {
      const node_record = this.service_discovery_store.upsertNodeRegistration({
        node_identity: request_message.node_identity,
        status: request_message.status,
        metrics: request_message.metrics,
        capability_list: request_message.capability_list,
        lease_ttl_ms: request_message.lease_ttl_ms,
        now_unix_ms: request_message.timestamp_unix_ms
      });

      this.bumpLeaseRevision({
        node_id: node_record.node_identity.node_id
      });

      return {
        node_record,
        lease_revision: this.getLeaseRevision({
          node_id: node_record.node_identity.node_id
        })
      };
    }

    if (request_message.message_type === 'cluster_service_discovery_heartbeat_node') {
      if (typeof request_message.lease_revision === 'number') {
        const existing_revision = this.getLeaseRevision({
          node_id: request_message.node_id
        });

        if (request_message.lease_revision < existing_revision) {
          ThrowProtocolError({
            error: BuildClusterServiceDiscoveryProtocolError({
              code: 'DISCOVERY_CONFLICT_STALE_TERM',
              message: 'Lease heartbeat revision is stale.',
              retryable: false,
              details: {
                node_id: request_message.node_id,
                expected_min_revision: existing_revision,
                received_revision: request_message.lease_revision
              }
            })
          });
        }
      }

      const node_record = this.service_discovery_store.updateNodeHeartbeat({
        node_id: request_message.node_id,
        status: request_message.status,
        metrics: request_message.metrics ?? {},
        lease_ttl_ms: request_message.lease_ttl_ms,
        now_unix_ms: request_message.timestamp_unix_ms
      });

      this.bumpLeaseRevision({
        node_id: request_message.node_id
      });

      return {
        node_record,
        lease_revision: this.getLeaseRevision({
          node_id: request_message.node_id
        })
      };
    }

    if (request_message.message_type === 'cluster_service_discovery_update_capability') {
      const node_record = this.service_discovery_store.updateNodeCapabilities({
        node_id: request_message.node_id,
        capability_list: request_message.capability_list,
        now_unix_ms: request_message.timestamp_unix_ms
      });

      return {
        node_record
      };
    }

    if (request_message.message_type === 'cluster_service_discovery_remove_node') {
      const removed = this.service_discovery_store.removeNode({
        node_id: request_message.node_id,
        reason: request_message.reason,
        now_unix_ms: request_message.timestamp_unix_ms
      });

      this.lease_revision_by_node_id.delete(request_message.node_id);

      return {
        removed
      };
    }

    const expire_result = this.service_discovery_store.expireStaleNodes({
      now_unix_ms: request_message.now_unix_ms
    });

    return {
      expired_node_id_list: expire_result.expired_node_id_list
    };
  }

  private handleConsensusRequestVote(params: {
    request_message: cluster_service_discovery_consensus_request_vote_request_message_i;
  }): cluster_service_discovery_response_success_message_i {
    const { request_message } = params;

    if (!this.ha_enabled) {
      ThrowProtocolError({
        error: BuildClusterServiceDiscoveryProtocolError({
          code: 'DISCOVERY_UNAVAILABLE',
          message: 'Consensus mode is disabled on this daemon.',
          retryable: false,
          details: {
            daemon_id: this.daemon_id
          }
        })
      });
    }

    if (request_message.term < this.current_term) {
      return this.buildSuccessMessage({
        request_message,
        data: {
          vote_granted: false,
          term: this.current_term,
          daemon_id: this.daemon_id
        }
      });
    }

    if (request_message.term > this.current_term) {
      this.transitionToFollower({
        next_term: request_message.term,
        leader_id: null,
        reason: 'request_vote_higher_term'
      });
    }

    const local_last_log_index = this.operation_log.length;
    const local_last_log_term = this.getLogTerm({
      index: local_last_log_index
    });

    const candidate_is_up_to_date =
      request_message.last_log_term > local_last_log_term ||
      (request_message.last_log_term === local_last_log_term &&
        request_message.last_log_index >= local_last_log_index);

    const can_vote =
      candidate_is_up_to_date &&
      (this.voted_for_daemon_id === null ||
        this.voted_for_daemon_id === request_message.candidate_id);

    if (can_vote) {
      this.voted_for_daemon_id = request_message.candidate_id;
      this.resetElectionDeadline();
      this.persistStateSnapshot();
    }

    return this.buildSuccessMessage({
      request_message,
      data: {
        vote_granted: can_vote,
        term: this.current_term,
        daemon_id: this.daemon_id
      }
    });
  }

  private async handleConsensusAppendEntries(params: {
    request_message: cluster_service_discovery_consensus_append_entries_request_message_i;
  }): Promise<cluster_service_discovery_response_success_message_i> {
    const { request_message } = params;

    if (!this.ha_enabled) {
      ThrowProtocolError({
        error: BuildClusterServiceDiscoveryProtocolError({
          code: 'DISCOVERY_UNAVAILABLE',
          message: 'Consensus mode is disabled on this daemon.',
          retryable: false,
          details: {
            daemon_id: this.daemon_id
          }
        })
      });
    }

    if (request_message.term < this.current_term) {
      return this.buildSuccessMessage({
        request_message,
        data: {
          success: false,
          term: this.current_term,
          match_index: this.operation_log.length,
          daemon_id: this.daemon_id,
          conflict_index: this.operation_log.length + 1
        }
      });
    }

    if (request_message.term > this.current_term || this.role !== 'follower') {
      this.transitionToFollower({
        next_term: request_message.term,
        leader_id: request_message.leader_id,
        reason: 'append_entries_received'
      });
    }

    this.leader_id = request_message.leader_id;
    this.last_leader_heartbeat_unix_ms = Date.now();
    this.resetElectionDeadline();

    const local_prev_log_term = this.getLogTerm({
      index: request_message.prev_log_index
    });

    if (request_message.prev_log_index > this.operation_log.length) {
      return this.buildSuccessMessage({
        request_message,
        data: {
          success: false,
          term: this.current_term,
          match_index: this.operation_log.length,
          daemon_id: this.daemon_id,
          conflict_index: this.operation_log.length + 1
        }
      });
    }

    if (request_message.prev_log_index > 0 && local_prev_log_term !== request_message.prev_log_term) {
      const truncated_length = Math.max(0, request_message.prev_log_index - 1);
      this.operation_log = this.operation_log.slice(0, truncated_length);

      return this.buildSuccessMessage({
        request_message,
        data: {
          success: false,
          term: this.current_term,
          match_index: this.operation_log.length,
          daemon_id: this.daemon_id,
          conflict_index: this.operation_log.length + 1
        }
      });
    }

    for (const incoming_entry of request_message.entry_list) {
      const local_entry = this.operation_log[incoming_entry.index - 1];

      if (local_entry && local_entry.term !== incoming_entry.term) {
        this.operation_log = this.operation_log.slice(0, incoming_entry.index - 1);
      }

      if (!this.operation_log[incoming_entry.index - 1]) {
        this.operation_log.push({
          index: incoming_entry.index,
          term: incoming_entry.term,
          request_id: incoming_entry.request_id,
          request_message: structuredClone(incoming_entry.request_message)
        });
      }
    }

    this.commit_index = Math.min(
      request_message.leader_commit_index,
      this.operation_log.length
    );

    await this.applyCommittedEntries();
    this.persistStateSnapshot();

    return this.buildSuccessMessage({
      request_message,
      data: {
        success: true,
        term: this.current_term,
        match_index: this.operation_log.length,
        daemon_id: this.daemon_id
      }
    });
  }

  private buildSuccessMessage(params: {
    request_message: cluster_service_discovery_request_message_t;
    data: cluster_service_discovery_response_success_message_i['data'];
  }): cluster_service_discovery_response_success_message_i {
    return {
      protocol_version: 1,
      message_type: 'cluster_service_discovery_response_success',
      timestamp_unix_ms: Date.now(),
      request_id: params.request_message.request_id,
      trace_id: params.request_message.trace_id,
      operation_message_type: params.request_message.message_type,
      data: params.data,
      ha_metadata: this.buildHaMetadata()
    };
  }

  private writeSuccessResponse(params: {
    response: Http2ServerResponse;
    success_message: cluster_service_discovery_response_success_message_i;
  }): void {
    params.response.statusCode = 200;
    params.response.setHeader('content-type', 'application/json');
    params.response.end(JSON.stringify(params.success_message));
  }

  private writeErrorResponse(params: {
    response: Http2ServerResponse;
    request_id: string;
    trace_id?: string;
    operation_message_type?: cluster_service_discovery_protocol_request_message_type_t;
    error: cluster_service_discovery_protocol_error_t;
  }): void {
    const error_message: cluster_service_discovery_response_error_message_i = {
      protocol_version: 1,
      message_type: 'cluster_service_discovery_response_error',
      timestamp_unix_ms: Date.now(),
      request_id: params.request_id,
      trace_id: params.trace_id,
      operation_message_type: params.operation_message_type,
      error: params.error,
      ha_metadata: this.buildHaMetadata()
    };

    params.response.statusCode = 200;
    params.response.setHeader('content-type', 'application/json');
    params.response.end(JSON.stringify(error_message));
  }

  private incrementFailureMetric(params: {
    reason: string;
    error_code: cluster_service_discovery_protocol_error_t['code'];
  }): void {
    this.daemon_metrics.request_failed_total += 1;

    if (params.reason === 'validation_failed') {
      this.daemon_metrics.validation_failed_total += 1;
    } else if (params.reason === 'auth_failed') {
      this.daemon_metrics.auth_failed_total += 1;
    } else if (params.reason === 'state_failed') {
      this.daemon_metrics.state_failed_total += 1;
    } else {
      this.daemon_metrics.internal_failed_total += 1;
    }

    const current_reason_count = this.daemon_metrics.request_failed_count_by_reason[params.reason] ?? 0;
    this.daemon_metrics.request_failed_count_by_reason[params.reason] =
      current_reason_count + 1;
  }

  private recordDaemonEvent(params: {
    event_name: cluster_service_discovery_daemon_event_name_t;
    request_id?: string;
    trace_id?: string;
    node_id?: string;
    details?: Record<string, unknown>;
  }): void {
    this.daemon_event_list.push({
      event_name: params.event_name,
      timestamp_unix_ms: Date.now(),
      request_id: params.request_id,
      trace_id: params.trace_id,
      node_id: params.node_id,
      details: params.details ? { ...params.details } : undefined
    });

    if (this.daemon_event_list.length > this.max_daemon_event_count) {
      this.daemon_event_list.splice(0, this.daemon_event_list.length - this.max_daemon_event_count);
    }
  }

  private buildHaMetadata(): cluster_service_discovery_response_success_message_i['ha_metadata'] {
    const leader_endpoint = this.getLeaderRedirectEndpoint();

    return {
      term: this.current_term,
      leader_id: this.leader_id ?? undefined,
      commit_index: this.commit_index,
      daemon_id: this.daemon_id,
      role: this.role,
      redirect_endpoint: leader_endpoint ?? undefined
    };
  }

  private throwNotLeaderProtocolError(params: {
    retryable: boolean;
  }): never {
    ThrowProtocolError({
      error: BuildClusterServiceDiscoveryProtocolError({
        code: 'DISCOVERY_NOT_LEADER',
        message: 'Discovery daemon is not leader for this request.',
        retryable: params.retryable,
        details: {
          daemon_id: this.daemon_id,
          leader_id: this.leader_id,
          redirect_endpoint: this.getLeaderRedirectEndpoint() ?? null
        }
      })
    });
  }

  private getLeaderRedirectEndpoint(): cluster_service_discovery_redirect_endpoint_t | undefined {
    if (!this.leader_id) {
      return undefined;
    }

    if (this.leader_id === this.daemon_id) {
      return {
        daemon_id: this.daemon_id,
        host: this.host,
        port: this.port,
        request_path: this.request_path,
        tls_mode: 'required'
      };
    }

    const endpoint = this.peer_endpoint_by_daemon_id.get(this.leader_id);
    if (!endpoint) {
      return undefined;
    }

    return {
      daemon_id: endpoint.daemon_id,
      host: endpoint.host,
      port: endpoint.port,
      request_path: BuildRequestPath({ request_path: endpoint.request_path }),
      tls_mode: endpoint.tls_mode ?? 'required'
    };
  }

  private isLeader(): boolean {
    return this.ha_enabled && this.role === 'leader' && this.leader_id === this.daemon_id;
  }

  private buildHaStatus() {
    const endpoint_by_daemon_id: Record<string, cluster_service_discovery_redirect_endpoint_t> = {};

    endpoint_by_daemon_id[this.daemon_id] = {
      daemon_id: this.daemon_id,
      host: this.host,
      port: this.port,
      request_path: this.request_path,
      tls_mode: 'required'
    };

    for (const [daemon_id, peer_endpoint] of this.peer_endpoint_by_daemon_id.entries()) {
      endpoint_by_daemon_id[daemon_id] = {
        daemon_id,
        host: peer_endpoint.host,
        port: peer_endpoint.port,
        request_path: BuildRequestPath({ request_path: peer_endpoint.request_path }),
        tls_mode: peer_endpoint.tls_mode ?? 'required'
      };
    }

    return {
      daemon_id: this.daemon_id,
      role: this.role,
      term: this.current_term,
      leader_id: this.leader_id,
      commit_index: this.commit_index,
      applied_index: this.applied_index,
      peer_daemon_id_list: [...this.peer_endpoint_by_daemon_id.keys()].sort(),
      quorum_size: this.getQuorumSize(),
      known_endpoint_by_daemon_id: endpoint_by_daemon_id
    };
  }

  private bumpLeaseRevision(params: { node_id: string }): void {
    const previous_revision = this.lease_revision_by_node_id.get(params.node_id) ?? 0;
    this.lease_revision_by_node_id.set(params.node_id, previous_revision + 1);
  }

  private getLeaseRevision(params: { node_id: string }): number {
    return this.lease_revision_by_node_id.get(params.node_id) ?? 0;
  }

  private hydrateFromStateStore(): void {
    if (!this.ha_enabled) {
      return;
    }

    const snapshot = this.ha_state_store.loadState();

    this.current_term = snapshot.daemon_metadata.current_term;
    this.voted_for_daemon_id = snapshot.daemon_metadata.voted_for_daemon_id;
    this.leader_id = snapshot.daemon_metadata.leader_id;
    this.role = 'follower';
    this.commit_index = snapshot.daemon_metadata.commit_index;
    this.applied_index = snapshot.daemon_metadata.last_applied_index;

    this.operation_log = snapshot.committed_operation_log.map((operation_entry) => {
      return {
        index: operation_entry.index,
        term: operation_entry.term,
        request_id: operation_entry.request_id,
        request_message: structuredClone(operation_entry.request_message)
      };
    });

    this.restoreNodeRecords({
      node_record_list: snapshot.node_record_list
    });

    for (const operation_entry of snapshot.committed_operation_log) {
      if (operation_entry.request_message.message_type === 'cluster_service_discovery_heartbeat_node') {
        this.bumpLeaseRevision({
          node_id: operation_entry.request_message.node_id
        });
      } else if (operation_entry.request_message.message_type === 'cluster_service_discovery_register_node') {
        this.bumpLeaseRevision({
          node_id: operation_entry.request_message.node_identity.node_id
        });
      }
    }

    this.daemon_metrics.ha.leader_term = this.current_term;
    this.daemon_metrics.ha.commit_index = this.commit_index;
    this.daemon_metrics.ha.applied_index = this.applied_index;
    this.daemon_metrics.ha.role = this.role;
    this.daemon_metrics.ha.leader_id = this.leader_id;
  }

  private restoreNodeRecords(params: {
    node_record_list: cluster_service_discovery_node_record_t[];
  }): void {
    const existing_node_list = this.service_discovery_store.listNodes({
      include_expired: true
    });

    for (const existing_node of existing_node_list) {
      this.service_discovery_store.removeNode({
        node_id: existing_node.node_identity.node_id,
        reason: 'ha_state_hydration_clear'
      });
    }

    for (const node_record of params.node_record_list) {
      if (node_record.status === 'expired') {
        continue;
      }

      const lease_ttl_ms = Math.max(1, node_record.lease_expires_unix_ms - node_record.last_heartbeat_unix_ms);

      this.service_discovery_store.upsertNodeRegistration({
        node_identity: node_record.node_identity,
        status: node_record.status,
        metrics: node_record.metrics,
        capability_list: node_record.capability_list,
        lease_ttl_ms,
        now_unix_ms: node_record.last_heartbeat_unix_ms
      });
    }

    this.service_discovery_store.expireStaleNodes({
      now_unix_ms: Date.now()
    });
  }

  private persistStateSnapshot(): void {
    if (!this.ha_enabled) {
      return;
    }

    const snapshot: cluster_service_discovery_daemon_state_snapshot_t = {
      schema_version: 1,
      daemon_metadata: {
        daemon_id: this.daemon_id,
        current_term: this.current_term,
        voted_for_daemon_id: this.voted_for_daemon_id,
        leader_id: this.leader_id,
        role: this.role,
        commit_index: this.commit_index,
        last_applied_index: this.applied_index,
        last_log_index: this.operation_log.length,
        updated_unix_ms: Date.now()
      },
      node_record_list: this.service_discovery_store.listNodes({
        include_expired: true
      }),
      committed_operation_log: this.operation_log.map((operation_entry): cluster_service_discovery_daemon_committed_operation_log_entry_t => {
        return {
          index: operation_entry.index,
          term: operation_entry.term,
          request_id: operation_entry.request_id,
          committed_unix_ms: Date.now(),
          request_message: structuredClone(operation_entry.request_message)
        };
      }),
      discovery_event_log: this.service_discovery_store.getRecentEvents({
        limit: this.state_retention_policy.max_discovery_event_count
      })
    };

    const compacted_snapshot = this.ha_state_store.compactState({
      state: snapshot,
      retention_policy: this.state_retention_policy
    });

    this.ha_state_store.saveState({
      state: compacted_snapshot
    });
  }

  private startConsensusLoop(): void {
    if (!this.ha_enabled) {
      return;
    }

    if (this.consensus_tick_interval_handle) {
      return;
    }

    this.last_leader_heartbeat_unix_ms = Date.now();
    this.resetElectionDeadline();

    this.consensus_tick_interval_handle = setInterval((): void => {
      void this.onConsensusTick();
    }, 50);

    this.consensus_tick_interval_handle.unref();
  }

  private stopConsensusLoop(): void {
    if (this.consensus_tick_interval_handle) {
      clearInterval(this.consensus_tick_interval_handle);
      this.consensus_tick_interval_handle = null;
    }

    if (this.leader_heartbeat_interval_handle) {
      clearInterval(this.leader_heartbeat_interval_handle);
      this.leader_heartbeat_interval_handle = null;
    }
  }

  private async onConsensusTick(): Promise<void> {
    if (!this.ha_enabled) {
      return;
    }

    if (this.isLeader()) {
      if (!this.leader_heartbeat_interval_handle) {
        this.startLeaderHeartbeatLoop();
      }
      return;
    }

    if (Date.now() < this.next_election_deadline_unix_ms) {
      return;
    }

    await this.startElection();
  }

  private startLeaderHeartbeatLoop(): void {
    if (this.leader_heartbeat_interval_handle) {
      return;
    }

    this.leader_heartbeat_interval_handle = setInterval((): void => {
      void this.broadcastAppendEntries({
        include_entries: false
      });
    }, this.leader_heartbeat_interval_ms);

    this.leader_heartbeat_interval_handle.unref();
  }

  private resetElectionDeadline(): void {
    this.next_election_deadline_unix_ms =
      Date.now() +
      BuildElectionTimeoutMs({
        daemon_id: this.daemon_id,
        term: this.current_term,
        min_timeout_ms: this.election_timeout_min_ms,
        max_timeout_ms: this.election_timeout_max_ms
      });
  }

  private async startElection(): Promise<void> {
    if (this.election_in_progress || !this.ha_enabled) {
      return;
    }

    this.election_in_progress = true;

    try {
      this.role = 'candidate';
      this.current_term += 1;
      this.voted_for_daemon_id = this.daemon_id;
      this.leader_id = null;
      this.daemon_metrics.ha.role = this.role;
      this.daemon_metrics.ha.leader_term = this.current_term;
      this.daemon_metrics.ha.leader_id = this.leader_id;
      this.persistStateSnapshot();
      this.resetElectionDeadline();

      let vote_count = 1;
      const quorum_size = this.getQuorumSize();

      const vote_result_list = await Promise.all(
        [...this.peer_endpoint_by_daemon_id.keys()].map(async (peer_daemon_id): Promise<boolean> => {
          try {
            const response_message = await this.sendConsensusRequestToPeer({
              peer_daemon_id,
              request_message: {
                protocol_version: 1,
                message_type: 'cluster_service_discovery_consensus_request_vote',
                timestamp_unix_ms: Date.now(),
                request_id: `vote_${this.daemon_id}_${this.current_term}_${peer_daemon_id}`,
                candidate_id: this.daemon_id,
                term: this.current_term,
                last_log_index: this.operation_log.length,
                last_log_term: this.getLogTerm({
                  index: this.operation_log.length
                })
              }
            });

            const data = response_message.data as {
              vote_granted?: boolean;
              term?: number;
            };

            if (typeof data.term === 'number' && data.term > this.current_term) {
              this.transitionToFollower({
                next_term: data.term,
                leader_id: null,
                reason: 'election_observed_higher_term'
              });

              return false;
            }

            return data.vote_granted === true;
          } catch {
            return false;
          }
        })
      );

      for (const vote_granted of vote_result_list) {
        if (!this.isElectionTermCurrent()) {
          break;
        }

        if (vote_granted) {
          vote_count += 1;
        }
      }

      if (!this.isElectionTermCurrent()) {
        return;
      }

      if (vote_count >= quorum_size) {
        this.role = 'leader';
        this.leader_id = this.daemon_id;
        this.voted_for_daemon_id = this.daemon_id;
        this.daemon_metrics.ha.role = this.role;
        this.daemon_metrics.ha.leader_id = this.leader_id;
        this.daemon_metrics.ha.leader_election_total += 1;

        for (const peer_daemon_id of this.peer_endpoint_by_daemon_id.keys()) {
          this.next_index_by_peer_id.set(peer_daemon_id, this.operation_log.length + 1);
          this.match_index_by_peer_id.set(peer_daemon_id, 0);
        }

        this.recordDaemonEvent({
          event_name: 'discovery_leader_elected',
          details: {
            daemon_id: this.daemon_id,
            term: this.current_term,
            vote_count,
            quorum_size
          }
        });

        await this.broadcastAppendEntries({
          include_entries: false
        });
      }

      this.persistStateSnapshot();
    } finally {
      this.election_in_progress = false;
    }
  }

  private isElectionTermCurrent(): boolean {
    return this.role === 'candidate' || this.role === 'leader';
  }

  private transitionToFollower(params: {
    next_term: number;
    leader_id: string | null;
    reason: string;
  }): void {
    const previous_role = this.role;
    const previous_leader_id = this.leader_id;

    this.role = 'follower';
    this.current_term = Math.max(this.current_term, params.next_term);
    this.leader_id = params.leader_id;
    this.voted_for_daemon_id = null;
    this.last_leader_heartbeat_unix_ms = Date.now();

    this.daemon_metrics.ha.role = this.role;
    this.daemon_metrics.ha.leader_term = this.current_term;
    this.daemon_metrics.ha.leader_id = this.leader_id;

    if (this.leader_heartbeat_interval_handle) {
      clearInterval(this.leader_heartbeat_interval_handle);
      this.leader_heartbeat_interval_handle = null;
    }

    if (previous_role === 'leader') {
      this.recordDaemonEvent({
        event_name: 'discovery_leader_lost',
        details: {
          daemon_id: this.daemon_id,
          previous_leader_id,
          next_leader_id: params.leader_id,
          term: this.current_term,
          reason: params.reason
        }
      });
    }

    this.persistStateSnapshot();
    this.resetElectionDeadline();
  }

  private getLogTerm(params: { index: number }): number {
    if (params.index <= 0) {
      return 0;
    }

    return this.operation_log[params.index - 1]?.term ?? 0;
  }

  private getQuorumSize(): number {
    const node_count = this.peer_endpoint_by_daemon_id.size + 1;
    return Math.floor(node_count / 2) + 1;
  }

  private recomputeQuorumSizeMetric(): void {
    this.daemon_metrics.ha.quorum_size = this.getQuorumSize();
  }

  private async replicateUntilQuorumCommitted(params: {
    target_index: number;
    request_id: string;
  }): Promise<boolean> {
    const quorum_size = this.getQuorumSize();
    const started_unix_ms = Date.now();

    while (Date.now() - started_unix_ms < this.replication_timeout_ms) {
      if (!this.isLeader()) {
        return false;
      }

      await this.broadcastAppendEntries({
        include_entries: true
      });

      let replicated_node_count = 1;
      for (const peer_daemon_id of this.peer_endpoint_by_daemon_id.keys()) {
        const peer_match_index = this.match_index_by_peer_id.get(peer_daemon_id) ?? 0;
        if (peer_match_index >= params.target_index) {
          replicated_node_count += 1;
        }
      }

      if (replicated_node_count >= quorum_size) {
        this.commit_index = Math.max(this.commit_index, params.target_index);
        this.daemon_metrics.ha.commit_index = this.commit_index;
        await this.applyCommittedEntries();
        this.persistStateSnapshot();

        await this.broadcastAppendEntries({
          include_entries: false
        });

        this.recordDaemonEvent({
          event_name: 'discovery_quorum_restored',
          details: {
            term: this.current_term,
            commit_index: this.commit_index,
            request_id: params.request_id,
            replicated_node_count,
            quorum_size
          }
        });

        return true;
      }

      this.recordDaemonEvent({
        event_name: 'discovery_quorum_degraded',
        details: {
          term: this.current_term,
          request_id: params.request_id,
          replicated_node_count,
          quorum_size,
          target_index: params.target_index
        }
      });

      await new Promise<void>((resolve): void => {
        setTimeout(resolve, 20);
      });
    }

    this.daemon_metrics.ha.replication_timeout_total += 1;

    this.recordDaemonEvent({
      event_name: 'discovery_replication_failed',
      details: {
        request_id: params.request_id,
        term: this.current_term,
        commit_index: this.commit_index,
        target_index: params.target_index
      }
    });

    return false;
  }

  private async broadcastAppendEntries(params: {
    include_entries: boolean;
  }): Promise<void> {
    const replication_result_list = await Promise.all(
      [...this.peer_endpoint_by_daemon_id.keys()].map(async (peer_daemon_id): Promise<void> => {
        await this.replicateToPeer({
          peer_daemon_id,
          include_entries: params.include_entries
        });
      })
    );

    void replication_result_list;
  }

  private async replicateToPeer(params: {
    peer_daemon_id: string;
    include_entries: boolean;
  }): Promise<void> {
    if (!this.isLeader()) {
      return;
    }

    const next_index = this.next_index_by_peer_id.get(params.peer_daemon_id) ?? 1;

    const prev_log_index = Math.max(0, next_index - 1);
    const prev_log_term = this.getLogTerm({
      index: prev_log_index
    });

    const entry_list = params.include_entries
      ? this.operation_log.slice(Math.max(0, next_index - 1))
      : [];

    try {
      const response_message = await this.sendConsensusRequestToPeer({
        peer_daemon_id: params.peer_daemon_id,
        request_message: {
          protocol_version: 1,
          message_type: 'cluster_service_discovery_consensus_append_entries',
          timestamp_unix_ms: Date.now(),
          request_id: `append_${this.daemon_id}_${this.current_term}_${params.peer_daemon_id}_${Date.now()}`,
          leader_id: this.daemon_id,
          term: this.current_term,
          prev_log_index,
          prev_log_term,
          entry_list,
          leader_commit_index: this.commit_index
        }
      });

      const append_data = response_message.data as {
        success?: boolean;
        term?: number;
        match_index?: number;
        conflict_index?: number;
      };

      if (typeof append_data.term === 'number' && append_data.term > this.current_term) {
        this.transitionToFollower({
          next_term: append_data.term,
          leader_id: null,
          reason: 'replication_higher_term'
        });
        return;
      }

      if (append_data.success === true) {
        const match_index = append_data.match_index ?? prev_log_index + entry_list.length;
        this.match_index_by_peer_id.set(params.peer_daemon_id, match_index);
        this.next_index_by_peer_id.set(params.peer_daemon_id, match_index + 1);
        this.daemon_metrics.ha.replication_lag_by_peer[params.peer_daemon_id] = Math.max(
          0,
          this.operation_log.length - match_index
        );
        return;
      }

      const conflict_index = append_data.conflict_index;
      if (typeof conflict_index === 'number' && conflict_index > 0) {
        this.next_index_by_peer_id.set(params.peer_daemon_id, conflict_index);
      } else {
        this.next_index_by_peer_id.set(
          params.peer_daemon_id,
          Math.max(1, next_index - 1)
        );
      }
    } catch {
      this.daemon_metrics.ha.replication_lag_by_peer[params.peer_daemon_id] = Math.max(
        0,
        this.operation_log.length - (this.match_index_by_peer_id.get(params.peer_daemon_id) ?? 0)
      );
    }
  }

  private async sendConsensusRequestToPeer(params: {
    peer_daemon_id: string;
    request_message:
      | cluster_service_discovery_consensus_request_vote_request_message_i
      | cluster_service_discovery_consensus_append_entries_request_message_i;
  }): Promise<cluster_service_discovery_response_success_message_i> {
    const peer_endpoint = this.peer_endpoint_by_daemon_id.get(params.peer_daemon_id);
    if (!peer_endpoint) {
      throw new Error(`Unknown peer daemon_id "${params.peer_daemon_id}".`);
    }

    const request_path = BuildRequestPath({
      request_path: peer_endpoint.request_path
    });

    let client_session: ClientHttp2Session | null = null;

    try {
      client_session = connect(
        BuildTransportAuthority({
          host: peer_endpoint.host,
          port: peer_endpoint.port,
          tls_mode: peer_endpoint.tls_mode ?? 'required'
        }),
        BuildTlsClientConnectOptions({
          tls_client_config: this.tls_client_config
        })
      );

      client_session.on('error', (): void => {
        // handled by stream error
      });

      const response_text = await new Promise<string>((resolve, reject): void => {
        const request_stream = client_session?.request({
          ':method': 'POST',
          ':path': request_path,
          'content-type': 'application/json',
          'x-wpc-discovery-peer': this.daemon_id
        });

        if (!request_stream) {
          reject(new Error('Failed to create consensus request stream.'));
          return;
        }

        const chunk_list: Buffer[] = [];
        const timeout_handle = setTimeout((): void => {
          try {
            request_stream.close();
          } catch {
            // Ignore close failures.
          }

          reject(new Error('Consensus peer request timed out.'));
        }, Math.max(100, this.replication_timeout_ms));

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

      const parsed_response = JSON.parse(response_text) as unknown;

      const success_result = ParseClusterServiceDiscoveryResponseSuccessMessage({
        message: parsed_response
      });
      if (success_result.ok) {
        return success_result.value;
      }

      const error_result = ParseClusterServiceDiscoveryResponseErrorMessage({
        message: parsed_response
      });
      if (!error_result.ok) {
        throw new Error(`Failed to parse peer response: ${error_result.error.message}`);
      }

      const error_message = error_result.value as cluster_service_discovery_response_error_message_i;
      const error = BuildClusterServiceDiscoveryProtocolError({
        code: error_message.error?.code ?? 'DISCOVERY_INTERNAL',
        message: error_message.error?.message ?? 'Unknown peer response error.',
        retryable: error_message.error?.retryable ?? true,
        details: error_message.error?.details
      });

      ThrowProtocolError({
        error
      });
    } finally {
      try {
        client_session?.close();
      } catch {
        // Ignore close failures.
      }
    }

    throw new Error('Unreachable consensus response state.');
  }

  private async applyCommittedEntries(): Promise<void> {
    while (this.applied_index < this.commit_index) {
      const next_index = this.applied_index + 1;
      const log_entry = this.operation_log[next_index - 1];
      if (!log_entry) {
        break;
      }

      const applied_data = this.applyWriteRequestMessage({
        request_message: log_entry.request_message
      });

      this.applied_result_by_request_id.set(log_entry.request_id, applied_data);
      this.applied_index = next_index;
      this.daemon_metrics.ha.applied_index = this.applied_index;
    }
  }
}
