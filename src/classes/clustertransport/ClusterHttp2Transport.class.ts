import {
  constants as http2_constants,
  createSecureServer,
  createServer,
  type Http2SecureServer,
  type Http2Server,
  type Http2ServerRequest,
  type Http2ServerResponse,
  type IncomingHttpHeaders,
  type SecureServerOptions,
  type ServerHttp2Session
} from 'node:http2';

import type {
  cluster_admin_mutation_request_message_i,
  cluster_call_cancel_ack_message_i,
  cluster_call_cancel_message_i,
  cluster_call_request_message_i
} from '../clusterprotocol/ClusterProtocolTypes';
import { ParseClusterProtocolMessage } from '../clusterprotocol/ClusterProtocolValidators';
import type {
  WorkerProcedureCall,
  handle_cluster_admin_mutation_response_t,
  handle_cluster_call_response_t
} from '../workerprocedurecall/WorkerProcedureCall.class';
import {
  ClusterTransportReplayProtection,
  ClusterTransportTokenValidator,
  type cluster_transport_replay_protection_config_t,
  type cluster_transport_token_validation_config_t,
  type cluster_transport_trusted_identity_t
} from './ClusterTransportAuth.class';

export type cluster_http2_session_identity_t = cluster_transport_trusted_identity_t;

export type cluster_http2_authenticate_request_result_t =
  | {
      ok: true;
      identity: cluster_http2_session_identity_t;
    }
  | {
      ok: false;
      message: string;
      details?: Record<string, unknown>;
    };

export type cluster_http2_authenticate_request_t = (params: {
  session_id: string;
  headers: IncomingHttpHeaders;
  existing_identity?: cluster_http2_session_identity_t;
}) =>
  | cluster_http2_authenticate_request_result_t
  | Promise<cluster_http2_authenticate_request_result_t>;

export type cluster_http2_transport_tls_mode_t =
  | 'disabled'
  | 'required'
  | 'terminated_upstream';

export type cluster_http2_transport_tls_config_t = {
  mode: cluster_http2_transport_tls_mode_t;
  key_pem?: string;
  cert_pem?: string;
  ca_pem_list?: string[];
  request_client_cert?: boolean;
  reject_unauthorized_client_cert?: boolean;
  terminated_upstream_assertion_header_name?: string;
  terminated_upstream_assertion_header_value?: string;
};

export type cluster_http2_transport_session_security_config_t = {
  max_identity_ttl_ms: number;
};

export type cluster_http2_transport_security_config_t = {
  tls?: Partial<cluster_http2_transport_tls_config_t>;
  token_validation?: cluster_transport_token_validation_config_t;
  replay_protection?: cluster_transport_replay_protection_config_t;
  session?: Partial<cluster_http2_transport_session_security_config_t>;
};

export type cluster_http2_transport_constructor_params_t = {
  workerprocedurecall: WorkerProcedureCall;
  node_id: string;
  host?: string;
  port?: number;
  request_path?: string;
  authenticate_request?: cluster_http2_authenticate_request_t;
  security?: cluster_http2_transport_security_config_t;
};

export type cluster_http2_transport_start_params_t = {
  host?: string;
  port?: number;
};

export type cluster_http2_transport_address_t = {
  host: string;
  port: number;
  request_path: string;
  tls_mode: cluster_http2_transport_tls_mode_t;
};

export type cluster_http2_transport_event_name_t =
  | 'session_connected'
  | 'session_authenticated'
  | 'session_disconnected'
  | 'request_received'
  | 'request_completed'
  | 'request_cancelled'
  | 'request_failed';

export type cluster_http2_transport_event_t = {
  event_name: cluster_http2_transport_event_name_t;
  timestamp_unix_ms: number;
  session_id?: string;
  request_id?: string;
  details?: Record<string, unknown>;
};

export type cluster_http2_transport_metrics_t = {
  session_connected_total: number;
  session_authenticated_total: number;
  session_disconnected_total: number;
  request_received_total: number;
  request_completed_total: number;
  request_cancelled_total: number;
  request_failed_total: number;
  auth_failed_total: number;
  replay_rejected_total: number;
  request_failed_count_by_reason: Record<string, number>;
  request_failed_count_by_error_code: Record<string, number>;
  max_in_flight_request_count: number;
  active_session_count: number;
  active_request_count: number;
};

export type cluster_http2_transport_event_listener_t = (
  event: cluster_http2_transport_event_t
) => void;

export type cluster_http2_session_snapshot_t = {
  session_id: string;
  connected_unix_ms: number;
  disconnected_unix_ms?: number;
  remote_address?: string;
  authenticated: boolean;
  identity_expires_unix_ms?: number;
  in_flight_request_count: number;
};

type cluster_http2_session_state_t = {
  session_id: string;
  connected_unix_ms: number;
  disconnected_unix_ms?: number;
  remote_address?: string;
  identity?: cluster_http2_session_identity_t;
  identity_expires_unix_ms?: number;
  in_flight_request_id_set: Set<string>;
};

type cluster_http2_in_flight_request_state_t = {
  request_id: string;
  session_id: string;
  started_unix_ms: number;
  cancelled: boolean;
  response: Http2ServerResponse;
};

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function BuildSessionId(params: { next_session_index: number }): string {
  return `session_${params.next_session_index}`;
}

async function ReadRequestBody(params: {
  request: Http2ServerRequest;
}): Promise<string> {
  const { request } = params;

  return await new Promise<string>((resolve, reject): void => {
    const chunk_list: Buffer[] = [];

    request.on('data', (chunk): void => {
      if (typeof chunk === 'string') {
        chunk_list.push(Buffer.from(chunk));
        return;
      }

      chunk_list.push(chunk as Buffer);
    });

    request.on('end', (): void => {
      resolve(Buffer.concat(chunk_list).toString('utf8'));
    });

    request.on('error', (error): void => {
      reject(error);
    });
  });
}

function BuildUnauthenticatedCallRequest(params: {
  request: cluster_call_request_message_i;
}): cluster_call_request_message_i {
  const { request } = params;

  return {
    ...request,
    caller_identity: {
      subject: 'unauthenticated',
      tenant_id: request.caller_identity.tenant_id,
      scopes: [],
      signed_claims: 'unauthenticated_signed_claims'
    }
  };
}

function ApplyTrustedIdentityToCallRequest(params: {
  request: cluster_call_request_message_i;
  trusted_identity: cluster_http2_session_identity_t;
}): cluster_call_request_message_i {
  const { request, trusted_identity } = params;

  return {
    ...request,
    caller_identity: {
      subject: trusted_identity.subject,
      tenant_id: trusted_identity.tenant_id,
      scopes: [...trusted_identity.scopes],
      signed_claims: trusted_identity.signed_claims
    }
  };
}

function BuildUnauthenticatedMutationRequest(params: {
  request: cluster_admin_mutation_request_message_i;
}): cluster_admin_mutation_request_message_i {
  const { request } = params;

  return {
    ...request,
    auth_context: {
      subject: 'unauthenticated',
      tenant_id: request.auth_context.tenant_id,
      environment: request.auth_context.environment,
      capability_claims: [],
      signed_claims: 'unauthenticated_signed_claims'
    }
  };
}

function ApplyTrustedIdentityToMutationRequest(params: {
  request: cluster_admin_mutation_request_message_i;
  trusted_identity: cluster_http2_session_identity_t;
}): cluster_admin_mutation_request_message_i {
  const { request, trusted_identity } = params;

  return {
    ...request,
    auth_context: {
      subject: trusted_identity.subject,
      tenant_id: trusted_identity.tenant_id,
      environment: trusted_identity.environment,
      capability_claims: [...trusted_identity.scopes],
      signed_claims: trusted_identity.signed_claims
    }
  };
}

function BuildCallCancelAckMessage(params: {
  request_id: string;
  cancelled: boolean;
}): cluster_call_cancel_ack_message_i {
  const { request_id, cancelled } = params;

  return {
    protocol_version: 1,
    message_type: 'cluster_call_cancel_ack',
    timestamp_unix_ms: Date.now(),
    request_id,
    cancelled,
    best_effort_only: true
  };
}

export class ClusterHttp2Transport {
  private readonly workerprocedurecall: WorkerProcedureCall;
  private readonly node_id: string;
  private readonly request_path: string;
  private readonly authenticate_request?: cluster_http2_authenticate_request_t;

  private host: string;
  private port: number;

  private http2_server: Http2Server | Http2SecureServer | null = null;
  private next_session_index = 1;

  private readonly session_state_by_session = new Map<
    ServerHttp2Session,
    cluster_http2_session_state_t
  >();

  private readonly in_flight_request_by_id = new Map<
    string,
    cluster_http2_in_flight_request_state_t
  >();

  private readonly transport_event_listener_by_id = new Map<
    number,
    cluster_http2_transport_event_listener_t
  >();

  private next_transport_event_listener_id = 1;
  private recent_transport_event_history: cluster_http2_transport_event_t[] = [];
  private max_transport_event_history_count = 2_000;
  private transport_metrics_base: Omit<
    cluster_http2_transport_metrics_t,
    'active_session_count' | 'active_request_count'
  > = {
    session_connected_total: 0,
    session_authenticated_total: 0,
    session_disconnected_total: 0,
    request_received_total: 0,
    request_completed_total: 0,
    request_cancelled_total: 0,
    request_failed_total: 0,
    auth_failed_total: 0,
    replay_rejected_total: 0,
    request_failed_count_by_reason: {},
    request_failed_count_by_error_code: {},
    max_in_flight_request_count: 0
  };

  private readonly token_validator = new ClusterTransportTokenValidator();
  private readonly replay_protection = new ClusterTransportReplayProtection();

  private tls_config: cluster_http2_transport_tls_config_t = {
    mode: 'disabled',
    request_client_cert: true,
    reject_unauthorized_client_cert: true
  };

  private session_security_config: cluster_http2_transport_session_security_config_t = {
    max_identity_ttl_ms: 300_000
  };

  constructor(params: cluster_http2_transport_constructor_params_t) {
    const {
      workerprocedurecall,
      node_id,
      host = '127.0.0.1',
      port = 0,
      request_path = '/wpc/cluster/protocol',
      authenticate_request,
      security
    } = params;

    if (typeof node_id !== 'string' || node_id.length === 0) {
      throw new Error('node_id must be a non-empty string.');
    }

    if (typeof host !== 'string' || host.length === 0) {
      throw new Error('host must be a non-empty string.');
    }

    if (!Number.isInteger(port) || port < 0 || port > 65535) {
      throw new Error('port must be an integer between 0 and 65535.');
    }

    if (typeof request_path !== 'string' || !request_path.startsWith('/')) {
      throw new Error('request_path must be a path string starting with "/".');
    }

    this.workerprocedurecall = workerprocedurecall;
    this.node_id = node_id;
    this.host = host;
    this.port = port;
    this.request_path = request_path;
    this.authenticate_request = authenticate_request;

    if (security) {
      this.configureSecurity({
        security_config: security
      });
    }
  }

  configureSecurity(params: {
    security_config: cluster_http2_transport_security_config_t;
  }): void {
    const { security_config } = params;

    if (security_config.tls) {
      this.tls_config = {
        mode: security_config.tls.mode ?? this.tls_config.mode,
        key_pem: security_config.tls.key_pem,
        cert_pem: security_config.tls.cert_pem,
        ca_pem_list: security_config.tls.ca_pem_list,
        request_client_cert:
          security_config.tls.request_client_cert ?? this.tls_config.request_client_cert,
        reject_unauthorized_client_cert:
          security_config.tls.reject_unauthorized_client_cert ??
          this.tls_config.reject_unauthorized_client_cert,
        terminated_upstream_assertion_header_name:
          security_config.tls.terminated_upstream_assertion_header_name,
        terminated_upstream_assertion_header_value:
          security_config.tls.terminated_upstream_assertion_header_value
      };
    }

    if (security_config.token_validation) {
      this.token_validator.setTokenValidationConfig({
        token_validation_config: security_config.token_validation
      });
      this.clearSessionIdentityCache();
    }

    if (security_config.replay_protection) {
      this.replay_protection.setReplayProtectionConfig({
        replay_protection_config: security_config.replay_protection
      });
    }

    if (security_config.session) {
      this.session_security_config = {
        max_identity_ttl_ms:
          security_config.session.max_identity_ttl_ms ??
          this.session_security_config.max_identity_ttl_ms
      };
    }
  }

  setTokenValidationConfig(params: {
    token_validation_config: cluster_transport_token_validation_config_t;
  }): void {
    this.token_validator.setTokenValidationConfig({
      token_validation_config: params.token_validation_config
    });
    this.clearSessionIdentityCache();
  }

  setTokenVerificationKeySet(params: {
    key_by_kid: Record<string, import('./ClusterTransportAuth.class').cluster_transport_jwt_key_t>;
  }): void {
    this.token_validator.setTokenVerificationKeySet({
      key_by_kid: params.key_by_kid
    });
    this.clearSessionIdentityCache();
  }

  upsertTokenVerificationKey(params: {
    key: import('./ClusterTransportAuth.class').cluster_transport_jwt_key_t;
  }): void {
    this.token_validator.upsertTokenVerificationKey({
      key: params.key
    });
    this.clearSessionIdentityCache();
  }

  onTransportEvent(params: {
    listener: cluster_http2_transport_event_listener_t;
  }): number {
    const { listener } = params;

    const listener_id = this.next_transport_event_listener_id;
    this.next_transport_event_listener_id += 1;

    this.transport_event_listener_by_id.set(listener_id, listener);
    return listener_id;
  }

  offTransportEvent(params: { listener_id: number }): void {
    const { listener_id } = params;
    this.transport_event_listener_by_id.delete(listener_id);
  }

  getAddress(): cluster_http2_transport_address_t {
    return {
      host: this.host,
      port: this.port,
      request_path: this.request_path,
      tls_mode: this.tls_config.mode
    };
  }

  getSessionSnapshot(): cluster_http2_session_snapshot_t[] {
    return Array.from(this.session_state_by_session.values()).map(
      (session_state): cluster_http2_session_snapshot_t => {
        return {
          session_id: session_state.session_id,
          connected_unix_ms: session_state.connected_unix_ms,
          disconnected_unix_ms: session_state.disconnected_unix_ms,
          remote_address: session_state.remote_address,
          authenticated: typeof session_state.identity !== 'undefined',
          identity_expires_unix_ms: session_state.identity_expires_unix_ms,
          in_flight_request_count: session_state.in_flight_request_id_set.size
        };
      }
    );
  }

  getTransportMetrics(): cluster_http2_transport_metrics_t {
    return {
      ...this.transport_metrics_base,
      request_failed_count_by_reason: {
        ...this.transport_metrics_base.request_failed_count_by_reason
      },
      request_failed_count_by_error_code: {
        ...this.transport_metrics_base.request_failed_count_by_error_code
      },
      active_session_count: this.session_state_by_session.size,
      active_request_count: this.in_flight_request_by_id.size
    };
  }

  getRecentTransportEvents(params?: {
    limit?: number;
    event_name?: cluster_http2_transport_event_name_t;
    request_id?: string;
    session_id?: string;
  }): cluster_http2_transport_event_t[] {
    let event_list = [...this.recent_transport_event_history];

    if (typeof params?.event_name === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.event_name === params.event_name;
      });
    }

    if (typeof params?.request_id === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.request_id === params.request_id;
      });
    }

    if (typeof params?.session_id === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.session_id === params.session_id;
      });
    }

    if (typeof params?.limit === 'number' && params.limit >= 0) {
      event_list = event_list.slice(Math.max(0, event_list.length - params.limit));
    }

    return event_list.map((event): cluster_http2_transport_event_t => {
      return {
        ...event,
        details: event.details ? { ...event.details } : undefined
      };
    });
  }

  async start(
    params: cluster_http2_transport_start_params_t = {}
  ): Promise<cluster_http2_transport_address_t> {
    if (this.http2_server) {
      throw new Error('ClusterHttp2Transport is already started.');
    }

    if (typeof params.host === 'string' && params.host.length > 0) {
      this.host = params.host;
    }

    if (typeof params.port === 'number') {
      if (!Number.isInteger(params.port) || params.port < 0 || params.port > 65535) {
        throw new Error('params.port must be an integer between 0 and 65535.');
      }
      this.port = params.port;
    }

    const server = this.createHttp2Server();
    this.http2_server = server;

    server.on('session', (session): void => {
      const session_id = BuildSessionId({
        next_session_index: this.next_session_index
      });
      this.next_session_index += 1;

      const session_state: cluster_http2_session_state_t = {
        session_id,
        connected_unix_ms: Date.now(),
        remote_address: session.socket.remoteAddress,
        in_flight_request_id_set: new Set<string>()
      };

      this.session_state_by_session.set(session, session_state);

      this.emitTransportEvent({
        event_name: 'session_connected',
        session_id,
        details: {
          remote_address: session_state.remote_address
        }
      });

      session.on('close', (): void => {
        this.handleSessionClosed({ session });
      });
    });

    server.on('request', (request, response): void => {
      void this.handleHttp2Request({ request, response });
    });

    await new Promise<void>((resolve, reject): void => {
      server.once('error', reject);
      server.listen(this.port, this.host, (): void => {
        server.off('error', reject);
        resolve();
      });
    });

    const address = server.address();
    if (!address || typeof address === 'string') {
      throw new Error('Failed to determine bound HTTP/2 server address.');
    }

    this.port = address.port;

    return this.getAddress();
  }

  async stop(): Promise<void> {
    if (!this.http2_server) {
      return;
    }

    const server = this.http2_server;
    this.http2_server = null;

    for (const session of this.session_state_by_session.keys()) {
      try {
        session.goaway(http2_constants.NGHTTP2_NO_ERROR);
      } catch {
        // Ignore session goaway failures.
      }

      try {
        session.close();
      } catch {
        // Ignore session close failures.
      }
    }

    await new Promise<void>((resolve): void => {
      server.close((): void => {
        resolve();
      });
    });

    this.in_flight_request_by_id.clear();
    this.session_state_by_session.clear();
  }

  private createHttp2Server(): Http2Server | Http2SecureServer {
    if (this.tls_config.mode !== 'required') {
      return createServer();
    }

    if (
      typeof this.tls_config.key_pem !== 'string' ||
      typeof this.tls_config.cert_pem !== 'string'
    ) {
      throw new Error(
        'TLS mode "required" needs key_pem and cert_pem to be configured.'
      );
    }

    const secure_server_options: SecureServerOptions = {
      key: this.tls_config.key_pem,
      cert: this.tls_config.cert_pem,
      ca: this.tls_config.ca_pem_list,
      allowHTTP1: false,
      requestCert: this.tls_config.request_client_cert ?? true,
      rejectUnauthorized: this.tls_config.reject_unauthorized_client_cert ?? true
    };

    return createSecureServer(secure_server_options);
  }

  private async handleHttp2Request(params: {
    request: Http2ServerRequest;
    response: Http2ServerResponse;
  }): Promise<void> {
    const { request, response } = params;

    const method = request.method ?? 'GET';
    const path = request.url ?? '/';
    const session = request.stream.session;

    const session_state =
      typeof session === 'undefined'
        ? undefined
        : this.session_state_by_session.get(session as ServerHttp2Session);

    if (method !== 'POST' || path !== this.request_path) {
      response.statusCode = 404;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(
        JSON.stringify({
          error: {
            code: 'NOT_FOUND',
            message: 'Use POST on the configured request path.'
          }
        })
      );
      return;
    }

    let request_message: unknown = {};

    try {
      const body_text = await ReadRequestBody({ request });

      if (body_text.length > 0) {
        request_message = JSON.parse(body_text) as unknown;
      }
    } catch (error) {
      await this.sendCallResponse({
        response,
        call_response: await this.workerprocedurecall.handleClusterCallRequest({
          message: {},
          node_id: this.node_id
        })
      });

      this.emitTransportEvent({
        event_name: 'request_failed',
        session_id: session_state?.session_id,
        details: {
          reason: 'invalid_json_body',
          error_message: error instanceof Error ? error.message : String(error)
        }
      });
      return;
    }

    const parsed_message_result = ParseClusterProtocolMessage({
      message: request_message,
      now_unix_ms: Date.now()
    });

    if (!parsed_message_result.ok) {
      if (IsRecordObject(request_message) && request_message.message_type === 'cluster_admin_mutation_request') {
        await this.sendMutationResponse({
          response,
          mutation_response:
            await this.workerprocedurecall.handleClusterAdminMutationRequest({
              message: request_message,
              node_id: this.node_id
            })
        });
      } else {
        await this.sendCallResponse({
          response,
          call_response: await this.workerprocedurecall.handleClusterCallRequest({
            message: request_message,
            node_id: this.node_id
          })
        });
      }

      this.emitTransportEvent({
        event_name: 'request_failed',
        session_id: session_state?.session_id,
        details: {
          reason: 'protocol_parse_failed',
          error_code: parsed_message_result.error.code
        }
      });
      return;
    }

    if (parsed_message_result.value.message_type === 'cluster_call_cancel') {
      await this.handleCallCancelMessage({
        response,
        cancel_message: parsed_message_result.value,
        session_state
      });
      return;
    }

    if (parsed_message_result.value.message_type === 'cluster_call_request') {
      await this.handleClusterCallRequestMessage({
        request,
        response,
        session_state,
        call_request: parsed_message_result.value
      });
      return;
    }

    if (parsed_message_result.value.message_type === 'cluster_admin_mutation_request') {
      await this.handleClusterAdminMutationRequestMessage({
        request,
        response,
        session_state,
        mutation_request: parsed_message_result.value
      });
      return;
    }

    await this.sendCallResponse({
      response,
      call_response: await this.workerprocedurecall.handleClusterCallRequest({
        message: request_message,
        node_id: this.node_id
      })
    });

    this.emitTransportEvent({
      event_name: 'request_failed',
      session_id: session_state?.session_id,
      details: {
        reason: 'unsupported_message_type_for_transport',
        message_type: parsed_message_result.value.message_type
      }
    });
  }

  private async handleClusterCallRequestMessage(params: {
    request: Http2ServerRequest;
    response: Http2ServerResponse;
    session_state: cluster_http2_session_state_t | undefined;
    call_request: cluster_call_request_message_i;
  }): Promise<void> {
    const { request, response, session_state, call_request } = params;

    this.emitTransportEvent({
      event_name: 'request_received',
      session_id: session_state?.session_id,
      request_id: call_request.request_id,
      details: {
        function_name: call_request.function_name
      }
    });

    const trusted_identity_result = await this.resolveTrustedIdentity({
      session_state,
      headers: request.headers,
      request_kind: 'call',
      fallback_identity: {
        subject: call_request.caller_identity.subject,
        tenant_id: call_request.caller_identity.tenant_id,
        scopes: [...call_request.caller_identity.scopes],
        signed_claims: call_request.caller_identity.signed_claims
      }
    });

    const effective_call_request = trusted_identity_result.ok
      ? ApplyTrustedIdentityToCallRequest({
          request: call_request,
          trusted_identity: trusted_identity_result.identity
        })
      : BuildUnauthenticatedCallRequest({
          request: call_request
        });

    const request_state = this.registerInFlightRequest({
      request_id: effective_call_request.request_id,
      response,
      session_state
    });

    let request_completed = false;
    request.on('close', (): void => {
      if (request_completed) {
        return;
      }

      this.cancelInFlightRequest({
        request_id: effective_call_request.request_id,
        cancellation_reason: 'client_stream_closed'
      });
    });

    try {
      const call_response = await this.workerprocedurecall.handleClusterCallRequest({
        message: effective_call_request,
        node_id: this.node_id,
        gateway_received_unix_ms: Date.now()
      });

      request_completed = true;
      this.completeInFlightRequest({
        request_id: effective_call_request.request_id
      });

      if (request_state.cancelled || response.writableEnded) {
        return;
      }

      await this.sendCallResponse({
        response,
        call_response
      });

      if (trusted_identity_result.ok) {
        this.emitTransportEvent({
          event_name: 'request_completed',
          session_id: session_state?.session_id,
          request_id: effective_call_request.request_id,
          details: {
            function_name: effective_call_request.function_name,
            terminal_message_type: call_response.terminal_message.message_type
          }
        });
      } else {
        this.emitTransportEvent({
          event_name: 'request_failed',
          session_id: session_state?.session_id,
          request_id: effective_call_request.request_id,
          details: {
            reason: 'authentication_failed',
            auth_error_message: trusted_identity_result.message,
            auth_error_details: trusted_identity_result.details,
            terminal_error_code:
              call_response.terminal_message.message_type ===
              'cluster_call_response_error'
                ? call_response.terminal_message.error.code
                : undefined
          }
        });
      }
    } catch (error) {
      request_completed = true;
      this.completeInFlightRequest({
        request_id: effective_call_request.request_id
      });

      this.emitTransportEvent({
        event_name: 'request_failed',
        session_id: session_state?.session_id,
        request_id: effective_call_request.request_id,
        details: {
          reason: 'request_execution_exception',
          error_message: error instanceof Error ? error.message : String(error)
        }
      });

      if (!response.writableEnded) {
        try {
          response.stream.close(http2_constants.NGHTTP2_INTERNAL_ERROR);
        } catch {
          // Ignore stream close failures.
        }
      }
    }
  }

  private async handleClusterAdminMutationRequestMessage(params: {
    request: Http2ServerRequest;
    response: Http2ServerResponse;
    session_state: cluster_http2_session_state_t | undefined;
    mutation_request: cluster_admin_mutation_request_message_i;
  }): Promise<void> {
    const { request, response, session_state, mutation_request } = params;

    this.emitTransportEvent({
      event_name: 'request_received',
      session_id: session_state?.session_id,
      request_id: mutation_request.request_id,
      details: {
        mutation_id: mutation_request.mutation_id,
        mutation_type: mutation_request.mutation_type
      }
    });

    const trusted_identity_result = await this.resolveTrustedIdentity({
      session_state,
      headers: request.headers,
      request_kind: 'mutation',
      fallback_identity: {
        subject: mutation_request.auth_context.subject,
        tenant_id: mutation_request.auth_context.tenant_id,
        scopes: [...mutation_request.auth_context.capability_claims],
        signed_claims: mutation_request.auth_context.signed_claims,
        environment: mutation_request.auth_context.environment
      }
    });

    if (!trusted_identity_result.ok) {
      const auth_failed_response = this.buildMutationAuthFailedResponse({
        request: mutation_request
      });

      await this.sendMutationResponse({
        response,
        mutation_response: auth_failed_response
      });

      this.emitTransportEvent({
        event_name: 'request_failed',
        session_id: session_state?.session_id,
        request_id: mutation_request.request_id,
        details: {
          reason: 'authentication_failed',
          auth_error_message: trusted_identity_result.message,
          auth_error_details: trusted_identity_result.details,
          terminal_error_code: 'ADMIN_AUTH_FAILED'
        }
      });
      return;
    }

    let effective_mutation_request = ApplyTrustedIdentityToMutationRequest({
      request: mutation_request,
      trusted_identity: trusted_identity_result.identity
    });

    const replay_config = this.replay_protection.getReplayProtectionConfig();
    const token_id = trusted_identity_result.identity.token_id;
    if (
      replay_config.enabled &&
      replay_config.require_token_id_for_privileged_requests &&
      typeof token_id !== 'string'
    ) {
      const auth_failed_response = this.buildMutationAuthFailedResponse({
        request: mutation_request
      });

      await this.sendMutationResponse({
        response,
        mutation_response: auth_failed_response
      });

      this.emitTransportEvent({
        event_name: 'request_failed',
        session_id: session_state?.session_id,
        request_id: mutation_request.request_id,
        details: {
          reason: 'replay_protection_requires_token_id',
          terminal_error_code: 'ADMIN_AUTH_FAILED'
        }
      });
      return;
    } else if (replay_config.enabled) {
      const replay_key = `${trusted_identity_result.identity.subject}:${token_id ?? 'no_jti'}`;
      const replay_result = this.replay_protection.checkAndRecordReplay({
        replay_key
      });

      if (!replay_result.ok) {
        const auth_failed_response = this.buildMutationAuthFailedResponse({
          request: mutation_request
        });

        await this.sendMutationResponse({
          response,
          mutation_response: auth_failed_response
        });

        this.emitTransportEvent({
          event_name: 'request_failed',
          session_id: session_state?.session_id,
          request_id: mutation_request.request_id,
          details: {
            reason: 'replay_detected',
            terminal_error_code: 'ADMIN_AUTH_FAILED'
          }
        });
        return;
      }
    }

    const mutation_response = await this.workerprocedurecall.handleClusterAdminMutationRequest({
      message: effective_mutation_request,
      node_id: this.node_id
    });

    await this.sendMutationResponse({
      response,
      mutation_response
    });

    if (
      mutation_response.terminal_message.message_type === 'cluster_admin_mutation_result' &&
      mutation_response.terminal_message.status === 'partially_failed'
    ) {
      this.emitTransportEvent({
        event_name: 'request_failed',
        session_id: session_state?.session_id,
        request_id: mutation_request.request_id,
        details: {
          reason: 'mutation_partially_failed',
          mutation_id: mutation_request.mutation_id
        }
      });
      return;
    }

    if (mutation_response.terminal_message.message_type === 'cluster_admin_mutation_error') {
      this.emitTransportEvent({
        event_name: 'request_failed',
        session_id: session_state?.session_id,
        request_id: mutation_request.request_id,
        details: {
          reason: 'mutation_error',
          mutation_id: mutation_request.mutation_id,
          error_code: mutation_response.terminal_message.error.code
        }
      });
      return;
    }

    this.emitTransportEvent({
      event_name: 'request_completed',
      session_id: session_state?.session_id,
      request_id: mutation_request.request_id,
      details: {
        mutation_id: mutation_request.mutation_id,
        status: mutation_response.terminal_message.status
      }
    });
  }

  private async resolveTrustedIdentity(params: {
    session_state: cluster_http2_session_state_t | undefined;
    headers: IncomingHttpHeaders;
    request_kind: 'call' | 'mutation';
    fallback_identity: cluster_http2_session_identity_t;
  }): Promise<cluster_http2_authenticate_request_result_t> {
    const { session_state, headers, fallback_identity } = params;

    const now_unix_ms = Date.now();

    if (
      session_state?.identity &&
      typeof session_state.identity_expires_unix_ms === 'number' &&
      session_state.identity_expires_unix_ms > now_unix_ms
    ) {
      return {
        ok: true,
        identity: session_state.identity
      };
    }

    const upstream_tls_result = this.validateUpstreamTlsAssertion({
      headers
    });

    if (!upstream_tls_result.ok) {
      return upstream_tls_result;
    }

    let identity_result: cluster_http2_authenticate_request_result_t;

    const token_validation_config = this.token_validator.getTokenValidationConfig();
    if (token_validation_config.enabled) {
      const token_result = this.token_validator.validateRequestHeaders({
        headers,
        now_unix_ms
      });

      if (!token_result.ok) {
        return {
          ok: false,
          message: 'Authentication failed.',
          details: {
            reason: token_result.details.reason
          }
        };
      }

      identity_result = {
        ok: true,
        identity: token_result.identity
      };
    } else if (this.authenticate_request) {
      identity_result = await this.authenticate_request({
        session_id: session_state?.session_id ?? 'unknown_session',
        headers,
        existing_identity: session_state?.identity
      });
    } else {
      identity_result = {
        ok: true,
        identity: fallback_identity
      };
    }

    if (!identity_result.ok) {
      return identity_result;
    }

    if (token_validation_config.enabled && this.authenticate_request) {
      const hook_result = await this.authenticate_request({
        session_id: session_state?.session_id ?? 'unknown_session',
        headers,
        existing_identity: identity_result.identity
      });

      if (!hook_result.ok) {
        return {
          ok: false,
          message: 'Authentication failed.',
          details: {
            reason: 'authenticate_hook_denied'
          }
        };
      }
    }

    if (session_state) {
      const identity_expires_unix_ms = Math.min(
        identity_result.identity.token_expires_unix_ms ??
          now_unix_ms + this.session_security_config.max_identity_ttl_ms,
        now_unix_ms + this.session_security_config.max_identity_ttl_ms
      );

      session_state.identity = identity_result.identity;
      session_state.identity_expires_unix_ms = identity_expires_unix_ms;

      this.emitTransportEvent({
        event_name: 'session_authenticated',
        session_id: session_state.session_id,
        details: {
          subject: identity_result.identity.subject,
          tenant_id: identity_result.identity.tenant_id,
          identity_expires_unix_ms
        }
      });
    }

    return identity_result;
  }

  private validateUpstreamTlsAssertion(params: {
    headers: IncomingHttpHeaders;
  }): cluster_http2_authenticate_request_result_t {
    const { headers } = params;

    if (this.tls_config.mode !== 'terminated_upstream') {
      return {
        ok: true,
        identity: {
          subject: 'upstream_tls_not_required',
          tenant_id: 'upstream_tls_not_required',
          scopes: [],
          signed_claims: ''
        }
      };
    }

    const assertion_header_name = this.tls_config.terminated_upstream_assertion_header_name;
    const assertion_header_value = this.tls_config.terminated_upstream_assertion_header_value;

    if (
      typeof assertion_header_name !== 'string' ||
      typeof assertion_header_value !== 'string'
    ) {
      return {
        ok: false,
        message: 'Upstream TLS assertion is not configured.',
        details: {
          reason: 'upstream_tls_assertion_not_configured'
        }
      };
    }

    const header_value = headers[assertion_header_name.toLowerCase()];

    if (
      (typeof header_value === 'string' && header_value === assertion_header_value) ||
      (Array.isArray(header_value) && header_value.includes(assertion_header_value))
    ) {
      return {
        ok: true,
        identity: {
          subject: 'upstream_tls_verified',
          tenant_id: 'upstream_tls_verified',
          scopes: [],
          signed_claims: ''
        }
      };
    }

    return {
      ok: false,
      message: 'Upstream TLS assertion failed.',
      details: {
        reason: 'upstream_tls_assertion_failed'
      }
    };
  }

  private registerInFlightRequest(params: {
    request_id: string;
    response: Http2ServerResponse;
    session_state: cluster_http2_session_state_t | undefined;
  }): cluster_http2_in_flight_request_state_t {
    const { request_id, response, session_state } = params;

    const request_state: cluster_http2_in_flight_request_state_t = {
      request_id,
      session_id: session_state?.session_id ?? 'unknown_session',
      started_unix_ms: Date.now(),
      cancelled: false,
      response
    };

    this.in_flight_request_by_id.set(request_id, request_state);
    session_state?.in_flight_request_id_set.add(request_id);

    this.transport_metrics_base.max_in_flight_request_count = Math.max(
      this.transport_metrics_base.max_in_flight_request_count,
      this.in_flight_request_by_id.size
    );

    return request_state;
  }

  private completeInFlightRequest(params: { request_id: string }): void {
    const { request_id } = params;

    const request_state = this.in_flight_request_by_id.get(request_id);
    if (!request_state) {
      return;
    }

    this.in_flight_request_by_id.delete(request_id);

    for (const session_state of this.session_state_by_session.values()) {
      session_state.in_flight_request_id_set.delete(request_id);
    }
  }

  private cancelInFlightRequest(params: {
    request_id: string;
    cancellation_reason:
      | 'client_cancel_message'
      | 'client_stream_closed'
      | 'session_closed';
  }): void {
    const { request_id, cancellation_reason } = params;

    const request_state = this.in_flight_request_by_id.get(request_id);
    if (!request_state) {
      return;
    }

    request_state.cancelled = true;
    this.in_flight_request_by_id.delete(request_id);

    for (const session_state of this.session_state_by_session.values()) {
      session_state.in_flight_request_id_set.delete(request_id);
    }

    try {
      request_state.response.stream.close(http2_constants.NGHTTP2_CANCEL);
    } catch {
      // Ignore stream close failures.
    }

    this.emitTransportEvent({
      event_name: 'request_cancelled',
      session_id: request_state.session_id,
      request_id,
      details: {
        cancellation_reason
      }
    });
  }

  private async handleCallCancelMessage(params: {
    response: Http2ServerResponse;
    cancel_message: cluster_call_cancel_message_i;
    session_state: cluster_http2_session_state_t | undefined;
  }): Promise<void> {
    const { response, cancel_message, session_state } = params;

    const request_state = this.in_flight_request_by_id.get(cancel_message.request_id);

    const is_same_session =
      typeof request_state !== 'undefined' &&
      request_state.session_id === (session_state?.session_id ?? 'unknown_session');

    if (is_same_session) {
      this.cancelInFlightRequest({
        request_id: cancel_message.request_id,
        cancellation_reason: 'client_cancel_message'
      });
    }

    const cancel_ack = BuildCallCancelAckMessage({
      request_id: cancel_message.request_id,
      cancelled: is_same_session
    });

    response.statusCode = 200;
    response.setHeader('content-type', 'application/json; charset=utf-8');
    response.end(
      JSON.stringify({
        cancel_ack
      })
    );
  }

  private async sendCallResponse(params: {
    response: Http2ServerResponse;
    call_response: handle_cluster_call_response_t;
  }): Promise<void> {
    const { response, call_response } = params;

    if (response.writableEnded) {
      return;
    }

    response.statusCode = 200;
    response.setHeader('content-type', 'application/json; charset=utf-8');
    response.end(
      JSON.stringify({
        ack: call_response.ack,
        terminal_message: call_response.terminal_message
      })
    );
  }

  private async sendMutationResponse(params: {
    response: Http2ServerResponse;
    mutation_response: handle_cluster_admin_mutation_response_t;
  }): Promise<void> {
    const { response, mutation_response } = params;

    if (response.writableEnded) {
      return;
    }

    response.statusCode = 200;
    response.setHeader('content-type', 'application/json; charset=utf-8');
    response.end(
      JSON.stringify({
        ack: mutation_response.ack,
        terminal_message: mutation_response.terminal_message
      })
    );
  }

  private handleSessionClosed(params: { session: ServerHttp2Session }): void {
    const { session } = params;

    const session_state = this.session_state_by_session.get(session);
    if (!session_state) {
      return;
    }

    session_state.disconnected_unix_ms = Date.now();

    for (const request_id of session_state.in_flight_request_id_set) {
      this.cancelInFlightRequest({
        request_id,
        cancellation_reason: 'session_closed'
      });
    }

    this.session_state_by_session.delete(session);

    this.emitTransportEvent({
      event_name: 'session_disconnected',
      session_id: session_state.session_id,
      details: {
        remote_address: session_state.remote_address
      }
    });
  }

  private emitTransportEvent(params: {
    event_name: cluster_http2_transport_event_name_t;
    session_id?: string;
    request_id?: string;
    details?: Record<string, unknown>;
  }): void {
    const event: cluster_http2_transport_event_t = {
      event_name: params.event_name,
      timestamp_unix_ms: Date.now(),
      session_id: params.session_id,
      request_id: params.request_id,
      details: params.details
    };

    this.recent_transport_event_history.push({
      ...event,
      details: event.details ? { ...event.details } : undefined
    });
    if (
      this.recent_transport_event_history.length >
      this.max_transport_event_history_count
    ) {
      this.recent_transport_event_history.splice(
        0,
        this.recent_transport_event_history.length -
          this.max_transport_event_history_count
      );
    }

    if (event.event_name === 'session_connected') {
      this.transport_metrics_base.session_connected_total += 1;
    } else if (event.event_name === 'session_authenticated') {
      this.transport_metrics_base.session_authenticated_total += 1;
    } else if (event.event_name === 'session_disconnected') {
      this.transport_metrics_base.session_disconnected_total += 1;
    } else if (event.event_name === 'request_received') {
      this.transport_metrics_base.request_received_total += 1;
    } else if (event.event_name === 'request_completed') {
      this.transport_metrics_base.request_completed_total += 1;
    } else if (event.event_name === 'request_cancelled') {
      this.transport_metrics_base.request_cancelled_total += 1;
    } else if (event.event_name === 'request_failed') {
      this.transport_metrics_base.request_failed_total += 1;
      let auth_failure_recorded = false;

      if (
        event.details &&
        typeof event.details.reason === 'string' &&
        event.details.reason.length > 0
      ) {
        const reason = event.details.reason;
        this.transport_metrics_base.request_failed_count_by_reason[reason] =
          (this.transport_metrics_base.request_failed_count_by_reason[reason] ?? 0) + 1;

        if (reason === 'authentication_failed') {
          this.transport_metrics_base.auth_failed_total += 1;
          auth_failure_recorded = true;
        } else if (reason === 'replay_detected') {
          this.transport_metrics_base.replay_rejected_total += 1;
        }
      }

      if (
        event.details &&
        typeof event.details.terminal_error_code === 'string' &&
        event.details.terminal_error_code.length > 0
      ) {
        const terminal_error_code = event.details.terminal_error_code;
        this.transport_metrics_base.request_failed_count_by_error_code[terminal_error_code] =
          (this.transport_metrics_base.request_failed_count_by_error_code[
            terminal_error_code
          ] ?? 0) + 1;

        if (
          !auth_failure_recorded &&
          (terminal_error_code === 'AUTH_FAILED' ||
            terminal_error_code === 'ADMIN_AUTH_FAILED')
        ) {
          this.transport_metrics_base.auth_failed_total += 1;
        }
      }
    }

    for (const listener of this.transport_event_listener_by_id.values()) {
      try {
        listener(event);
      } catch {
        // Ignore listener failures.
      }
    }
  }

  private clearSessionIdentityCache(): void {
    for (const session_state of this.session_state_by_session.values()) {
      session_state.identity = undefined;
      session_state.identity_expires_unix_ms = undefined;
    }
  }

  private buildMutationAuthFailedResponse(params: {
    request: cluster_admin_mutation_request_message_i;
  }): handle_cluster_admin_mutation_response_t {
    const { request } = params;
    const now_unix_ms = Date.now();

    return {
      ack: {
        protocol_version: 1,
        message_type: 'cluster_admin_mutation_ack',
        timestamp_unix_ms: now_unix_ms,
        mutation_id: request.mutation_id,
        request_id: request.request_id,
        accepted: false,
        planner_id: this.node_id,
        target_node_count: 0,
        dry_run: request.dry_run
      },
      terminal_message: {
        protocol_version: 1,
        message_type: 'cluster_admin_mutation_error',
        timestamp_unix_ms: now_unix_ms,
        mutation_id: request.mutation_id,
        request_id: request.request_id,
        error: {
          code: 'ADMIN_AUTH_FAILED',
          message: 'Authentication failed for admin mutation request.',
          retryable: false,
          unknown_outcome: false,
          details: {}
        }
      }
    };
  }
}
