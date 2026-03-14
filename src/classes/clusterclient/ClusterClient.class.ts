import { connect, constants as http2_constants, type ClientHttp2Session } from 'node:http2';

import {
  ParseClusterAdminMutationErrorMessage,
  ParseClusterAdminMutationResultMessage,
  ParseClusterCallAckMessage,
  ParseClusterCallResponseErrorMessage,
  ParseClusterCallResponseSuccessMessage
} from '../clusterprotocol/ClusterProtocolValidators';
import {
  BuildHttpsAuthority,
  BuildTlsClientConnectOptions,
  ValidateTlsClientConfig,
  type cluster_tls_client_config_t
} from '../clustertransport/ClusterTlsSecurity.class';
import type {
  cluster_admin_mutation_error_message_i,
  cluster_admin_mutation_request_message_i,
  cluster_admin_mutation_result_message_i,
  cluster_call_ack_message_i,
  cluster_call_request_message_i,
  cluster_call_response_error_message_i,
  cluster_call_response_success_message_i,
  cluster_protocol_error_code_t
} from '../clusterprotocol/ClusterProtocolTypes';

export type cluster_client_auth_context_t = {
  subject: string;
  tenant_id: string;
  scopes: string[];
  signed_claims: string;
};

export type cluster_client_auth_headers_provider_t = () =>
  | Record<string, string>
  | Promise<Record<string, string>>;

export type cluster_client_retry_policy_t = {
  max_attempts: number;
  base_delay_ms: number;
  max_delay_ms: number;
};

export type cluster_client_reconnect_policy_t = {
  enabled: boolean;
  max_attempts: number;
  backoff_ms: number;
};

export type cluster_client_constructor_params_t = {
  host: string;
  port: number;
  request_path?: string;
  protocol_version?: 1;
  auth_context: cluster_client_auth_context_t;
  auth_headers?: Record<string, string>;
  auth_headers_provider?: cluster_client_auth_headers_provider_t;
  default_call_timeout_ms?: number;
  retry_policy?: Partial<cluster_client_retry_policy_t>;
  reconnect_policy?: Partial<cluster_client_reconnect_policy_t>;
  transport_security?: (cluster_tls_client_config_t & {
    use_tls?: boolean;
  });
};

export type cluster_client_error_code_t =
  | cluster_protocol_error_code_t
  | 'INGRESS_VALIDATION_FAILED'
  | 'INGRESS_AUTH_FAILED'
  | 'INGRESS_FORBIDDEN'
  | 'INGRESS_NO_TARGET'
  | 'INGRESS_TARGET_UNAVAILABLE'
  | 'INGRESS_DISPATCH_RETRYABLE'
  | 'INGRESS_TIMEOUT'
  | 'INGRESS_INTERNAL'
  | 'CLIENT_TIMEOUT'
  | 'CLIENT_DISCONNECTED'
  | 'CLIENT_PROTOCOL_RESPONSE_INVALID'
  | 'CLIENT_TRANSPORT_ERROR'
  | 'ADMIN_MUTATION_UNAVAILABLE';

export type cluster_client_error_t = {
  code: cluster_client_error_code_t;
  message: string;
  retryable: boolean;
  unknown_outcome: boolean;
  details: Record<string, unknown>;
};

export type cluster_client_call_params_t<args_t extends unknown[] = unknown[]> = {
  function_name: string;
  args?: args_t;
  function_hash_sha1?: string;
  timeout_ms?: number;
  deadline_unix_ms?: number;
  routing_hint?: {
    mode: 'auto' | 'target_node' | 'affinity';
    target_node_id?: string;
    affinity_key?: string;
    zone?: string;
  };
  metadata?: Record<string, unknown>;
};

export type cluster_client_call_success_t<return_t = unknown> = {
  ack: cluster_call_ack_message_i;
  terminal_message: cluster_call_response_success_message_i;
  return_value: return_t;
};

export type cluster_client_admin_mutate_params_t = {
  mutation_type: cluster_admin_mutation_request_message_i['mutation_type'];
  payload: Record<string, unknown>;
  mutation_id: string;
  request_id?: string;
  trace_id?: string;
  timeout_ms?: number;
  deadline_unix_ms?: number;
  target_scope?: cluster_admin_mutation_request_message_i['target_scope'];
  target_selector?: cluster_admin_mutation_request_message_i['target_selector'];
  dry_run?: boolean;
  rollout_strategy?: cluster_admin_mutation_request_message_i['rollout_strategy'];
  expected_version?: cluster_admin_mutation_request_message_i['expected_version'];
  in_flight_policy?: cluster_admin_mutation_request_message_i['in_flight_policy'];
};

type cluster_client_call_wire_response_t = {
  ack: unknown;
  terminal_message: unknown;
};

type cluster_client_admin_mutation_wire_response_t = {
  ack?: unknown;
  terminal_message?: unknown;
};

type cluster_client_in_flight_call_state_t = {
  request_id: string;
  reject: (error: Error) => void;
  stream_close_func: () => void;
};

function BuildRequestId(params: { prefix: string; next_request_index: number }): string {
  const { prefix, next_request_index } = params;
  return `${prefix}_${Date.now()}_${next_request_index}`;
}

function BuildClusterClientError(params: {
  code: cluster_client_error_code_t;
  message: string;
  retryable: boolean;
  unknown_outcome: boolean;
  details?: Record<string, unknown>;
}): cluster_client_error_t {
  return {
    code: params.code,
    message: params.message,
    retryable: params.retryable,
    unknown_outcome: params.unknown_outcome,
    details: params.details ? { ...params.details } : {}
  };
}

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function ToError(params: { error: unknown }): Error {
  const { error } = params;
  if (error instanceof Error) {
    return error;
  }

  return new Error(String(error));
}

function ToClusterClientError(params: { error: unknown }): cluster_client_error_t {
  const { error } = params;

  if (error instanceof ClusterClientError) {
    return {
      code: error.code,
      message: error.message,
      retryable: error.retryable,
      unknown_outcome: error.unknown_outcome,
      details: { ...error.details }
    };
  }

  const normalized_error = ToError({ error });

  return BuildClusterClientError({
    code: 'CLIENT_TRANSPORT_ERROR',
    message: normalized_error.message,
    retryable: true,
    unknown_outcome: true,
    details: {
      reason: 'transport_or_runtime_error'
    }
  });
}

function BuildRetryDelayMs(params: {
  attempt_index: number;
  base_delay_ms: number;
  max_delay_ms: number;
}): number {
  const { attempt_index, base_delay_ms, max_delay_ms } = params;
  const computed_delay_ms = base_delay_ms * 2 ** Math.max(0, attempt_index - 1);
  return Math.min(max_delay_ms, computed_delay_ms);
}

function Sleep(params: { delay_ms: number }): Promise<void> {
  const { delay_ms } = params;

  return new Promise<void>((resolve): void => {
    setTimeout(resolve, delay_ms);
  });
}

function ComputeDeadlineUnixMs(params: {
  call_timeout_ms: number;
  explicit_deadline_unix_ms?: number;
}): number {
  const { call_timeout_ms, explicit_deadline_unix_ms } = params;

  if (typeof explicit_deadline_unix_ms === 'number') {
    return explicit_deadline_unix_ms;
  }

  return Date.now() + call_timeout_ms;
}

function BuildAttemptTimeoutMs(params: {
  deadline_unix_ms: number;
  now_unix_ms: number;
}): number {
  const { deadline_unix_ms, now_unix_ms } = params;
  return Math.max(1, deadline_unix_ms - now_unix_ms);
}

function ShouldRetryForError(params: {
  error: cluster_client_error_t;
  attempt_index: number;
  max_attempts: number;
  deadline_unix_ms: number;
  retry_delay_ms: number;
}): boolean {
  const { error, attempt_index, max_attempts, deadline_unix_ms, retry_delay_ms } = params;

  if (!error.retryable) {
    return false;
  }

  if (attempt_index >= max_attempts) {
    return false;
  }

  if (Date.now() + retry_delay_ms >= deadline_unix_ms) {
    return false;
  }

  return true;
}

export class ClusterClientError extends Error {
  public readonly code: cluster_client_error_code_t;
  public readonly retryable: boolean;
  public readonly unknown_outcome: boolean;
  public readonly details: Record<string, unknown>;

  constructor(params: cluster_client_error_t) {
    super(params.message);
    this.name = 'ClusterClientError';
    this.code = params.code;
    this.retryable = params.retryable;
    this.unknown_outcome = params.unknown_outcome;
    this.details = { ...params.details };
  }
}

export class ClusterClient {
  private readonly host: string;
  private readonly port: number;
  private readonly request_path: string;
  private readonly protocol_version: 1;
  private readonly auth_context: cluster_client_auth_context_t;

  private readonly static_auth_headers: Record<string, string>;
  private readonly auth_headers_provider?: cluster_client_auth_headers_provider_t;

  private readonly default_call_timeout_ms: number;
  private readonly retry_policy: cluster_client_retry_policy_t;
  private readonly reconnect_policy: cluster_client_reconnect_policy_t;
  private readonly transport_security: cluster_tls_client_config_t;

  private client_http2_session: ClientHttp2Session | null = null;
  private connect_in_progress_promise: Promise<void> | null = null;
  private next_request_index = 1;
  private manually_closed = false;

  private readonly in_flight_call_state_by_request_id = new Map<
    string,
    cluster_client_in_flight_call_state_t
  >();

  constructor(params: cluster_client_constructor_params_t) {
    const {
      host,
      port,
      request_path = '/wpc/cluster/protocol',
      protocol_version = 1,
      auth_context,
      auth_headers,
      auth_headers_provider,
      default_call_timeout_ms = 10_000,
      retry_policy,
      reconnect_policy,
      transport_security
    } = params;

    if (typeof host !== 'string' || host.length === 0) {
      throw new Error('host must be a non-empty string.');
    }

    if (!Number.isInteger(port) || port < 0 || port > 65535) {
      throw new Error('port must be an integer between 0 and 65535.');
    }

    if (typeof request_path !== 'string' || !request_path.startsWith('/')) {
      throw new Error('request_path must start with "/".');
    }

    if (!Number.isInteger(default_call_timeout_ms) || default_call_timeout_ms <= 0) {
      throw new Error('default_call_timeout_ms must be a positive integer.');
    }

    this.host = host;
    this.port = port;
    this.request_path = request_path;
    this.protocol_version = protocol_version;
    this.auth_context = {
      subject: auth_context.subject,
      tenant_id: auth_context.tenant_id,
      scopes: [...auth_context.scopes],
      signed_claims: auth_context.signed_claims
    };
    this.static_auth_headers = auth_headers ? { ...auth_headers } : {};
    this.auth_headers_provider = auth_headers_provider;
    this.default_call_timeout_ms = default_call_timeout_ms;
    this.retry_policy = {
      max_attempts: retry_policy?.max_attempts ?? 3,
      base_delay_ms: retry_policy?.base_delay_ms ?? 25,
      max_delay_ms: retry_policy?.max_delay_ms ?? 500
    };
    this.reconnect_policy = {
      enabled: reconnect_policy?.enabled ?? true,
      max_attempts: reconnect_policy?.max_attempts ?? 3,
      backoff_ms: reconnect_policy?.backoff_ms ?? 100
    };

    if (typeof transport_security?.use_tls === 'boolean' && !transport_security.use_tls) {
      throw new Error(
        'Insecure transport is disabled. ClusterClient transport_security.use_tls must be true when provided.'
      );
    }

    this.transport_security = ValidateTlsClientConfig({
      tls_client_config: transport_security,
      field_prefix: 'transport_security'
    });
  }

  async connect(): Promise<void> {
    await this.ensureConnected({
      allow_reconnect: true
    });
  }

  async close(): Promise<void> {
    this.manually_closed = true;

    if (this.connect_in_progress_promise) {
      try {
        await this.connect_in_progress_promise;
      } catch {
        // Ignore connect failures on close.
      }
    }

    const session = this.client_http2_session;
    this.client_http2_session = null;

    if (session) {
      try {
        session.goaway(http2_constants.NGHTTP2_NO_ERROR);
      } catch {
        // Ignore goaway failures.
      }

      try {
        session.close();
      } catch {
        // Ignore close failures.
      }
    }

    this.rejectAllInFlightCalls({
      error: new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_DISCONNECTED',
          message: 'ClusterClient was closed while calls were in flight.',
          retryable: false,
          unknown_outcome: true,
          details: {
            reason: 'client_close'
          }
        })
      )
    });
  }

  async call<return_t = unknown, args_t extends unknown[] = unknown[]>(
    params: cluster_client_call_params_t<args_t>
  ): Promise<return_t> {
    const { function_name } = params;

    if (typeof function_name !== 'string' || function_name.length === 0) {
      throw new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_PROTOCOL_RESPONSE_INVALID',
          message: 'function_name must be a non-empty string.',
          retryable: false,
          unknown_outcome: false,
          details: {
            field_name: 'function_name'
          }
        })
      );
    }

    const max_attempts = this.retry_policy.max_attempts;
    const deadline_unix_ms = ComputeDeadlineUnixMs({
      call_timeout_ms: params.timeout_ms ?? this.default_call_timeout_ms,
      explicit_deadline_unix_ms: params.deadline_unix_ms
    });

    const request_id = BuildRequestId({
      prefix: 'cluster_call',
      next_request_index: this.next_request_index
    });
    this.next_request_index += 1;

    let last_error: cluster_client_error_t | null = null;

    for (let attempt_index = 1; attempt_index <= max_attempts; attempt_index += 1) {
      const now_unix_ms = Date.now();
      if (now_unix_ms >= deadline_unix_ms) {
        throw new ClusterClientError(
          BuildClusterClientError({
            code: 'CLIENT_TIMEOUT',
            message: `Call "${function_name}" exceeded deadline before dispatch.`,
            retryable: false,
            unknown_outcome: true,
            details: {
              request_id,
              attempt_index
            }
          })
        );
      }

      const attempt_timeout_ms = BuildAttemptTimeoutMs({
        deadline_unix_ms,
        now_unix_ms
      });

      try {
        const call_response = await this.executeCallAttempt({
          request_id,
          function_name,
          args: params.args ?? [],
          function_hash_sha1: params.function_hash_sha1,
          attempt_index,
          max_attempts,
          deadline_unix_ms,
          request_timeout_ms: attempt_timeout_ms,
          routing_hint: params.routing_hint,
          metadata: params.metadata
        });

        return call_response.return_value as return_t;
      } catch (error) {
        const cluster_client_error = ToClusterClientError({ error });
        last_error = cluster_client_error;

        const retry_delay_ms = BuildRetryDelayMs({
          attempt_index,
          base_delay_ms: this.retry_policy.base_delay_ms,
          max_delay_ms: this.retry_policy.max_delay_ms
        });

        if (
          !ShouldRetryForError({
            error: cluster_client_error,
            attempt_index,
            max_attempts,
            deadline_unix_ms,
            retry_delay_ms
          })
        ) {
          throw new ClusterClientError(cluster_client_error);
        }

        await Sleep({ delay_ms: retry_delay_ms });
      }
    }

    throw new ClusterClientError(
      last_error ??
        BuildClusterClientError({
          code: 'CLIENT_TRANSPORT_ERROR',
          message: 'Call failed with an unknown retry state.',
          retryable: false,
          unknown_outcome: true,
          details: {
            request_id
          }
        })
    );
  }

  async adminMutate(
    params: cluster_client_admin_mutate_params_t
  ): Promise<cluster_admin_mutation_result_message_i> {
    const deadline_unix_ms = ComputeDeadlineUnixMs({
      call_timeout_ms: params.timeout_ms ?? this.default_call_timeout_ms,
      explicit_deadline_unix_ms: params.deadline_unix_ms
    });

    const request_id = params.request_id ?? BuildRequestId({
      prefix: 'cluster_admin_mutation',
      next_request_index: this.next_request_index
    });
    this.next_request_index += 1;

    const mutation_request_message: cluster_admin_mutation_request_message_i = {
      protocol_version: this.protocol_version,
      message_type: 'cluster_admin_mutation_request',
      timestamp_unix_ms: Date.now(),
      mutation_id: params.mutation_id,
      request_id,
      trace_id: params.trace_id ?? request_id,
      deadline_unix_ms,
      target_scope: params.target_scope ?? 'single_node',
      target_selector: params.target_selector,
      mutation_type: params.mutation_type,
      payload: params.payload,
      expected_version: params.expected_version,
      dry_run: params.dry_run ?? false,
      rollout_strategy: params.rollout_strategy ?? {
        mode: 'single_node'
      },
      in_flight_policy: params.in_flight_policy,
      auth_context: {
        subject: this.auth_context.subject,
        tenant_id: this.auth_context.tenant_id,
        capability_claims: [...this.auth_context.scopes],
        signed_claims: this.auth_context.signed_claims
      }
    };

    const raw_response = await this.sendRawProtocolRequest({
      request_id,
      request_timeout_ms: BuildAttemptTimeoutMs({
        deadline_unix_ms,
        now_unix_ms: Date.now()
      }),
      message: mutation_request_message
    });

    const parsed_response = this.parseAdminMutationWireResponse({
      response_record: raw_response
    });

    if (parsed_response.terminal_message.message_type === 'cluster_admin_mutation_result') {
      return parsed_response.terminal_message;
    }

    throw new ClusterClientError(
      BuildClusterClientError({
        code: parsed_response.terminal_message.error.code,
        message: parsed_response.terminal_message.error.message,
        retryable: parsed_response.terminal_message.error.retryable,
        unknown_outcome: parsed_response.terminal_message.error.unknown_outcome,
        details: parsed_response.terminal_message.error.details ?? {}
      })
    );
  }

  private async executeCallAttempt(params: {
    request_id: string;
    function_name: string;
    args: unknown[];
    function_hash_sha1?: string;
    attempt_index: number;
    max_attempts: number;
    deadline_unix_ms: number;
    request_timeout_ms: number;
    routing_hint?: cluster_client_call_params_t['routing_hint'];
    metadata?: Record<string, unknown>;
  }): Promise<cluster_client_call_success_t<unknown>> {
    const {
      request_id,
      function_name,
      args,
      function_hash_sha1,
      attempt_index,
      max_attempts,
      deadline_unix_ms,
      request_timeout_ms,
      routing_hint,
      metadata
    } = params;

    await this.ensureConnected({
      allow_reconnect: true
    });

    const message: cluster_call_request_message_i = {
      protocol_version: this.protocol_version,
      message_type: 'cluster_call_request',
      timestamp_unix_ms: Date.now(),
      request_id,
      trace_id: request_id,
      span_id: `${request_id}_attempt_${attempt_index}`,
      attempt_index,
      max_attempts,
      deadline_unix_ms,
      function_name,
      function_hash_sha1,
      args,
      routing_hint: routing_hint ?? {
        mode: 'auto'
      },
      caller_identity: {
        subject: this.auth_context.subject,
        tenant_id: this.auth_context.tenant_id,
        scopes: [...this.auth_context.scopes],
        signed_claims: this.auth_context.signed_claims
      },
      metadata
    };

    const raw_response = await this.sendRawProtocolRequest({
      request_id,
      request_timeout_ms,
      message
    });

    const parsed_response = this.parseCallWireResponse({
      expected_request_id: request_id,
      response_record: raw_response
    });

    if (parsed_response.terminal_message.message_type === 'cluster_call_response_error') {
      throw new ClusterClientError(
        BuildClusterClientError({
          code: parsed_response.terminal_message.error.code as cluster_client_error_code_t,
          message: parsed_response.terminal_message.error.message,
          retryable: parsed_response.terminal_message.error.retryable,
          unknown_outcome: parsed_response.terminal_message.error.unknown_outcome,
          details: parsed_response.terminal_message.error.details ?? {}
        })
      );
    }

    return {
      ack: parsed_response.ack,
      terminal_message: parsed_response.terminal_message,
      return_value: parsed_response.terminal_message.return_value
    };
  }

  private async sendRawProtocolRequest(params: {
    request_id: string;
    request_timeout_ms: number;
    message: unknown;
  }): Promise<Record<string, unknown>> {
    const { request_id, request_timeout_ms, message } = params;

    const session = this.client_http2_session;
    if (!session || session.closed || session.destroyed) {
      throw new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_DISCONNECTED',
          message: 'HTTP/2 session is not connected.',
          retryable: true,
          unknown_outcome: true,
          details: {
            request_id
          }
        })
      );
    }

    const auth_headers = await this.getAuthHeaders();

    return await new Promise<Record<string, unknown>>((resolve, reject): void => {
      const request_headers: Record<string, string> = {
        ':method': 'POST',
        ':path': this.request_path,
        'content-type': 'application/json'
      };

      for (const [header_name, header_value] of Object.entries(auth_headers)) {
        request_headers[header_name] = header_value;
      }

      const request_stream = session.request(request_headers);
      const response_chunk_list: Buffer[] = [];

      const timeout_handle = setTimeout((): void => {
        try {
          request_stream.close(http2_constants.NGHTTP2_CANCEL);
        } catch {
          // Ignore stream close errors.
        }

        reject(
          new ClusterClientError(
            BuildClusterClientError({
              code: 'CLIENT_TIMEOUT',
              message: `Request "${request_id}" timed out after ${request_timeout_ms}ms.`,
              retryable: true,
              unknown_outcome: true,
              details: {
                request_id,
                request_timeout_ms
              }
            })
          )
        );
      }, request_timeout_ms);

      const in_flight_state: cluster_client_in_flight_call_state_t = {
        request_id,
        reject,
        stream_close_func: (): void => {
          try {
            request_stream.close(http2_constants.NGHTTP2_CANCEL);
          } catch {
            // Ignore stream close failures.
          }
        }
      };

      this.in_flight_call_state_by_request_id.set(request_id, in_flight_state);

      request_stream.on('response', (): void => {
        // No-op: response headers are not currently used.
      });

      request_stream.on('data', (chunk): void => {
        if (typeof chunk === 'string') {
          response_chunk_list.push(Buffer.from(chunk));
          return;
        }

        response_chunk_list.push(chunk as Buffer);
      });

      request_stream.on('end', (): void => {
        clearTimeout(timeout_handle);
        this.in_flight_call_state_by_request_id.delete(request_id);

        try {
          const response_text = Buffer.concat(response_chunk_list).toString('utf8');
          const response_payload =
            response_text.length > 0
              ? (JSON.parse(response_text) as unknown)
              : ({} as unknown);

          if (!IsRecordObject(response_payload)) {
            reject(
              new ClusterClientError(
                BuildClusterClientError({
                  code: 'CLIENT_PROTOCOL_RESPONSE_INVALID',
                  message: 'Response body is not an object.',
                  retryable: false,
                  unknown_outcome: true,
                  details: {
                    request_id
                  }
                })
              )
            );
            return;
          }

          resolve(response_payload);
        } catch (error) {
          reject(
            new ClusterClientError(
              BuildClusterClientError({
                code: 'CLIENT_PROTOCOL_RESPONSE_INVALID',
                message: `Failed to parse response JSON: ${ToError({ error }).message}`,
                retryable: false,
                unknown_outcome: true,
                details: {
                  request_id
                }
              })
            )
          );
        }
      });

      request_stream.on('error', (error): void => {
        clearTimeout(timeout_handle);
        this.in_flight_call_state_by_request_id.delete(request_id);

        reject(
          new ClusterClientError(
            BuildClusterClientError({
              code: 'CLIENT_TRANSPORT_ERROR',
              message: ToError({ error }).message,
              retryable: true,
              unknown_outcome: true,
              details: {
                request_id
              }
            })
          )
        );
      });

      request_stream.end(JSON.stringify(message));
    });
  }

  private parseCallWireResponse(params: {
    expected_request_id: string;
    response_record: Record<string, unknown>;
  }): {
    ack: cluster_call_ack_message_i;
    terminal_message:
      | cluster_call_response_success_message_i
      | cluster_call_response_error_message_i;
  } {
    const { expected_request_id, response_record } = params;

    const ack_result = ParseClusterCallAckMessage({
      message: response_record.ack
    });

    if (!ack_result.ok) {
      throw new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_PROTOCOL_RESPONSE_INVALID',
          message: `Invalid call ACK payload: ${ack_result.error.message}`,
          retryable: false,
          unknown_outcome: true,
          details: {
            request_id: expected_request_id,
            parse_error_code: ack_result.error.code,
            parse_error_details: ack_result.error.details
          }
        })
      );
    }

    if (ack_result.value.request_id !== expected_request_id) {
      throw new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_PROTOCOL_RESPONSE_INVALID',
          message: 'ACK request_id does not match request.',
          retryable: false,
          unknown_outcome: true,
          details: {
            request_id: expected_request_id,
            received_request_id: ack_result.value.request_id
          }
        })
      );
    }

    const success_result = ParseClusterCallResponseSuccessMessage({
      message: response_record.terminal_message
    });

    if (success_result.ok) {
      if (success_result.value.request_id !== expected_request_id) {
        throw new ClusterClientError(
          BuildClusterClientError({
            code: 'CLIENT_PROTOCOL_RESPONSE_INVALID',
            message: 'Success response request_id does not match request.',
            retryable: false,
            unknown_outcome: true,
            details: {
              request_id: expected_request_id,
              received_request_id: success_result.value.request_id
            }
          })
        );
      }

      return {
        ack: ack_result.value,
        terminal_message: success_result.value
      };
    }

    const error_result = ParseClusterCallResponseErrorMessage({
      message: response_record.terminal_message
    });

    if (!error_result.ok) {
      throw new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_PROTOCOL_RESPONSE_INVALID',
          message: `Invalid terminal call response payload: ${error_result.error.message}`,
          retryable: false,
          unknown_outcome: true,
          details: {
            request_id: expected_request_id,
            parse_error_code: error_result.error.code,
            parse_error_details: error_result.error.details
          }
        })
      );
    }

    if (error_result.value.request_id !== expected_request_id) {
      throw new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_PROTOCOL_RESPONSE_INVALID',
          message: 'Error response request_id does not match request.',
          retryable: false,
          unknown_outcome: true,
          details: {
            request_id: expected_request_id,
            received_request_id: error_result.value.request_id
          }
        })
      );
    }

    return {
      ack: ack_result.value,
      terminal_message: error_result.value
    };
  }

  private parseAdminMutationWireResponse(params: {
    response_record: Record<string, unknown>;
  }): {
    terminal_message:
      | cluster_admin_mutation_result_message_i
      | cluster_admin_mutation_error_message_i;
  } {
    const { response_record } = params;

    const result_message = ParseClusterAdminMutationResultMessage({
      message: response_record.terminal_message
    });

    if (result_message.ok) {
      return {
        terminal_message: result_message.value
      };
    }

    const error_message = ParseClusterAdminMutationErrorMessage({
      message: response_record.terminal_message
    });

    if (error_message.ok) {
      return {
        terminal_message: error_message.value
      };
    }

    throw new ClusterClientError(
      BuildClusterClientError({
        code: 'ADMIN_MUTATION_UNAVAILABLE',
        message:
          'Admin mutation transport is unavailable on the current endpoint. Phase 9 provides only stub compatibility.',
        retryable: false,
        unknown_outcome: false,
        details: {
          reason: 'mutation_plane_not_available_on_current_transport'
        }
      })
    );
  }

  private async ensureConnected(params: { allow_reconnect: boolean }): Promise<void> {
    if (!params.allow_reconnect && !this.client_http2_session) {
      throw new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_DISCONNECTED',
          message: 'ClusterClient is disconnected.',
          retryable: true,
          unknown_outcome: true,
          details: {
            reason: 'ensure_connected_without_reconnect'
          }
        })
      );
    }

    const existing_session = this.client_http2_session;
    if (existing_session && !existing_session.closed && !existing_session.destroyed) {
      return;
    }

    if (this.connect_in_progress_promise) {
      await this.connect_in_progress_promise;
      return;
    }

    const reconnect_attempts = params.allow_reconnect && this.reconnect_policy.enabled
      ? this.reconnect_policy.max_attempts
      : 1;

    this.connect_in_progress_promise = this.connectWithRetry({
      max_attempts: reconnect_attempts
    });

    try {
      await this.connect_in_progress_promise;
    } finally {
      this.connect_in_progress_promise = null;
    }
  }

  private async connectWithRetry(params: { max_attempts: number }): Promise<void> {
    const { max_attempts } = params;

    let last_error: Error | null = null;

    for (let attempt_index = 1; attempt_index <= max_attempts; attempt_index += 1) {
      try {
        await this.openNewSession();
        return;
      } catch (error) {
        last_error = ToError({ error });

        if (attempt_index >= max_attempts) {
          break;
        }

        await Sleep({
          delay_ms: this.reconnect_policy.backoff_ms
        });
      }
    }

    throw new ClusterClientError(
      BuildClusterClientError({
        code: 'CLIENT_TRANSPORT_ERROR',
        message: `Failed to connect to ${this.host}:${this.port}: ${
          last_error ? last_error.message : 'unknown error'
        }`,
        retryable: true,
        unknown_outcome: true,
        details: {
          host: this.host,
          port: this.port
        }
      })
    );
  }

  private async openNewSession(): Promise<void> {
    if (this.manually_closed) {
      throw new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_DISCONNECTED',
          message: 'ClusterClient is closed and cannot reconnect.',
          retryable: false,
          unknown_outcome: true,
          details: {
            reason: 'client_closed'
          }
        })
      );
    }

    const authority = BuildHttpsAuthority({
      host: this.host,
      port: this.port
    });
    const connect_options = BuildTlsClientConnectOptions({
      tls_client_config: this.transport_security
    });

    const session = connect(authority, connect_options);

    await new Promise<void>((resolve, reject): void => {
      const on_error = (error: Error): void => {
        session.off('connect', on_connect);
        reject(error);
      };

      const on_connect = (): void => {
        session.off('error', on_error);
        resolve();
      };

      session.once('error', on_error);
      session.once('connect', on_connect);
    });

    session.on('close', (): void => {
      this.handleSessionClosed({
        reason: 'session_close'
      });
    });

    session.on('goaway', (): void => {
      this.handleSessionClosed({
        reason: 'session_goaway'
      });
    });

    session.on('error', (): void => {
      // Error details are propagated through per-request stream errors.
    });

    this.client_http2_session = session;
  }

  private handleSessionClosed(params: { reason: string }): void {
    const { reason } = params;

    if (this.client_http2_session) {
      this.client_http2_session = null;
    }

    this.rejectAllInFlightCalls({
      error: new ClusterClientError(
        BuildClusterClientError({
          code: 'CLIENT_DISCONNECTED',
          message: 'ClusterClient disconnected while request was in flight.',
          retryable: true,
          unknown_outcome: true,
          details: {
            reason
          }
        })
      )
    });
  }

  private rejectAllInFlightCalls(params: { error: Error }): void {
    const { error } = params;

    for (const in_flight_state of this.in_flight_call_state_by_request_id.values()) {
      try {
        in_flight_state.stream_close_func();
      } catch {
        // Ignore stream close failures.
      }

      try {
        in_flight_state.reject(error);
      } catch {
        // Ignore rejection handler failures.
      }
    }

    this.in_flight_call_state_by_request_id.clear();
  }

  private async getAuthHeaders(): Promise<Record<string, string>> {
    const merged_headers: Record<string, string> = {
      ...this.static_auth_headers
    };

    if (this.auth_headers_provider) {
      const provided_headers = await this.auth_headers_provider();

      for (const [header_name, header_value] of Object.entries(provided_headers)) {
        merged_headers[header_name] = header_value;
      }
    }

    return merged_headers;
  }
}
