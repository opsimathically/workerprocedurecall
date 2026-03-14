import {
  connect,
  type ClientHttp2Session
} from 'node:http2';
import {
  BuildHttpsAuthority,
  BuildTlsClientConnectOptions,
  ValidateTlsClientConfig,
  type cluster_tls_client_config_t
} from '../clustertransport/ClusterTlsSecurity.class';

import type {
  cluster_call_ack_message_i,
  cluster_call_request_message_i,
  cluster_call_response_error_message_i,
  cluster_call_response_success_message_i
} from '../clusterprotocol/ClusterProtocolTypes';
import {
  BuildIngressBalancerError,
  ParseIngressForwardWireResponse
} from './ClusterIngressBalancerProtocolValidators';
import type {
  ingress_balancer_forwarding_attempt_t,
  ingress_balancer_error_code_t,
  ingress_balancer_target_record_t
} from './ClusterIngressBalancerProtocol';

export type cluster_ingress_forwarder_auth_headers_provider_t = () =>
  | Record<string, string>
  | Promise<Record<string, string>>;

export type cluster_ingress_forwarder_constructor_params_t = {
  request_timeout_ms?: number;
  static_request_headers?: Record<string, string>;
  auth_headers_provider?: cluster_ingress_forwarder_auth_headers_provider_t;
  transport_security?: cluster_tls_client_config_t;
};

function BuildTransportAuthority(params: {
  host: string;
  port: number;
  tls_mode: 'required';
}): string {
  if (params.tls_mode !== 'required') {
    throw new Error('Only tls_mode="required" is supported for ingress forwarding.');
  }

  return BuildHttpsAuthority({
    host: params.host,
    port: params.port
  });
}

function NormalizeHeaders(params: {
  headers: Record<string, string> | undefined;
}): Record<string, string> {
  const normalized_headers: Record<string, string> = {};

  if (!params.headers) {
    return normalized_headers;
  }

  for (const [header_name, header_value] of Object.entries(params.headers)) {
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

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export class ClusterIngressForwarder {
  private readonly request_timeout_ms: number;
  private readonly static_request_headers: Record<string, string>;
  private readonly auth_headers_provider:
    | cluster_ingress_forwarder_auth_headers_provider_t
    | undefined;
  private readonly transport_security: cluster_tls_client_config_t;

  constructor(params: cluster_ingress_forwarder_constructor_params_t = {}) {
    this.request_timeout_ms = params.request_timeout_ms ?? 5_000;
    this.static_request_headers = NormalizeHeaders({
      headers: params.static_request_headers
    });
    this.auth_headers_provider = params.auth_headers_provider;
    this.transport_security = ValidateTlsClientConfig({
      tls_client_config: params.transport_security,
      field_prefix: 'cluster_ingress_forwarder.transport_security'
    });
  }

  async dispatchWithFailover(params: {
    request: cluster_call_request_message_i;
    ordered_candidate_list: ingress_balancer_target_record_t[];
    max_attempts: number;
  }): Promise<{
    call_response: {
      ack: cluster_call_ack_message_i;
      terminal_message:
        | cluster_call_response_success_message_i
        | cluster_call_response_error_message_i;
    };
    forwarding_attempt_list: ingress_balancer_forwarding_attempt_t[];
  }> {
    const forwarding_attempt_list: ingress_balancer_forwarding_attempt_t[] = [];
    const max_attempts = Math.max(1, params.max_attempts);
    let last_terminal_error_message: cluster_call_response_error_message_i | null = null;

    for (
      let attempt_index = 1;
      attempt_index <= max_attempts &&
      attempt_index <= params.ordered_candidate_list.length;
      attempt_index += 1
    ) {
      const candidate = params.ordered_candidate_list[attempt_index - 1];
      const started_unix_ms = Date.now();

      try {
        const forwarded_request: cluster_call_request_message_i = {
          ...params.request,
          attempt_index,
          max_attempts
        };

        const response_text = await this.dispatchToCandidate({
          request: forwarded_request,
          candidate
        });
        const parsed_json = JSON.parse(response_text) as unknown;
        const parse_result = ParseIngressForwardWireResponse({
          response: parsed_json
        });

        if (!parse_result.ok) {
          const finished_unix_ms = Date.now();
          forwarding_attempt_list.push({
            request_id: params.request.request_id,
            trace_id: params.request.trace_id,
            attempt_index,
            target_id: candidate.target_id,
            started_unix_ms,
            finished_unix_ms,
            duration_ms: finished_unix_ms - started_unix_ms,
            outcome: parse_result.error.retryable
              ? 'retryable_error'
              : 'non_retryable_error',
            error: parse_result.error
          });

          if (!parse_result.error.retryable || attempt_index >= max_attempts) {
            return {
              call_response: {
                ack: {
                  protocol_version: 1,
                  message_type: 'cluster_call_ack',
                  timestamp_unix_ms: Date.now(),
                  request_id: params.request.request_id,
                  attempt_index,
                  node_id: candidate.node_id,
                  accepted: false,
                  queue_position: 0,
                  estimated_start_delay_ms: 0
                },
                terminal_message: {
                  protocol_version: 1,
                  message_type: 'cluster_call_response_error',
                  timestamp_unix_ms: Date.now(),
                  request_id: params.request.request_id,
                  attempt_index,
                  node_id: candidate.node_id,
                  error: parse_result.error,
                  timing: {
                    gateway_received_unix_ms: params.request.timestamp_unix_ms,
                    last_attempt_started_unix_ms: started_unix_ms
                  }
                }
              },
              forwarding_attempt_list
            };
          }

          continue;
        }

        const finished_unix_ms = Date.now();
        if (parse_result.value.terminal_message.message_type === 'cluster_call_response_success') {
          forwarding_attempt_list.push({
            request_id: params.request.request_id,
            trace_id: params.request.trace_id,
            attempt_index,
            target_id: candidate.target_id,
            started_unix_ms,
            finished_unix_ms,
            duration_ms: finished_unix_ms - started_unix_ms,
            outcome: 'success'
          });

          return {
            call_response: {
              ack: parse_result.value.ack,
              terminal_message: parse_result.value.terminal_message
            },
            forwarding_attempt_list
          };
        }

        const terminal_error = parse_result.value.terminal_message;
        last_terminal_error_message = terminal_error;

        forwarding_attempt_list.push({
          request_id: params.request.request_id,
          trace_id: params.request.trace_id,
          attempt_index,
          target_id: candidate.target_id,
          started_unix_ms,
          finished_unix_ms,
          duration_ms: finished_unix_ms - started_unix_ms,
          outcome: terminal_error.error.retryable
            ? 'retryable_error'
            : 'non_retryable_error',
          error: BuildIngressBalancerError({
            code: terminal_error.error.retryable
              ? 'INGRESS_DISPATCH_RETRYABLE'
              : 'INGRESS_TARGET_UNAVAILABLE',
            message: terminal_error.error.message,
            retryable: terminal_error.error.retryable,
            unknown_outcome: terminal_error.error.unknown_outcome,
            details: terminal_error.error.details ?? {}
          })
        });

        if (!terminal_error.error.retryable || attempt_index >= max_attempts) {
          return {
            call_response: {
              ack: parse_result.value.ack,
              terminal_message: terminal_error
            },
            forwarding_attempt_list
          };
        }
      } catch (error) {
        const finished_unix_ms = Date.now();
        const error_message = error instanceof Error ? error.message : String(error);
        const unknown_outcome = true;

        forwarding_attempt_list.push({
          request_id: params.request.request_id,
          trace_id: params.request.trace_id,
          attempt_index,
          target_id: candidate.target_id,
          started_unix_ms,
          finished_unix_ms,
          duration_ms: finished_unix_ms - started_unix_ms,
          outcome: 'transport_error',
          error: BuildIngressBalancerError({
            code: 'INGRESS_DISPATCH_RETRYABLE',
            message: error_message,
            retryable: true,
            unknown_outcome,
            details: {
              target_id: candidate.target_id
            }
          })
        });

        if (attempt_index >= max_attempts) {
          break;
        }
      }
    }

    const last_attempt = forwarding_attempt_list[forwarding_attempt_list.length - 1];
    const fallback_node_id =
      params.ordered_candidate_list[params.ordered_candidate_list.length - 1]?.node_id ??
      'ingress_unknown_target';

    return {
      call_response: {
        ack: {
          protocol_version: 1,
          message_type: 'cluster_call_ack',
          timestamp_unix_ms: Date.now(),
          request_id: params.request.request_id,
          attempt_index: last_attempt?.attempt_index ?? 1,
          node_id: fallback_node_id,
          accepted: false,
          queue_position: 0,
          estimated_start_delay_ms: 0
        },
        terminal_message:
          last_terminal_error_message ??
          {
            protocol_version: 1,
            message_type: 'cluster_call_response_error',
            timestamp_unix_ms: Date.now(),
            request_id: params.request.request_id,
            attempt_index: last_attempt?.attempt_index ?? 1,
            node_id: fallback_node_id,
            error: BuildIngressBalancerError({
              code: 'INGRESS_TARGET_UNAVAILABLE',
              message: 'All ingress forwarding attempts failed.',
              retryable: true,
              unknown_outcome: true,
              details: {
                forwarding_attempt_count: forwarding_attempt_list.length
              }
            }),
            timing: {
              gateway_received_unix_ms: params.request.timestamp_unix_ms,
              last_attempt_started_unix_ms:
                last_attempt?.started_unix_ms ?? params.request.timestamp_unix_ms
            }
          }
      },
      forwarding_attempt_list
    };
  }

  private async dispatchToCandidate(params: {
    request: cluster_call_request_message_i;
    candidate: ingress_balancer_target_record_t;
  }): Promise<string> {
    const authority = BuildTransportAuthority({
      host: params.candidate.endpoint.host,
      port: params.candidate.endpoint.port,
      tls_mode: params.candidate.endpoint.tls_mode
    });
    const client_session: ClientHttp2Session = connect(
      authority,
      BuildTlsClientConnectOptions({
        tls_client_config: this.transport_security
      })
    );
    client_session.on('error', (): void => {
      // request stream handles operation errors.
    });

    return await new Promise<string>(async (resolve, reject): Promise<void> => {
      const timeout_deadline_unix_ms = Math.min(
        params.request.deadline_unix_ms,
        Date.now() + this.request_timeout_ms
      );
      const timeout_ms = Math.max(1, timeout_deadline_unix_ms - Date.now());

      const timeout_handle = setTimeout((): void => {
        try {
          client_session.close();
        } catch {
          // ignore close errors
        }
        reject(new Error(`Forward dispatch timed out after ${timeout_ms}ms.`));
      }, timeout_ms);

      try {
        const runtime_auth_headers = this.auth_headers_provider
          ? NormalizeHeaders({
              headers: await this.auth_headers_provider()
            })
          : {};

        const headers: Record<string, string> = {
          ':method': 'POST',
          ':path': params.candidate.endpoint.request_path,
          'content-type': 'application/json',
          ...this.static_request_headers,
          ...runtime_auth_headers
        };

        const request_stream = client_session.request(headers);
        const chunk_list: Buffer[] = [];

        request_stream.on('data', (chunk): void => {
          if (typeof chunk === 'string') {
            chunk_list.push(Buffer.from(chunk));
            return;
          }

          chunk_list.push(chunk as Buffer);
        });

        request_stream.on('end', (): void => {
          clearTimeout(timeout_handle);
          try {
            client_session.close();
          } catch {
            // ignore close errors
          }
          resolve(Buffer.concat(chunk_list).toString('utf8'));
        });

        request_stream.on('error', (error): void => {
          clearTimeout(timeout_handle);
          try {
            client_session.close();
          } catch {
            // ignore close errors
          }
          reject(error);
        });

        request_stream.end(JSON.stringify(params.request));
      } catch (error) {
        clearTimeout(timeout_handle);
        try {
          client_session.close();
        } catch {
          // ignore close errors
        }
        reject(error);
      }
    });
  }

  static buildIngressNoTargetResponse(params: {
    request: cluster_call_request_message_i;
    error_code?: ingress_balancer_error_code_t;
    message?: string;
    details?: Record<string, unknown>;
  }): {
    ack: cluster_call_ack_message_i;
    terminal_message: cluster_call_response_error_message_i;
  } {
    const error_code = params.error_code ?? 'INGRESS_NO_TARGET';
    const message = params.message ?? 'No eligible ingress targets were available.';

    return {
      ack: {
        protocol_version: 1,
        message_type: 'cluster_call_ack',
        timestamp_unix_ms: Date.now(),
        request_id: params.request.request_id,
        attempt_index: params.request.attempt_index,
        node_id: 'ingress',
        accepted: false,
        queue_position: 0,
        estimated_start_delay_ms: 0
      },
      terminal_message: {
        protocol_version: 1,
        message_type: 'cluster_call_response_error',
        timestamp_unix_ms: Date.now(),
        request_id: params.request.request_id,
        attempt_index: params.request.attempt_index,
        node_id: 'ingress',
        error: BuildIngressBalancerError({
          code: error_code,
          message,
          retryable: error_code !== 'INGRESS_FORBIDDEN',
          unknown_outcome: false,
          details: {
            ...(params.details ?? {})
          }
        }),
        timing: {
          gateway_received_unix_ms: params.request.timestamp_unix_ms,
          last_attempt_started_unix_ms: Date.now()
        }
      }
    };
  }

  static parseRawWireResponse(params: {
    response_text: string;
  }): Record<string, unknown> {
    const parsed_response = JSON.parse(params.response_text) as unknown;
    if (!IsRecordObject(parsed_response)) {
      throw new Error('Forwarded wire response was not an object.');
    }

    return parsed_response;
  }
}
