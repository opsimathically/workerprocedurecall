import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type { Http2ServerRequest, Http2ServerResponse, IncomingHttpHeaders } from 'node:http2';
import test from 'node:test';

import { ClusterHttp2Transport, WorkerProcedureCall, type cluster_call_request_message_i } from '../../src/index';
import {
  BuildRandomJsonValue,
  InstallProcessCrashGuard,
  RandomAlphaNumericString,
  RandomIntegerInRange,
  RunFuzzSuite
} from './fuzz_harness';

type mock_http2_response_collector_t = {
  response: Http2ServerResponse;
  getBodyText: () => string;
  getStatusCode: () => number;
  getHeaderMap: () => Record<string, unknown>;
};

type fuzz_wire_response_t = {
  ack?: {
    message_type?: string;
    accepted?: boolean;
  };
  terminal_message?: {
    message_type?: string;
    error?: {
      code?: string;
      message?: string;
    };
    return_value?: unknown;
  };
  cancel_ack?: {
    cancelled?: boolean;
  };
};

type transport_private_api_t = {
  handleHttp2Request: (params: {
    request: Http2ServerRequest;
    response: Http2ServerResponse;
  }) => Promise<void>;
};

function BuildValidCallRequest(params: {
  request_id: string;
  function_name: string;
  args?: unknown[];
  deadline_unix_ms?: number;
}): cluster_call_request_message_i {
  return {
    protocol_version: 1,
    message_type: 'cluster_call_request',
    timestamp_unix_ms: Date.now(),
    request_id: params.request_id,
    trace_id: `trace_${params.request_id}`,
    span_id: `span_${params.request_id}`,
    attempt_index: 1,
    max_attempts: 3,
    deadline_unix_ms: params.deadline_unix_ms ?? Date.now() + 20_000,
    function_name: params.function_name,
    args: params.args ?? [],
    routing_hint: {
      mode: 'auto'
    },
    caller_identity: {
      subject: 'transport_fuzz_client',
      tenant_id: 'tenant_fuzz',
      scopes: ['rpc.call:*'],
      signed_claims: 'transport_fuzz_signed_claims'
    }
  };
}

function BuildMalformedJsonBody(params: {
  random_func: () => number;
}): string {
  const malformed_mode = RandomIntegerInRange({
    random_func: params.random_func,
    minimum_value: 0,
    maximum_value: 4
  });

  if (malformed_mode === 0) {
    return '{';
  }
  if (malformed_mode === 1) {
    return '{"protocol_version":1,"message_type":"cluster_call_request"';
  }
  if (malformed_mode === 2) {
    return `{"message_type":${RandomAlphaNumericString({
      random_func: params.random_func,
      minimum_length: 3,
      maximum_length: 12
    })}}`;
  }
  if (malformed_mode === 3) {
    return '[1,2,3';
  }

  return `{"junk":"${RandomAlphaNumericString({
    random_func: params.random_func,
    minimum_length: 1,
    maximum_length: 64
  })}"`;
}

function BuildMockRequest(params: {
  method?: string;
  url?: string;
  headers?: IncomingHttpHeaders;
  body_text: string;
  body_chunk_count: number;
}): Http2ServerRequest {
  const request_event_emitter = new EventEmitter();
  const request = request_event_emitter as unknown as Http2ServerRequest;

  (request as unknown as { method?: string }).method = params.method ?? 'POST';
  (request as unknown as { url?: string }).url = params.url ?? '/wpc/cluster/protocol';
  (request as unknown as { headers: IncomingHttpHeaders }).headers = params.headers ?? {};
  (
    request as unknown as {
      stream: {
        session?: unknown;
      };
    }
  ).stream = {};

  const body_text = params.body_text;
  const body_chunk_count = Math.max(1, params.body_chunk_count);
  const chunk_size = Math.max(1, Math.ceil(body_text.length / body_chunk_count));

  setImmediate((): void => {
    if (body_text.length === 0) {
      request_event_emitter.emit('end');
      return;
    }

    for (let chunk_start = 0; chunk_start < body_text.length; chunk_start += chunk_size) {
      const chunk_end = Math.min(body_text.length, chunk_start + chunk_size);
      request_event_emitter.emit('data', Buffer.from(body_text.slice(chunk_start, chunk_end), 'utf8'));
    }

    request_event_emitter.emit('end');
  });

  return request;
}

function BuildMockResponseCollector(): mock_http2_response_collector_t {
  let body_text = '';
  let status_code = 0;
  const header_by_name: Record<string, unknown> = {};

  const response_object: {
    statusCode: number;
    writableEnded: boolean;
    setHeader: (name: string, value: unknown) => void;
    end: (value?: string) => void;
    stream: {
      close: () => void;
    };
  } = {
    statusCode: 0,
    writableEnded: false,
    setHeader: (name: string, value: unknown): void => {
      header_by_name[name] = value;
    },
    end: (value?: string): void => {
      if (typeof value === 'string') {
        body_text = value;
      }
      status_code = response_object.statusCode;
      response_object.writableEnded = true;
    },
    stream: {
      close: (): void => {}
    }
  };

  return {
    response: response_object as unknown as Http2ServerResponse,
    getBodyText: (): string => {
      return body_text;
    },
    getStatusCode: (): number => {
      return status_code;
    },
    getHeaderMap: (): Record<string, unknown> => {
      return { ...header_by_name };
    }
  };
}

async function SendTransportMessageWithoutNetwork(params: {
  transport: ClusterHttp2Transport;
  authorization_header: string;
  message?: unknown;
  raw_body?: string;
  body_chunk_count: number;
}): Promise<fuzz_wire_response_t> {
  const request = BuildMockRequest({
    headers: {
      authorization: params.authorization_header
    },
    body_text:
      typeof params.raw_body === 'string'
        ? params.raw_body
        : JSON.stringify(params.message ?? {}),
    body_chunk_count: params.body_chunk_count
  });
  const response_collector = BuildMockResponseCollector();

  await (params.transport as unknown as transport_private_api_t).handleHttp2Request({
    request,
    response: response_collector.response
  });

  assert.equal(response_collector.getStatusCode(), 200);
  assert.equal(response_collector.getHeaderMap()['content-type'], 'application/json; charset=utf-8');

  const response_text = response_collector.getBodyText();
  assert.equal(typeof response_text, 'string');
  assert.equal(response_text.length > 0, true);

  return JSON.parse(response_text) as fuzz_wire_response_t;
}

test('transport message fuzz stability', async function (): Promise<void> {
  const process_guard = InstallProcessCrashGuard({
    suite_name: 'transport_message_fuzz'
  });

  const workerprocedurecall = new WorkerProcedureCall();
  const transport = new ClusterHttp2Transport({
    workerprocedurecall,
    node_id: 'transport_fuzz_node',
    request_path: '/wpc/cluster/protocol',
    authenticate_request: function (params) {
      const authorization_header =
        typeof params.headers.authorization === 'string'
          ? params.headers.authorization
          : Array.isArray(params.headers.authorization)
            ? params.headers.authorization[0]
            : undefined;

      if (authorization_header !== 'Bearer transport_fuzz_token') {
        return {
          ok: false,
          message: 'Invalid authorization header for transport fuzz test.'
        };
      }

      return {
        ok: true,
        identity: {
          subject: 'transport_fuzz_subject',
          tenant_id: 'tenant_fuzz',
          scopes: ['rpc.call:*'],
          signed_claims: 'transport_fuzz_identity'
        }
      };
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: [
        {
          policy_id: 'transport_fuzz_allow_calls',
          effect: 'allow',
          capabilities: ['rpc.call:*']
        }
      ]
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'TransportFuzzEcho',
      worker_func: async function (params?: { value?: string }): Promise<string> {
        return `ECHO:${params?.value ?? 'none'}`;
      }
    });

    await workerprocedurecall.startWorkers({
      count: 1
    });

    await RunFuzzSuite({
      suite_name: 'transport_message_fuzz',
      default_iterations: 80,
      default_case_timeout_ms: 500,
      run_case_func: async (params): Promise<void> => {
        const fuzz_mode = RandomIntegerInRange({
          random_func: params.random_func,
          minimum_value: 0,
          maximum_value: 5
        });
        const body_chunk_count = RandomIntegerInRange({
          random_func: params.random_func,
          minimum_value: 1,
          maximum_value: 8
        });

        if (fuzz_mode === 0) {
          await SendTransportMessageWithoutNetwork({
            transport,
            authorization_header: 'Bearer transport_fuzz_token',
            raw_body: BuildMalformedJsonBody({
              random_func: params.random_func
            }),
            body_chunk_count
          });
        } else if (fuzz_mode === 1) {
          const random_payload = BuildRandomJsonValue({
            random_func: params.random_func,
            depth: 0,
            max_depth: 4,
            max_array_length: 6,
            max_object_keys: 6,
            max_string_length: 128
          });
          await SendTransportMessageWithoutNetwork({
            transport,
            authorization_header: 'Bearer transport_fuzz_token',
            message: random_payload,
            body_chunk_count
          });
        } else if (fuzz_mode === 2) {
          const message = BuildValidCallRequest({
            request_id: `transport_fuzz_invalid_deadline_${params.case_index}`,
            function_name: 'TransportFuzzEcho',
            deadline_unix_ms: Date.now() - RandomIntegerInRange({
              random_func: params.random_func,
              minimum_value: 1,
              maximum_value: 5_000
            }),
            args: [
              {
                value: RandomAlphaNumericString({
                  random_func: params.random_func,
                  minimum_length: 0,
                  maximum_length: 32
                })
              }
            ]
          });
          await SendTransportMessageWithoutNetwork({
            transport,
            authorization_header: 'Bearer transport_fuzz_token',
            message,
            body_chunk_count
          });
        } else if (fuzz_mode === 3) {
          const duplicate_request_id = `transport_fuzz_duplicate_${Math.floor(params.case_index / 2)}`;
          await Promise.all([
            SendTransportMessageWithoutNetwork({
              transport,
              authorization_header: 'Bearer transport_fuzz_token',
              message: BuildValidCallRequest({
                request_id: duplicate_request_id,
                function_name: 'TransportFuzzEcho',
                args: [{ value: 'a' }]
              }),
              body_chunk_count
            }),
            SendTransportMessageWithoutNetwork({
              transport,
              authorization_header: 'Bearer transport_fuzz_token',
              message: BuildValidCallRequest({
                request_id: duplicate_request_id,
                function_name: 'TransportFuzzEcho',
                args: [{ value: 'b' }]
              }),
              body_chunk_count
            })
          ]);
        } else if (fuzz_mode === 4) {
          await SendTransportMessageWithoutNetwork({
            transport,
            authorization_header: 'Bearer bad_token',
            message: BuildValidCallRequest({
              request_id: `transport_fuzz_bad_auth_${params.case_index}`,
              function_name: 'TransportFuzzEcho'
            }),
            body_chunk_count
          });
        } else {
          await SendTransportMessageWithoutNetwork({
            transport,
            authorization_header: 'Bearer transport_fuzz_token',
            message: {
              protocol_version: RandomIntegerInRange({
                random_func: params.random_func,
                minimum_value: -3,
                maximum_value: 5
              }),
              message_type: 'cluster_call_cancel',
              timestamp_unix_ms: Date.now(),
              request_id: `transport_fuzz_cancel_${params.case_index}`
            },
            body_chunk_count
          });
        }

        if (params.case_index % 5 === 0) {
          const healthy_response = await SendTransportMessageWithoutNetwork({
            transport,
            authorization_header: 'Bearer transport_fuzz_token',
            message: BuildValidCallRequest({
              request_id: `transport_fuzz_health_${params.case_index}`,
              function_name: 'TransportFuzzEcho',
              args: [{ value: `${params.case_index}` }]
            }),
            body_chunk_count: 1
          });

          assert.equal(typeof healthy_response, 'object');
          assert.notEqual(healthy_response, null);
          assert.equal(
            typeof healthy_response.terminal_message?.message_type === 'string',
            true
          );
        }
      }
    });
  } finally {
    await workerprocedurecall.stopWorkers().catch((): void => {});

    const process_event_list = process_guard.dispose();
    assert.equal(process_event_list.length, 0);
  }
});
