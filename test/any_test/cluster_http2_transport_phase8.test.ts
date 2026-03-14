import assert from 'node:assert/strict';
import { connect, type ClientHttp2Session } from 'node:http2';
import test from 'node:test';

import {
  ClusterNodeAgent,
  WorkerProcedureCall,
  type cluster_call_request_message_i,
  type cluster_http2_transport_address_t,
  type cluster_http2_transport_event_t
} from '../../src/index';
import {
  BuildSecureClientTlsConfig,
  BuildSecureNodeTransportSecurity
} from '../fixtures/secure_transport_config';

type cluster_call_wire_response_t = {
  ack: {
    message_type: string;
    accepted: boolean;
  };
  terminal_message: {
    message_type: string;
    error?: {
      code: string;
      message: string;
      details?: Record<string, unknown>;
    };
    return_value?: unknown;
  };
};

function BuildCallRequest(params: {
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
    max_attempts: 1,
    deadline_unix_ms: params.deadline_unix_ms ?? Date.now() + 20_000,
    function_name: params.function_name,
    args: params.args ?? [],
    routing_hint: {
      mode: 'auto'
    },
    caller_identity: {
      subject: 'client_subject',
      tenant_id: 'tenant_1',
      scopes: ['rpc.call:*'],
      signed_claims: 'unsigned'
    }
  };
}

function ReadAuthorizationHeader(params: {
  authorization_header_value: string | string[] | undefined;
}): string | undefined {
  const { authorization_header_value } = params;

  if (typeof authorization_header_value === 'string') {
    return authorization_header_value;
  }

  if (Array.isArray(authorization_header_value)) {
    return authorization_header_value[0];
  }

  return undefined;
}

async function SendProtocolMessage(params: {
  address: cluster_http2_transport_address_t;
  message?: unknown;
  raw_body?: string;
  authorization_header?: string;
}): Promise<cluster_call_wire_response_t | { cancel_ack: { cancelled: boolean } }> {
  const { address, message, raw_body, authorization_header } = params;
  const transport_security = BuildSecureClientTlsConfig();

  const client = connect(`https://${address.host}:${address.port}`, {
    ca: transport_security.ca_pem_list,
    cert: transport_security.client_cert_pem,
    key: transport_security.client_key_pem,
    rejectUnauthorized: transport_security.reject_unauthorized,
    servername: transport_security.servername
  });

  try {
    const request_headers: Record<string, string> = {
      ':method': 'POST',
      ':path': address.request_path,
      'content-type': 'application/json'
    };

    if (typeof authorization_header === 'string') {
      request_headers.authorization = authorization_header;
    }

    const request_stream = client.request(request_headers);

    const response_text_promise = new Promise<string>((resolve, reject): void => {
      const response_chunk_list: Buffer[] = [];

      request_stream.on('data', (chunk): void => {
        if (typeof chunk === 'string') {
          response_chunk_list.push(Buffer.from(chunk));
          return;
        }

        response_chunk_list.push(chunk as Buffer);
      });

      request_stream.on('end', (): void => {
        resolve(Buffer.concat(response_chunk_list).toString('utf8'));
      });

      request_stream.on('error', (error): void => {
        reject(error);
      });
    });

    if (typeof raw_body === 'string') {
      request_stream.end(raw_body);
    } else {
      request_stream.end(JSON.stringify(message ?? {}));
    }

    const response_text = await response_text_promise;
    return JSON.parse(response_text) as
      | cluster_call_wire_response_t
      | { cancel_ack: { cancelled: boolean } };
  } finally {
    client.close();
  }
}

async function WaitForCondition(params: {
  timeout_ms: number;
  condition_func: () => boolean;
}): Promise<void> {
  const { timeout_ms, condition_func } = params;

  const started_unix_ms = Date.now();

  while (Date.now() - started_unix_ms < timeout_ms) {
    if (condition_func()) {
      return;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 10);
    });
  }

  throw new Error(`Condition did not become true within ${timeout_ms}ms.`);
}

test('http2 transport executes cluster call over network path with deterministic response', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'node_local_phase8',
    worker_start_count: 1,
    transport: {
      security: BuildSecureNodeTransportSecurity(),
      authenticate_request: function (params) {
        const authorization_header = ReadAuthorizationHeader({
          authorization_header_value: params.headers.authorization
        });

        if (authorization_header !== 'Bearer phase8_valid_token') {
          return {
            ok: false,
            message: 'Invalid bearer token.'
          };
        }

        return {
          ok: true,
          identity: {
            subject: 'network_client',
            tenant_id: 'tenant_1',
            scopes: ['rpc.call:*'],
            signed_claims: 'signed_phase8_claims'
          }
        };
      }
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: [
        {
          policy_id: 'allow_all_calls_phase8_test',
          effect: 'allow',
          capabilities: ['rpc.call:*']
        }
      ]
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'NetworkEcho',
      worker_func: async function (params: { value: string }): Promise<string> {
        return `echo:${params.value}`;
      }
    });

    const address = await cluster_node_agent.start();

    const response = await SendProtocolMessage({
      address,
      authorization_header: 'Bearer phase8_valid_token',
      message: BuildCallRequest({
        request_id: 'phase8_req_success',
        function_name: 'NetworkEcho',
        args: [{ value: 'ok' }]
      })
    });

    assert.equal('ack' in response, true);
    if (!('ack' in response)) {
      return;
    }

    assert.equal(response.ack.message_type, 'cluster_call_ack');
    assert.equal(response.ack.accepted, true);
    assert.equal(response.terminal_message.message_type, 'cluster_call_response_success');
    assert.equal(response.terminal_message.return_value, 'echo:ok');
  } finally {
    await cluster_node_agent.stop();
  }
});

test('http2 transport returns deterministic errors for malformed payload, auth fail, missing function, and expired deadline', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'node_local_phase8_errors',
    worker_start_count: 1,
    transport: {
      security: BuildSecureNodeTransportSecurity(),
      authenticate_request: function (params) {
        const authorization_header = ReadAuthorizationHeader({
          authorization_header_value: params.headers.authorization
        });

        if (authorization_header !== 'Bearer phase8_valid_token') {
          return {
            ok: false,
            message: 'Invalid bearer token.'
          };
        }

        return {
          ok: true,
          identity: {
            subject: 'network_client',
            tenant_id: 'tenant_1',
            scopes: ['rpc.call:*'],
            signed_claims: 'signed_phase8_claims'
          }
        };
      }
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: [
        {
          policy_id: 'allow_all_calls_phase8_errors_test',
          effect: 'allow',
          capabilities: ['rpc.call:*']
        }
      ]
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'NetworkEchoErrors',
      worker_func: async function (): Promise<string> {
        return 'ok';
      }
    });

    const address = await cluster_node_agent.start();

    const malformed_response = await SendProtocolMessage({
      address,
      raw_body: '{ bad json',
      authorization_header: 'Bearer phase8_valid_token'
    });
    assert.equal('ack' in malformed_response, true);
    if ('ack' in malformed_response) {
      assert.equal(malformed_response.terminal_message.message_type, 'cluster_call_response_error');
      assert.equal(malformed_response.terminal_message.error?.code, 'PROTOCOL_VALIDATION_FAILED');
    }

    const auth_failed_response = await SendProtocolMessage({
      address,
      message: BuildCallRequest({
        request_id: 'phase8_req_auth_fail',
        function_name: 'NetworkEchoErrors'
      })
    });
    assert.equal('ack' in auth_failed_response, true);
    if ('ack' in auth_failed_response) {
      assert.equal(auth_failed_response.terminal_message.message_type, 'cluster_call_response_error');
      assert.equal(auth_failed_response.terminal_message.error?.code, 'AUTH_FAILED');
    }

    const function_not_found_response = await SendProtocolMessage({
      address,
      authorization_header: 'Bearer phase8_valid_token',
      message: BuildCallRequest({
        request_id: 'phase8_req_missing_function',
        function_name: 'FunctionThatDoesNotExist'
      })
    });
    assert.equal('ack' in function_not_found_response, true);
    if ('ack' in function_not_found_response) {
      assert.equal(
        function_not_found_response.terminal_message.message_type,
        'cluster_call_response_error'
      );
      assert.equal(function_not_found_response.terminal_message.error?.code, 'REMOTE_FUNCTION_ERROR');
    }

    const expired_deadline_response = await SendProtocolMessage({
      address,
      authorization_header: 'Bearer phase8_valid_token',
      message: BuildCallRequest({
        request_id: 'phase8_req_expired_deadline',
        function_name: 'NetworkEchoErrors',
        deadline_unix_ms: Date.now() - 1
      })
    });

    assert.equal('ack' in expired_deadline_response, true);
    if ('ack' in expired_deadline_response) {
      assert.equal(
        expired_deadline_response.terminal_message.message_type,
        'cluster_call_response_error'
      );
      assert.equal(expired_deadline_response.terminal_message.error?.code, 'PROTOCOL_VALIDATION_FAILED');
      assert.equal(
        expired_deadline_response.terminal_message.error?.details?.reason,
        'expired_deadline'
      );
    }
  } finally {
    await cluster_node_agent.stop();
  }
});

test('http2 transport session disconnect cleans in-flight request tracking', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  const observed_transport_event_list: cluster_http2_transport_event_t[] = [];

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'node_local_phase8_disconnect',
    worker_start_count: 1,
    transport: {
      security: BuildSecureNodeTransportSecurity(),
      authenticate_request: function (params) {
        const authorization_header = ReadAuthorizationHeader({
          authorization_header_value: params.headers.authorization
        });

        if (authorization_header !== 'Bearer phase8_valid_token') {
          return {
            ok: false,
            message: 'Invalid bearer token.'
          };
        }

        return {
          ok: true,
          identity: {
            subject: 'network_client',
            tenant_id: 'tenant_1',
            scopes: ['rpc.call:*'],
            signed_claims: 'signed_phase8_claims'
          }
        };
      }
    }
  });

  const listener_id = cluster_node_agent.onTransportEvent({
    listener: function (event): void {
      observed_transport_event_list.push(event);
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: [
        {
          policy_id: 'allow_all_calls_phase8_disconnect_test',
          effect: 'allow',
          capabilities: ['rpc.call:*']
        }
      ]
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'NetworkSlowFunction',
      worker_func: async function (): Promise<string> {
        await new Promise<void>((resolve): void => {
          setTimeout(resolve, 250);
        });
        return 'slow_done';
      }
    });

    const address = await cluster_node_agent.start();

    const transport_security = BuildSecureClientTlsConfig();
    const client: ClientHttp2Session = connect(`https://${address.host}:${address.port}`, {
      ca: transport_security.ca_pem_list,
      cert: transport_security.client_cert_pem,
      key: transport_security.client_key_pem,
      rejectUnauthorized: transport_security.reject_unauthorized,
      servername: transport_security.servername
    });
    client.on('error', (): void => {
      // Ignore expected disconnect errors during cancellation simulation.
    });

    const request_stream = client.request({
      ':method': 'POST',
      ':path': address.request_path,
      'content-type': 'application/json',
      authorization: 'Bearer phase8_valid_token'
    });
    request_stream.on('error', (): void => {
      // Ignore expected stream cancellation errors.
    });

    request_stream.end(
      JSON.stringify(
        BuildCallRequest({
          request_id: 'phase8_req_disconnect',
          function_name: 'NetworkSlowFunction'
        })
      )
    );

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 20);
    });
    client.destroy();

    await WaitForCondition({
      timeout_ms: 2_000,
      condition_func: function (): boolean {
        return observed_transport_event_list.some((event): boolean => {
          return (
            event.event_name === 'request_cancelled' &&
            event.request_id === 'phase8_req_disconnect'
          );
        });
      }
    });

    const session_snapshot = cluster_node_agent.getSessionSnapshot();
    assert.equal(session_snapshot.length, 0);

    assert.equal(
      observed_transport_event_list.some((event): boolean => {
        return event.event_name === 'session_connected';
      }),
      true
    );

    assert.equal(
      observed_transport_event_list.some((event): boolean => {
        return event.event_name === 'session_disconnected';
      }),
      true
    );
  } finally {
    cluster_node_agent.offTransportEvent({ listener_id });
    await cluster_node_agent.stop();
  }
});
