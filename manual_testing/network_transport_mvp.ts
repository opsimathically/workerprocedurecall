import { connect } from 'node:http2';

import {
  ClusterNodeAgent,
  WorkerProcedureCall
} from '../src/index';

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

async function Run(): Promise<void> {
  const workerprocedurecall = new WorkerProcedureCall();

  const node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'node_manual_mvp',
    worker_start_count: 2,
    transport: {
      authenticate_request: function (params) {
        const authorization_header = ReadAuthorizationHeader({
          authorization_header_value: params.headers.authorization
        });

        if (authorization_header !== 'Bearer demo_token') {
          return {
            ok: false,
            message: 'Invalid token.'
          };
        }

        return {
          ok: true,
          identity: {
            subject: 'demo_client',
            tenant_id: 'tenant_demo',
            scopes: ['rpc.call:*'],
            signed_claims: 'demo_signed_claims'
          }
        };
      }
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: [
        {
          policy_id: 'allow_all_calls_manual_network_mvp',
          effect: 'allow',
          capabilities: ['rpc.call:*']
        }
      ]
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'NetworkHello',
      worker_func: async function (params: { value: string }): Promise<string> {
        return `hello:${params.value}`;
      }
    });

    const address = await node_agent.start();

    const client = connect(`http://${address.host}:${address.port}`);

    const request_stream = client.request({
      ':method': 'POST',
      ':path': address.request_path,
      'content-type': 'application/json',
      authorization: 'Bearer demo_token'
    });

    const response_text_promise = new Promise<string>((resolve, reject): void => {
      const chunk_list: Buffer[] = [];

      request_stream.on('data', (chunk): void => {
        if (typeof chunk === 'string') {
          chunk_list.push(Buffer.from(chunk));
          return;
        }

        chunk_list.push(chunk as Buffer);
      });

      request_stream.on('end', (): void => {
        resolve(Buffer.concat(chunk_list).toString('utf8'));
      });

      request_stream.on('error', (error): void => {
        reject(error);
      });
    });

    request_stream.end(
      JSON.stringify({
        protocol_version: 1,
        message_type: 'cluster_call_request',
        timestamp_unix_ms: Date.now(),
        request_id: 'manual_req_1',
        trace_id: 'manual_trace_1',
        span_id: 'manual_span_1',
        attempt_index: 1,
        max_attempts: 1,
        deadline_unix_ms: Date.now() + 20_000,
        function_name: 'NetworkHello',
        args: [{ value: 'world' }],
        routing_hint: {
          mode: 'auto'
        },
        caller_identity: {
          subject: 'ignored_by_transport',
          tenant_id: 'ignored_by_transport',
          scopes: [],
          signed_claims: ''
        }
      })
    );

    const response_text = await response_text_promise;
    console.log('network call response:', response_text);

    client.close();
  } finally {
    await node_agent.stop();
  }
}

void Run();
