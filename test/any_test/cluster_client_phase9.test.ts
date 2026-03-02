import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import test from 'node:test';

import {
  ClusterClient,
  ClusterClientError,
  ClusterNodeAgent,
  WorkerProcedureCall
} from '../../src/index';

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

async function ReserveTcpPort(): Promise<number> {
  return await new Promise<number>((resolve, reject): void => {
    const server = createServer();

    server.on('error', (error): void => {
      reject(error);
    });

    server.listen(0, '127.0.0.1', (): void => {
      const address = server.address();

      if (!address || typeof address === 'string') {
        server.close((): void => {
          reject(new Error('Failed to reserve a TCP port.'));
        });
        return;
      }

      const reserved_port = address.port;

      server.close((close_error): void => {
        if (close_error) {
          reject(close_error);
          return;
        }

        resolve(reserved_port);
      });
    });
  });
}

function BuildAllowAllCallPolicyList(): {
  policy_id: string;
  effect: 'allow';
  capabilities: string[];
}[] {
  return [
    {
      policy_id: 'allow_all_calls_phase9',
      effect: 'allow',
      capabilities: ['rpc.call:*']
    }
  ];
}

test('cluster client executes successful network call', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase9_success_node',
    worker_start_count: 1,
    transport: {
      authenticate_request: function (params) {
        const authorization_header = ReadAuthorizationHeader({
          authorization_header_value: params.headers.authorization
        });

        if (authorization_header !== 'Bearer phase9_token') {
          return {
            ok: false,
            message: 'Invalid bearer token.'
          };
        }

        return {
          ok: true,
          identity: {
            subject: 'phase9_client',
            tenant_id: 'tenant_phase9',
            scopes: ['rpc.call:*'],
            signed_claims: 'signed_phase9'
          }
        };
      }
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: BuildAllowAllCallPolicyList()
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase9Echo',
      worker_func: async function (params: { value: string }): Promise<string> {
        return `phase9:${params.value}`;
      }
    });

    const address = await cluster_node_agent.start();

    const cluster_client = new ClusterClient({
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      auth_context: {
        subject: 'phase9_client',
        tenant_id: 'tenant_phase9',
        scopes: ['rpc.call:*'],
        signed_claims: 'signed_phase9'
      },
      auth_headers: {
        authorization: 'Bearer phase9_token'
      }
    });

    try {
      await cluster_client.connect();

      const result = await cluster_client.call<string>({
        function_name: 'Phase9Echo',
        args: [{ value: 'ok' }]
      });

      assert.equal(result, 'phase9:ok');
    } finally {
      await cluster_client.close();
    }
  } finally {
    await cluster_node_agent.stop();
  }
});

test('cluster client maps auth failure to deterministic protocol error', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase9_auth_node',
    worker_start_count: 1,
    transport: {
      authenticate_request: function (params) {
        const authorization_header = ReadAuthorizationHeader({
          authorization_header_value: params.headers.authorization
        });

        if (authorization_header !== 'Bearer phase9_token') {
          return {
            ok: false,
            message: 'Invalid bearer token.'
          };
        }

        return {
          ok: true,
          identity: {
            subject: 'phase9_client',
            tenant_id: 'tenant_phase9',
            scopes: ['rpc.call:*'],
            signed_claims: 'signed_phase9'
          }
        };
      }
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: BuildAllowAllCallPolicyList()
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase9AuthFunction',
      worker_func: async function (): Promise<string> {
        return 'ok';
      }
    });

    const address = await cluster_node_agent.start();

    const cluster_client = new ClusterClient({
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      auth_context: {
        subject: 'phase9_client',
        tenant_id: 'tenant_phase9',
        scopes: ['rpc.call:*'],
        signed_claims: 'signed_phase9'
      },
      auth_headers: {
        authorization: 'Bearer invalid_token'
      },
      retry_policy: {
        max_attempts: 1
      }
    });

    try {
      await cluster_client.connect();

      await assert.rejects(
        async function (): Promise<void> {
          await cluster_client.call({
            function_name: 'Phase9AuthFunction'
          });
        },
        function (error: unknown): boolean {
          assert(error instanceof ClusterClientError);
          assert.equal(error.code, 'AUTH_FAILED');
          return true;
        }
      );
    } finally {
      await cluster_client.close();
    }
  } finally {
    await cluster_node_agent.stop();
  }
});

test('cluster client enforces timeout deadline deterministically', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase9_timeout_node',
    worker_start_count: 1,
    transport: {
      authenticate_request: function () {
        return {
          ok: true,
          identity: {
            subject: 'phase9_client',
            tenant_id: 'tenant_phase9',
            scopes: ['rpc.call:*'],
            signed_claims: 'signed_phase9'
          }
        };
      }
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: BuildAllowAllCallPolicyList()
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase9SlowFunction',
      worker_func: async function (): Promise<string> {
        await new Promise<void>((resolve): void => {
          setTimeout(resolve, 120);
        });

        return 'done';
      }
    });

    const address = await cluster_node_agent.start();

    const cluster_client = new ClusterClient({
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      auth_context: {
        subject: 'phase9_client',
        tenant_id: 'tenant_phase9',
        scopes: ['rpc.call:*'],
        signed_claims: 'signed_phase9'
      },
      retry_policy: {
        max_attempts: 1
      }
    });

    try {
      await cluster_client.connect();

      await assert.rejects(
        async function (): Promise<void> {
          await cluster_client.call({
            function_name: 'Phase9SlowFunction',
            timeout_ms: 20
          });
        },
        function (error: unknown): boolean {
          assert(error instanceof ClusterClientError);
          assert.equal(error.code, 'CLIENT_TIMEOUT');
          return true;
        }
      );
    } finally {
      await cluster_client.close();
    }
  } finally {
    await cluster_node_agent.stop();
  }
});

test('cluster client reconnects and continues calling after server restart', async function () {
  const port = await ReserveTcpPort();

  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase9_reconnect_node',
    worker_start_count: 1,
    transport: {
      port,
      authenticate_request: function () {
        return {
          ok: true,
          identity: {
            subject: 'phase9_client',
            tenant_id: 'tenant_phase9',
            scopes: ['rpc.call:*'],
            signed_claims: 'signed_phase9'
          }
        };
      }
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: BuildAllowAllCallPolicyList()
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase9ReconnectFunction',
      worker_func: async function (): Promise<string> {
        return 'reconnect_ok';
      }
    });

    const first_address = await cluster_node_agent.start();

    const cluster_client = new ClusterClient({
      host: first_address.host,
      port: first_address.port,
      request_path: first_address.request_path,
      auth_context: {
        subject: 'phase9_client',
        tenant_id: 'tenant_phase9',
        scopes: ['rpc.call:*'],
        signed_claims: 'signed_phase9'
      },
      reconnect_policy: {
        enabled: true,
        max_attempts: 10,
        backoff_ms: 50
      }
    });

    try {
      await cluster_client.connect();

      const first_result = await cluster_client.call<string>({
        function_name: 'Phase9ReconnectFunction'
      });
      assert.equal(first_result, 'reconnect_ok');

      await cluster_node_agent.stop();
      await cluster_node_agent.start();

      const second_result = await cluster_client.call<string>({
        function_name: 'Phase9ReconnectFunction'
      });

      assert.equal(second_result, 'reconnect_ok');
    } finally {
      await cluster_client.close();
    }
  } finally {
    await cluster_node_agent.stop();
  }
});

test('cluster client retries retryable transport failures and succeeds once server comes online', async function () {
  const port = await ReserveTcpPort();

  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase9_retry_node',
    worker_start_count: 1,
    transport: {
      port,
      authenticate_request: function () {
        return {
          ok: true,
          identity: {
            subject: 'phase9_client',
            tenant_id: 'tenant_phase9',
            scopes: ['rpc.call:*'],
            signed_claims: 'signed_phase9'
          }
        };
      }
    }
  });

  let node_agent_started = false;

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: BuildAllowAllCallPolicyList()
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase9RetryFunction',
      worker_func: async function (): Promise<string> {
        return 'retry_ok';
      }
    });

    const delayed_start_promise = new Promise<void>((resolve, reject): void => {
      setTimeout((): void => {
        void cluster_node_agent
          .start()
          .then((): void => {
            node_agent_started = true;
            resolve();
          })
          .catch(reject);
      }, 120);
    });

    const cluster_client = new ClusterClient({
      host: '127.0.0.1',
      port,
      auth_context: {
        subject: 'phase9_client',
        tenant_id: 'tenant_phase9',
        scopes: ['rpc.call:*'],
        signed_claims: 'signed_phase9'
      },
      retry_policy: {
        max_attempts: 8,
        base_delay_ms: 40,
        max_delay_ms: 120
      },
      reconnect_policy: {
        enabled: true,
        max_attempts: 1,
        backoff_ms: 20
      }
    });

    try {
      const result = await cluster_client.call<string>({
        function_name: 'Phase9RetryFunction',
        timeout_ms: 2_000
      });

      await delayed_start_promise;

      assert.equal(result, 'retry_ok');
    } finally {
      await cluster_client.close();
    }
  } finally {
    if (node_agent_started) {
      await cluster_node_agent.stop();
    }
  }
});
