import assert from 'node:assert/strict';
import { connect, type ClientHttp2Session } from 'node:http2';
import test from 'node:test';

import {
  ClusterNodeAgent,
  ClusterServiceDiscoveryDaemon,
  ParseClusterServiceDiscoveryRequestMessage,
  WorkerProcedureCall,
  type cluster_call_request_message_i,
  type cluster_service_discovery_request_message_t,
  type cluster_service_discovery_response_error_message_i,
  type cluster_service_discovery_response_success_message_i
} from '../../src/index';
import {
  BuildSecureClientTlsConfig,
  BuildSecureNodeTransportSecurity,
  BuildSecureServerTlsConfig
} from '../fixtures/secure_transport_config';

async function WaitForCondition(params: {
  timeout_ms: number;
  condition_func: () => boolean;
}): Promise<void> {
  const started_unix_ms = Date.now();

  while (Date.now() - started_unix_ms <= params.timeout_ms) {
    if (params.condition_func()) {
      return;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 10);
    });
  }

  throw new Error(`Condition was not satisfied within ${params.timeout_ms}ms.`);
}

function BuildAllowAllCallPolicyList(): {
  policy_id: string;
  effect: 'allow';
  capabilities: string[];
}[] {
  return [
    {
      policy_id: 'phase15_allow_all_calls',
      effect: 'allow',
      capabilities: ['rpc.call:*']
    }
  ];
}

function BuildCallRequest(params: {
  request_id: string;
  function_name: string;
  args?: unknown[];
}): cluster_call_request_message_i {
  return {
    protocol_version: 1,
    message_type: 'cluster_call_request',
    timestamp_unix_ms: Date.now(),
    request_id: params.request_id,
    trace_id: `${params.request_id}_trace`,
    span_id: `${params.request_id}_span`,
    attempt_index: 1,
    max_attempts: 3,
    deadline_unix_ms: Date.now() + 20_000,
    function_name: params.function_name,
    args: params.args ?? [],
    routing_hint: {
      mode: 'auto'
    },
    caller_identity: {
      subject: 'phase15_client',
      tenant_id: 'tenant_1',
      scopes: ['rpc.call:*'],
      signed_claims: 'phase15_signed_claims'
    }
  };
}

function BuildPermissiveAuthenticateRequest() {
  return function () {
    return {
      ok: true as const,
      identity: {
        subject: 'phase15_inter_node',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase15_transport_signed_claims'
      }
    };
  };
}

async function SendDiscoveryMessage(params: {
  host: string;
  port: number;
  request_path: string;
  message: unknown;
}): Promise<
  cluster_service_discovery_response_success_message_i | cluster_service_discovery_response_error_message_i
> {
  const client_session: ClientHttp2Session = connect(
    `https://${params.host}:${params.port}`,
    {
      ca: BuildSecureClientTlsConfig().ca_pem_list,
      cert: BuildSecureClientTlsConfig().client_cert_pem,
      key: BuildSecureClientTlsConfig().client_key_pem,
      rejectUnauthorized: BuildSecureClientTlsConfig().reject_unauthorized ?? true,
      servername: BuildSecureClientTlsConfig().servername
    }
  );

  try {
    const response_text = await new Promise<string>((resolve, reject): void => {
      const request_stream = client_session.request({
        ':method': 'POST',
        ':path': params.request_path,
        'content-type': 'application/json'
      });

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

      request_stream.end(JSON.stringify(params.message));
    });

    return JSON.parse(response_text) as
      | cluster_service_discovery_response_success_message_i
      | cluster_service_discovery_response_error_message_i;
  } finally {
    client_session.close();
  }
}

test('phase15 protocol validator accepts valid request and rejects malformed request', function () {
  const valid_request: cluster_service_discovery_request_message_t = {
    protocol_version: 1,
    message_type: 'cluster_service_discovery_list_nodes',
    timestamp_unix_ms: Date.now(),
    request_id: 'phase15_valid_protocol',
    include_expired: false
  };

  const valid_parse_result = ParseClusterServiceDiscoveryRequestMessage({
    message: valid_request
  });
  assert.equal(valid_parse_result.ok, true);

  const invalid_parse_result = ParseClusterServiceDiscoveryRequestMessage({
    message: {
      protocol_version: 1,
      message_type: 'unknown_message_type',
      timestamp_unix_ms: Date.now(),
      request_id: 'phase15_invalid_protocol'
    }
  });

  assert.equal(invalid_parse_result.ok, false);
  if (!invalid_parse_result.ok) {
    assert.equal(invalid_parse_result.error.code, 'DISCOVERY_VALIDATION_FAILED');
  }
});

test('phase15 discovery daemon handles register heartbeat capability remove and validation failure', async function () {
  const discovery_daemon = new ClusterServiceDiscoveryDaemon({
    host: '127.0.0.1',
    port: 0,
    security: {
      tls: BuildSecureServerTlsConfig()
    },
    transport_security: BuildSecureClientTlsConfig()
  });

  try {
    const daemon_address = await discovery_daemon.start();

    const register_response = await SendDiscoveryMessage({
      host: daemon_address.host,
      port: daemon_address.port,
      request_path: daemon_address.request_path,
      message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_register_node',
        timestamp_unix_ms: Date.now(),
        request_id: 'phase15_register_1',
        node_identity: {
          node_id: 'phase15_node_1',
          address: {
            host: '127.0.0.1',
            port: 8080,
            request_path: '/wpc/cluster/protocol',
            tls_mode: 'required'
          },
          labels: {
            env: 'test'
          },
          zones: ['zone_a']
        },
        status: 'ready',
        capability_list: [
          {
            function_name: 'Phase15Function',
            function_hash_sha1: 'phase15_hash',
            installed: true
          }
        ]
      }
    });

    assert.equal(register_response.message_type, 'cluster_service_discovery_response_success');

    const heartbeat_response = await SendDiscoveryMessage({
      host: daemon_address.host,
      port: daemon_address.port,
      request_path: daemon_address.request_path,
      message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_heartbeat_node',
        timestamp_unix_ms: Date.now(),
        request_id: 'phase15_heartbeat_1',
        node_id: 'phase15_node_1',
        status: 'degraded',
        metrics: {
          inflight_calls: 2
        }
      }
    });

    assert.equal(heartbeat_response.message_type, 'cluster_service_discovery_response_success');

    const capability_response = await SendDiscoveryMessage({
      host: daemon_address.host,
      port: daemon_address.port,
      request_path: daemon_address.request_path,
      message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_update_capability',
        timestamp_unix_ms: Date.now(),
        request_id: 'phase15_capability_1',
        node_id: 'phase15_node_1',
        capability_list: [
          {
            function_name: 'Phase15FunctionV2',
            function_hash_sha1: 'phase15_hash_v2',
            installed: true
          }
        ]
      }
    });

    assert.equal(capability_response.message_type, 'cluster_service_discovery_response_success');

    const list_response = await SendDiscoveryMessage({
      host: daemon_address.host,
      port: daemon_address.port,
      request_path: daemon_address.request_path,
      message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_list_nodes',
        timestamp_unix_ms: Date.now(),
        request_id: 'phase15_list_1',
        include_expired: false
      }
    });

    assert.equal(list_response.message_type, 'cluster_service_discovery_response_success');
    if (list_response.message_type === 'cluster_service_discovery_response_success') {
      const node_list = (list_response.data as { node_list?: unknown }).node_list as
        | { node_identity: { node_id: string } }[]
        | undefined;
      assert.equal(Array.isArray(node_list), true);
      assert.equal(node_list?.some((node): boolean => node.node_identity.node_id === 'phase15_node_1'), true);
    }

    const malformed_response = await SendDiscoveryMessage({
      host: daemon_address.host,
      port: daemon_address.port,
      request_path: daemon_address.request_path,
      message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_heartbeat_node',
        timestamp_unix_ms: Date.now(),
        request_id: 'phase15_bad_heartbeat'
      }
    });

    assert.equal(malformed_response.message_type, 'cluster_service_discovery_response_error');
    if (malformed_response.message_type === 'cluster_service_discovery_response_error') {
      assert.equal(malformed_response.error.code, 'DISCOVERY_VALIDATION_FAILED');
    }

    const remove_response = await SendDiscoveryMessage({
      host: daemon_address.host,
      port: daemon_address.port,
      request_path: daemon_address.request_path,
      message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_remove_node',
        timestamp_unix_ms: Date.now(),
        request_id: 'phase15_remove_1',
        node_id: 'phase15_node_1'
      }
    });

    assert.equal(remove_response.message_type, 'cluster_service_discovery_response_success');

    const daemon_snapshot = discovery_daemon.getSnapshot();
    assert.equal(daemon_snapshot.discovery_metrics.node_count, 0);
  } finally {
    await discovery_daemon.stop();
  }
});

test('phase15 external daemon integration auto-discovers nodes and reroutes on node stop', async function () {
  const discovery_daemon = new ClusterServiceDiscoveryDaemon({
    host: '127.0.0.1',
    port: 0,
    security: {
      tls: BuildSecureServerTlsConfig()
    },
    transport_security: BuildSecureClientTlsConfig()
  });

  const workerprocedurecall_gateway = new WorkerProcedureCall();
  const workerprocedurecall_node_b = new WorkerProcedureCall();
  const workerprocedurecall_node_c = new WorkerProcedureCall();

  workerprocedurecall_gateway.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });
  workerprocedurecall_node_b.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });
  workerprocedurecall_node_c.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });

  await workerprocedurecall_node_b.defineWorkerFunction({
    name: 'Phase15RouteFunction',
    worker_func: async function (): Promise<string> {
      return 'node_b';
    }
  });

  await workerprocedurecall_node_c.defineWorkerFunction({
    name: 'Phase15RouteFunction',
    worker_func: async function (): Promise<string> {
      return 'node_c';
    }
  });

  let cluster_node_agent_gateway: ClusterNodeAgent | null = null;
  let cluster_node_agent_b: ClusterNodeAgent | null = null;
  let cluster_node_agent_c: ClusterNodeAgent | null = null;
  let node_b_stopped = false;

  try {
    const daemon_address = await discovery_daemon.start();

    cluster_node_agent_gateway = new ClusterNodeAgent({
      workerprocedurecall: workerprocedurecall_gateway,
      node_id: 'phase15_gateway',
      transport: {
        host: 'localhost',
        authenticate_request: BuildPermissiveAuthenticateRequest(),
        security: BuildSecureNodeTransportSecurity()
      },
      discovery: {
        enabled: true,
        external_daemon: {
          host: daemon_address.host,
          port: daemon_address.port,
          request_path: daemon_address.request_path,
          transport_security: BuildSecureClientTlsConfig(),
          synchronization_interval_ms: 25,
          request_timeout_ms: 500
        },
        heartbeat_interval_ms: 25,
        lease_ttl_ms: 150,
        sync_interval_ms: 25
      }
    });

    cluster_node_agent_b = new ClusterNodeAgent({
      workerprocedurecall: workerprocedurecall_node_b,
      node_id: 'phase15_node_b',
      worker_start_count: 1,
      transport: {
        host: 'localhost',
        authenticate_request: BuildPermissiveAuthenticateRequest(),
        security: BuildSecureNodeTransportSecurity()
      },
      discovery: {
        enabled: true,
        external_daemon: {
          host: daemon_address.host,
          port: daemon_address.port,
          request_path: daemon_address.request_path,
          transport_security: BuildSecureClientTlsConfig(),
          synchronization_interval_ms: 25,
          request_timeout_ms: 500
        },
        heartbeat_interval_ms: 25,
        lease_ttl_ms: 150,
        sync_interval_ms: 25
      }
    });

    cluster_node_agent_c = new ClusterNodeAgent({
      workerprocedurecall: workerprocedurecall_node_c,
      node_id: 'phase15_node_c',
      worker_start_count: 1,
      transport: {
        host: 'localhost',
        authenticate_request: BuildPermissiveAuthenticateRequest(),
        security: BuildSecureNodeTransportSecurity()
      },
      discovery: {
        enabled: true,
        external_daemon: {
          host: daemon_address.host,
          port: daemon_address.port,
          request_path: daemon_address.request_path,
          transport_security: BuildSecureClientTlsConfig(),
          synchronization_interval_ms: 25,
          request_timeout_ms: 500
        },
        heartbeat_interval_ms: 25,
        lease_ttl_ms: 150,
        sync_interval_ms: 25
      }
    });

    await cluster_node_agent_gateway.start();
    await cluster_node_agent_b.start();
    await cluster_node_agent_c.start();

    await WaitForCondition({
      timeout_ms: 4_000,
      condition_func: (): boolean => {
        const node_id_set = new Set(
          workerprocedurecall_gateway
            .getClusterCallNodes()
            .map((node): string => node.node_id)
        );

        return node_id_set.has('phase15_node_b') && node_id_set.has('phase15_node_c');
      }
    });

    const initial_response = await workerprocedurecall_gateway.handleClusterCallRequest({
      node_id: 'phase15_gateway',
      message: BuildCallRequest({
        request_id: 'phase15_route_initial',
        function_name: 'Phase15RouteFunction'
      })
    });

    assert.equal(initial_response.terminal_message.message_type, 'cluster_call_response_success');

    await cluster_node_agent_b.stop();
    node_b_stopped = true;

    await WaitForCondition({
      timeout_ms: 4_000,
      condition_func: (): boolean => {
        const node_id_set = new Set(
          workerprocedurecall_gateway
            .getClusterCallNodes()
            .map((node): string => node.node_id)
        );

        return !node_id_set.has('phase15_node_b') && node_id_set.has('phase15_node_c');
      }
    });

    const daemon_snapshot = discovery_daemon.getSnapshot();
    const active_node_id_set = new Set(
      daemon_snapshot.recent_discovery_events
        .map((event): string | undefined => event.node_id)
        .filter((node_id): node_id is string => typeof node_id === 'string')
    );
    assert.equal(active_node_id_set.has('phase15_node_b'), true);

    for (let call_index = 0; call_index < 5; call_index += 1) {
      const response = await workerprocedurecall_gateway.handleClusterCallRequest({
        node_id: 'phase15_gateway',
        message: BuildCallRequest({
          request_id: `phase15_route_after_stop_${call_index}`,
          function_name: 'Phase15RouteFunction'
        })
      });

      assert.equal(response.terminal_message.message_type, 'cluster_call_response_success');
      if (response.terminal_message.message_type === 'cluster_call_response_success') {
        assert.equal(response.terminal_message.node_id, 'phase15_node_c');
      }
    }
  } finally {
    if (cluster_node_agent_gateway) {
      await cluster_node_agent_gateway.stop();
    }
    if (cluster_node_agent_b && !node_b_stopped) {
      await cluster_node_agent_b.stop();
    }
    if (cluster_node_agent_c) {
      await cluster_node_agent_c.stop();
    }

    await discovery_daemon.stop();
  }
});

test('phase15 daemon unavailable does not crash node agent and records discovery sync errors', async function () {
  const unavailable_port = 65_534;

  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase15_unavailable_node',
    worker_start_count: 1,
    transport: {
      host: 'localhost',
      authenticate_request: BuildPermissiveAuthenticateRequest(),
      security: BuildSecureNodeTransportSecurity()
    },
    discovery: {
      enabled: true,
      external_daemon: {
        host: '127.0.0.1',
        port: unavailable_port,
        transport_security: BuildSecureClientTlsConfig(),
        synchronization_interval_ms: 25,
        request_timeout_ms: 100,
        retry_base_delay_ms: 10,
        retry_max_delay_ms: 50
      },
      heartbeat_interval_ms: 25,
      lease_ttl_ms: 100,
      sync_interval_ms: 25
    }
  });

  try {
    await cluster_node_agent.start();

    await WaitForCondition({
      timeout_ms: 2_500,
      condition_func: (): boolean => {
        const discovery_snapshot = cluster_node_agent.getDiscoverySnapshot();
        return (discovery_snapshot.store_metrics?.update_error_total ?? 0) > 0;
      }
    });

    const discovery_snapshot = cluster_node_agent.getDiscoverySnapshot();
    assert.equal(discovery_snapshot.enabled, true);
    assert.equal((discovery_snapshot.store_metrics?.update_error_total ?? 0) > 0, true);
  } finally {
    await cluster_node_agent.stop();
  }
});
