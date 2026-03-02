import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ClusterInMemoryServiceDiscoveryStore,
  ClusterNodeAgent,
  WorkerProcedureCall,
  type cluster_call_request_message_i,
  type handle_cluster_call_response_t
} from '../../src/index';

function BuildAllowAllCallPolicyList(): {
  policy_id: string;
  effect: 'allow';
  capabilities: string[];
}[] {
  return [
    {
      policy_id: 'phase14_allow_all_calls',
      effect: 'allow',
      capabilities: ['rpc.call:*']
    }
  ];
}

function BuildCallRequest(params: {
  request_id: string;
  function_name: string;
  args?: unknown[];
  routing_hint?: {
    mode: 'auto' | 'target_node' | 'affinity';
    target_node_id?: string;
    affinity_key?: string;
    zone?: string;
  };
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
    routing_hint: params.routing_hint ?? {
      mode: 'auto'
    },
    caller_identity: {
      subject: 'phase14_client',
      tenant_id: 'tenant_1',
      scopes: ['rpc.call:*'],
      signed_claims: 'phase14_signed_claims'
    }
  };
}

function BuildSuccessfulNodeResponse(params: {
  request: cluster_call_request_message_i;
  node_id: string;
  return_value: unknown;
}): handle_cluster_call_response_t {
  const { request, node_id, return_value } = params;
  const now_unix_ms = Date.now();

  return {
    ack: {
      protocol_version: 1,
      message_type: 'cluster_call_ack',
      timestamp_unix_ms: now_unix_ms,
      request_id: request.request_id,
      attempt_index: request.attempt_index,
      node_id,
      accepted: true,
      queue_position: 0,
      estimated_start_delay_ms: 0
    },
    terminal_message: {
      protocol_version: 1,
      message_type: 'cluster_call_response_success',
      timestamp_unix_ms: now_unix_ms,
      request_id: request.request_id,
      attempt_index: request.attempt_index,
      node_id,
      function_name: request.function_name,
      function_hash_sha1: request.function_hash_sha1 ?? 'phase14_hash',
      return_value,
      timing: {
        gateway_received_unix_ms: now_unix_ms,
        node_received_unix_ms: now_unix_ms,
        worker_started_unix_ms: now_unix_ms,
        worker_finished_unix_ms: now_unix_ms
      }
    }
  };
}

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

function BuildPermissiveAuthenticateRequest() {
  return function () {
    return {
      ok: true as const,
      identity: {
        subject: 'phase14_inter_node',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase14_transport_signed_claims'
      }
    };
  };
}

test('service discovery store supports register heartbeat capability query and remove', function () {
  const discovery_store = new ClusterInMemoryServiceDiscoveryStore();

  discovery_store.setConfig({
    config: {
      default_lease_ttl_ms: 5_000,
      expiration_check_interval_ms: 100,
      event_history_max_count: 100
    }
  });

  discovery_store.upsertNodeRegistration({
    node_identity: {
      node_id: 'node_store_1',
      address: {
        host: '127.0.0.1',
        port: 9001,
        request_path: '/wpc/cluster/protocol',
        tls_mode: 'disabled'
      },
      labels: {
        env: 'test',
        role: 'api'
      },
      zones: ['zone_a']
    },
    status: 'ready',
    metrics: {
      inflight_calls: 1
    },
    capability_list: [
      {
        function_name: 'Phase14FunctionA',
        function_hash_sha1: 'hash_a',
        installed: true
      }
    ]
  });

  discovery_store.updateNodeHeartbeat({
    node_id: 'node_store_1',
    status: 'degraded',
    metrics: {
      pending_calls: 2,
      ewma_latency_ms: 12
    },
    lease_ttl_ms: 6_000
  });

  discovery_store.updateNodeCapabilities({
    node_id: 'node_store_1',
    capability_list: [
      {
        function_name: 'Phase14FunctionB',
        function_hash_sha1: 'hash_b',
        installed: true
      }
    ]
  });

  const queried_nodes = discovery_store.queryNodes({
    label_match: {
      env: 'test'
    },
    zone: 'zone_a',
    capability_function_name: 'Phase14FunctionB'
  });

  assert.equal(queried_nodes.length, 1);
  assert.equal(queried_nodes[0].node_identity.node_id, 'node_store_1');
  assert.equal(queried_nodes[0].status, 'degraded');

  const metrics = discovery_store.getMetrics();
  assert.equal(metrics.node_count, 1);
  assert.equal(metrics.node_registered_total > 0, true);
  assert.equal(metrics.node_heartbeat_updated_total > 0, true);
  assert.equal(metrics.node_capability_updated_total > 0, true);

  const removed = discovery_store.removeNode({
    node_id: 'node_store_1',
    reason: 'phase14_test_cleanup'
  });

  assert.equal(removed, true);
  assert.equal(
    discovery_store.getNodeById({
      node_id: 'node_store_1',
      include_expired: true
    }),
    null
  );
});

test('service discovery store expires stale nodes deterministically', function () {
  const discovery_store = new ClusterInMemoryServiceDiscoveryStore();

  const now_unix_ms = Date.now();

  discovery_store.upsertNodeRegistration({
    node_identity: {
      node_id: 'node_expire_1',
      address: {
        host: '127.0.0.1',
        port: 9002,
        request_path: '/wpc/cluster/protocol',
        tls_mode: 'disabled'
      }
    },
    status: 'ready',
    lease_ttl_ms: 25,
    now_unix_ms
  });

  const expire_result = discovery_store.expireStaleNodes({
    now_unix_ms: now_unix_ms + 30
  });

  assert.deepEqual(expire_result.expired_node_id_list, ['node_expire_1']);

  const active_nodes = discovery_store.listNodes({
    include_expired: false
  });
  assert.equal(active_nodes.length, 0);

  const node_record = discovery_store.getNodeById({
    node_id: 'node_expire_1',
    include_expired: true
  });

  assert.notEqual(node_record, null);
  assert.equal(node_record?.status, 'expired');

  const recent_expire_events = discovery_store.getRecentEvents({
    event_name: 'node_expired'
  });
  assert.equal(recent_expire_events.length > 0, true);
});

test('node agent auto-registers in discovery, publishes capabilities, and deregisters on stop', async function () {
  const discovery_store = new ClusterInMemoryServiceDiscoveryStore();
  const workerprocedurecall = new WorkerProcedureCall();

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });

  await workerprocedurecall.defineWorkerFunction({
    name: 'Phase14DiscoveryLocalFunction',
    worker_func: async function (): Promise<string> {
      return 'phase14_discovery_ok';
    }
  });

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'node_phase14_discovery_local',
    worker_start_count: 1,
    transport: {
      authenticate_request: BuildPermissiveAuthenticateRequest()
    },
    discovery: {
      enabled: true,
      service_discovery_store: discovery_store,
      heartbeat_interval_ms: 30,
      lease_ttl_ms: 200,
      sync_interval_ms: 30,
      expiration_check_interval_ms: 30
    }
  });

  try {
    await cluster_node_agent.start();

    await WaitForCondition({
      timeout_ms: 2_500,
      condition_func: (): boolean => {
        const record = discovery_store.getNodeById({
          node_id: 'node_phase14_discovery_local',
          include_expired: true
        });

        if (!record) {
          return false;
        }

        return record.capability_list.some((capability): boolean => {
          return capability.function_name === 'Phase14DiscoveryLocalFunction';
        });
      }
    });

    await WaitForCondition({
      timeout_ms: 2_500,
      condition_func: (): boolean => {
        return (
          cluster_node_agent.getDiscoverySnapshot().metrics.heartbeat_update_total > 0
        );
      }
    });

    const discovery_snapshot = cluster_node_agent.getDiscoverySnapshot();
    assert.equal(discovery_snapshot.enabled, true);
    assert.equal(discovery_snapshot.metrics.register_success_total > 0, true);
    assert.equal(discovery_snapshot.metrics.heartbeat_update_total > 0, true);
  } finally {
    await cluster_node_agent.stop();
  }

  await WaitForCondition({
    timeout_ms: 1_000,
    condition_func: (): boolean => {
      return (
        discovery_store.getNodeById({
          node_id: 'node_phase14_discovery_local',
          include_expired: true
        }) === null
      );
    }
  });
});

test('routing uses discovery state and reroutes after discovered node changes', async function () {
  const discovery_store = new ClusterInMemoryServiceDiscoveryStore();

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
    name: 'Phase14RouteFunction',
    worker_func: async function (): Promise<string> {
      return 'node_b';
    }
  });

  await workerprocedurecall_node_c.defineWorkerFunction({
    name: 'Phase14RouteFunction',
    worker_func: async function (): Promise<string> {
      return 'node_c';
    }
  });

  const cluster_node_agent_gateway = new ClusterNodeAgent({
    workerprocedurecall: workerprocedurecall_gateway,
    node_id: 'node_phase14_gateway',
    transport: {
      authenticate_request: BuildPermissiveAuthenticateRequest()
    },
    discovery: {
      enabled: true,
      service_discovery_store: discovery_store,
      heartbeat_interval_ms: 30,
      lease_ttl_ms: 180,
      sync_interval_ms: 30,
      expiration_check_interval_ms: 30
    }
  });

  const cluster_node_agent_b = new ClusterNodeAgent({
    workerprocedurecall: workerprocedurecall_node_b,
    node_id: 'node_phase14_b',
    worker_start_count: 1,
    transport: {
      authenticate_request: BuildPermissiveAuthenticateRequest()
    },
    discovery: {
      enabled: true,
      service_discovery_store: discovery_store,
      heartbeat_interval_ms: 30,
      lease_ttl_ms: 180,
      sync_interval_ms: 30,
      expiration_check_interval_ms: 30
    }
  });

  const cluster_node_agent_c = new ClusterNodeAgent({
    workerprocedurecall: workerprocedurecall_node_c,
    node_id: 'node_phase14_c',
    worker_start_count: 1,
    transport: {
      authenticate_request: BuildPermissiveAuthenticateRequest()
    },
    discovery: {
      enabled: true,
      service_discovery_store: discovery_store,
      heartbeat_interval_ms: 30,
      lease_ttl_ms: 180,
      sync_interval_ms: 30,
      expiration_check_interval_ms: 30
    }
  });

  let node_b_stopped = false;

  try {
    await cluster_node_agent_gateway.start();
    await cluster_node_agent_b.start();
    await cluster_node_agent_c.start();

    await WaitForCondition({
      timeout_ms: 2_500,
      condition_func: (): boolean => {
        const node_id_set = new Set(
          workerprocedurecall_gateway
            .getClusterCallNodes()
            .map((node): string => node.node_id)
        );

        return (
          node_id_set.has('node_phase14_b') &&
          node_id_set.has('node_phase14_c')
        );
      }
    });

    const warmup_response = await workerprocedurecall_gateway.handleClusterCallRequest({
      node_id: 'node_phase14_gateway',
      message: BuildCallRequest({
        request_id: 'phase14_route_warmup',
        function_name: 'Phase14RouteFunction'
      })
    });

    assert.equal(warmup_response.terminal_message.message_type, 'cluster_call_response_success');

    await cluster_node_agent_b.stop();
    node_b_stopped = true;

    await WaitForCondition({
      timeout_ms: 2_500,
      condition_func: (): boolean => {
        const node_id_set = new Set(
          workerprocedurecall_gateway
            .getClusterCallNodes()
            .map((node): string => node.node_id)
        );

        return (
          !node_id_set.has('node_phase14_b') &&
          node_id_set.has('node_phase14_c')
        );
      }
    });

    for (let call_index = 0; call_index < 5; call_index += 1) {
      const response = await workerprocedurecall_gateway.handleClusterCallRequest({
        node_id: 'node_phase14_gateway',
        message: BuildCallRequest({
          request_id: `phase14_route_after_stop_${call_index}`,
          function_name: 'Phase14RouteFunction'
        })
      });

      assert.equal(response.terminal_message.message_type, 'cluster_call_response_success');
      if (response.terminal_message.message_type === 'cluster_call_response_success') {
        assert.equal(response.terminal_message.node_id, 'node_phase14_c');
      }
    }

    discovery_store.upsertNodeRegistration({
      node_identity: {
        node_id: 'node_phase14_fake_expiring',
        address: {
          host: '127.0.0.1',
          port: 65530,
          request_path: '/wpc/cluster/protocol',
          tls_mode: 'disabled'
        }
      },
      status: 'ready',
      capability_list: [
        {
          function_name: 'Phase14RouteFunction',
          function_hash_sha1: 'hash_fake',
          installed: true
        }
      ],
      lease_ttl_ms: 15
    });

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 20);
    });

    discovery_store.expireStaleNodes();

    await WaitForCondition({
      timeout_ms: 2_000,
      condition_func: (): boolean => {
        const node_id_set = new Set(
          workerprocedurecall_gateway
            .getClusterCallNodes()
            .map((node): string => node.node_id)
        );

        return !node_id_set.has('node_phase14_fake_expiring');
      }
    });

    const post_expiry_response = await workerprocedurecall_gateway.handleClusterCallRequest({
      node_id: 'node_phase14_gateway',
      message: BuildCallRequest({
        request_id: 'phase14_route_after_fake_expiry',
        function_name: 'Phase14RouteFunction'
      })
    });
    assert.equal(post_expiry_response.terminal_message.message_type, 'cluster_call_response_success');
    if (post_expiry_response.terminal_message.message_type === 'cluster_call_response_success') {
      assert.equal(post_expiry_response.terminal_message.node_id, 'node_phase14_c');
    }
  } finally {
    await cluster_node_agent_gateway.stop();
    if (!node_b_stopped) {
      await cluster_node_agent_b.stop();
    }
    await cluster_node_agent_c.stop();
  }
});

test('discovery disabled mode preserves manual node registration behavior', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_phase14_manual'
    },
    capability_list: [
      {
        function_name: 'Phase14ManualFunction',
        function_hash_sha1: 'phase14_manual_hash',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_phase14_manual',
        return_value: 'manual_ok'
      });
    }
  });

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'node_phase14_manual_gateway',
    transport: {
      authenticate_request: BuildPermissiveAuthenticateRequest()
    },
    discovery: {
      enabled: false
    }
  });

  try {
    await cluster_node_agent.start();

    const discovery_snapshot = cluster_node_agent.getDiscoverySnapshot();
    assert.equal(discovery_snapshot.enabled, false);

    const response = await workerprocedurecall.handleClusterCallRequest({
      node_id: 'node_phase14_manual_gateway',
      message: BuildCallRequest({
        request_id: 'phase14_manual_call',
        function_name: 'Phase14ManualFunction',
        routing_hint: {
          mode: 'target_node',
          target_node_id: 'node_phase14_manual'
        }
      })
    });

    assert.equal(response.terminal_message.message_type, 'cluster_call_response_success');
    if (response.terminal_message.message_type === 'cluster_call_response_success') {
      assert.equal(response.terminal_message.return_value, 'manual_ok');
      assert.equal(response.terminal_message.node_id, 'node_phase14_manual');
    }
  } finally {
    await cluster_node_agent.stop();
  }
});

test('discovery update staleness races do not crash call handling path', async function () {
  const discovery_store = new ClusterInMemoryServiceDiscoveryStore();

  const workerprocedurecall_gateway = new WorkerProcedureCall();
  const workerprocedurecall_node_c = new WorkerProcedureCall();

  workerprocedurecall_gateway.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });
  workerprocedurecall_node_c.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });

  await workerprocedurecall_node_c.defineWorkerFunction({
    name: 'Phase14RaceFunction',
    worker_func: async function (): Promise<string> {
      return 'race_ok';
    }
  });

  const cluster_node_agent_gateway = new ClusterNodeAgent({
    workerprocedurecall: workerprocedurecall_gateway,
    node_id: 'node_phase14_race_gateway',
    worker_start_count: 1,
    transport: {
      authenticate_request: BuildPermissiveAuthenticateRequest()
    },
    discovery: {
      enabled: true,
      service_discovery_store: discovery_store,
      heartbeat_interval_ms: 25,
      lease_ttl_ms: 140,
      sync_interval_ms: 25,
      expiration_check_interval_ms: 25
    }
  });

  const cluster_node_agent_c = new ClusterNodeAgent({
    workerprocedurecall: workerprocedurecall_node_c,
    node_id: 'node_phase14_race_c',
    worker_start_count: 1,
    transport: {
      authenticate_request: BuildPermissiveAuthenticateRequest()
    },
    discovery: {
      enabled: true,
      service_discovery_store: discovery_store,
      heartbeat_interval_ms: 25,
      lease_ttl_ms: 140,
      sync_interval_ms: 25,
      expiration_check_interval_ms: 25
    }
  });

  try {
    await cluster_node_agent_gateway.start();
    await cluster_node_agent_c.start();

    await WaitForCondition({
      timeout_ms: 2_500,
      condition_func: (): boolean => {
        return workerprocedurecall_gateway
          .getClusterCallNodes()
          .some((node): boolean => node.node_id === 'node_phase14_race_c');
      }
    });

    const call_response_promise_list: Promise<handle_cluster_call_response_t>[] = [];

    for (let call_index = 0; call_index < 25; call_index += 1) {
      call_response_promise_list.push(
        workerprocedurecall_gateway.handleClusterCallRequest({
          node_id: 'node_phase14_race_gateway',
          message: BuildCallRequest({
            request_id: `phase14_race_call_${call_index}`,
            function_name: 'Phase14RaceFunction'
          })
        })
      );
    }

    for (let update_index = 0; update_index < 20; update_index += 1) {
      discovery_store.updateNodeCapabilities({
        node_id: 'node_phase14_race_c',
        capability_list: [
          {
            function_name: 'Phase14RaceFunction',
            function_hash_sha1: `phase14_race_hash_${update_index % 2}`,
            installed: update_index % 3 !== 0
          }
        ]
      });

      if (update_index % 4 === 0) {
        discovery_store.updateNodeHeartbeat({
          node_id: 'node_phase14_race_c',
          status: update_index % 8 === 0 ? 'degraded' : 'ready',
          metrics: {
            inflight_calls: update_index
          },
          lease_ttl_ms: 140
        });
      }

      await new Promise<void>((resolve): void => {
        setTimeout(resolve, 5);
      });
    }

    const call_response_list = await Promise.all(call_response_promise_list);
    assert.equal(call_response_list.length, 25);

    for (const response of call_response_list) {
      assert.equal(
        response.terminal_message.message_type === 'cluster_call_response_success' ||
          response.terminal_message.message_type === 'cluster_call_response_error',
        true
      );
    }
  } finally {
    await cluster_node_agent_gateway.stop();
    await cluster_node_agent_c.stop();
  }
});
