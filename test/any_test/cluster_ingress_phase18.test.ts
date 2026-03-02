import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ClusterClient,
  ClusterClientError,
  ClusterControlPlaneService,
  ClusterIngressBalancerService,
  ClusterIngressRoutingEngine,
  ClusterIngressTargetResolver,
  ClusterNodeAgent,
  ParseIngressCallRequestMessage,
  WorkerProcedureCall,
  type cluster_call_request_message_i
} from '../../src/index';

function Sleep(params: { delay_ms: number }): Promise<void> {
  return new Promise<void>((resolve): void => {
    setTimeout(resolve, params.delay_ms);
  });
}

async function WaitForCondition(params: {
  timeout_ms: number;
  condition_func: () => boolean | Promise<boolean>;
}): Promise<void> {
  const started_unix_ms = Date.now();

  while (Date.now() - started_unix_ms <= params.timeout_ms) {
    if (await params.condition_func()) {
      return;
    }

    await Sleep({
      delay_ms: 25
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
      policy_id: 'phase18_allow_all_calls',
      effect: 'allow',
      capabilities: ['rpc.call:*']
    }
  ];
}

function BuildValidCallRequest(params: {
  request_id: string;
  function_name?: string;
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
    deadline_unix_ms: Date.now() + 10_000,
    function_name: params.function_name ?? 'Phase18Function',
    args: [],
    routing_hint: {
      mode: 'auto'
    },
    caller_identity: {
      subject: 'phase18_client_subject',
      tenant_id: 'tenant_1',
      scopes: ['rpc.call:*'],
      signed_claims: 'phase18_signed_claims'
    }
  };
}

test('phase18 ingress validator accepts valid call request and rejects invalid deadline', function () {
  const valid_parse_result = ParseIngressCallRequestMessage({
    message: BuildValidCallRequest({
      request_id: 'phase18_validate_1'
    })
  });
  assert.equal(valid_parse_result.ok, true);

  const invalid_request = BuildValidCallRequest({
    request_id: 'phase18_validate_2'
  });
  invalid_request.deadline_unix_ms = Date.now() - 100;

  const invalid_parse_result = ParseIngressCallRequestMessage({
    message: invalid_request
  });
  assert.equal(invalid_parse_result.ok, false);
});

test('phase18 routing engine deterministic tie-break and target pinning work', function () {
  const routing_engine = new ClusterIngressRoutingEngine({
    default_mode: 'least_loaded'
  });

  const base_request = BuildValidCallRequest({
    request_id: 'phase18_routing_1'
  });

  const candidate_list = [
    {
      target_id: 'node_b',
      node_id: 'node_b',
      endpoint: {
        host: '127.0.0.1',
        port: 9002,
        request_path: '/wpc/cluster/protocol',
        tls_mode: 'disabled' as const
      },
      source: 'static' as const,
      health_state: 'ready' as const,
      inflight_calls: 1,
      pending_calls: 0,
      success_rate_1m: 1,
      timeout_rate_1m: 0,
      ewma_latency_ms: 10,
      capability_hash_by_function_name: {},
      updated_unix_ms: Date.now()
    },
    {
      target_id: 'node_a',
      node_id: 'node_a',
      endpoint: {
        host: '127.0.0.1',
        port: 9001,
        request_path: '/wpc/cluster/protocol',
        tls_mode: 'disabled' as const
      },
      source: 'static' as const,
      health_state: 'ready' as const,
      inflight_calls: 1,
      pending_calls: 0,
      success_rate_1m: 1,
      timeout_rate_1m: 0,
      ewma_latency_ms: 10,
      capability_hash_by_function_name: {},
      updated_unix_ms: Date.now()
    }
  ];

  const least_loaded_plan = routing_engine.buildDispatchPlan({
    request: base_request,
    candidate_list
  });
  assert.equal(least_loaded_plan.ordered_candidate_list[0].target_id, 'node_a');

  const pin_request = {
    ...base_request,
    request_id: 'phase18_routing_2',
    routing_hint: {
      mode: 'target_node' as const,
      target_node_id: 'node_b'
    }
  };

  const pinned_plan = routing_engine.buildDispatchPlan({
    request: pin_request,
    candidate_list
  });
  assert.equal(pinned_plan.ordered_candidate_list[0].target_id, 'node_b');
});

test('phase18 resolver filtering honors tenant/zone/capability constraints', async function () {
  const resolver = new ClusterIngressTargetResolver({
    static_target_list: [
      {
        target_id: 'target_match',
        endpoint: {
          host: '127.0.0.1',
          port: 9101,
          request_path: '/wpc/cluster/protocol',
          tls_mode: 'disabled'
        },
        zones: ['zone_1'],
        capability_hash_by_function_name: {
          Phase18Function: 'hash_1'
        },
        labels: {
          env: 'prod'
        }
      },
      {
        target_id: 'target_wrong_zone',
        endpoint: {
          host: '127.0.0.1',
          port: 9102,
          request_path: '/wpc/cluster/protocol',
          tls_mode: 'disabled'
        },
        zones: ['zone_2'],
        capability_hash_by_function_name: {
          Phase18Function: 'hash_1'
        }
      }
    ],
    refresh_interval_ms: 50,
    stale_snapshot_max_age_ms: 5_000
  });

  try {
    resolver.start();
    await resolver.refreshTargetSnapshot();

    const request = BuildValidCallRequest({
      request_id: 'phase18_filter_1'
    });
    request.function_hash_sha1 = 'hash_1';
    request.routing_hint.zone = 'zone_1';

    const eligible_result = await resolver.getEligibleTargetsForRequest({
      request
    });
    assert.equal(eligible_result.candidate_list.length, 1);
    assert.equal(eligible_result.candidate_list[0].target_id, 'target_match');
  } finally {
    resolver.stop();
  }
});

test('phase18 ingress routes client call through static target successfully', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });

  await workerprocedurecall.defineWorkerFunction({
    name: 'Phase18Echo',
    worker_func: async function (): Promise<string> {
      return 'PHASE18_OK';
    }
  });

  const node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase18_node_single',
    worker_start_count: 1,
    discovery: {
      enabled: false
    }
  });

  let ingress_service: ClusterIngressBalancerService | null = null;
  let cluster_client: ClusterClient | null = null;

  try {
    const node_address = await node_agent.start();

    ingress_service = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      static_target_list: [
        {
          target_id: 'phase18_node_single',
          endpoint: {
            host: node_address.host,
            port: node_address.port,
            request_path: node_address.request_path,
            tls_mode: node_address.tls_mode
          },
          capability_hash_by_function_name: {
            Phase18Echo: 'unknown'
          }
        }
      ]
    });

    const ingress_address = await ingress_service.start();

    cluster_client = new ClusterClient({
      host: ingress_address.host,
      port: ingress_address.port,
      request_path: ingress_address.request_path,
      auth_context: {
        subject: 'phase18_client',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase18_signed_claims'
      }
    });

    await cluster_client.connect();

    const call_result = await cluster_client.call<string>({
      function_name: 'Phase18Echo'
    });

    assert.equal(call_result, 'PHASE18_OK');
    assert.equal(ingress_service.getMetrics().ingress_success_total > 0, true);
  } finally {
    await cluster_client?.close();
    await ingress_service?.stop();
    await node_agent.stop();
  }
});

test('phase18 ingress failover retries alternate target when primary target is unavailable', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });

  await workerprocedurecall.defineWorkerFunction({
    name: 'Phase18Failover',
    worker_func: async function (): Promise<string> {
      return 'PHASE18_FAILOVER_OK';
    }
  });

  const healthy_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase18_node_healthy',
    worker_start_count: 1,
    discovery: {
      enabled: false
    }
  });

  let ingress_service: ClusterIngressBalancerService | null = null;
  let cluster_client: ClusterClient | null = null;

  try {
    const healthy_address = await healthy_node_agent.start();

    ingress_service = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      max_attempts: 2,
      static_target_list: [
        {
          target_id: 'phase18_down_target',
          endpoint: {
            host: '127.0.0.1',
            port: 65_000,
            request_path: '/wpc/cluster/protocol',
            tls_mode: 'disabled'
          }
        },
        {
          target_id: 'phase18_node_healthy',
          endpoint: {
            host: healthy_address.host,
            port: healthy_address.port,
            request_path: healthy_address.request_path,
            tls_mode: healthy_address.tls_mode
          }
        }
      ]
    });

    const ingress_address = await ingress_service.start();
    cluster_client = new ClusterClient({
      host: ingress_address.host,
      port: ingress_address.port,
      request_path: ingress_address.request_path,
      auth_context: {
        subject: 'phase18_client_failover',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase18_failover_signed_claims'
      }
    });

    await cluster_client.connect();
    const result = await cluster_client.call<string>({
      function_name: 'Phase18Failover'
    });
    assert.equal(result, 'PHASE18_FAILOVER_OK');

    const ingress_metrics = ingress_service.getMetrics();
    assert.equal(ingress_metrics.ingress_retry_total >= 1, true);
    assert.equal(ingress_metrics.ingress_failover_total >= 1, true);
  } finally {
    await cluster_client?.close();
    await ingress_service?.stop();
    await healthy_node_agent.stop();
  }
});

test('phase18 ingress honors explicit target pinning and rejects invalid pin', async function () {
  const workerprocedurecall_a = new WorkerProcedureCall();
  const workerprocedurecall_b = new WorkerProcedureCall();

  workerprocedurecall_a.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });
  workerprocedurecall_b.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });

  await workerprocedurecall_a.defineWorkerFunction({
    name: 'Phase18WhoAmI',
    worker_func: async function (): Promise<string> {
      return 'NODE_A';
    }
  });
  await workerprocedurecall_b.defineWorkerFunction({
    name: 'Phase18WhoAmI',
    worker_func: async function (): Promise<string> {
      return 'NODE_B';
    }
  });

  const node_agent_a = new ClusterNodeAgent({
    workerprocedurecall: workerprocedurecall_a,
    node_id: 'node_a',
    worker_start_count: 1,
    discovery: {
      enabled: false
    }
  });
  const node_agent_b = new ClusterNodeAgent({
    workerprocedurecall: workerprocedurecall_b,
    node_id: 'node_b',
    worker_start_count: 1,
    discovery: {
      enabled: false
    }
  });

  let ingress_service: ClusterIngressBalancerService | null = null;
  let cluster_client: ClusterClient | null = null;

  try {
    const address_a = await node_agent_a.start();
    const address_b = await node_agent_b.start();

    ingress_service = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      static_target_list: [
        {
          target_id: 'node_a',
          node_id: 'node_a',
          endpoint: {
            host: address_a.host,
            port: address_a.port,
            request_path: address_a.request_path,
            tls_mode: address_a.tls_mode
          }
        },
        {
          target_id: 'node_b',
          node_id: 'node_b',
          endpoint: {
            host: address_b.host,
            port: address_b.port,
            request_path: address_b.request_path,
            tls_mode: address_b.tls_mode
          }
        }
      ]
    });

    const ingress_address = await ingress_service.start();
    cluster_client = new ClusterClient({
      host: ingress_address.host,
      port: ingress_address.port,
      request_path: ingress_address.request_path,
      auth_context: {
        subject: 'phase18_client_pin',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase18_pin_signed_claims'
      }
    });

    await cluster_client.connect();
    const pinned_result = await cluster_client.call<string>({
      function_name: 'Phase18WhoAmI',
      routing_hint: {
        mode: 'target_node',
        target_node_id: 'node_b'
      }
    });
    assert.equal(pinned_result, 'NODE_B');

    await assert.rejects(
      async (): Promise<void> => {
        await cluster_client?.call<string>({
          function_name: 'Phase18WhoAmI',
          routing_hint: {
            mode: 'target_node',
            target_node_id: 'missing_node'
          }
        });
      },
      (error: unknown): boolean => {
        return (
          error instanceof ClusterClientError &&
          error.code === 'INGRESS_FORBIDDEN'
        );
      }
    );
  } finally {
    await cluster_client?.close();
    await ingress_service?.stop();
    await node_agent_a.stop();
    await node_agent_b.stop();
  }
});

test('phase18 ingress uses control-plane topology updates without restart', async function () {
  const control_plane_service = new ClusterControlPlaneService({
    host: '127.0.0.1',
    port: 0
  });

  const workerprocedurecall_a = new WorkerProcedureCall();
  const workerprocedurecall_b = new WorkerProcedureCall();
  workerprocedurecall_a.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });
  workerprocedurecall_b.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });

  await workerprocedurecall_a.defineWorkerFunction({
    name: 'Phase18ControlPlaneWhoAmI',
    worker_func: async function (): Promise<string> {
      return 'CP_NODE_A';
    }
  });
  await workerprocedurecall_b.defineWorkerFunction({
    name: 'Phase18ControlPlaneWhoAmI',
    worker_func: async function (): Promise<string> {
      return 'CP_NODE_B';
    }
  });

  let node_agent_a: ClusterNodeAgent | null = null;
  let node_agent_b: ClusterNodeAgent | null = null;
  let ingress_service: ClusterIngressBalancerService | null = null;
  let cluster_client: ClusterClient | null = null;

  try {
    const control_plane_address = await control_plane_service.start();

    node_agent_a = new ClusterNodeAgent({
      workerprocedurecall: workerprocedurecall_a,
      node_id: 'cp_node_a',
      worker_start_count: 1,
      discovery: {
        enabled: false
      },
      control_plane: {
        enabled: true,
        endpoint: {
          host: control_plane_address.host,
          port: control_plane_address.port,
          request_path: control_plane_address.request_path,
          tls_mode: 'disabled'
        },
        sync_interval_ms: 100,
        heartbeat_interval_ms: 100
      }
    });

    node_agent_b = new ClusterNodeAgent({
      workerprocedurecall: workerprocedurecall_b,
      node_id: 'cp_node_b',
      worker_start_count: 1,
      discovery: {
        enabled: false
      },
      control_plane: {
        enabled: true,
        endpoint: {
          host: control_plane_address.host,
          port: control_plane_address.port,
          request_path: control_plane_address.request_path,
          tls_mode: 'disabled'
        },
        sync_interval_ms: 100,
        heartbeat_interval_ms: 100
      }
    });

    await node_agent_a.start();
    await node_agent_b.start();

    ingress_service = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      target_refresh_interval_ms: 100,
      stale_snapshot_max_age_ms: 5_000,
      target_resolver_config: {
        control_plane: {
          endpoint: {
            host: control_plane_address.host,
            port: control_plane_address.port,
            request_path: control_plane_address.request_path,
            tls_mode: 'disabled'
          }
        },
        refresh_interval_ms: 100,
        stale_snapshot_max_age_ms: 5_000
      }
    });

    const ingress_address = await ingress_service.start();
    cluster_client = new ClusterClient({
      host: ingress_address.host,
      port: ingress_address.port,
      request_path: ingress_address.request_path,
      auth_context: {
        subject: 'phase18_cp_client',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase18_cp_signed_claims'
      }
    });
    await cluster_client.connect();

    await WaitForCondition({
      timeout_ms: 5_000,
      condition_func: (): boolean => {
        return ingress_service?.getSnapshot().target_snapshot.target_list.length === 2;
      }
    });

    await node_agent_a.stop();
    node_agent_a = null;

    await WaitForCondition({
      timeout_ms: 5_000,
      condition_func: (): boolean => {
        const target_list = ingress_service?.getSnapshot().target_snapshot.target_list ?? [];
        const target_a = target_list.find((target) => target.node_id === 'cp_node_a');
        const target_b = target_list.find((target) => target.node_id === 'cp_node_b');

        return (
          target_a?.health_state === 'stopped' &&
          target_b?.health_state === 'ready'
        );
      }
    });

    const result = await cluster_client.call<string>({
      function_name: 'Phase18ControlPlaneWhoAmI'
    });
    assert.equal(result, 'CP_NODE_B');
  } finally {
    await cluster_client?.close();
    await ingress_service?.stop();
    await node_agent_a?.stop().catch((): void => {});
    await node_agent_b?.stop().catch((): void => {});
    await control_plane_service.stop();
  }
});

test('phase18 ingress degraded mode uses last-known-good snapshot and then fails closed after staleness limit', async function () {
  const control_plane_service = new ClusterControlPlaneService({
    host: '127.0.0.1',
    port: 0
  });

  const workerprocedurecall = new WorkerProcedureCall();
  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });
  await workerprocedurecall.defineWorkerFunction({
    name: 'Phase18DegradedCall',
    worker_func: async function (): Promise<string> {
      return 'PHASE18_DEGRADED_OK';
    }
  });

  let node_agent: ClusterNodeAgent | null = null;
  let ingress_service: ClusterIngressBalancerService | null = null;
  let cluster_client: ClusterClient | null = null;

  try {
    const control_plane_address = await control_plane_service.start();

    node_agent = new ClusterNodeAgent({
      workerprocedurecall,
      node_id: 'phase18_degraded_node',
      worker_start_count: 1,
      discovery: {
        enabled: false
      },
      control_plane: {
        enabled: true,
        endpoint: {
          host: control_plane_address.host,
          port: control_plane_address.port,
          request_path: control_plane_address.request_path,
          tls_mode: 'disabled'
        },
        sync_interval_ms: 100,
        heartbeat_interval_ms: 100
      }
    });

    await node_agent.start();

    ingress_service = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      target_refresh_interval_ms: 100,
      stale_snapshot_max_age_ms: 500,
      target_resolver_config: {
        control_plane: {
          endpoint: {
            host: control_plane_address.host,
            port: control_plane_address.port,
            request_path: control_plane_address.request_path,
            tls_mode: 'disabled'
          }
        },
        refresh_interval_ms: 100,
        stale_snapshot_max_age_ms: 500
      }
    });

    const ingress_address = await ingress_service.start();

    cluster_client = new ClusterClient({
      host: ingress_address.host,
      port: ingress_address.port,
      request_path: ingress_address.request_path,
      auth_context: {
        subject: 'phase18_degraded_client',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase18_degraded_signed_claims'
      }
    });
    await cluster_client.connect();

    const initial_result = await cluster_client.call<string>({
      function_name: 'Phase18DegradedCall'
    });
    assert.equal(initial_result, 'PHASE18_DEGRADED_OK');

    await control_plane_service.stop();

    const stale_but_usable_result = await cluster_client.call<string>({
      function_name: 'Phase18DegradedCall'
    });
    assert.equal(stale_but_usable_result, 'PHASE18_DEGRADED_OK');

    await Sleep({
      delay_ms: 800
    });

    await assert.rejects(
      async (): Promise<void> => {
        await cluster_client?.call<string>({
          function_name: 'Phase18DegradedCall'
        });
      },
      (error: unknown): boolean => {
        return (
          error instanceof ClusterClientError &&
          error.code === 'INGRESS_NO_TARGET'
        );
      }
    );
  } finally {
    await cluster_client?.close();
    await ingress_service?.stop();
    await node_agent?.stop().catch((): void => {});
    await control_plane_service.stop().catch((): void => {});
  }
});
