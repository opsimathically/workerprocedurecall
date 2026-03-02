import assert from 'node:assert/strict';
import test from 'node:test';

import {
  WorkerProcedureCall,
  type cluster_call_request_message_i,
  type handle_cluster_call_response_t
} from '../../src/index';

function BuildClusterCallRequest(params: {
  request_id: string;
  function_name: string;
  routing_hint?: {
    mode: 'auto' | 'target_node' | 'affinity';
    target_node_id?: string;
    affinity_key?: string;
    zone?: string;
  };
  metadata?: Record<string, unknown>;
}): cluster_call_request_message_i {
  const { request_id, function_name, routing_hint, metadata } = params;
  return {
    protocol_version: 1,
    message_type: 'cluster_call_request',
    timestamp_unix_ms: Date.now(),
    request_id,
    trace_id: `${request_id}_trace`,
    span_id: `${request_id}_span`,
    attempt_index: 1,
    max_attempts: 1,
    deadline_unix_ms: Date.now() + 30_000,
    function_name,
    args: [],
    routing_hint: routing_hint ?? {
      mode: 'auto'
    },
    caller_identity: {
      subject: 'phase12_client',
      tenant_id: 'tenant_1',
      scopes: ['rpc.call:*'],
      signed_claims: 'phase12_signed_claims'
    },
    metadata
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
      function_hash_sha1: request.function_hash_sha1 ?? 'phase12_hash',
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

test('phase12 routing distributes load across healthy nodes', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  let node_a_call_count = 0;
  let node_b_call_count = 0;

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase12_route_distribution_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*']
      }
    ]
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_a'
    },
    capability_list: [
      {
        function_name: 'Phase12RouteFunction',
        function_hash_sha1: 'hash_phase12_route',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      node_a_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_a',
        return_value: 'node_a'
      });
    }
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_b'
    },
    capability_list: [
      {
        function_name: 'Phase12RouteFunction',
        function_hash_sha1: 'hash_phase12_route',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      node_b_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_b',
        return_value: 'node_b'
      });
    }
  });

  const total_request_count = 200;
  for (let request_index = 0; request_index < total_request_count; request_index += 1) {
    const response = await workerprocedurecall.handleClusterCallRequest({
      node_id: 'node_local_phase12_distribution',
      message: BuildClusterCallRequest({
        request_id: `phase12_distribution_${request_index}`,
        function_name: 'Phase12RouteFunction'
      })
    });

    assert.equal(response.terminal_message.message_type, 'cluster_call_response_success');
  }

  assert.equal(node_a_call_count + node_b_call_count, total_request_count);
  assert.equal(node_a_call_count > 0, true);
  assert.equal(node_b_call_count > 0, true);
});

test('phase12 routing avoids unhealthy nodes', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  let healthy_node_call_count = 0;
  let unhealthy_node_call_count = 0;

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase12_route_health_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*']
      }
    ]
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_healthy'
    },
    initial_health_state: 'ready',
    capability_list: [
      {
        function_name: 'Phase12HealthFunction',
        function_hash_sha1: 'hash_phase12_health',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      healthy_node_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_healthy',
        return_value: 'healthy'
      });
    }
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_unhealthy'
    },
    initial_health_state: 'stopped',
    capability_list: [
      {
        function_name: 'Phase12HealthFunction',
        function_hash_sha1: 'hash_phase12_health',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      unhealthy_node_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_unhealthy',
        return_value: 'unhealthy'
      });
    }
  });

  const response = await workerprocedurecall.handleClusterCallRequest({
    node_id: 'node_local_phase12_health',
    message: BuildClusterCallRequest({
      request_id: 'phase12_health_1',
      function_name: 'Phase12HealthFunction'
    })
  });

  assert.equal(response.terminal_message.message_type, 'cluster_call_response_success');
  if (response.terminal_message.message_type === 'cluster_call_response_success') {
    assert.equal(response.terminal_message.node_id, 'node_healthy');
  }
  assert.equal(healthy_node_call_count, 1);
  assert.equal(unhealthy_node_call_count, 0);
});

test('phase12 routing applies heartbeat and capability updates to node health decisions', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  let node_primary_call_count = 0;
  let node_secondary_call_count = 0;

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase12_route_heartbeat_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*']
      }
    ]
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_primary'
    },
    initial_health_state: 'ready',
    capability_list: [
      {
        function_name: 'Phase12HeartbeatFunction',
        function_hash_sha1: 'hash_phase12_heartbeat',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      node_primary_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_primary',
        return_value: 'primary'
      });
    }
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_secondary'
    },
    initial_health_state: 'stopped',
    capability_list: [],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      node_secondary_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_secondary',
        return_value: 'secondary'
      });
    }
  });

  const initial_response = await workerprocedurecall.handleClusterCallRequest({
    node_id: 'node_local_phase12_heartbeat',
    message: BuildClusterCallRequest({
      request_id: 'phase12_heartbeat_before_update',
      function_name: 'Phase12HeartbeatFunction'
    })
  });
  assert.equal(initial_response.terminal_message.message_type, 'cluster_call_response_success');
  if (initial_response.terminal_message.message_type === 'cluster_call_response_success') {
    assert.equal(initial_response.terminal_message.node_id, 'node_primary');
  }

  const heartbeat_timestamp_unix_ms = Date.now();
  workerprocedurecall.recordClusterCallNodeHeartbeat({
    heartbeat: {
      protocol_version: 1,
      message_type: 'node_heartbeat',
      timestamp_unix_ms: heartbeat_timestamp_unix_ms,
      node_id: 'node_primary',
      health_state: 'stopped',
      metrics: {
        inflight_calls: 0,
        pending_calls: 0,
        success_rate_1m: 1,
        timeout_rate_1m: 0,
        ewma_latency_ms: 0
      }
    }
  });

  workerprocedurecall.recordClusterCallNodeHeartbeat({
    heartbeat: {
      protocol_version: 1,
      message_type: 'node_heartbeat',
      timestamp_unix_ms: heartbeat_timestamp_unix_ms,
      node_id: 'node_secondary',
      health_state: 'ready',
      metrics: {
        inflight_calls: 0,
        pending_calls: 0,
        success_rate_1m: 1,
        timeout_rate_1m: 0,
        ewma_latency_ms: 0
      }
    }
  });

  workerprocedurecall.recordClusterCallNodeCapabilities({
    announcement: {
      protocol_version: 1,
      message_type: 'node_capability_announce',
      timestamp_unix_ms: heartbeat_timestamp_unix_ms,
      node_id: 'node_secondary',
      capabilities: [
        {
          function_name: 'Phase12HeartbeatFunction',
          function_hash_sha1: 'hash_phase12_heartbeat',
          installed: true
        }
      ]
    }
  });

  const updated_response = await workerprocedurecall.handleClusterCallRequest({
    node_id: 'node_local_phase12_heartbeat',
    message: BuildClusterCallRequest({
      request_id: 'phase12_heartbeat_after_update',
      function_name: 'Phase12HeartbeatFunction'
    })
  });
  assert.equal(updated_response.terminal_message.message_type, 'cluster_call_response_success');
  if (updated_response.terminal_message.message_type === 'cluster_call_response_success') {
    assert.equal(updated_response.terminal_message.node_id, 'node_secondary');
  }

  assert.equal(node_primary_call_count, 1);
  assert.equal(node_secondary_call_count, 1);
});

test('phase12 routing honors pinned target node requests', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  let pinned_node_call_count = 0;
  let other_node_call_count = 0;

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase12_route_pinning_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*']
      }
    ]
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_pinned'
    },
    capability_list: [
      {
        function_name: 'Phase12PinnedFunction',
        function_hash_sha1: 'hash_phase12_pinned',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      pinned_node_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_pinned',
        return_value: 'pinned'
      });
    }
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_other'
    },
    capability_list: [
      {
        function_name: 'Phase12PinnedFunction',
        function_hash_sha1: 'hash_phase12_pinned',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      other_node_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_other',
        return_value: 'other'
      });
    }
  });

  const response = await workerprocedurecall.handleClusterCallRequest({
    node_id: 'node_local_phase12_pinning',
    message: BuildClusterCallRequest({
      request_id: 'phase12_pinned_1',
      function_name: 'Phase12PinnedFunction',
      routing_hint: {
        mode: 'target_node',
        target_node_id: 'node_pinned'
      }
    })
  });

  assert.equal(response.terminal_message.message_type, 'cluster_call_response_success');
  if (response.terminal_message.message_type === 'cluster_call_response_success') {
    assert.equal(response.terminal_message.node_id, 'node_pinned');
  }
  assert.equal(pinned_node_call_count, 1);
  assert.equal(other_node_call_count, 0);
});

test('phase12 routing retries alternate node on retryable node dispatch failure', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  let failing_node_call_count = 0;
  let fallback_node_call_count = 0;

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase12_route_failover_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*']
      }
    ]
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_fail'
    },
    initial_metrics: {
      inflight_calls: 0,
      pending_calls: 0,
      ewma_latency_ms: 1,
      success_rate_1m: 1,
      timeout_rate_1m: 0
    },
    capability_list: [
      {
        function_name: 'Phase12FailoverFunction',
        function_hash_sha1: 'hash_phase12_failover',
        installed: true
      }
    ],
    call_executor: async function (): Promise<handle_cluster_call_response_t> {
      failing_node_call_count += 1;
      throw new Error('phase12_simulated_dispatch_failure');
    }
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_fallback'
    },
    initial_metrics: {
      inflight_calls: 200,
      pending_calls: 200,
      ewma_latency_ms: 500,
      success_rate_1m: 1,
      timeout_rate_1m: 0
    },
    capability_list: [
      {
        function_name: 'Phase12FailoverFunction',
        function_hash_sha1: 'hash_phase12_failover',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      fallback_node_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_fallback',
        return_value: 'fallback'
      });
    }
  });

  const response = await workerprocedurecall.handleClusterCallRequest({
    node_id: 'node_local_phase12_failover',
    message: BuildClusterCallRequest({
      request_id: 'phase12_failover_1',
      function_name: 'Phase12FailoverFunction'
    })
  });

  assert.equal(response.terminal_message.message_type, 'cluster_call_response_success');
  if (response.terminal_message.message_type === 'cluster_call_response_success') {
    assert.equal(response.terminal_message.node_id, 'node_fallback');
  }
  assert.equal(failing_node_call_count, 1);
  assert.equal(fallback_node_call_count, 1);

  const routing_metrics = workerprocedurecall.getClusterCallRoutingMetrics();
  assert.equal(routing_metrics.failover_attempt_total > 0, true);
  assert.equal(routing_metrics.dispatch_retryable_total > 0, true);
});

test('phase12 routing integrates authorization scope checks with node selectors', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  let allowed_node_call_count = 0;
  let denied_node_call_count = 0;

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase12_route_scope_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*'],
        constraints: {
          tenant: 'tenant_1',
          environment: 'prod',
          cluster: 'cluster_alpha',
          node_selector: {
            labels: {
              routing_group: 'allowed'
            }
          }
        }
      }
    ]
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_allowed',
      labels: {
        routing_group: 'allowed'
      }
    },
    capability_list: [
      {
        function_name: 'Phase12ScopedFunction',
        function_hash_sha1: 'hash_phase12_scope',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      allowed_node_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_allowed',
        return_value: 'allowed'
      });
    }
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'node_denied',
      labels: {
        routing_group: 'denied'
      }
    },
    capability_list: [
      {
        function_name: 'Phase12ScopedFunction',
        function_hash_sha1: 'hash_phase12_scope',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      denied_node_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'node_denied',
        return_value: 'denied'
      });
    }
  });

  const response = await workerprocedurecall.handleClusterCallRequest({
    node_id: 'node_local_phase12_scope',
    message: BuildClusterCallRequest({
      request_id: 'phase12_scope_1',
      function_name: 'Phase12ScopedFunction',
      metadata: {
        environment: 'prod',
        cluster: 'cluster_alpha'
      }
    })
  });

  assert.equal(response.terminal_message.message_type, 'cluster_call_response_success');
  if (response.terminal_message.message_type === 'cluster_call_response_success') {
    assert.equal(response.terminal_message.node_id, 'node_allowed');
  }

  assert.equal(allowed_node_call_count, 1);
  assert.equal(denied_node_call_count, 0);

  const routing_metrics = workerprocedurecall.getClusterCallRoutingMetrics();
  assert.equal(routing_metrics.filtered_authorization_node_total > 0, true);
});
