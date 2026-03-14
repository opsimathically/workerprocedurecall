import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ClusterClient,
  ClusterClientError,
  ClusterNodeAgent,
  WorkerProcedureCall,
  type cluster_call_request_message_i,
  type handle_cluster_call_response_t
} from '../../src/index';
import {
  BuildSecureClientTlsConfig,
  BuildSecureNodeTransportSecurity
} from '../fixtures/secure_transport_config';

let phase13_sequence_number = 20_000;

function BuildSequenceToken(params: { prefix: string }): string {
  phase13_sequence_number += 1;
  return `${params.prefix}_${phase13_sequence_number}`;
}

function BuildCallRequest(params: {
  request_id: string;
  function_name: string;
  trace_id?: string;
  args?: unknown[];
}): cluster_call_request_message_i {
  return {
    protocol_version: 1,
    message_type: 'cluster_call_request',
    timestamp_unix_ms: Date.now(),
    request_id: params.request_id,
    trace_id: params.trace_id ?? `trace_${params.request_id}`,
    span_id: `span_${params.request_id}`,
    attempt_index: 1,
    max_attempts: 1,
    deadline_unix_ms: Date.now() + 30_000,
    function_name: params.function_name,
    args: params.args ?? [],
    routing_hint: {
      mode: 'auto'
    },
    caller_identity: {
      subject: 'phase13_client',
      tenant_id: 'tenant_1',
      scopes: ['rpc.call:*'],
      signed_claims: 'phase13_signed_claims'
    },
    metadata: {
      environment: 'prod',
      cluster: 'cluster_phase13'
    }
  };
}

function BuildSuccessfulNodeResponse(params: {
  request: cluster_call_request_message_i;
  node_id: string;
  return_value: unknown;
}): handle_cluster_call_response_t {
  const now_unix_ms = Date.now();

  return {
    ack: {
      protocol_version: 1,
      message_type: 'cluster_call_ack',
      timestamp_unix_ms: now_unix_ms,
      request_id: params.request.request_id,
      attempt_index: params.request.attempt_index,
      node_id: params.node_id,
      accepted: true,
      queue_position: 0,
      estimated_start_delay_ms: 0
    },
    terminal_message: {
      protocol_version: 1,
      message_type: 'cluster_call_response_success',
      timestamp_unix_ms: now_unix_ms,
      request_id: params.request.request_id,
      attempt_index: params.request.attempt_index,
      node_id: params.node_id,
      function_name: params.request.function_name,
      function_hash_sha1: params.request.function_hash_sha1 ?? 'phase13_hash',
      return_value: params.return_value,
      timing: {
        gateway_received_unix_ms: now_unix_ms,
        node_received_unix_ms: now_unix_ms,
        worker_started_unix_ms: now_unix_ms,
        worker_finished_unix_ms: now_unix_ms
      }
    }
  };
}

function BuildMutationRequest(params: {
  mutation_type: string;
  payload: Record<string, unknown>;
  target_scope: 'single_node' | 'node_selector' | 'cluster_wide';
  target_selector?: Record<string, unknown>;
  rollout_mode: 'single_node' | 'all_at_once' | 'rolling_percent' | 'canary_then_expand';
  rollback_policy?: Record<string, unknown>;
}): Record<string, unknown> {
  const mutation_suffix = BuildSequenceToken({ prefix: 'mutation_phase13' });

  return {
    protocol_version: 1,
    message_type: 'cluster_admin_mutation_request',
    timestamp_unix_ms: Date.now(),
    mutation_id: mutation_suffix,
    request_id: `request_${mutation_suffix}`,
    trace_id: `trace_${mutation_suffix}`,
    deadline_unix_ms: Date.now() + 60_000,
    target_scope: params.target_scope,
    target_selector: params.target_selector,
    mutation_type: params.mutation_type,
    payload: params.payload,
    dry_run: false,
    rollout_strategy: {
      mode: params.rollout_mode,
      min_success_percent: 100,
      batch_percent: 100,
      canary_node_count: 1,
      apply_timeout_ms: 5_000,
      verify_timeout_ms: 5_000,
      rollback_policy: params.rollback_policy
    },
    auth_context: {
      subject: 'admin_phase13@example.com',
      tenant_id: 'tenant_1',
      capability_claims: ['rpc.admin.mutate:*'],
      signed_claims: 'phase13_admin_signed_claims'
    }
  };
}

function BuildAuthHeaderProvider(params: { token: string }): () => Record<string, string> {
  return function (): Record<string, string> {
    return {
      authorization: `Bearer ${params.token}`
    };
  };
}

test('phase13 observability surfaces include structured lifecycle events with trace correlation', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase13_observability_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*', 'rpc.admin.mutate:*']
      }
    ]
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'phase13_obs_node'
    },
    capability_list: [
      {
        function_name: 'Phase13ObservedFunction',
        function_hash_sha1: 'phase13_observed_hash',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'phase13_obs_node',
        return_value: 'phase13_ok'
      });
    }
  });

  const request_id = BuildSequenceToken({ prefix: 'phase13_obs_call' });
  const trace_id = BuildSequenceToken({ prefix: 'trace_phase13_obs' });

  const call_response = await workerprocedurecall.handleClusterCallRequest({
    node_id: 'node_local_phase13_obs',
    message: BuildCallRequest({
      request_id,
      trace_id,
      function_name: 'Phase13ObservedFunction'
    })
  });

  assert.equal(call_response.terminal_message.message_type, 'cluster_call_response_success');

  const lifecycle_event_list = workerprocedurecall.getClusterOperationLifecycleEvents({
    request_id
  });

  assert.equal(lifecycle_event_list.some((event): boolean => {
    return event.event_name === 'cluster_call_received' && event.trace_id === trace_id;
  }), true);
  assert.equal(lifecycle_event_list.some((event): boolean => {
    return event.event_name === 'cluster_call_routed';
  }), true);
  assert.equal(lifecycle_event_list.some((event): boolean => {
    return event.event_name === 'cluster_call_completed';
  }), true);

  const observability_snapshot = workerprocedurecall.getClusterOperationsObservabilitySnapshot();
  assert.equal(observability_snapshot.call_routing_metrics.call_requests_total >= 1, true);
  assert.equal(observability_snapshot.cluster_call_node_summary.registered_node_count >= 1, true);

  const mutation_response = await workerprocedurecall.handleClusterAdminMutationRequest({
    node_id: 'node_local_phase13_obs',
    message: BuildMutationRequest({
      mutation_type: 'define_constant',
      payload: {
        name: 'phase13_observed_constant',
        value: 1
      },
      target_scope: 'single_node',
      target_selector: {
        node_ids: ['node_local_phase13_obs']
      },
      rollout_mode: 'single_node'
    })
  });

  assert.equal(mutation_response.terminal_message.message_type, 'cluster_admin_mutation_result');

  const mutation_event_list = workerprocedurecall.getClusterOperationLifecycleEvents({
    mutation_id: mutation_response.ack.mutation_id
  });
  assert.equal(mutation_event_list.some((event): boolean => {
    return event.event_name === 'admin_mutation_requested';
  }), true);
});

test('phase13 chaos: node crash during active load keeps call outcomes deterministic via failover', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase13_failover_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*']
      }
    ]
  });

  let primary_node_call_count = 0;
  let fallback_node_call_count = 0;
  let primary_node_marked_stopped = false;

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'phase13_primary_node'
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
        function_name: 'Phase13CrashFunction',
        function_hash_sha1: 'phase13_crash_hash',
        installed: true
      }
    ],
    call_executor: async function (): Promise<handle_cluster_call_response_t> {
      primary_node_call_count += 1;

      if (!primary_node_marked_stopped) {
        primary_node_marked_stopped = true;
        workerprocedurecall.recordClusterCallNodeHeartbeat({
          heartbeat: {
            protocol_version: 1,
            message_type: 'node_heartbeat',
            timestamp_unix_ms: Date.now(),
            node_id: 'phase13_primary_node',
            health_state: 'stopped',
            metrics: {
              inflight_calls: 0,
              pending_calls: 0,
              success_rate_1m: 0,
              timeout_rate_1m: 1,
              ewma_latency_ms: 999
            }
          }
        });
      }

      throw new Error('phase13_simulated_primary_node_crash');
    }
  });

  workerprocedurecall.registerClusterCallNode({
    node: {
      node_id: 'phase13_fallback_node'
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
        function_name: 'Phase13CrashFunction',
        function_hash_sha1: 'phase13_crash_hash',
        installed: true
      }
    ],
    call_executor: async function (params): Promise<handle_cluster_call_response_t> {
      fallback_node_call_count += 1;
      return BuildSuccessfulNodeResponse({
        request: params.request,
        node_id: 'phase13_fallback_node',
        return_value: 'recovered'
      });
    }
  });

  const request_count = 75;
  const response_list = await Promise.all(
    Array.from({ length: request_count }).map(async function (_unused, index): Promise<string> {
      const response = await workerprocedurecall.handleClusterCallRequest({
        node_id: 'node_local_phase13_crash',
        message: BuildCallRequest({
          request_id: `phase13_crash_req_${index}`,
          function_name: 'Phase13CrashFunction'
        })
      });

      if (response.terminal_message.message_type !== 'cluster_call_response_success') {
        return response.terminal_message.error.code;
      }

      return 'success';
    })
  );

  assert.equal(response_list.every((value): boolean => value === 'success'), true);
  assert.equal(primary_node_call_count >= 1, true);
  assert.equal(fallback_node_call_count >= request_count, true);

  const routing_metrics = workerprocedurecall.getClusterCallRoutingMetrics();
  assert.equal(routing_metrics.failover_attempt_total >= 1, true);
  assert.equal(routing_metrics.dispatch_retryable_total >= 1, true);
});

test('phase13 chaos: network partition simulation has deterministic client errors and recovery', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'node_local_phase13_partition',
    worker_start_count: 1,
    transport: {
      security: BuildSecureNodeTransportSecurity(),
      authenticate_request: function (params) {
        const authorization_value = params.headers.authorization;
        const authorization = typeof authorization_value === 'string'
          ? authorization_value
          : Array.isArray(authorization_value)
            ? authorization_value[0]
            : undefined;

        if (authorization !== 'Bearer phase13_token') {
          return {
            ok: false,
            message: 'Unauthorized'
          };
        }

        return {
          ok: true,
          identity: {
            subject: 'phase13_client',
            tenant_id: 'tenant_1',
            scopes: ['rpc.call:*'],
            signed_claims: 'phase13_signed_claims'
          }
        };
      }
    }
  });

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase13_partition_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*']
      }
    ]
  });

  await workerprocedurecall.defineWorkerFunction({
    name: 'Phase13PartitionFunction',
    worker_func: async function (): Promise<string> {
      return 'partition_ok';
    }
  });

  let cluster_client: ClusterClient | null = null;

  try {
    const address = await cluster_node_agent.start();

    cluster_client = new ClusterClient({
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      auth_context: {
        subject: 'phase13_client',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase13_signed_claims'
      },
      auth_headers_provider: BuildAuthHeaderProvider({ token: 'phase13_token' }),
      retry_policy: {
        max_attempts: 1,
        base_delay_ms: 10,
        max_delay_ms: 25
      },
      reconnect_policy: {
        enabled: true,
        max_attempts: 2,
        backoff_ms: 50
      },
      default_call_timeout_ms: 500,
      transport_security: BuildSecureClientTlsConfig()
    });

    await cluster_client.connect();

    const success_value = await cluster_client.call<string>({
      function_name: 'Phase13PartitionFunction',
      args: []
    });
    assert.equal(success_value, 'partition_ok');

    await cluster_node_agent.stop();

    let partition_error_code = '';
    try {
      await cluster_client.call<string>({
        function_name: 'Phase13PartitionFunction',
        args: [],
        timeout_ms: 300
      });
    } catch (error) {
      if (error instanceof ClusterClientError) {
        partition_error_code = error.code;
      } else {
        partition_error_code = 'CLIENT_TRANSPORT_ERROR';
      }
    }

    assert.equal(partition_error_code.length > 0, true);
    assert.equal(
      ['CLIENT_TRANSPORT_ERROR', 'CLIENT_TIMEOUT', 'CLIENT_DISCONNECTED'].includes(
        partition_error_code
      ),
      true
    );

    await cluster_node_agent.start();

    const recovered_value = await cluster_client.call<string>({
      function_name: 'Phase13PartitionFunction',
      args: [],
      timeout_ms: 2_000
    });
    assert.equal(recovered_value, 'partition_ok');
  } finally {
    if (cluster_client) {
      await cluster_client.close();
    }
    await cluster_node_agent.stop();
  }
});

test('phase13 stress: reconnect storms remain stable and observable', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'node_local_phase13_storm',
    worker_start_count: 1,
    transport: {
      security: BuildSecureNodeTransportSecurity(),
      authenticate_request: function () {
        return {
          ok: true,
          identity: {
            subject: 'phase13_storm_client',
            tenant_id: 'tenant_1',
            scopes: ['rpc.call:*'],
            signed_claims: 'phase13_signed_claims'
          }
        };
      }
    }
  });

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase13_storm_allow',
        effect: 'allow',
        capabilities: ['rpc.call:*']
      }
    ]
  });

  await workerprocedurecall.defineWorkerFunction({
    name: 'Phase13StormFunction',
    worker_func: async function (): Promise<string> {
      return 'storm_ok';
    }
  });

  const address = await cluster_node_agent.start();

  try {
    const client_count = 6;
    const round_count = 4;

    for (let round_index = 0; round_index < round_count; round_index += 1) {
      await Promise.all(
        Array.from({ length: client_count }).map(async function (_unused, client_index): Promise<void> {
          const cluster_client = new ClusterClient({
            host: address.host,
            port: address.port,
            request_path: address.request_path,
            auth_context: {
              subject: `phase13_storm_client_${client_index}`,
              tenant_id: 'tenant_1',
              scopes: ['rpc.call:*'],
              signed_claims: 'phase13_signed_claims'
            },
            retry_policy: {
              max_attempts: 1,
              base_delay_ms: 10,
              max_delay_ms: 25
            },
            reconnect_policy: {
              enabled: true,
              max_attempts: 1,
              backoff_ms: 10
            },
            default_call_timeout_ms: 500,
            transport_security: BuildSecureClientTlsConfig()
          });

          try {
            await cluster_client.connect();
            const call_result = await cluster_client.call<string>({
              function_name: 'Phase13StormFunction',
              args: []
            });
            assert.equal(call_result, 'storm_ok');
          } finally {
            await cluster_client.close();
          }
        })
      );
    }

    const transport_metrics = cluster_node_agent.getTransportMetrics();

    assert.equal(
      transport_metrics.session_connected_total >= client_count * round_count,
      true
    );
    assert.equal(transport_metrics.request_completed_total >= client_count * round_count, true);
    assert.equal(transport_metrics.max_in_flight_request_count >= 1, true);

    const observability_snapshot = cluster_node_agent.getOperationsObservabilitySnapshot();
    assert.equal(observability_snapshot.transport.request_completed_total >= 1, true);
  } finally {
    await cluster_node_agent.stop();
  }
});

test('phase13 chaos: mutation rollout during degraded cluster emits deterministic failure signals', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase13_mutation_allow',
        effect: 'allow',
        capabilities: ['rpc.admin.mutate:*']
      }
    ]
  });

  const remote_node_id_list = ['phase13_mutation_node_a', 'phase13_mutation_node_b'];

  for (const node_id of remote_node_id_list) {
    workerprocedurecall.registerClusterMutationNode({
      node: {
        node_id,
        labels: {
          pool: 'phase13_mutation'
        }
      },
      mutation_executor: async function (params): Promise<{
        applied_version: string;
        verification_passed: boolean;
      }> {
        if (params.mode === 'apply' && params.node.node_id === 'phase13_mutation_node_a') {
          throw new Error('phase13_simulated_degraded_mutation_node');
        }

        return {
          applied_version: `phase13_version_${params.node.node_id}`,
          verification_passed: true
        };
      }
    });
  }

  const response = await workerprocedurecall.handleClusterAdminMutationRequest({
    node_id: 'node_local_phase13_mutation',
    message: BuildMutationRequest({
      mutation_type: 'define_constant',
      payload: {
        name: 'phase13_degraded_constant',
        value: 5
      },
      target_scope: 'node_selector',
      target_selector: {
        labels: {
          pool: 'phase13_mutation'
        }
      },
      rollout_mode: 'all_at_once',
      rollback_policy: {
        auto_rollback: true,
        rollback_on_partial_failure: true,
        rollback_on_verification_failure: true
      }
    })
  });

  assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_result');
  if (response.terminal_message.message_type !== 'cluster_admin_mutation_result') {
    return;
  }

  assert.equal(response.terminal_message.status === 'completed', false);
  assert.equal(response.terminal_message.summary.failed_node_count >= 1, true);

  const mutation_metrics = workerprocedurecall.getClusterAdminMutationMetrics();
  assert.equal(mutation_metrics.admin_mutation_failure_total >= 1, true);

  const mutation_events = workerprocedurecall.getClusterOperationLifecycleEvents({
    mutation_id: response.terminal_message.mutation_id
  });

  assert.equal(mutation_events.some((event): boolean => {
    return (
      event.event_name === 'admin_mutation_partially_failed' ||
      event.event_name === 'admin_mutation_rolled_back'
    );
  }), true);

  const audit_record_list = workerprocedurecall.getClusterAdminMutationAuditRecords({
    mutation_id: response.terminal_message.mutation_id,
    limit: 1
  });
  assert.equal(audit_record_list.length, 1);
});
