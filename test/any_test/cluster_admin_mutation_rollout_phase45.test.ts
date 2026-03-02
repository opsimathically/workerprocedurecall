import assert from 'node:assert/strict';
import test from 'node:test';

import { WorkerProcedureCall } from '../../src/index';

let mutation_sequence_number = 10_000;

function BuildMutationRequest(params: {
  mutation_type?: string;
  payload?: Record<string, unknown>;
  target_scope: 'single_node' | 'node_selector' | 'cluster_wide';
  target_selector?: Record<string, unknown>;
  rollout_mode: 'single_node' | 'all_at_once' | 'rolling_percent' | 'canary_then_expand';
  min_success_percent?: number;
  batch_percent?: number;
  canary_node_count?: number;
  apply_timeout_ms?: number;
  verify_timeout_ms?: number;
  in_flight_policy?: 'no_interruption' | 'drain_and_swap';
  expected_version?: Record<string, unknown>;
  rollback_policy?: Record<string, unknown>;
}): Record<string, unknown> {
  const {
    mutation_type = 'define_constant',
    payload = {
      name: 'phase45_constant',
      value: mutation_sequence_number
    },
    target_scope,
    target_selector,
    rollout_mode,
    min_success_percent = 100,
    batch_percent,
    canary_node_count,
    apply_timeout_ms,
    verify_timeout_ms,
    in_flight_policy,
    expected_version,
    rollback_policy
  } = params;

  mutation_sequence_number += 1;
  const sequence_number = mutation_sequence_number;

  return {
    protocol_version: 1,
    message_type: 'cluster_admin_mutation_request',
    timestamp_unix_ms: Date.now(),
    mutation_id: `mutation_phase45_${sequence_number}`,
    request_id: `request_phase45_${sequence_number}`,
    trace_id: `trace_phase45_${sequence_number}`,
    deadline_unix_ms: Date.now() + 60_000,
    target_scope,
    target_selector,
    mutation_type,
    payload,
    expected_version,
    dry_run: false,
    rollout_strategy: {
      mode: rollout_mode,
      min_success_percent,
      batch_percent,
      canary_node_count,
      apply_timeout_ms,
      verify_timeout_ms,
      rollback_policy
    },
    in_flight_policy,
    auth_context: {
      subject: 'admin@example.com',
      tenant_id: 'tenant_1',
      capability_claims: ['rpc.admin.mutate:*'],
      signed_claims: 'signed-token'
    }
  };
}

function InstallAdminPolicy(workerprocedurecall: WorkerProcedureCall): void {
  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase45_allow_admin',
        effect: 'allow',
        capabilities: ['rpc.admin.mutate:*'],
        constraints: {
          tenant: '*'
        }
      }
    ]
  });
}

test('rollout strategy single_node applies local mutation', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    InstallAdminPolicy(workerprocedurecall);

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationRequest({
        target_scope: 'single_node',
        target_selector: {
          node_ids: [node_id]
        },
        rollout_mode: 'single_node',
        payload: {
          name: 'phase45_single_node',
          value: 1
        }
      })
    });

    assert.equal(response.ack.accepted, true);
    assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_result');
    if (response.terminal_message.message_type !== 'cluster_admin_mutation_result') {
      return;
    }

    assert.equal(response.terminal_message.status, 'completed');
    assert.equal(response.terminal_message.summary.target_node_count, 1);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('rollout strategy all_at_once applies across node_selector targets', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';
  const applied_node_id_list: string[] = [];

  try {
    InstallAdminPolicy(workerprocedurecall);

    for (const remote_node_id of ['node_r1', 'node_r2', 'node_r3']) {
      workerprocedurecall.registerClusterMutationNode({
        node: {
          node_id: remote_node_id,
          labels: {
            pool: 'phase45'
          },
          zones: ['z1']
        },
        mutation_executor: async function (params): Promise<{
          applied_version: string;
          verification_passed: boolean;
        }> {
          if (params.mode === 'apply') {
            applied_node_id_list.push(params.node.node_id);
          }
          return {
            applied_version: `version_${params.node.node_id}`,
            verification_passed: true
          };
        }
      });
    }

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationRequest({
        target_scope: 'node_selector',
        target_selector: {
          labels: {
            pool: 'phase45'
          }
        },
        rollout_mode: 'all_at_once',
        payload: {
          name: 'phase45_all_at_once',
          value: 1
        }
      })
    });

    assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_result');
    if (response.terminal_message.message_type !== 'cluster_admin_mutation_result') {
      return;
    }

    assert.equal(response.terminal_message.status, 'completed');
    assert.equal(response.terminal_message.summary.target_node_count, 3);
    assert.equal(applied_node_id_list.length, 3);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('rollout strategy canary_then_expand runs canary first and succeeds', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';
  const call_order: string[] = [];

  try {
    InstallAdminPolicy(workerprocedurecall);

    for (const remote_node_id of ['node_c1', 'node_c2', 'node_c3']) {
      workerprocedurecall.registerClusterMutationNode({
        node: {
          node_id: remote_node_id,
          labels: {
            pool: 'canary'
          }
        },
        mutation_executor: async function (params): Promise<{
          applied_version: string;
          verification_passed: boolean;
        }> {
          call_order.push(`${params.mode}:${params.node.node_id}`);
          return {
            applied_version: `version_${params.node.node_id}`,
            verification_passed: true
          };
        }
      });
    }

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationRequest({
        target_scope: 'node_selector',
        target_selector: {
          labels: {
            pool: 'canary'
          }
        },
        rollout_mode: 'canary_then_expand',
        canary_node_count: 1,
        batch_percent: 100,
        payload: {
          name: 'phase45_canary_success',
          value: 2
        }
      })
    });

    assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_result');
    if (response.terminal_message.message_type !== 'cluster_admin_mutation_result') {
      return;
    }

    assert.equal(response.terminal_message.status, 'completed');
    assert.equal(call_order[0], 'apply:node_c1');
    assert.equal(response.terminal_message.summary.target_node_count, 3);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('canary failure triggers auto rollback with rolled_back result', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';
  const rollback_call_list: string[] = [];

  try {
    InstallAdminPolicy(workerprocedurecall);

    for (const remote_node_id of ['node_f1', 'node_f2', 'node_f3']) {
      workerprocedurecall.registerClusterMutationNode({
        node: {
          node_id: remote_node_id,
          labels: {
            pool: 'canary_fail'
          }
        },
        mutation_executor: async function (params): Promise<{
          applied_version: string;
          verification_passed: boolean;
          snapshot: { node_id: string };
        }> {
          if (params.mode === 'rollback') {
            rollback_call_list.push(params.node.node_id);
            return {
              applied_version: `rollback_${params.node.node_id}`,
              verification_passed: true,
              snapshot: { node_id: params.node.node_id }
            };
          }

          return {
            applied_version: `apply_${params.node.node_id}`,
            verification_passed: params.node.node_id !== 'node_f1',
            snapshot: { node_id: params.node.node_id }
          };
        }
      });
    }

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationRequest({
        target_scope: 'node_selector',
        target_selector: {
          labels: {
            pool: 'canary_fail'
          }
        },
        rollout_mode: 'canary_then_expand',
        canary_node_count: 1,
        batch_percent: 100,
        rollback_policy: {
          auto_rollback: true,
          rollback_on_partial_failure: true,
          rollback_on_verification_failure: true
        },
        payload: {
          name: 'phase45_canary_failure',
          value: 3
        }
      })
    });

    assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_result');
    if (response.terminal_message.message_type !== 'cluster_admin_mutation_result') {
      return;
    }

    assert.equal(response.terminal_message.status, 'rolled_back');
    assert.equal(rollback_call_list.includes('node_f1'), true);
    assert.equal(response.terminal_message.summary.rolled_back_node_count >= 1, true);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('rolling_percent timeout produces deterministic partially_failed status', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    InstallAdminPolicy(workerprocedurecall);

    for (const remote_node_id of ['node_rp1', 'node_rp2', 'node_rp3', 'node_rp4']) {
      workerprocedurecall.registerClusterMutationNode({
        node: {
          node_id: remote_node_id,
          labels: {
            pool: 'rolling'
          }
        },
        mutation_executor: async function (params): Promise<{
          applied_version: string;
          verification_passed: boolean;
        }> {
          if (params.node.node_id === 'node_rp3' && params.mode === 'apply') {
            await new Promise<void>((resolve): void => {
              setTimeout(resolve, 50);
            });
          }
          return {
            applied_version: `version_${params.node.node_id}`,
            verification_passed: true
          };
        }
      });
    }

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationRequest({
        target_scope: 'node_selector',
        target_selector: {
          labels: {
            pool: 'rolling'
          }
        },
        rollout_mode: 'rolling_percent',
        batch_percent: 50,
        apply_timeout_ms: 10,
        min_success_percent: 100,
        payload: {
          name: 'phase45_rolling_timeout',
          value: 4
        }
      })
    });

    assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_result');
    if (response.terminal_message.message_type !== 'cluster_admin_mutation_result') {
      return;
    }

    assert.equal(response.terminal_message.status, 'partially_failed');
    assert.equal(response.terminal_message.summary.failed_node_count >= 1, true);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('CAS conflict returns ADMIN_CONFLICT deterministically', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    InstallAdminPolicy(workerprocedurecall);

    const first_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationRequest({
        target_scope: 'single_node',
        target_selector: {
          node_ids: [node_id]
        },
        rollout_mode: 'single_node',
        payload: {
          name: 'phase45_cas_constant',
          value: 11
        }
      })
    });
    assert.equal(first_response.terminal_message.message_type, 'cluster_admin_mutation_result');

    const conflict_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationRequest({
        target_scope: 'single_node',
        target_selector: {
          node_ids: [node_id]
        },
        rollout_mode: 'single_node',
        payload: {
          name: 'phase45_cas_constant',
          value: 12
        },
        expected_version: {
          entity_name: 'define_constant:phase45_cas_constant',
          entity_version: 'definitely_wrong_version',
          compare_mode: 'exact'
        }
      })
    });

    assert.equal(conflict_response.terminal_message.message_type, 'cluster_admin_mutation_error');
    if (conflict_response.terminal_message.message_type !== 'cluster_admin_mutation_error') {
      return;
    }

    assert.equal(conflict_response.terminal_message.error.code, 'ADMIN_CONFLICT');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('in_flight_policy no_interruption does not block mutation apply on local node', async function () {
  const workerprocedurecall = new WorkerProcedureCall({
    call_timeout_ms: 2_000
  });
  const node_id = 'node_local_1';

  try {
    InstallAdminPolicy(workerprocedurecall);
    await workerprocedurecall.startWorkers({ count: 1 });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase45SleepFunction',
      worker_func: async function (): Promise<string> {
        await new Promise<void>((resolve): void => {
          setTimeout(resolve, 120);
        });
        return 'ok';
      }
    });

    const in_flight_call = workerprocedurecall.call.Phase45SleepFunction();

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationRequest({
        target_scope: 'single_node',
        target_selector: {
          node_ids: [node_id]
        },
        rollout_mode: 'single_node',
        in_flight_policy: 'no_interruption',
        payload: {
          name: 'phase45_no_interrupt_constant',
          value: 21
        }
      })
    });

    assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_result');
    if (response.terminal_message.message_type !== 'cluster_admin_mutation_result') {
      return;
    }
    assert.equal(response.terminal_message.status, 'completed');
    assert.equal(await in_flight_call, 'ok');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('in_flight_policy drain_and_swap waits for local calls and can timeout', async function () {
  const workerprocedurecall = new WorkerProcedureCall({
    call_timeout_ms: 2_000
  });
  const node_id = 'node_local_1';

  try {
    InstallAdminPolicy(workerprocedurecall);
    await workerprocedurecall.startWorkers({ count: 1 });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase45DrainSleepFunction',
      worker_func: async function (): Promise<string> {
        await new Promise<void>((resolve): void => {
          setTimeout(resolve, 120);
        });
        return 'ok';
      }
    });

    const in_flight_call = workerprocedurecall.call.Phase45DrainSleepFunction();

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationRequest({
        target_scope: 'single_node',
        target_selector: {
          node_ids: [node_id]
        },
        rollout_mode: 'single_node',
        in_flight_policy: 'drain_and_swap',
        apply_timeout_ms: 10,
        payload: {
          name: 'phase45_drain_timeout_constant',
          value: 33
        }
      })
    });

    if (response.terminal_message.message_type === 'cluster_admin_mutation_result') {
      assert.equal(response.terminal_message.status, 'partially_failed');
      assert.equal(response.terminal_message.summary.failed_node_count, 1);
    } else {
      assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_error');
      assert.equal(response.terminal_message.error.code, 'ADMIN_TIMEOUT');
    }
    await in_flight_call;
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});
