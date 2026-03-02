import assert from 'node:assert/strict';
import test from 'node:test';

import { WorkerProcedureCall, type worker_event_t } from '../../src/index';

let reliability_sequence_number = 20_000;

function BuildMutationMessage(params: {
  mutation_id?: string;
  request_id?: string;
  target_scope?: 'single_node' | 'node_selector' | 'cluster_wide';
  target_selector?: Record<string, unknown>;
  mutation_type?: string;
  payload?: Record<string, unknown>;
  capability_claims?: string[];
  rollout_mode?: 'single_node' | 'all_at_once' | 'rolling_percent' | 'canary_then_expand';
  rollback_policy?: Record<string, unknown>;
  apply_timeout_ms?: number;
  expected_version?: Record<string, unknown>;
  change_context?: Record<string, unknown>;
}): Record<string, unknown> {
  reliability_sequence_number += 1;
  const sequence_number = reliability_sequence_number;

  return {
    protocol_version: 1,
    message_type: 'cluster_admin_mutation_request',
    timestamp_unix_ms: Date.now(),
    mutation_id: params.mutation_id ?? `mutation_reliability_${sequence_number}`,
    request_id: params.request_id ?? `request_reliability_${sequence_number}`,
    trace_id: `trace_reliability_${sequence_number}`,
    deadline_unix_ms: Date.now() + 20_000,
    target_scope: params.target_scope ?? 'single_node',
    target_selector:
      params.target_selector ??
      ({
        node_ids: ['node_local_1']
      } as Record<string, unknown>),
    mutation_type: params.mutation_type ?? 'define_constant',
    payload:
      params.payload ??
      ({
        name: `reliability_constant_${sequence_number}`,
        value: sequence_number
      } as Record<string, unknown>),
    dry_run: false,
    rollout_strategy: {
      mode: params.rollout_mode ?? 'single_node',
      min_success_percent: 100,
      apply_timeout_ms: params.apply_timeout_ms,
      rollback_policy: params.rollback_policy
    },
    expected_version: params.expected_version,
    change_context: params.change_context,
    auth_context: {
      subject: 'admin@example.com',
      tenant_id: 'tenant_1',
      capability_claims: params.capability_claims ?? ['rpc.admin.mutate:*'],
      signed_claims: 'signed-token'
    }
  };
}

function InstallMutationAllowPolicy(workerprocedurecall: WorkerProcedureCall): void {
  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase67_allow_all_mutations',
        effect: 'allow',
        capabilities: ['rpc.admin.mutate:*']
      }
    ]
  });
}

test('mutation_id dedupe replays completed result deterministically', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    InstallMutationAllowPolicy(workerprocedurecall);
    const mutation_id = 'mutation_dedupe_complete';

    const first_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id,
        request_id: 'request_dedupe_first',
        payload: {
          name: 'phase67_dedupe_constant',
          value: 1
        }
      })
    });
    const second_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id,
        request_id: 'request_dedupe_second',
        payload: {
          name: 'phase67_dedupe_constant',
          value: 999
        }
      })
    });

    assert.deepEqual(second_response, first_response);

    const metrics = workerprocedurecall.getClusterAdminMutationMetrics();
    assert.equal(metrics.admin_mutation_requests_total, 2);
    assert.equal(metrics.admin_mutation_success_total, 1);

    const audit_record_list = workerprocedurecall.getClusterAdminMutationAuditRecords({
      mutation_id
    });
    assert.equal(audit_record_list.length, 1);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('in-progress dedupe returns ADMIN_DISPATCH_RETRYABLE while first mutation is running', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    InstallMutationAllowPolicy(workerprocedurecall);
    const mutation_id = 'mutation_dedupe_in_progress';

    workerprocedurecall.registerClusterMutationNode({
      node: {
        node_id: 'node_remote_slow',
        labels: {
          pool: 'dedupe'
        }
      },
      mutation_executor: async function (params): Promise<{
        applied_version: string;
        verification_passed: boolean;
      }> {
        if (params.mode === 'apply') {
          await new Promise<void>((resolve): void => {
            setTimeout(resolve, 80);
          });
        }

        return {
          applied_version: 'remote_v1',
          verification_passed: true
        };
      }
    });

    const first_request_promise = workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id,
        request_id: 'request_in_progress_first',
        target_scope: 'node_selector',
        target_selector: {
          labels: {
            pool: 'dedupe'
          }
        },
        rollout_mode: 'all_at_once',
        payload: {
          name: 'phase67_in_progress_constant',
          value: 1
        }
      })
    });

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 10);
    });

    const second_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id,
        request_id: 'request_in_progress_second',
        target_scope: 'node_selector',
        target_selector: {
          labels: {
            pool: 'dedupe'
          }
        },
        rollout_mode: 'all_at_once',
        payload: {
          name: 'phase67_in_progress_constant',
          value: 2
        }
      })
    });
    assert.equal(second_response.terminal_message.message_type, 'cluster_admin_mutation_error');
    if (second_response.terminal_message.message_type !== 'cluster_admin_mutation_error') {
      return;
    }
    assert.equal(second_response.terminal_message.error.code, 'ADMIN_DISPATCH_RETRYABLE');

    const first_response = await first_request_promise;
    assert.equal(first_response.terminal_message.message_type, 'cluster_admin_mutation_result');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('retry controller retries retryable rollout failures and can recover to completed', async function () {
  const workerprocedurecall = new WorkerProcedureCall({
    mutation_retry_max_attempts: 3,
    mutation_retry_base_delay_ms: 5,
    mutation_retry_max_delay_ms: 20
  });
  const node_id = 'node_local_1';
  let apply_attempt_count = 0;

  try {
    InstallMutationAllowPolicy(workerprocedurecall);

    workerprocedurecall.registerClusterMutationNode({
      node: {
        node_id: 'node_retry_1',
        labels: {
          pool: 'retry'
        }
      },
      mutation_executor: async function (): Promise<{
        applied_version: string;
        verification_passed: boolean;
      }> {
        apply_attempt_count += 1;
        if (apply_attempt_count < 3) {
          throw new Error('simulated_dispatch_failure');
        }
        return {
          applied_version: `retry_version_${apply_attempt_count}`,
          verification_passed: true
        };
      }
    });

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id: 'mutation_retry_success',
        target_scope: 'node_selector',
        target_selector: {
          labels: {
            pool: 'retry'
          }
        },
        rollout_mode: 'all_at_once',
        payload: {
          name: 'phase67_retry_constant',
          value: 7
        }
      })
    });

    assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_result');
    if (response.terminal_message.message_type !== 'cluster_admin_mutation_result') {
      return;
    }
    assert.equal(response.terminal_message.status, 'completed');
    assert.equal(apply_attempt_count, 3);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('governance hooks enforce change reason/ticket and dual authorization', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    InstallMutationAllowPolicy(workerprocedurecall);
    workerprocedurecall.setMutationGovernancePolicy({
      policy: {
        require_change_reason: true,
        require_change_ticket_id: true,
        enforce_dual_authorization: true
      }
    });

    const denied_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id: 'mutation_governance_denied',
        payload: {
          name: 'phase67_governance_constant',
          value: 1
        }
      })
    });
    assert.equal(denied_response.terminal_message.message_type, 'cluster_admin_mutation_error');

    const allowed_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id: 'mutation_governance_allowed',
        payload: {
          name: 'phase67_governance_constant',
          value: 2
        },
        change_context: {
          reason: 'break_glass approved change',
          change_ticket_id: 'TICKET-123',
          dual_authorization: {
            required: true,
            approver_subject: 'security-admin@example.com'
          }
        }
      })
    });
    assert.equal(allowed_response.terminal_message.message_type, 'cluster_admin_mutation_result');

    const governance_policy = workerprocedurecall.getMutationGovernancePolicy();
    assert.equal(governance_policy.enforce_dual_authorization, true);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('audit records include required fields and metrics/events reflect success/deny/rollback paths', async function () {
  const workerprocedurecall = new WorkerProcedureCall({
    mutation_retry_max_attempts: 1
  });
  const node_id = 'node_local_1';
  const observed_event_name_list: string[] = [];

  const listener_id = workerprocedurecall.onWorkerEvent({
    listener: function (worker_event: worker_event_t): void {
      if (worker_event.source === 'parent') {
        observed_event_name_list.push(worker_event.event_name);
      }
    }
  });

  try {
    InstallMutationAllowPolicy(workerprocedurecall);

    const success_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id: 'mutation_audit_success',
        payload: {
          name: 'phase67_audit_success',
          value: 1
        }
      })
    });
    assert.equal(success_response.terminal_message.message_type, 'cluster_admin_mutation_result');

    workerprocedurecall.registerClusterMutationNode({
      node: {
        node_id: 'node_rollback_phase67',
        labels: {
          pool: 'rollback_phase67'
        }
      },
      mutation_executor: async function (params): Promise<{
        applied_version: string;
        verification_passed: boolean;
        snapshot: { node_id: string };
      }> {
        if (params.mode === 'rollback') {
          return {
            applied_version: 'rollback_ok',
            verification_passed: true,
            snapshot: { node_id: params.node.node_id }
          };
        }
        return {
          applied_version: 'apply_failed_verify',
          verification_passed: false,
          snapshot: { node_id: params.node.node_id }
        };
      }
    });

    const rollback_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id: 'mutation_audit_rollback',
        target_scope: 'node_selector',
        target_selector: {
          labels: {
            pool: 'rollback_phase67'
          }
        },
        rollout_mode: 'canary_then_expand',
        rollback_policy: {
          auto_rollback: true,
          rollback_on_verification_failure: true
        },
        payload: {
          name: 'phase67_audit_rollback',
          value: 2
        }
      })
    });
    assert.equal(rollback_response.terminal_message.message_type, 'cluster_admin_mutation_result');
    if (rollback_response.terminal_message.message_type === 'cluster_admin_mutation_result') {
      assert.equal(rollback_response.terminal_message.status, 'rolled_back');
    }

    workerprocedurecall.registerClusterMutationNode({
      node: {
        node_id: 'node_partial_phase67',
        labels: {
          pool: 'partial_phase67'
        }
      },
      mutation_executor: async function (): Promise<{
        applied_version: string;
        verification_passed: boolean;
      }> {
        await new Promise<void>((resolve): void => {
          setTimeout(resolve, 40);
        });
        return {
          applied_version: 'never_reached',
          verification_passed: true
        };
      }
    });

    const partial_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id: 'mutation_audit_partial',
        target_scope: 'node_selector',
        target_selector: {
          labels: {
            pool: 'partial_phase67'
          }
        },
        rollout_mode: 'all_at_once',
        apply_timeout_ms: 5,
        payload: {
          name: 'phase67_audit_partial',
          value: 33
        }
      })
    });
    if (partial_response.terminal_message.message_type === 'cluster_admin_mutation_result') {
      assert.equal(partial_response.terminal_message.status, 'partially_failed');
    } else {
      assert.equal(partial_response.terminal_message.error.code, 'ADMIN_TIMEOUT');
    }

    const denied_response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id,
      message: BuildMutationMessage({
        mutation_id: 'mutation_audit_denied',
        capability_claims: [],
        payload: {
          name: 'phase67_audit_denied',
          value: 3
        }
      })
    });
    assert.equal(denied_response.terminal_message.message_type, 'cluster_admin_mutation_error');

    const audit_record_list = workerprocedurecall.getClusterAdminMutationAuditRecords({
      mutation_id: 'mutation_audit_success'
    });
    assert.equal(audit_record_list.length, 1);
    const audit_record = audit_record_list[0];
    assert.equal(typeof audit_record.actor_identity.subject, 'string');
    assert.equal(typeof audit_record.payload_hash_sha256, 'string');
    assert.equal(Array.isArray(audit_record.resolved_node_id_list), true);
    assert.equal(typeof audit_record.timing.duration_ms, 'number');
    assert.equal(audit_record.immutable, true);

    const metrics = workerprocedurecall.getClusterAdminMutationMetrics();
    assert.equal(metrics.admin_mutation_requests_total >= 3, true);
    assert.equal(metrics.admin_mutation_success_total >= 1, true);
    assert.equal(metrics.admin_mutation_failure_total >= 2, true);
    assert.equal(metrics.admin_mutation_rollback_total >= 1, true);
    assert.equal(metrics.admin_mutation_authz_denied_total >= 1, true);
    assert.equal(metrics.admin_mutation_rollout_duration_ms.length >= 3, true);

    assert.equal(observed_event_name_list.includes('admin_mutation_requested'), true);
    assert.equal(observed_event_name_list.includes('admin_mutation_authorized'), true);
    assert.equal(observed_event_name_list.includes('admin_mutation_applied'), true);
    if (partial_response.terminal_message.message_type === 'cluster_admin_mutation_result') {
      assert.equal(observed_event_name_list.includes('admin_mutation_partially_failed'), true);
    } else {
      assert.equal(partial_response.terminal_message.error.code, 'ADMIN_TIMEOUT');
      assert.equal(observed_event_name_list.includes('admin_mutation_denied'), true);
    }
    assert.equal(observed_event_name_list.includes('admin_mutation_rolled_back'), true);
    assert.equal(observed_event_name_list.includes('admin_mutation_denied'), true);
  } finally {
    workerprocedurecall.offWorkerEvent({ listener_id });
    await workerprocedurecall.stopWorkers();
  }
});
