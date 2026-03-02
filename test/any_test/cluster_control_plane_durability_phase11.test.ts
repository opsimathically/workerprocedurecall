import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join as JoinPath } from 'node:path';
import test from 'node:test';

import { WorkerProcedureCall } from '../../src/index';

let phase11_sequence_number = 40_000;

function BuildMutationMessage(params: {
  mutation_id: string;
  request_id: string;
  payload: Record<string, unknown>;
  expected_version?: Record<string, unknown>;
}): Record<string, unknown> {
  phase11_sequence_number += 1;
  const sequence_number = phase11_sequence_number;

  return {
    protocol_version: 1,
    message_type: 'cluster_admin_mutation_request',
    timestamp_unix_ms: Date.now(),
    mutation_id: params.mutation_id,
    request_id: params.request_id,
    trace_id: `trace_phase11_${sequence_number}`,
    deadline_unix_ms: Date.now() + 30_000,
    target_scope: 'single_node',
    target_selector: {
      node_ids: ['phase11_node_local']
    },
    mutation_type: 'define_constant',
    payload: params.payload,
    dry_run: false,
    expected_version: params.expected_version,
    rollout_strategy: {
      mode: 'single_node',
      min_success_percent: 100
    },
    auth_context: {
      subject: 'phase11_admin@example.com',
      tenant_id: 'tenant_phase11',
      capability_claims: ['rpc.admin.mutate:*'],
      signed_claims: 'phase11_signed_claims'
    }
  };
}

function InstallMutationAllowPolicy(params: {
  workerprocedurecall: WorkerProcedureCall;
}): void {
  params.workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'phase11_allow_all_mutations',
        effect: 'allow',
        capabilities: ['rpc.admin.mutate:*']
      }
    ]
  });
}

function BuildDurableStateFilePath(): {
  root_directory_path: string;
  state_file_path: string;
} {
  const root_directory_path = mkdtempSync(
    JoinPath(tmpdir(), 'wpc-phase11-control-plane-')
  );

  return {
    root_directory_path,
    state_file_path: JoinPath(root_directory_path, 'control_plane_state.json')
  };
}

function BuildExpectedConstantVersion(params: {
  payload: Record<string, unknown>;
}): string {
  const entity_name = `define_constant:${String(params.payload.name ?? '')}`;
  const serialized_payload = JSON.stringify({
    mutation_type: 'define_constant',
    payload: params.payload
  });

  return createHash('sha1')
    .update(`${entity_name}|${serialized_payload}`, 'utf8')
    .digest('hex');
}

test('phase11 duplicate mutation_id replay survives restart', async function () {
  const durable_state = BuildDurableStateFilePath();

  try {
    const first_instance = new WorkerProcedureCall({
      control_plane_durable_state_enabled: true,
      control_plane_state_file_path: durable_state.state_file_path,
      mutation_dedupe_retention_ms: 60_000
    });

    let first_response: unknown;
    try {
      InstallMutationAllowPolicy({
        workerprocedurecall: first_instance
      });
      first_response = await first_instance.handleClusterAdminMutationRequest({
        node_id: 'phase11_node_local',
        message: BuildMutationMessage({
          mutation_id: 'phase11_dedupe_mutation',
          request_id: 'phase11_dedupe_request_1',
          payload: {
            name: 'phase11_dedupe_constant',
            value: 1
          }
        })
      });
    } finally {
      await first_instance.stopWorkers();
    }

    const second_instance = new WorkerProcedureCall({
      control_plane_durable_state_enabled: true,
      control_plane_state_file_path: durable_state.state_file_path,
      mutation_dedupe_retention_ms: 60_000
    });

    try {
      InstallMutationAllowPolicy({
        workerprocedurecall: second_instance
      });
      const replay_response = await second_instance.handleClusterAdminMutationRequest({
        node_id: 'phase11_node_local',
        message: BuildMutationMessage({
          mutation_id: 'phase11_dedupe_mutation',
          request_id: 'phase11_dedupe_request_2',
          payload: {
            name: 'phase11_dedupe_constant',
            value: 999
          }
        })
      });

      const first_typed_response = first_response as {
        ack: { request_id: string; mutation_id: string };
        terminal_message: {
          message_type: string;
          request_id: string;
          mutation_id: string;
          status?: string;
          per_node_results?: unknown[];
        };
      };

      assert.equal(
        replay_response.ack.request_id,
        first_typed_response.ack.request_id
      );
      assert.equal(
        replay_response.ack.mutation_id,
        first_typed_response.ack.mutation_id
      );
      assert.equal(
        replay_response.terminal_message.message_type,
        first_typed_response.terminal_message.message_type
      );
      assert.equal(
        replay_response.terminal_message.request_id,
        first_typed_response.terminal_message.request_id
      );
      assert.equal(
        replay_response.terminal_message.mutation_id,
        first_typed_response.terminal_message.mutation_id
      );

      if (
        replay_response.terminal_message.message_type === 'cluster_admin_mutation_result' &&
        first_typed_response.terminal_message.message_type === 'cluster_admin_mutation_result'
      ) {
        assert.equal(
          replay_response.terminal_message.status,
          first_typed_response.terminal_message.status
        );
        assert.deepEqual(
          replay_response.terminal_message.summary,
          (first_typed_response.terminal_message as {
            summary?: unknown;
          }).summary
        );
      }
    } finally {
      await second_instance.stopWorkers();
    }
  } finally {
    rmSync(durable_state.root_directory_path, {
      recursive: true,
      force: true
    });
  }
});

test('phase11 CAS version checks survive restart and remain deterministic', async function () {
  const durable_state = BuildDurableStateFilePath();
  const first_payload = {
    name: 'phase11_cas_constant',
    value: 1
  };
  const expected_persisted_version = BuildExpectedConstantVersion({
    payload: first_payload
  });

  try {
    const first_instance = new WorkerProcedureCall({
      control_plane_durable_state_enabled: true,
      control_plane_state_file_path: durable_state.state_file_path
    });

    try {
      InstallMutationAllowPolicy({
        workerprocedurecall: first_instance
      });
      const first_response = await first_instance.handleClusterAdminMutationRequest({
        node_id: 'phase11_node_local',
        message: BuildMutationMessage({
          mutation_id: 'phase11_cas_init_mutation',
          request_id: 'phase11_cas_init_request',
          payload: first_payload
        })
      });

      assert.equal(first_response.terminal_message.message_type, 'cluster_admin_mutation_result');
    } finally {
      await first_instance.stopWorkers();
    }

    const second_instance = new WorkerProcedureCall({
      control_plane_durable_state_enabled: true,
      control_plane_state_file_path: durable_state.state_file_path
    });

    try {
      InstallMutationAllowPolicy({
        workerprocedurecall: second_instance
      });

      const matching_cas_response =
        await second_instance.handleClusterAdminMutationRequest({
          node_id: 'phase11_node_local',
          message: BuildMutationMessage({
            mutation_id: 'phase11_cas_after_restart_match',
            request_id: 'phase11_cas_request_match',
            payload: {
              name: 'phase11_cas_constant',
              value: 2
            },
            expected_version: {
              entity_name: 'define_constant:phase11_cas_constant',
              entity_version: expected_persisted_version,
              compare_mode: 'exact'
            }
          })
        });

      assert.equal(
        matching_cas_response.terminal_message.message_type,
        'cluster_admin_mutation_result'
      );

      const conflicting_cas_response =
        await second_instance.handleClusterAdminMutationRequest({
          node_id: 'phase11_node_local',
          message: BuildMutationMessage({
            mutation_id: 'phase11_cas_after_restart_conflict',
            request_id: 'phase11_cas_request_conflict',
            payload: {
              name: 'phase11_cas_constant',
              value: 3
            },
            expected_version: {
              entity_name: 'define_constant:phase11_cas_constant',
              entity_version: 'definitely_not_the_current_version',
              compare_mode: 'exact'
            }
          })
        });

      assert.equal(
        conflicting_cas_response.terminal_message.message_type,
        'cluster_admin_mutation_error'
      );
      if (
        conflicting_cas_response.terminal_message.message_type ===
        'cluster_admin_mutation_error'
      ) {
        assert.equal(conflicting_cas_response.terminal_message.error.code, 'ADMIN_CONFLICT');
      }
    } finally {
      await second_instance.stopWorkers();
    }
  } finally {
    rmSync(durable_state.root_directory_path, {
      recursive: true,
      force: true
    });
  }
});

test('phase11 audit records persist across restart and are queryable', async function () {
  const durable_state = BuildDurableStateFilePath();
  const mutation_id = 'phase11_audit_mutation';

  try {
    const first_instance = new WorkerProcedureCall({
      control_plane_durable_state_enabled: true,
      control_plane_state_file_path: durable_state.state_file_path
    });

    try {
      InstallMutationAllowPolicy({
        workerprocedurecall: first_instance
      });
      await first_instance.handleClusterAdminMutationRequest({
        node_id: 'phase11_node_local',
        message: BuildMutationMessage({
          mutation_id,
          request_id: 'phase11_audit_request_1',
          payload: {
            name: 'phase11_audit_constant',
            value: 111
          }
        })
      });
    } finally {
      await first_instance.stopWorkers();
    }

    const second_instance = new WorkerProcedureCall({
      control_plane_durable_state_enabled: true,
      control_plane_state_file_path: durable_state.state_file_path
    });

    try {
      const audit_record_list = second_instance.getClusterAdminMutationAuditRecords({
        mutation_id
      });

      assert.equal(audit_record_list.length, 1);
      assert.equal(audit_record_list[0].mutation_id, mutation_id);
      assert.equal(typeof audit_record_list[0].timestamp_unix_ms, 'number');
      assert.equal(audit_record_list[0].immutable, true);
    } finally {
      await second_instance.stopWorkers();
    }
  } finally {
    rmSync(durable_state.root_directory_path, {
      recursive: true,
      force: true
    });
  }
});

test('phase11 retention compaction removes expired dedupe and audit records durably', async function () {
  const durable_state = BuildDurableStateFilePath();
  const mutation_id = 'phase11_retention_mutation';

  try {
    const first_instance = new WorkerProcedureCall({
      control_plane_durable_state_enabled: true,
      control_plane_state_file_path: durable_state.state_file_path,
      mutation_dedupe_retention_ms: 15,
      control_plane_audit_retention_ms: 15
    });

    try {
      InstallMutationAllowPolicy({
        workerprocedurecall: first_instance
      });
      await first_instance.handleClusterAdminMutationRequest({
        node_id: 'phase11_node_local',
        message: BuildMutationMessage({
          mutation_id,
          request_id: 'phase11_retention_request_1',
          payload: {
            name: 'phase11_retention_constant',
            value: 1
          }
        })
      });

      await new Promise<void>((resolve): void => {
        setTimeout(resolve, 40);
      });

      const compaction_result = first_instance.compactControlPlaneState({
        now_unix_ms: Date.now()
      });
      assert.equal(compaction_result.removed_dedupe_record_count >= 1, true);
      assert.equal(compaction_result.removed_audit_record_count >= 1, true);
    } finally {
      await first_instance.stopWorkers();
    }

    const second_instance = new WorkerProcedureCall({
      control_plane_durable_state_enabled: true,
      control_plane_state_file_path: durable_state.state_file_path,
      mutation_dedupe_retention_ms: 15,
      control_plane_audit_retention_ms: 15
    });

    try {
      InstallMutationAllowPolicy({
        workerprocedurecall: second_instance
      });

      const post_compaction_response =
        await second_instance.handleClusterAdminMutationRequest({
          node_id: 'phase11_node_local',
          message: BuildMutationMessage({
            mutation_id,
            request_id: 'phase11_retention_request_2',
            payload: {
              name: 'phase11_retention_constant',
              value: 2
            }
          })
        });

      assert.equal(post_compaction_response.ack.request_id, 'phase11_retention_request_2');

      const audit_record_list = second_instance.getClusterAdminMutationAuditRecords({
        mutation_id
      });
      assert.equal(audit_record_list.length, 1);
    } finally {
      await second_instance.stopWorkers();
    }
  } finally {
    rmSync(durable_state.root_directory_path, {
      recursive: true,
      force: true
    });
  }
});
