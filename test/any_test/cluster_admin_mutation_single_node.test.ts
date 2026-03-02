import assert from 'node:assert/strict';
import test from 'node:test';

import { WorkerProcedureCall, type worker_event_t } from '../../src/index';

let mutation_sequence_number = 1;

async function ApplySingleNodeMutation(params: {
  workerprocedurecall: WorkerProcedureCall;
  node_id: string;
  mutation_type: string;
  payload: Record<string, unknown>;
  dry_run?: boolean;
  expected_version?: Record<string, unknown>;
}): Promise<{
  ack: unknown;
  terminal_message: unknown;
}> {
  const {
    workerprocedurecall,
    node_id,
    mutation_type,
    payload,
    dry_run = false,
    expected_version
  } = params;

  const sequence_number = mutation_sequence_number;
  mutation_sequence_number += 1;

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'test_allow_admin_mutations',
        effect: 'allow',
        capabilities: ['rpc.admin.mutate:*']
      }
    ]
  });

  return await workerprocedurecall.handleClusterAdminMutationRequest({
    node_id,
    now_unix_ms: Date.now(),
    message: {
      protocol_version: 1,
      message_type: 'cluster_admin_mutation_request',
      timestamp_unix_ms: Date.now(),
      mutation_id: `mutation_${sequence_number}`,
      request_id: `request_${sequence_number}`,
      trace_id: `trace_${sequence_number}`,
      deadline_unix_ms: Date.now() + 30_000,
      target_scope: 'single_node',
      target_selector: {
        node_ids: [node_id]
      },
      mutation_type,
      payload,
      expected_version,
      dry_run,
      rollout_strategy: {
        mode: 'single_node',
        min_success_percent: 100
      },
      auth_context: {
        subject: 'admin@example.com',
        tenant_id: 'tenant_1',
        capability_claims: ['rpc.admin.mutate:*'],
        signed_claims: 'signed-token'
      }
    }
  });
}

test('single-node mutation define_function works end-to-end', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    const mutation_response = await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'define_function',
      payload: {
        name: 'RemoteHotDefineFunction',
        worker_func_source: 'async function (params) { return params.value + 1; }'
      }
    });

    assert.equal((mutation_response.ack as { accepted: boolean }).accepted, true);
    assert.equal(
      (mutation_response.terminal_message as { message_type: string }).message_type,
      'cluster_admin_mutation_result'
    );
    assert.equal(
      (mutation_response.terminal_message as { status: string }).status,
      'completed'
    );

    const value = await workerprocedurecall.call.RemoteHotDefineFunction({
      value: 41
    });
    assert.equal(value, 42);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation redefine_function updates callable behavior', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'define_function',
      payload: {
        name: 'RemoteHotRedefineFunction',
        worker_func_source: 'async function (params) { return params.value + 1; }'
      }
    });

    const before_redefine_value =
      await workerprocedurecall.call.RemoteHotRedefineFunction({
        value: 10
      });
    assert.equal(before_redefine_value, 11);

    const mutation_response = await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'redefine_function',
      payload: {
        name: 'RemoteHotRedefineFunction',
        worker_func_source: 'async function (params) { return params.value + 2; }'
      }
    });
    assert.equal((mutation_response.ack as { accepted: boolean }).accepted, true);
    assert.equal(
      (mutation_response.terminal_message as { status: string }).status,
      'completed'
    );

    const after_redefine_value =
      await workerprocedurecall.call.RemoteHotRedefineFunction({
        value: 10
      });
    assert.equal(after_redefine_value, 12);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation undefine_function removes function', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    await workerprocedurecall.defineWorkerFunction({
      name: 'RemoteHotUndefineFunction',
      worker_func: async function (): Promise<string> {
        return 'still here';
      }
    });

    const mutation_response = await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'undefine_function',
      payload: {
        name: 'RemoteHotUndefineFunction'
      }
    });
    assert.equal((mutation_response.ack as { accepted: boolean }).accepted, true);
    assert.equal(
      (mutation_response.terminal_message as { status: string }).status,
      'completed'
    );

    await assert.rejects(
      async function (): Promise<unknown> {
        return await workerprocedurecall.call.RemoteHotUndefineFunction();
      },
      /not defined/i
    );
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation define_dependency works with worker import', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'define_dependency',
      payload: {
        alias: 'path_dep_remote',
        module_specifier: 'node:path'
      }
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'UseRemoteDefinedDependency',
      worker_func: async function (): Promise<string> {
        const path_module = await wpc_import<typeof import('node:path')>({
          alias: 'path_dep_remote'
        });
        return path_module.basename('/tmp/demo.txt');
      }
    });

    const value = await workerprocedurecall.call.UseRemoteDefinedDependency();
    assert.equal(value, 'demo.txt');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation undefine_dependency removes dependency availability', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    await workerprocedurecall.defineWorkerDependency({
      alias: 'path_dep_remove_remote',
      module_specifier: 'node:path'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'UseRemovedRemoteDependency',
      worker_func: async function (): Promise<string> {
        const path_module = await wpc_import<typeof import('node:path')>({
          alias: 'path_dep_remove_remote'
        });
        return path_module.basename('/tmp/removed.txt');
      }
    });

    assert.equal(
      await workerprocedurecall.call.UseRemovedRemoteDependency(),
      'removed.txt'
    );

    await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'undefine_dependency',
      payload: {
        alias: 'path_dep_remove_remote'
      }
    });

    await assert.rejects(
      async function (): Promise<unknown> {
        return await workerprocedurecall.call.UseRemovedRemoteDependency();
      },
      /not defined|dependency/i
    );
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation define_constant works', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    const mutation_response = await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'define_constant',
      payload: {
        name: 'REMOTE_DEFINED_CONSTANT',
        value: 'const-value'
      }
    });

    assert.equal((mutation_response.ack as { accepted: boolean }).accepted, true);
    assert.equal(
      (mutation_response.terminal_message as { status: string }).status,
      'completed'
    );

    await workerprocedurecall.defineWorkerFunction({
      name: 'ReadRemoteDefinedConstant',
      worker_func: async function (): Promise<string> {
        return wpc_constant('REMOTE_DEFINED_CONSTANT') as string;
      }
    });

    assert.equal(
      await workerprocedurecall.call.ReadRemoteDefinedConstant(),
      'const-value'
    );
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation undefine_constant removes constant', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    await workerprocedurecall.defineWorkerConstant({
      name: 'REMOTE_REMOVE_CONSTANT',
      value: 'to-remove'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'ReadRemovedRemoteConstant',
      worker_func: async function (): Promise<string> {
        return wpc_constant('REMOTE_REMOVE_CONSTANT') as string;
      }
    });

    assert.equal(await workerprocedurecall.call.ReadRemovedRemoteConstant(), 'to-remove');

    await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'undefine_constant',
      payload: {
        name: 'REMOTE_REMOVE_CONSTANT'
      }
    });

    await assert.rejects(
      async function (): Promise<unknown> {
        return await workerprocedurecall.call.ReadRemovedRemoteConstant();
      },
      /not defined/i
    );
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation define_database_connection works', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'define_database_connection',
      payload: {
        name: 'remote_db_connection_1',
        connector: {
          type: 'sqlite',
          semantics: {
            filename: ':memory:',
            driver: 'better-sqlite3'
          }
        }
      }
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'ReadRemoteDefinedDatabaseConnection',
      worker_func: async function (): Promise<number> {
        const sqlite_database =
          await wpc_database_connection<import('better-sqlite3').Database>({
            name: 'remote_db_connection_1',
            type: 'sqlite'
          });
        sqlite_database.exec('CREATE TABLE IF NOT EXISTS x (id INTEGER)');
        sqlite_database.prepare('INSERT INTO x (id) VALUES (?)').run(1);
        const row = sqlite_database.prepare('SELECT COUNT(*) as count FROM x').get() as {
          count: number;
        };
        return row.count;
      }
    });

    assert.equal(await workerprocedurecall.call.ReadRemoteDefinedDatabaseConnection(), 1);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation undefine_database_connection removes connection', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    await workerprocedurecall.defineDatabaseConnection({
      name: 'remote_db_connection_remove_1',
      connector: {
        type: 'sqlite',
        semantics: {
          filename: ':memory:',
          driver: 'better-sqlite3'
        }
      }
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'ReadRemovedRemoteDatabaseConnection',
      worker_func: async function (): Promise<string> {
        await wpc_database_connection<import('better-sqlite3').Database>({
          name: 'remote_db_connection_remove_1',
          type: 'sqlite'
        });
        return 'connected';
      }
    });

    assert.equal(
      await workerprocedurecall.call.ReadRemovedRemoteDatabaseConnection(),
      'connected'
    );

    await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'undefine_database_connection',
      payload: {
        name: 'remote_db_connection_remove_1'
      }
    });

    await assert.rejects(
      async function (): Promise<unknown> {
        return await workerprocedurecall.call.ReadRemovedRemoteDatabaseConnection();
      },
      /not defined/i
    );
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation dry-run returns dry_run_completed and has no side effects', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    const mutation_response = await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'define_constant',
      dry_run: true,
      payload: {
        name: 'DRY_RUN_CONSTANT',
        value: 'dry-run-value'
      }
    });

    assert.equal((mutation_response.ack as { accepted: boolean }).accepted, true);
    assert.equal(
      (mutation_response.terminal_message as { message_type: string }).message_type,
      'cluster_admin_mutation_result'
    );
    assert.equal(
      (mutation_response.terminal_message as { status: string }).status,
      'dry_run_completed'
    );

    await workerprocedurecall.defineWorkerFunction({
      name: 'ReadDryRunConstant',
      worker_func: async function (): Promise<string> {
        return wpc_constant('DRY_RUN_CONSTANT') as string;
      }
    });

    await assert.rejects(
      async function (): Promise<unknown> {
        return await workerprocedurecall.call.ReadDryRunConstant();
      },
      /not defined/i
    );
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation unsupported mutation type and invalid payload return deterministic errors', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    const unsupported_mutation_response = await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'shared_clear_all_chunks',
      payload: {}
    });
    assert.equal(
      (unsupported_mutation_response.terminal_message as { message_type: string })
        .message_type,
      'cluster_admin_mutation_error'
    );
    assert.equal(
      (unsupported_mutation_response.terminal_message as { error: { code: string } }).error
        .code,
      'ADMIN_VALIDATION_FAILED'
    );

    const invalid_payload_response = await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'define_dependency',
      payload: {
        alias: 'missing_module_specifier_only'
      }
    });
    assert.equal(
      (invalid_payload_response.terminal_message as { message_type: string }).message_type,
      'cluster_admin_mutation_error'
    );
    assert.equal(
      (invalid_payload_response.terminal_message as { error: { code: string } }).error.code,
      'ADMIN_VALIDATION_FAILED'
    );
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('single-node mutation emits requested/authorized/applied/denied lifecycle events', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';
  const observed_event_name_list: string[] = [];
  const listener_id = workerprocedurecall.onWorkerEvent({
    listener: function (worker_event: worker_event_t): void {
      observed_event_name_list.push(worker_event.event_name);
    }
  });

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'define_constant',
      payload: {
        name: 'ADMIN_EVENT_CONSTANT',
        value: 'x'
      }
    });

    await ApplySingleNodeMutation({
      workerprocedurecall,
      node_id,
      mutation_type: 'define_dependency',
      payload: {
        alias: 'invalid_dependency_missing_module_specifier'
      }
    });

    assert.equal(observed_event_name_list.includes('admin_mutation_requested'), true);
    assert.equal(observed_event_name_list.includes('admin_mutation_authorized'), true);
    assert.equal(observed_event_name_list.includes('admin_mutation_applied'), true);
    assert.equal(observed_event_name_list.includes('admin_mutation_denied'), true);
  } finally {
    workerprocedurecall.offWorkerEvent({
      listener_id
    });
    await workerprocedurecall.stopWorkers();
  }
});
