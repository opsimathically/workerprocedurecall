import assert from 'node:assert/strict';
import test from 'node:test';

import { WorkerProcedureCall } from '../../src/index';

async function Sleep(params: { delay_ms: number }): Promise<void> {
  const { delay_ms } = params;
  await new Promise((resolve): void => {
    setTimeout(resolve, delay_ms);
  });
}

async function WaitForSuccessfulWorkerCall(params: {
  workerprocedurecall: WorkerProcedureCall;
  function_name: string;
  call_args: unknown[];
  attempts: number;
  delay_ms: number;
}): Promise<unknown> {
  const { workerprocedurecall, function_name, call_args, attempts, delay_ms } = params;

  let last_error: unknown;
  for (let attempt_index = 0; attempt_index < attempts; attempt_index += 1) {
    try {
      const dynamic_call = workerprocedurecall.call[function_name];
      return await dynamic_call(...call_args);
    } catch (error) {
      last_error = error;
      await Sleep({ delay_ms });
    }
  }

  throw new Error(
    `Worker call "${function_name}" failed after ${attempts} attempts: ${
      last_error instanceof Error ? last_error.message : String(last_error)
    }`
  );
}

test('shared memory create/access/write/release/free works from parent', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  await workerprocedurecall.sharedCreate({
    id: 'shared_parent_json_1',
    type: 'json',
    note: 'parent create/write/read test',
    content: {
      version: 1,
      enabled: true
    }
  });

  const first_value = await workerprocedurecall.sharedAccess<{
    version: number;
    enabled: boolean;
  }>({
    id: 'shared_parent_json_1'
  });

  assert.deepEqual(first_value, {
    version: 1,
    enabled: true
  });

  await workerprocedurecall.sharedWrite({
    id: 'shared_parent_json_1',
    content: {
      version: 2,
      enabled: false
    }
  });

  await workerprocedurecall.sharedRelease({
    id: 'shared_parent_json_1'
  });

  const second_value = await workerprocedurecall.sharedAccess<{
    version: number;
    enabled: boolean;
  }>({
    id: 'shared_parent_json_1'
  });

  assert.deepEqual(second_value, {
    version: 2,
    enabled: false
  });

  await workerprocedurecall.sharedRelease({
    id: 'shared_parent_json_1'
  });

  await workerprocedurecall.sharedFree({
    id: 'shared_parent_json_1'
  });

  await assert.rejects(
    async function (): Promise<unknown> {
      return await workerprocedurecall.sharedAccess({
        id: 'shared_parent_json_1'
      });
    },
    /not found/i
  );
});

test('shared memory free rejects while chunk is locked', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  await workerprocedurecall.sharedCreate({
    id: 'shared_free_locked_1',
    type: 'text',
    content: 'hello'
  });

  const value = await workerprocedurecall.sharedAccess<string>({
    id: 'shared_free_locked_1'
  });
  assert.equal(value, 'hello');

  await assert.rejects(
    async function (): Promise<void> {
      await workerprocedurecall.sharedFree({
        id: 'shared_free_locked_1'
      });
    },
    /locked/i
  );

  await workerprocedurecall.sharedRelease({
    id: 'shared_free_locked_1'
  });
  await workerprocedurecall.sharedFree({
    id: 'shared_free_locked_1'
  });
});

test('worker shared lock contention serializes writes safely', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({
      count: 2
    });

    await workerprocedurecall.sharedCreate({
      id: 'shared_counter_1',
      type: 'number',
      content: 0
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'IncrementSharedCounter',
      worker_func: async function (params: {
        id: string;
        delay_ms: number;
      }): Promise<number> {
        const current_value = await wpc_shared_access<number>({
          id: params.id
        });

        await new Promise((resolve): void => {
          setTimeout(resolve, params.delay_ms);
        });

        await wpc_shared_write<number>({
          id: params.id,
          content: current_value + 1
        });

        await wpc_shared_release({
          id: params.id
        });

        return current_value + 1;
      }
    });

    const increment_promises: Promise<unknown>[] = [];
    for (let operation_index = 0; operation_index < 8; operation_index += 1) {
      increment_promises.push(
        workerprocedurecall.call.IncrementSharedCounter({
          id: 'shared_counter_1',
          delay_ms: 8
        })
      );
    }

    await Promise.all(increment_promises);

    const final_counter_value = await workerprocedurecall.sharedAccess<number>({
      id: 'shared_counter_1'
    });
    assert.equal(final_counter_value, 8);

    await workerprocedurecall.sharedRelease({
      id: 'shared_counter_1'
    });
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('shared lock is reclaimed when worker exits unexpectedly', async function () {
  const workerprocedurecall = new WorkerProcedureCall({
    restart_on_failure: false
  });

  try {
    await workerprocedurecall.startWorkers({
      count: 2
    });

    await workerprocedurecall.sharedCreate({
      id: 'shared_worker_crash_lock_1',
      type: 'text',
      content: 'still_available'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'CrashWhileHoldingSharedLock',
      worker_func: async function (params: { id: string }): Promise<void> {
        await wpc_shared_access<string>({
          id: params.id
        });

        process.exit(1);
      }
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'ReadReleaseSharedAfterCrash',
      worker_func: async function (params: { id: string }): Promise<string> {
        const value = await wpc_shared_access<string>({
          id: params.id
        });

        await wpc_shared_release({
          id: params.id
        });

        return value;
      }
    });

    await assert.rejects(
      async function (): Promise<unknown> {
        return await workerprocedurecall.call.CrashWhileHoldingSharedLock({
          id: 'shared_worker_crash_lock_1'
        });
      },
      /exited|stopped|failed/i
    );

    await Sleep({ delay_ms: 100 });

    const post_crash_value = await WaitForSuccessfulWorkerCall({
      workerprocedurecall,
      function_name: 'ReadReleaseSharedAfterCrash',
      call_args: [
        {
          id: 'shared_worker_crash_lock_1'
        }
      ],
      attempts: 5,
      delay_ms: 50
    });

    assert.equal(post_crash_value, 'still_available');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('shared lock debug info reports lock holder and waiters', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({
      count: 1
    });

    await workerprocedurecall.sharedCreate({
      id: 'shared_debug_1',
      type: 'number',
      content: 42
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'ReadAndReleaseSharedDebug',
      worker_func: async function (params: { id: string }): Promise<number> {
        const value = await wpc_shared_access<number>({
          id: params.id,
          timeout_ms: 5000
        });

        await wpc_shared_release({
          id: params.id
        });

        return value;
      }
    });

    const parent_locked_value = await workerprocedurecall.sharedAccess<number>({
      id: 'shared_debug_1'
    });
    assert.equal(parent_locked_value, 42);

    const waiting_worker_call = workerprocedurecall.call.ReadAndReleaseSharedDebug({
      id: 'shared_debug_1'
    });

    await Sleep({ delay_ms: 50 });

    const lock_debug_information = await workerprocedurecall.sharedGetLockDebugInfo({
      include_history: true
    });
    const chunk_debug_entry = lock_debug_information.chunks.find((chunk) => {
      return chunk.id === 'shared_debug_1';
    });

    assert(chunk_debug_entry);
    assert.equal(chunk_debug_entry.is_locked, true);
    assert.ok((chunk_debug_entry.waiter_count ?? 0) >= 1);
    assert.equal(chunk_debug_entry.lock_owner_kind, 'parent');

    await workerprocedurecall.sharedRelease({
      id: 'shared_debug_1'
    });

    const resolved_worker_value = await waiting_worker_call;
    assert.equal(resolved_worker_value, 42);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});
