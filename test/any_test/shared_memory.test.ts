import assert from 'node:assert/strict';
import test from 'node:test';

import { WorkerProcedureCall, type worker_event_t } from '../../src/index';

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

test('shared lock leaks are auto-released after successful worker calls', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const observed_worker_event_list: worker_event_t[] = [];
  const listener_id = workerprocedurecall.onWorkerEvent({
    listener: (worker_event): void => {
      observed_worker_event_list.push(worker_event);
    }
  });

  try {
    await workerprocedurecall.startWorkers({
      count: 1
    });

    await workerprocedurecall.sharedCreate({
      id: 'shared_auto_release_success_1',
      type: 'text',
      content: 'success_value'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'AcquireSharedWithoutReleaseAndReturn',
      worker_func: async function (params: { id: string }): Promise<string> {
        const value = await wpc_shared_access<string>({
          id: params.id
        });
        return value;
      }
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'AcquireReleaseSharedAndReturn',
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

    const first_result = await workerprocedurecall.call.AcquireSharedWithoutReleaseAndReturn({
      id: 'shared_auto_release_success_1'
    });
    assert.equal(first_result, 'success_value');

    const second_result = await workerprocedurecall.call.AcquireReleaseSharedAndReturn({
      id: 'shared_auto_release_success_1'
    });
    assert.equal(second_result, 'success_value');

    const lock_debug_information = await workerprocedurecall.sharedGetLockDebugInfo({
      include_history: true
    });
    const auto_release_event = lock_debug_information.recent_events?.find((event_entry) => {
      return (
        event_entry.chunk_id === 'shared_auto_release_success_1' &&
        event_entry.event_name === 'call_complete_auto_release'
      );
    });
    assert(auto_release_event);
    assert.equal(typeof auto_release_event.details?.worker_id, 'number');
    assert.equal(typeof auto_release_event.details?.call_request_id, 'string');
    assert.equal(auto_release_event.details?.lock_count_released, 1);

    const parent_auto_release_event = observed_worker_event_list.find((event_entry) => {
      return (
        event_entry.source === 'parent' &&
        event_entry.event_name === 'call_complete_auto_release'
      );
    });
    assert(parent_auto_release_event);
    assert.equal(typeof parent_auto_release_event.details?.lock_count_released, 'number');
    assert.equal(typeof parent_auto_release_event.details?.call_request_id, 'string');
  } finally {
    workerprocedurecall.offWorkerEvent({
      listener_id
    });
    await workerprocedurecall.stopWorkers();
  }
});

test('shared lock leaks are auto-released when worker call throws', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({
      count: 1
    });

    await workerprocedurecall.sharedCreate({
      id: 'shared_auto_release_throw_1',
      type: 'text',
      content: 'throw_value'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'AcquireSharedThenThrow',
      worker_func: async function (params: { id: string }): Promise<void> {
        await wpc_shared_access<string>({
          id: params.id
        });
        throw new Error('intentional_throw_after_lock');
      }
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'AcquireReleaseSharedAfterThrow',
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
        return await workerprocedurecall.call.AcquireSharedThenThrow({
          id: 'shared_auto_release_throw_1'
        });
      },
      /intentional_throw_after_lock/i
    );

    const recovered_value = await workerprocedurecall.call.AcquireReleaseSharedAfterThrow({
      id: 'shared_auto_release_throw_1'
    });
    assert.equal(recovered_value, 'throw_value');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('shared lock leaks are auto-released on worker call timeout', async function () {
  const workerprocedurecall = new WorkerProcedureCall({
    call_timeout_ms: 50
  });

  try {
    await workerprocedurecall.startWorkers({
      count: 2
    });

    await workerprocedurecall.sharedCreate({
      id: 'shared_auto_release_timeout_1',
      type: 'text',
      content: 'timeout_value'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'AcquireSharedAndSleepPastTimeout',
      worker_func: async function (params: {
        id: string;
        delay_ms: number;
      }): Promise<string> {
        const value = await wpc_shared_access<string>({
          id: params.id
        });

        await new Promise((resolve): void => {
          setTimeout(resolve, params.delay_ms);
        });

        return value;
      }
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'AcquireReleaseSharedAfterTimeout',
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
        return await workerprocedurecall.call.AcquireSharedAndSleepPastTimeout({
          id: 'shared_auto_release_timeout_1',
          delay_ms: 600
        });
      },
      /timed out/i
    );

    const second_call_start_timestamp_ms = Date.now();
    const recovered_value = await workerprocedurecall.call.AcquireReleaseSharedAfterTimeout({
      id: 'shared_auto_release_timeout_1'
    });
    const second_call_elapsed_ms = Date.now() - second_call_start_timestamp_ms;

    assert.equal(recovered_value, 'timeout_value');
    assert.ok(
      second_call_elapsed_ms < 450,
      `Expected post-timeout lock acquisition to be reclaimed quickly; took ${second_call_elapsed_ms}ms.`
    );

    const lock_debug_information = await workerprocedurecall.sharedGetLockDebugInfo({
      include_history: true
    });
    const timeout_auto_release_event = lock_debug_information.recent_events?.find(
      (event_entry) => {
        return (
          event_entry.chunk_id === 'shared_auto_release_timeout_1' &&
          event_entry.event_name === 'call_timeout_auto_release'
        );
      }
    );
    assert(timeout_auto_release_event);
    assert.equal(typeof timeout_auto_release_event.details?.worker_id, 'number');
    assert.equal(typeof timeout_auto_release_event.details?.call_request_id, 'string');
    assert.equal(timeout_auto_release_event.details?.lock_count_released, 1);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('manual shared release is idempotent with automatic lifecycle release checks', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({
      count: 1
    });

    await workerprocedurecall.sharedCreate({
      id: 'shared_manual_release_idempotent_1',
      type: 'number',
      content: 9
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'AcquireReleaseSharedNumber',
      worker_func: async function (params: { id: string }): Promise<number> {
        const value = await wpc_shared_access<number>({
          id: params.id
        });
        await wpc_shared_release({
          id: params.id
        });
        return value;
      }
    });

    const first_value = await workerprocedurecall.call.AcquireReleaseSharedNumber({
      id: 'shared_manual_release_idempotent_1'
    });
    assert.equal(first_value, 9);

    const second_value = await workerprocedurecall.call.AcquireReleaseSharedNumber({
      id: 'shared_manual_release_idempotent_1'
    });
    assert.equal(second_value, 9);

    const lock_debug_information = await workerprocedurecall.sharedGetLockDebugInfo({
      include_history: true
    });
    const call_complete_auto_release_events =
      lock_debug_information.recent_events?.filter((event_entry) => {
        return (
          event_entry.chunk_id === 'shared_manual_release_idempotent_1' &&
          event_entry.event_name === 'call_complete_auto_release'
        );
      }) ?? [];

    assert.equal(call_complete_auto_release_events.length, 0);
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

    const lock_debug_information = await workerprocedurecall.sharedGetLockDebugInfo({
      include_history: true
    });
    const worker_exit_auto_release_event = lock_debug_information.recent_events?.find(
      (event_entry) => {
        return (
          event_entry.chunk_id === 'shared_worker_crash_lock_1' &&
          event_entry.event_name === 'worker_exit_auto_release'
        );
      }
    );
    assert(worker_exit_auto_release_event);
    assert.equal(typeof worker_exit_auto_release_event.details?.worker_id, 'number');
    assert.equal(
      typeof worker_exit_auto_release_event.details?.call_request_id,
      'string'
    );
    assert.equal(worker_exit_auto_release_event.details?.lock_count_released, 1);
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
