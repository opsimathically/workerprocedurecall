import assert from 'node:assert/strict';
import test from 'node:test';

import { WorkerProcedureCall } from '../../src/index';

declare function wpc_import(alias: string): Promise<unknown>;
declare function wpc_constant(name: string): unknown;

async function WaitForSuccessfulCall(params: {
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
      await new Promise((resolve) => {
        setTimeout(resolve, delay_ms);
      });
    }
  }

  throw new Error(
    `Call "${function_name}" did not succeed after ${attempts} attempts. Last error: ${
      last_error instanceof Error ? last_error.message : String(last_error)
    }`
  );
}

test('define before start installs on start and supports calls', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunction1',
      worker_func: async function (params: { test_val: string }): Promise<string> {
        return `${params.test_val}_from_worker`;
      }
    });

    await workerprocedurecall.startWorkers({ count: 2 });

    const return_val = await workerprocedurecall.call.WPCFunction1({
      test_val: 'hello'
    });

    assert.equal(return_val, 'hello_from_worker');

    const remote_function_information = await workerprocedurecall.getRemoteFunctions();
    const found_function = remote_function_information.find((function_definition) => {
      return function_definition.name === 'WPCFunction1';
    });

    assert.equal(found_function?.installed_worker_count, 2);
    assert.equal(found_function?.parameter_signature, 'params');
    assert.match(found_function?.function_hash_sha1 ?? '', /^[a-f0-9]{40}$/);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('define after start installs across existing workers', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({ count: 3 });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunction2',
      worker_func: async function (params: { test_val: string }): Promise<string> {
        return `${params.test_val}_late_define`;
      }
    });

    const return_val = await workerprocedurecall.call.WPCFunction2({
      test_val: 'world'
    });

    assert.equal(return_val, 'world_late_define');

    const remote_function_information = await workerprocedurecall.getRemoteFunctions();
    const found_function = remote_function_information.find((function_definition) => {
      return function_definition.name === 'WPCFunction2';
    });

    assert.equal(found_function?.installed_worker_count, 3);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('function hash is stable for unchanged source and changes when redefined', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionHash',
      worker_func: async function (): Promise<string> {
        return 'hash_v1';
      }
    });

    await workerprocedurecall.startWorkers({ count: 1 });

    const first_metadata = await workerprocedurecall.getRemoteFunctions();
    const first_hash = first_metadata.find((function_definition) => {
      return function_definition.name === 'WPCFunctionHash';
    })?.function_hash_sha1;

    assert.equal(await workerprocedurecall.call.WPCFunctionHash(), 'hash_v1');

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionHash',
      worker_func: async function (): Promise<string> {
        return 'hash_v1';
      }
    });

    const second_metadata = await workerprocedurecall.getRemoteFunctions();
    const second_hash = second_metadata.find((function_definition) => {
      return function_definition.name === 'WPCFunctionHash';
    })?.function_hash_sha1;

    assert.equal(second_hash, first_hash);

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionHash',
      worker_func: async function (): Promise<string> {
        return 'hash_v2';
      }
    });

    const third_metadata = await workerprocedurecall.getRemoteFunctions();
    const third_hash = third_metadata.find((function_definition) => {
      return function_definition.name === 'WPCFunctionHash';
    })?.function_hash_sha1;

    assert.notEqual(third_hash, first_hash);
    assert.equal(await workerprocedurecall.call.WPCFunctionHash(), 'hash_v2');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('worker function errors propagate through RPC response', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionError',
      worker_func: async function (): Promise<void> {
        const worker_error = new Error('intentional worker failure');
        worker_error.name = 'WorkerBoom';
        throw worker_error;
      }
    });

    await workerprocedurecall.startWorkers({ count: 1 });

    await assert.rejects(
      async function (): Promise<unknown> {
        return await workerprocedurecall.call.WPCFunctionError({});
      },
      function (error: unknown): boolean {
        assert(error instanceof Error);
        assert.equal(error.name, 'WorkerBoom');
        assert.match(error.message, /intentional worker failure/i);
        return true;
      }
    );
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('undefine removes function from registry and prevents future calls', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({ count: 2 });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunction3',
      worker_func: async function (params: { test_val: string }): Promise<string> {
        return params.test_val;
      }
    });

    const initial_return_val = await workerprocedurecall.call.WPCFunction3({
      test_val: 'works'
    });

    assert.equal(initial_return_val, 'works');

    await workerprocedurecall.undefineWokerFunction({ name: 'WPCFunction3' });

    const remote_function_information = await workerprocedurecall.getRemoteFunctions();
    const removed_function = remote_function_information.find((function_definition) => {
      return function_definition.name === 'WPCFunction3';
    });

    assert.equal(removed_function, undefined);

    await assert.rejects(async function (): Promise<unknown> {
      return await workerprocedurecall.call.WPCFunction3({ test_val: 'nope' });
    }, /not defined/i);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('call timeout rejects when worker call exceeds configured timeout', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionSlow',
      worker_func: async function (): Promise<string> {
        await new Promise((resolve) => {
          setTimeout(resolve, 100);
        });

        return 'done';
      }
    });

    await workerprocedurecall.startWorkers({
      count: 1,
      call_timeout_ms: 25
    });

    await assert.rejects(async function (): Promise<unknown> {
      return await workerprocedurecall.call.WPCFunctionSlow({});
    }, /timed out/i);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('define dependency before start supports import-like loading in worker', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerDependency({
      alias: 'path_dep',
      module_specifier: 'node:path'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionWithDependencyBeforeStart',
      worker_func: async function (params: { value: string }): Promise<string> {
        const path_module = (await wpc_import('path_dep')) as {
          basename: (value: string) => string;
        };

        return path_module.basename(params.value);
      }
    });

    await workerprocedurecall.startWorkers({ count: 2 });

    const return_val = await workerprocedurecall.call
      .WPCFunctionWithDependencyBeforeStart({
        value: '/tmp/alpha.txt'
      });

    assert.equal(return_val, 'alpha.txt');

    const dependency_information = await workerprocedurecall.getWorkerDependencies();
    const found_dependency = dependency_information.find((dependency_definition) => {
      return dependency_definition.alias === 'path_dep';
    });

    assert.equal(found_dependency?.installed_worker_count, 2);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('define dependency after start installs into running workers', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({ count: 2 });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionWithDependencyAfterStart',
      worker_func: async function (): Promise<number> {
        const crypto_module = (await wpc_import('crypto_dep')) as {
          randomUUID: () => string;
        };

        return crypto_module.randomUUID().length;
      }
    });

    await workerprocedurecall.defineWorkerDependency({
      alias: 'crypto_dep',
      module_specifier: 'node:crypto'
    });

    const return_val = await workerprocedurecall.call
      .WPCFunctionWithDependencyAfterStart();

    assert.equal(return_val, 36);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('undefining dependency prevents subsequent function calls', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerDependency({
      alias: 'path_dep_remove',
      module_specifier: 'node:path'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionDependsOnRemovedDep',
      worker_func: async function (params: { val: string }): Promise<string> {
        const path_module = (await wpc_import('path_dep_remove')) as {
          basename: (value: string) => string;
        };

        return path_module.basename(params.val);
      }
    });

    await workerprocedurecall.startWorkers({ count: 1 });

    assert.equal(
      await workerprocedurecall.call.WPCFunctionDependsOnRemovedDep({
        val: '/tmp/test.txt'
      }),
      'test.txt'
    );

    await workerprocedurecall.undefineWorkerDependency({
      alias: 'path_dep_remove'
    });

    await assert.rejects(async function (): Promise<unknown> {
      return await workerprocedurecall.call.WPCFunctionDependsOnRemovedDep({
        val: '/tmp/test.txt'
      });
    }, /requires dependency alias "path_dep_remove" which is not defined/i);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('dependency load failures report worker and module details', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({ count: 1 });

    await assert.rejects(
      async function (): Promise<void> {
        await workerprocedurecall.defineWorkerDependency({
          alias: 'bad_dep',
          module_specifier: 'this_module_does_not_exist_for_workerprocedurecall'
        });
      },
      function (error: unknown): boolean {
        assert(error instanceof Error);
        assert.match(error.message, /Failed to install dependency "bad_dep"/i);
        assert.match(
          error.message,
          /this_module_does_not_exist_for_workerprocedurecall/i
        );
        assert.match(error.message, /Worker 1/i);
        return true;
      }
    );
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('worker restart rehydrates dependencies for dependent calls', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerDependency({
      alias: 'path_dep_restart',
      module_specifier: 'node:path'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionDependencyAfterRestart',
      worker_func: async function (params: { input: string }): Promise<string> {
        const path_module = (await wpc_import('path_dep_restart')) as {
          basename: (value: string) => string;
        };

        return path_module.basename(params.input);
      }
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionCrashWorker',
      worker_func: async function (): Promise<void> {
        process.exit(1);
      }
    });

    await workerprocedurecall.startWorkers({ count: 1 });

    assert.equal(
      await workerprocedurecall.call.WPCFunctionDependencyAfterRestart({
        input: '/tmp/restart.txt'
      }),
      'restart.txt'
    );

    await assert.rejects(async function (): Promise<unknown> {
      return await workerprocedurecall.call.WPCFunctionCrashWorker();
    });

    const return_val = await WaitForSuccessfulCall({
      workerprocedurecall,
      function_name: 'WPCFunctionDependencyAfterRestart',
      call_args: [{ input: '/tmp/restart_after.txt' }],
      attempts: 25,
      delay_ms: 60
    });

    assert.equal(return_val, 'restart_after.txt');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('worker constants can be defined and consumed in worker functions', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerConstant({
      name: 'GREETING',
      value: 'hello'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionWithConstant',
      worker_func: async function (name: string): Promise<string> {
        const greeting = wpc_constant('GREETING') as string;
        return `${greeting}, ${name}`;
      }
    });

    await workerprocedurecall.startWorkers({ count: 1 });

    const return_val = await workerprocedurecall.call.WPCFunctionWithConstant(
      'world'
    );

    assert.equal(return_val, 'hello, world');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('parallel calls are balanced across workers with least in-flight scheduling', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionThreadId',
      worker_func: async function (): Promise<number> {
        await new Promise((resolve) => {
          setTimeout(resolve, 25);
        });

        const worker_threads = require('node:worker_threads') as {
          threadId: number;
        };

        return worker_threads.threadId;
      }
    });

    await workerprocedurecall.startWorkers({ count: 3 });

    const call_promises = Array.from({ length: 30 }, async (): Promise<number> => {
      const return_val = await workerprocedurecall.call.WPCFunctionThreadId({});
      return return_val as number;
    });

    const worker_ids = await Promise.all(call_promises);
    const call_count_by_worker_id = new Map<number, number>();

    for (const worker_id of worker_ids) {
      call_count_by_worker_id.set(
        worker_id,
        (call_count_by_worker_id.get(worker_id) ?? 0) + 1
      );
    }

    assert.equal(call_count_by_worker_id.size, 3);

    const call_counts = Array.from(call_count_by_worker_id.values());
    const max_call_count = Math.max(...call_counts);
    const min_call_count = Math.min(...call_counts);

    assert(max_call_count - min_call_count <= 1);
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('saturation guard rejects calls when all workers exceed pending limit', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionBlocking',
      worker_func: async function (): Promise<string> {
        await new Promise((resolve) => {
          setTimeout(resolve, 100);
        });

        return 'ok';
      }
    });

    await workerprocedurecall.startWorkers({
      count: 1,
      max_pending_calls_per_worker: 1
    });

    const first_call_promise = workerprocedurecall.call.WPCFunctionBlocking({});

    await assert.rejects(async function (): Promise<unknown> {
      return await workerprocedurecall.call.WPCFunctionBlocking({});
    }, /saturated/i);

    assert.equal(await first_call_promise, 'ok');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('stopWorkers is graceful and idempotent', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunction4',
      worker_func: async function (): Promise<string> {
        return 'ok';
      }
    });

    await workerprocedurecall.startWorkers({ count: 1 });
    await workerprocedurecall.stopWorkers();

    await assert.rejects(async function (): Promise<unknown> {
      return await workerprocedurecall.call.WPCFunction4({});
    }, /not running/i);

    await workerprocedurecall.stopWorkers();
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});
