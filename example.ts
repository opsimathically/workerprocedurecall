import { WorkerProcedureCall } from './src/index';

(async function () {
  const workerprocedurecall = new WorkerProcedureCall({
    call_timeout_ms: 30_000,
    control_timeout_ms: 10_000,
    restart_on_failure: true,
    max_restarts_per_worker: 3,
    max_pending_calls_per_worker: 1_000,
    restart_base_delay_ms: 100,
    restart_max_delay_ms: 5_000,
    restart_jitter_ms: 100
  });

  const worker_event_listener_id = workerprocedurecall.onWorkerEvent({
    listener: function (worker_event): void {
      console.log('[worker_event]', worker_event);
    }
  });

  try {
    // Register reusable worker-side data before startup.
    await workerprocedurecall.defineWorkerConstant({
      name: 'SERVICE_PREFIX',
      value: 'worker-service'
    });

    // Register dependency alias before startup.
    await workerprocedurecall.defineWorkerDependency({
      alias: 'path_dep',
      module_specifier: 'node:path'
    });

    // Use both import-like dependency loading and constants in the worker function.
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunction1',
      worker_func: async function (params: {
        file_path: string;
      }): Promise<string> {
        const path_module = (await wpc_import('path_dep')) as {
          basename: (value: string) => string;
        };

        const service_prefix = wpc_constant('SERVICE_PREFIX') as string;

        return `${service_prefix}:${path_module.basename(params.file_path)}`;
      }
    });

    await workerprocedurecall.startWorkers({
      count: 4,
      call_timeout_ms: 20_000,
      restart_on_failure: true
    });

    // Register dependency alias after workers are running.
    await workerprocedurecall.defineWorkerDependency({
      alias: 'crypto_dep',
      module_specifier: 'node:crypto'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunction2',
      worker_func: async function (): Promise<string> {
        const crypto_module = (await wpc_import('crypto_dep')) as {
          randomUUID: () => string;
        };

        return crypto_module.randomUUID();
      }
    });

    // Intentionally throws to trigger a worker_event (call_execution_failed).
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionTriggerEvent',
      worker_func: async function (): Promise<string> {
        throw new Error('intentional event trigger from worker function');
      }
    });

    const remote_function_information =
      await workerprocedurecall.getRemoteFunctions();
    const remote_dependency_information =
      await workerprocedurecall.getWorkerDependencies();
    const remote_constant_information =
      await workerprocedurecall.getWorkerConstants();

    console.log('Remote functions:', remote_function_information);
    console.log('Remote dependencies:', remote_dependency_information);
    console.log('Remote constants:', remote_constant_information);

    const return_val_1 = await workerprocedurecall.call.WPCFunction1({
      file_path: '/tmp/example-file.txt'
    });

    const return_val_2 = await workerprocedurecall.call.WPCFunction2();

    console.log('WPCFunction1 return:', return_val_1);
    console.log('WPCFunction2 return:', return_val_2);

    try {
      await workerprocedurecall.call.WPCFunctionTriggerEvent();
    } catch (error) {
      console.log('WPCFunctionTriggerEvent call error (expected):', error);
    }

    console.log('Worker health states:', workerprocedurecall.getWorkerHealthStates());

    await workerprocedurecall.undefineWokerFunction({
      name: 'WPCFunctionTriggerEvent'
    });
    await workerprocedurecall.undefineWorkerDependency({ alias: 'crypto_dep' });
    await workerprocedurecall.undefineWokerFunction({ name: 'WPCFunction2' });
  } finally {
    workerprocedurecall.offWorkerEvent({
      listener_id: worker_event_listener_id
    });
    await workerprocedurecall.stopWorkers();
  }
})();
