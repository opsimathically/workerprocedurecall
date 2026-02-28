import { WorkerProcedureCall } from './src/index';
import type * as BetterSqlite3 from 'better-sqlite3';

type shared_profile_t = {
  account_id: string;
  request_count: number;
  tags: string[];
};

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

    await workerprocedurecall.defineDatabaseConnection({
      name: 'db_connection_1',
      connector: {
        type: 'sqlite',
        semantics: {
          filename: ':memory:',
          driver: 'better-sqlite3'
        }
      }
    });

    // Use both import-like dependency loading and constants in the worker function.
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunction1',
      worker_func: async function (params: {
        file_path: string;
      }): Promise<string> {
        const path_module = await wpc_import<typeof import('node:path')>({
          alias: 'path_dep'
        });

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
        const crypto_module = await wpc_import<typeof import('node:crypto')>({
          alias: 'crypto_dep'
        });

        return crypto_module.randomUUID();
      }
    });

    // Shared memory chunk creation (fails if id is not unique).
    await workerprocedurecall.sharedCreate<shared_profile_t>({
      id: 'shared_profile_1',
      note: 'Shared profile object used across parent and workers.',
      type: 'json',
      content: {
        account_id: 'acct_001',
        request_count: 0,
        tags: ['example', 'shared-memory']
      }
    });

    // Parent-side lock, access, write, and release.
    const shared_profile = await workerprocedurecall.sharedAccess<shared_profile_t>({
      id: 'shared_profile_1'
    });

    await workerprocedurecall.sharedWrite<shared_profile_t>({
      id: 'shared_profile_1',
      content: {
        ...shared_profile,
        request_count: shared_profile.request_count + 1
      }
    });

    await workerprocedurecall.sharedRelease({
      id: 'shared_profile_1'
    });

    // Worker-side shared memory access through wpc_shared_* APIs.
    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionSharedIncrement',
      worker_func: async function (params: {
        shared_chunk_id: string;
      }): Promise<number> {
        const shared_profile = await wpc_shared_access<shared_profile_t>({
          id: params.shared_chunk_id,
          timeout_ms: 5_000
        });

        const next_request_count = shared_profile.request_count + 1;

        await wpc_shared_write<shared_profile_t>({
          id: params.shared_chunk_id,
          content: {
            ...shared_profile,
            request_count: next_request_count
          }
        });

        await wpc_shared_release({
          id: params.shared_chunk_id
        });

        return next_request_count;
      }
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCFunctionDb',
      worker_func: async function (params: {
        record_name: string;
      }): Promise<number> {
        const sqlite_database =
          await wpc_database_connection<BetterSqlite3.Database>({
            name: 'db_connection_1',
            type: 'sqlite'
          });

        sqlite_database.exec(
          'CREATE TABLE IF NOT EXISTS records (record_name TEXT NOT NULL)'
        );
        sqlite_database
          .prepare('INSERT INTO records (record_name) VALUES (?)')
          .run(params.record_name);

        const count_row = sqlite_database
          .prepare('SELECT COUNT(*) as total_record_count FROM records')
          .get() as {
          total_record_count: number;
        };

        console.log({ TOTAL_RECORD_COUNT: count_row.total_record_count });
        return count_row.total_record_count;
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
    const remote_database_connection_information =
      await workerprocedurecall.getWorkerDatabaseConnections();

    console.log('Remote functions:', remote_function_information);
    console.log('Remote dependencies:', remote_dependency_information);
    console.log('Remote constants:', remote_constant_information);
    console.log(
      'Remote database connections:',
      remote_database_connection_information
    );

    const return_val_1 = await workerprocedurecall.call.WPCFunction1({
      file_path: '/tmp/example-file.txt'
    });

    const return_val_2 = await workerprocedurecall.call.WPCFunction2();
    const return_val_db = await workerprocedurecall.call.WPCFunctionDb({
      record_name: 'example'
    });
    const shared_next_count =
      await workerprocedurecall.call.WPCFunctionSharedIncrement({
        shared_chunk_id: 'shared_profile_1'
      });

    console.log('WPCFunction1 return:', return_val_1);
    console.log('WPCFunction2 return:', return_val_2);
    console.log('WPCFunctionDb return:', return_val_db);
    console.log('WPCFunctionSharedIncrement return:', shared_next_count);

    const shared_lock_debug_information =
      await workerprocedurecall.sharedGetLockDebugInfo({
        include_history: true
      });
    console.log('Shared lock debug info:', shared_lock_debug_information);

    try {
      await workerprocedurecall.call.WPCFunctionTriggerEvent();
    } catch (error) {
      console.log('WPCFunctionTriggerEvent call error (expected):', error);
    }

    console.log(
      'Worker health states:',
      workerprocedurecall.getWorkerHealthStates()
    );

    await workerprocedurecall.undefineWokerFunction({
      name: 'WPCFunctionTriggerEvent'
    });
    await workerprocedurecall.undefineDatabaseConnection({
      name: 'db_connection_1'
    });
    await workerprocedurecall.sharedFree({
      id: 'shared_profile_1'
    });
    await workerprocedurecall.undefineWorkerDependency({ alias: 'crypto_dep' });
    await workerprocedurecall.undefineWokerFunction({
      name: 'WPCFunctionSharedIncrement'
    });
    await workerprocedurecall.undefineWokerFunction({ name: 'WPCFunction2' });
  } finally {
    workerprocedurecall.offWorkerEvent({
      listener_id: worker_event_listener_id
    });
    await workerprocedurecall.stopWorkers();
  }
})();
