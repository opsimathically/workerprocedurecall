# workerprocedurecall

Worker-thread RPC for Node.js with dynamic function, dependency, and constant registration via message passing.

## Install

```bash
npm install @opsimathically/workerprocedurecall
```

## API

```typescript
import { WorkerProcedureCall } from '@opsimathically/workerprocedurecall';
import type * as BetterSqlite3 from 'better-sqlite3';

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
    listener: (worker_event) => {
      console.log('Worker event:', worker_event);
    }
  });

  // Optional constants for worker functions.
  await workerprocedurecall.defineWorkerConstant({
    name: 'SERVICE_PREFIX',
    value: 'api-v1'
  });

  // Define dependency alias before workers start.
  await workerprocedurecall.defineWorkerDependency({
    alias: 'path_dep',
    module_specifier: 'node:path'
  });

  await workerprocedurecall.defineDatabaseConnection({
    name: 'sqlite_main',
    connector: {
      type: 'sqlite',
      semantics: {
        filename: ':memory:',
        driver: 'better-sqlite3'
      }
    }
  });

  await workerprocedurecall.defineWorkerFunction({
    name: 'WPCFunction1',
    worker_func: async function (file_path: string): Promise<string> {
      const path_module = await wpc_import<typeof import('node:path')>({
        alias: 'path_dep'
      });

      const prefix = wpc_constant('SERVICE_PREFIX') as string;
      return `${prefix}:${path_module.basename(file_path)}`;
    }
  });

  await workerprocedurecall.startWorkers({ count: 4 });

  // Define dependency after workers already started.
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

  await workerprocedurecall.defineWorkerFunction({
    name: 'WPCFunctionDb',
    worker_func: async function (record_name: string): Promise<number> {
      const sqlite_database = await wpc_database_connection<
        BetterSqlite3.Database
      >({
        name: 'sqlite_main',
        type: 'sqlite'
      });

      sqlite_database.exec(
        'CREATE TABLE IF NOT EXISTS records (record_name TEXT NOT NULL)'
      );

      sqlite_database.prepare('INSERT INTO records (record_name) VALUES (?)').run(
        record_name
      );
      const row = sqlite_database
        .prepare('SELECT COUNT(*) as total_record_count FROM records')
        .get() as { total_record_count: number };

      return row.total_record_count;
    }
  });

  const remote_function_information = await workerprocedurecall.getRemoteFunctions();
  const dependency_information = await workerprocedurecall.getWorkerDependencies();
  const constant_information = await workerprocedurecall.getWorkerConstants();
  const database_connection_information =
    await workerprocedurecall.getWorkerDatabaseConnections();

  // Each function metadata entry includes function_hash_sha1.
  console.log(
    remote_function_information,
    dependency_information,
    constant_information,
    database_connection_information
  );

  const function1_return_val = await workerprocedurecall.call.WPCFunction1(
    '/tmp/example.txt'
  );

  const function2_return_val = await workerprocedurecall.call.WPCFunction2();
  const function_db_return_val = await workerprocedurecall.call.WPCFunctionDb('alpha');

  console.log(function1_return_val, function2_return_val, function_db_return_val);

  await workerprocedurecall.undefineDatabaseConnection({ name: 'sqlite_main' });
  await workerprocedurecall.undefineWorkerDependency({ alias: 'crypto_dep' });
  await workerprocedurecall.undefineWokerFunction({ name: 'WPCFunction2' });
  workerprocedurecall.offWorkerEvent({ listener_id: worker_event_listener_id });

  await workerprocedurecall.stopWorkers();
})();
```

## How Dependency Loading Works

- Use `defineWorkerDependency` to register an alias to a module specifier.
- In worker functions, call `await wpc_import('alias')`.
- For explicit typed handles without casts, use:
  - `await wpc_import<typeof import('node:path')>({ alias: 'path_dep' })`
- Optional alternative: augment `wpc_dependency_by_alias_i` for alias-based inference so `wpc_import('path_dep')` returns the mapped type.
- Only registered aliases are accessible (allowlist model).
- Worker runtime resolves modules using `require(...)`, then falls back to dynamic `import(...)`.
- Module results are cached in each worker's dependency registry.

## Resilience Model

- Worker runtime catches command/call failures and reports them as `worker_event` messages.
- Global worker guards are enabled:
  - `process.on('uncaughtException', ...)`
  - `process.on('unhandledRejection', ...)`
- Parent thread exposes callback subscription:
  - `onWorkerEvent({ listener })`
  - `offWorkerEvent({ listener_id })`
- Parent-side supervisor restarts failed workers with bounded retry + exponential backoff + jitter.
- Scheduler avoids workers not in `ready` health state.
- Worker health can be inspected via `getWorkerHealthStates()`.
- Unavoidable failures still exist (OOM/native abort/SIGKILL/runtime fatal failures) and cannot always be recovered in-process.

### Worker Event Shape

- `event_id: string`
- `worker_id: number`
- `source: 'worker' | 'parent'`
- `event_name: string`
- `severity: 'info' | 'warn' | 'error'`
- `timestamp: string`
- `correlation_id?: string`
- `error?: { name, message, stack? }`
- `details?: Record<string, unknown>`

## How Constants Work

- Use `defineWorkerConstant({ name, value })` to define structured-clone-safe constant values.
- In worker functions, call `wpc_constant('NAME')`.
- Constants can be added/updated/removed before or after workers start.

## How Database Connections Work

- Use `defineDatabaseConnection({ name, connector: { type, semantics } })`.
- Connections are created when each worker installs the definition.
- If any worker fails to connect, the define/start operation fails with worker + connector details.
- In worker functions, call `await wpc_database_connection('name')`.
- For explicit typed handles with no global type declarations, use the generic/object form:
  ```typescript
  import type * as BetterSqlite3 from 'better-sqlite3';

  const sqlite_database = await wpc_database_connection<BetterSqlite3.Database>({
    name: 'sqlite_main',
    type: 'sqlite'
  });
  ```
- Optional alternative: use global declaration merging via `wpc_database_connection_handle_by_name_i` if you prefer name-based inference.
- For custom type overrides, augment either:
  - `wpc_database_connection_handle_by_name_i` (name-specific handle type)
  - `wpc_database_connector_handle_overrides_i` (connector-type handle override)
- Connections are kept in each worker and reused across calls.
- On connection loss, worker runtime marks the connection unavailable and reconnects on next access.
- Connections are closed on undefine and on worker shutdown.

### Connector Semantics Examples

```typescript
await workerprocedurecall.defineDatabaseConnection({
  name: 'mongodb_main',
  connector: {
    type: 'mongodb',
    semantics: {
      uri: 'mongodb://127.0.0.1:27017',
      database_name: 'app_db',
      client_options: {
        maxPoolSize: 10
      }
    }
  }
});

await workerprocedurecall.defineDatabaseConnection({
  name: 'postgresql_main',
  connector: {
    type: 'postgresql',
    semantics: {
      connection_string: 'postgresql://user:password@127.0.0.1:5432/app_db',
      use_pool: true,
      pool_options: {
        max: 10
      }
    }
  }
});

await workerprocedurecall.defineDatabaseConnection({
  name: 'mysql_main',
  connector: {
    type: 'mysql',
    semantics: {
      use_pool: true,
      pool_options: {
        host: '127.0.0.1',
        port: 3306,
        user: 'user',
        password: 'password',
        database: 'app_db'
      }
    }
  }
});

await workerprocedurecall.defineDatabaseConnection({
  name: 'mariadb_main',
  connector: {
    type: 'mariadb',
    semantics: {
      use_pool: true,
      pool_options: {
        host: '127.0.0.1',
        port: 3306,
        user: 'user',
        password: 'password',
        database: 'app_db'
      }
    }
  }
});

await workerprocedurecall.defineDatabaseConnection({
  name: 'sqlite_main',
  connector: {
    type: 'sqlite',
    semantics: {
      filename: ':memory:',
      driver: 'better-sqlite3'
    }
  }
});
```

## Behavior Notes

- Parent thread is the source of truth for functions, dependencies, constants, and database connections.
- Parent thread is also the source of truth for shared-memory chunk metadata and lock ownership.
- Functions, dependencies, constants, and database connections can be defined before startup or while workers are running.
- Function metadata includes `function_hash_sha1` so callers can detect function version drift.
- New workers (including restarted workers) receive full registry synchronization on startup.
- Calls use request/response correlation IDs and timeout handling.
- Worker errors are propagated with `name`, `message`, and `stack` when available.
- Worker shutdown is graceful first, then force-terminate fallback on timeout.
- Scheduling is least in-flight across eligible workers, with deterministic tie-breaking.
- Calls are dispatched only to workers that are ready and have function + required dependency/constant/database connection installation confirmed.
- Optional per-worker saturation protection is available with `max_pending_calls_per_worker`.
- Restart pacing is configurable with:
  - `restart_base_delay_ms`
  - `restart_max_delay_ms`
  - `restart_jitter_ms`

## Shared Memory Chunks

- Shared chunks are stored in a parent-managed `SharedArrayBuffer` heap.
- Supported chunk types:
  - `json`
  - `text`
  - `number`
  - `binary`
- Parent APIs:
  - `sharedCreate({ id, note?, type, content })`
  - `sharedAccess({ id, timeout_ms? })`
  - `sharedWrite({ id, content })`
  - `sharedRelease({ id })`
  - `sharedFree({ id, require_unlocked? })`
  - `sharedGetLockDebugInfo({ include_history?, min_held_ms? })`
- Worker APIs (inside worker functions):
  - `await wpc_shared_create(...)`
  - `await wpc_shared_access(...)`
  - `await wpc_shared_write(...)`
  - `await wpc_shared_release(...)`
  - `await wpc_shared_free(...)`
- Locking model:
  - `sharedAccess` acquires an exclusive per-chunk lock.
  - `sharedWrite`/`sharedRelease` require the same owner context that acquired the lock.
  - competing access waits until release or timeout.
- Free/delete safety:
  - `sharedFree` rejects when chunk is locked by default.
  - with `require_unlocked: false`, free is allowed only when safe for the current owner context.
- Debugging:
  - `sharedGetLockDebugInfo` reports lock holder, hold time, waiters, contention counters, timeout counters, and optional recent lock events.
- Crash/timeout cleanup:
  - worker-owned locks are automatically reclaimed when calls finish, timeout, or worker processes exit.

## Public Methods

- `defineWorkerFunction({ name, worker_func })`
- `undefineWokerFunction({ name })`
- `undefineWorkerFunction({ name })`
- `getRemoteFunctions()`

- `defineWorkerDependency({ alias, module_specifier, export_name?, is_default_export? })`
- `undefineWorkerDependency({ alias })`
- `getWorkerDependencies()`

- `defineWorkerConstant({ name, value })`
- `undefineWorkerConstant({ name })`
- `getWorkerConstants()`

- `defineDatabaseConnection({ name, connector: { type, semantics } })`
- `undefineDatabaseConnection({ name })`
- `getWorkerDatabaseConnections()`

- `sharedCreate({ id, note?, type, content })`
- `sharedAccess({ id, timeout_ms? })`
- `sharedWrite({ id, content })`
- `sharedRelease({ id })`
- `sharedFree({ id, require_unlocked? })`
- `sharedGetLockDebugInfo({ include_history?, min_held_ms? })`

- `startWorkers({ count, ...options })`
- `stopWorkers()`
- `onWorkerEvent({ listener })`
- `offWorkerEvent({ listener_id })`
- `getWorkerHealthStates()`

## Function Serialization and Dependency Limitations

- Worker functions are serialized using `worker_func.toString()` and evaluated in workers.
- `function_hash_sha1` is computed from a normalized source (`toString()` with normalized line endings and trimmed boundary whitespace), then SHA-1 hashed.
- Worker functions must be self-contained and must not rely on parent closure state.
- Dependency detection for scheduling currently uses static string-literal patterns:
  - `wpc_import('alias')`
  - `wpc_import({ alias: 'alias', ... })`
  - `wpc_constant('NAME')`
  - `wpc_database_connection('NAME')`
  - `wpc_database_connection({ name: 'NAME', ... })`
  - `context.dependencies.alias`
  - `context.constants.NAME`
  - `context.database_connections.NAME`
- Dynamically computed alias/name values still work at runtime, but may not be recognized in pre-dispatch dependency eligibility checks.

## Migration Note

- `defineDatabaseConnection`, `undefineDatabaseConnection`, and `getWorkerDatabaseConnections` are new public APIs.
- Worker functions can now access connection handles through:
  - `await wpc_database_connection('NAME')`
  - `await wpc_database_connection<connection_handle_t>({ name: 'NAME', type: '...' })`

## Hash Drift Check Pattern

```typescript
const remote_functions = await workerprocedurecall.getRemoteFunctions();
const remote_hash = remote_functions.find((f) => f.name === 'WPCFunction1')?.function_hash_sha1;

if (remote_hash !== expected_hash_sha1) {
  console.log('Remote function is outdated or changed.');
}
```

## Building from Source

```bash
npm run build
```

## Running Tests

```bash
npm test
```

## Running Benchmarks

```bash
npm run benchmark:worker-vs-single
npm run benchmark:worker-vs-single-sha1
npm run benchmark:worker-vs-single-sha1-heavy
npm run benchmark:shared-vs-ipc
npm run benchmark:shared-vs-ipc-heavy
```

Benchmark mode note:
- Worker benchmark paths run with bounded parallel RPC calls (`worker_execution_mode=parallel`).
- Local benchmark paths remain single-thread sequential baselines.
