# workerprocedurecall

Worker-thread RPC for Node.js with dynamic function, dependency, and constant registration via message passing.

## Install

```bash
npm install @opsimathically/workerprocedurecall
```

## API

```typescript
import { WorkerProcedureCall } from '@opsimathically/workerprocedurecall';

(async function () {
  const workerprocedurecall = new WorkerProcedureCall({
    call_timeout_ms: 30_000,
    control_timeout_ms: 10_000,
    restart_on_failure: true,
    max_restarts_per_worker: 3,
    max_pending_calls_per_worker: 1_000
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

  await workerprocedurecall.defineWorkerFunction({
    name: 'WPCFunction1',
    worker_func: async function (file_path: string): Promise<string> {
      const path_module = (await wpc_import('path_dep')) as {
        basename: (value: string) => string;
      };

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
      const crypto_module = (await wpc_import('crypto_dep')) as {
        randomUUID: () => string;
      };

      return crypto_module.randomUUID();
    }
  });

  const remote_function_information = await workerprocedurecall.getRemoteFunctions();
  const dependency_information = await workerprocedurecall.getWorkerDependencies();
  const constant_information = await workerprocedurecall.getWorkerConstants();

  console.log(remote_function_information, dependency_information, constant_information);

  const function1_return_val = await workerprocedurecall.call.WPCFunction1(
    '/tmp/example.txt'
  );

  const function2_return_val = await workerprocedurecall.call.WPCFunction2();

  console.log(function1_return_val, function2_return_val);

  await workerprocedurecall.undefineWorkerDependency({ alias: 'crypto_dep' });
  await workerprocedurecall.undefineWokerFunction({ name: 'WPCFunction2' });

  await workerprocedurecall.stopWorkers();
})();
```

## How Dependency Loading Works

- Use `defineWorkerDependency` to register an alias to a module specifier.
- In worker functions, call `await wpc_import('alias')`.
- Only registered aliases are accessible (allowlist model).
- Worker runtime resolves modules using `require(...)`, then falls back to dynamic `import(...)`.
- Module results are cached in each worker's dependency registry.

## How Constants Work

- Use `defineWorkerConstant({ name, value })` to define structured-clone-safe constant values.
- In worker functions, call `wpc_constant('NAME')`.
- Constants can be added/updated/removed before or after workers start.

## Behavior Notes

- Parent thread is the source of truth for functions, dependencies, and constants.
- Functions, dependencies, and constants can be defined before startup or while workers are running.
- New workers (including restarted workers) receive full registry synchronization on startup.
- Calls use request/response correlation IDs and timeout handling.
- Worker errors are propagated with `name`, `message`, and `stack` when available.
- Worker shutdown is graceful first, then force-terminate fallback on timeout.
- Scheduling is least in-flight across eligible workers, with deterministic tie-breaking.
- Calls are dispatched only to workers that are ready and have function + required dependency/constant installation confirmed.
- Optional per-worker saturation protection is available with `max_pending_calls_per_worker`.

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

- `startWorkers({ count, ...options })`
- `stopWorkers()`

## Function Serialization and Dependency Limitations

- Worker functions are serialized using `worker_func.toString()` and evaluated in workers.
- Worker functions must be self-contained and must not rely on parent closure state.
- Dependency detection for scheduling currently uses static string-literal patterns:
  - `wpc_import('alias')`
  - `wpc_constant('NAME')`
  - `context.dependencies.alias`
  - `context.constants.NAME`
- Dynamically computed alias/name values still work at runtime, but may not be recognized in pre-dispatch dependency eligibility checks.

## Building from Source

```bash
npm run build
```

## Running Tests

```bash
npm test
```
