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

## Multi-Gateway Control Plane (Phase 17)

- `ClusterControlPlaneService` is an optional always-on control-plane layer for gateway membership + policy/config distribution.
- `ClusterNodeAgent` can opt in with `control_plane.enabled=true` and a control-plane endpoint list.
- Gateways register + heartbeat, pull topology/policy snapshots, and acknowledge applied policy versions.
- If the control plane is temporarily unavailable, gateways keep serving using last-known-good runtime config and report staleness in `getControlPlaneSnapshot()`.
- Backward compatibility remains: disabling control plane preserves prior discovery/manual behavior.

```typescript
import {
  ClusterControlPlaneService,
  ClusterNodeAgent,
  WorkerProcedureCall
} from '@opsimathically/workerprocedurecall';

const control_plane_service = new ClusterControlPlaneService({
  host: '127.0.0.1',
  port: 9001
});

await control_plane_service.start();

const workerprocedurecall = new WorkerProcedureCall();

const node_agent = new ClusterNodeAgent({
  workerprocedurecall,
  node_id: 'gateway_node_1',
  worker_start_count: 4,
  discovery: {
    enabled: false
  },
  control_plane: {
    enabled: true,
    endpoint: {
      host: '127.0.0.1',
      port: 9001,
      request_path: '/wpc/cluster/control-plane',
      tls_mode: 'disabled'
    },
    heartbeat_interval_ms: 1_000,
    sync_interval_ms: 1_000
  }
});

await node_agent.start();
console.log(node_agent.getControlPlaneSnapshot());
```

## Built-In Global Ingress Balancer (Phase 18)

- `ClusterIngressBalancerService` is a first-party multi-gateway front door.
- Ingress validates/authenticates requests, resolves eligible targets (control-plane/discovery/static), selects route, forwards over HTTP/2, and performs bounded failover.
- Routing modes:
  - `least_loaded`
  - `weighted_rr`
  - `affinity`
- Explicit target pinning can be requested with `routing_hint.mode='target_node'` when policy allows.
- Degraded mode:
  - Ingress can serve from last-known-good target snapshot for a bounded window.
  - When snapshot staleness exceeds configured threshold, ingress fails closed with deterministic no-target errors.

```typescript
import {
  ClusterClient,
  ClusterIngressBalancerService,
  ClusterNodeAgent,
  WorkerProcedureCall
} from '@opsimathically/workerprocedurecall';

const workerprocedurecall = new WorkerProcedureCall();

await workerprocedurecall.defineWorkerFunction({
  name: 'IngressEcho',
  worker_func: async function (): Promise<string> {
    return 'INGRESS_OK';
  }
});

const node_agent = new ClusterNodeAgent({
  workerprocedurecall,
  node_id: 'ingress_node_1',
  worker_start_count: 2,
  discovery: {
    enabled: false
  }
});

const node_address = await node_agent.start();

const ingress_service = new ClusterIngressBalancerService({
  host: '127.0.0.1',
  port: 9100,
  request_path: '/wpc/cluster/ingress',
  routing_mode: 'least_loaded',
  max_attempts: 3,
  request_timeout_ms: 5_000,
  stale_snapshot_max_age_ms: 10_000,
  static_target_list: [
    {
      target_id: 'ingress_node_1',
      node_id: 'ingress_node_1',
      endpoint: {
        host: node_address.host,
        port: node_address.port,
        request_path: node_address.request_path,
        tls_mode: node_address.tls_mode
      },
      capability_hash_by_function_name: {
        IngressEcho: 'unknown'
      }
    }
  ]
});

await ingress_service.start();

const cluster_client = new ClusterClient({
  host: '127.0.0.1',
  port: 9100,
  request_path: '/wpc/cluster/ingress',
  auth_context: {
    subject: 'example_client',
    tenant_id: 'tenant_1',
    scopes: ['rpc.call:*'],
    signed_claims: 'example_signed_claims'
  }
});

await cluster_client.connect();
const return_value = await cluster_client.call<string>({
  function_name: 'IngressEcho'
});

console.log(return_value);
console.log(ingress_service.getMetrics());
console.log(ingress_service.getSnapshot());

await cluster_client.close();
await ingress_service.stop();
await node_agent.stop();
```

## Global Ingress Orchestration + Geo Control Plane (Phase 19)

- `ClusterGeoIngressControlPlaneService` coordinates ingress instances across regions with lease-based liveness.
- Regional ingress instances register/heartbeat into geo control plane.
- Global ingress instances resolve regional ingress candidates from geo snapshots and perform deterministic cross-region failover.
- Geo routing supports:
  - latency-aware region preference
  - capacity-aware fallback
  - bounded cross-region attempts
  - stale-control fail-closed behavior after configured threshold

### Phase 19 Ingress Flags

- `geo_ingress.enabled`
- `geo_ingress.endpoint` / `geo_ingress.endpoint_list`
- `geo_ingress.role` (`global` or `regional`)
- `geo_ingress.region_id`
- `geo_ingress.sync_interval_ms`
- `geo_ingress.lease_ttl_ms`
- `geo_ingress.stale_snapshot_max_age_ms`
- `geo_ingress.max_cross_region_attempts`

### Minimal Geo Example

```typescript
import {
  ClusterGeoIngressControlPlaneService,
  ClusterIngressBalancerService
} from '@opsimathically/workerprocedurecall';

const geo_control_plane_service = new ClusterGeoIngressControlPlaneService({
  host: '127.0.0.1',
  port: 9200
});

await geo_control_plane_service.start();

const regional_ingress = new ClusterIngressBalancerService({
  host: '127.0.0.1',
  port: 9301,
  static_target_list: [
    // local node-agent target(s) for this region
  ],
  geo_ingress: {
    enabled: true,
    role: 'regional',
    region_id: 'us-east-1',
    endpoint: {
      host: '127.0.0.1',
      port: 9200,
      request_path: '/wpc/cluster/geo-ingress',
      tls_mode: 'disabled'
    }
  }
});

const global_ingress = new ClusterIngressBalancerService({
  host: '127.0.0.1',
  port: 9400,
  geo_ingress: {
    enabled: true,
    role: 'global',
    region_id: 'global',
    endpoint: {
      host: '127.0.0.1',
      port: 9200,
      request_path: '/wpc/cluster/geo-ingress',
      tls_mode: 'disabled'
    },
    stale_snapshot_max_age_ms: 15_000,
    max_cross_region_attempts: 2
  }
});
```

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
  - worker-owned locks are call-scoped (`worker:<worker_id>:<call_request_id>`).
- Free/delete safety:
  - `sharedFree` rejects when chunk is locked by default.
  - with `require_unlocked: false`, free is allowed only when safe for the current owner context.
- Debugging:
  - `sharedGetLockDebugInfo` reports lock holder, hold time, waiters, contention counters, timeout counters, and optional recent lock events.
- Auto-release signals:
  - forced lock cleanup is recorded with event names:
    - `call_complete_auto_release`
    - `call_timeout_auto_release`
    - `worker_exit_auto_release`
  - events include owner context details (`worker_id`, `call_request_id`) and lock release counts.
- Crash/timeout cleanup:
  - worker-owned locks are automatically reclaimed when calls finish, timeout, worker exits, or pending calls are rejected during shutdown.
  - locks cannot intentionally outlive the RPC call lifecycle.
  - background async work that continues after a worker function returns must not assume shared locks are still held.

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

## Network Transport MVP (Phase 8)

- Transport choice: HTTP/2 (`node:http2`) for built-in persistent sessions, connect/disconnect lifecycle visibility, and no external dependency.
- Data-plane scope only in this phase:
  - `cluster_call_request`
  - `cluster_call_cancel`
- Mutation-plane messages remain protocol-compatible but are not executed through this MVP transport path.

Manual local network flow:

```bash
npm run manual:network-transport-mvp
```

The manual script starts:
- local `WorkerProcedureCall` workers,
- `ClusterNodeAgent` with `ClusterHttp2Transport`,
- one sample network call (`client -> transport -> handleClusterCallRequest -> worker`).

## Client SDK MVP (Phase 9)

Quickstart (`connect -> call -> close`):

```typescript
import { ClusterClient } from '@opsimathically/workerprocedurecall';

const cluster_client = new ClusterClient({
  host: '127.0.0.1',
  port: 8080,
  request_path: '/wpc/cluster/protocol',
  auth_context: {
    subject: 'app_client',
    tenant_id: 'tenant_1',
    scopes: ['rpc.call:*'],
    signed_claims: 'signed_claims_blob'
  },
  auth_headers: {
    authorization: 'Bearer your_access_token'
  }
});

await cluster_client.connect();
const result = await cluster_client.call<string>({
  function_name: 'SomeRemoteFunction',
  args: [{ value: 'hello' }],
  timeout_ms: 10_000
});
await cluster_client.close();
```

Notes:
- `call` supports bounded retries for retryable failures, bounded by deadline.
- auth context is attached to every request (`caller_identity`).
- `adminMutate` is available as a protocol-compatible stub for Phase 9 and will report transport unavailability if mutation-plane transport is not enabled on the target endpoint.

## Production Auth Hardening (Phase 10)

The transport layer supports hardened server-side authentication and replay protection for both data-plane and mutation-plane requests.

### Security controls

- mTLS on the transport:
  - `security.tls.mode: 'required'` enforces TLS + client cert validation at the server.
  - `security.tls.mode: 'terminated_upstream'` supports external TLS termination using a trusted assertion header.
- Signed token validation:
  - Configure `security.token_validation` with `key_by_kid`, `expected_issuer`, and `expected_audience`.
  - Supported JWT signature algorithms: `HS256` and `RS256`.
  - Enforced checks: signature, `exp`, `nbf`, issuer, audience, and required identity claims.
- Replay protection for privileged admin mutations:
  - Configure `security.replay_protection`.
  - Replayed privileged requests are rejected deterministically with `ADMIN_AUTH_FAILED`.

### Credential requirements

- Use short-lived tokens (`exp`) and include `jti` for privileged/admin mutation requests.
- Include `sub` and `tenant_id` claims for trusted identity extraction.
- Use per-environment issuer/audience pairs and dedicated signing keys.
- Prefer mTLS client certificates for all production clients.

### Key rotation operations

`ClusterNodeAgent` exposes runtime hooks for token key rotation:

- `setTokenValidationConfig({ token_validation_config })`
- `setTokenVerificationKeySet({ key_by_kid })`
- `upsertTokenVerificationKey({ key })`

These operations update active verification keys and clear cached session identity state so subsequent requests re-authenticate with current keys.

## Durable Control-Plane State (Phase 11)

Phase 11 adds restart-safe durability for mutation reliability metadata:

- mutation dedupe/idempotency records,
- entity version (CAS) metadata,
- immutable mutation audit records.

### Enabling durable state

```typescript
const workerprocedurecall = new WorkerProcedureCall({
  control_plane_durable_state_enabled: true,
  control_plane_state_file_path: './.workerprocedurecall/control_plane_state.json',
  mutation_dedupe_retention_ms: 300_000,
  control_plane_audit_retention_ms: 86_400_000,
  control_plane_audit_max_record_count: 10_000
});
```

Notes:
- Persistence uses an atomic file write boundary (`write temp` -> `rename`) for deterministic snapshots.
- In-memory maps remain as runtime cache; durable state is rehydrated on construction.
- Retention/compaction hooks are available via `compactControlPlaneState(...)`.
- Persistence info is inspectable with `getControlPlaneStatePersistenceInfo()`.

Durability limits:
- This is single-process local durability for node-level restart safety.
- It is not a distributed consensus log and does not provide cross-node linearizability by itself.

## Routing + Balancing (Phase 12)

Phase 12 adds capacity-aware call routing for cluster call execution.

### Routing modes

- `auto`: capacity-aware least-loaded routing (inflight + latency + error weighted score).
- `affinity`: sticky/session-aware routing with TTL-bound affinity cache.
- `target_node`: explicit node pinning when requested.

### Health and failover behavior

- Candidate nodes are filtered by health (`ready`, optional `degraded`), heartbeat freshness, zone/capability constraints, and authorization scope.
- Retryable dispatch failures can fail over to alternate candidate nodes when safe.
- Function/version pinning is preserved via `function_hash_sha1` checks.

### Observability

- Routing counters are available through `getClusterCallRoutingMetrics()`.
- Runtime policy is configurable with `setClusterCallRoutingPolicy(...)` and inspectable via `getClusterCallRoutingPolicy()`.
- Node registration and updates:
  - `registerClusterCallNode(...)`
  - `recordClusterCallNodeHeartbeat(...)`
  - `recordClusterCallNodeCapabilities(...)`

Tradeoffs:
- `auto` improves throughput under uneven load but can add cross-node variability.
- `affinity` improves cache/session locality but can temporarily skew load.
- `target_node` is deterministic but bypasses balancing.

## Global Service Discovery (Phase 14)

Phase 14 adds discovery-backed node lifecycle for dynamic multi-node candidate management.

### What discovery adds

- Automatic node registration when `ClusterNodeAgent` starts.
- Periodic heartbeats with lease TTLs.
- Capability publication from runtime function inventory.
- Automatic stale-node expiration and exclusion from routing.
- Discovery event history and metrics snapshots for diagnostics.

### Discovery mode and backward compatibility

- Discovery is additive to existing routing and transport paths.
- Manual registration remains supported:
  - `registerClusterCallNode(...)` still works.
  - You can disable discovery with `discovery.enabled: false`.
- For multi-agent auto-discovery, agents must point at the same discovery store instance.

### Node agent discovery wiring (example)

```typescript
import {
  ClusterInMemoryServiceDiscoveryStore,
  ClusterNodeAgent,
  WorkerProcedureCall
} from '@opsimathically/workerprocedurecall';

const shared_discovery_store = new ClusterInMemoryServiceDiscoveryStore();

const workerprocedurecall = new WorkerProcedureCall();

const cluster_node_agent = new ClusterNodeAgent({
  workerprocedurecall,
  node_id: 'node_a',
  worker_start_count: 4,
  discovery: {
    enabled: true,
    service_discovery_store: shared_discovery_store,
    heartbeat_interval_ms: 2_000,
    lease_ttl_ms: 8_000,
    sync_interval_ms: 2_000
  }
});

await cluster_node_agent.start();
```

### Discovery observability surfaces

- `ClusterNodeAgent.getDiscoverySnapshot()`
- `ClusterNodeAgent.getDiscoveryEvents(...)`
- `ClusterInMemoryServiceDiscoveryStore.getMetrics()`
- `ClusterInMemoryServiceDiscoveryStore.queryNodes(...)`

## External Discovery Daemon MVP (Phase 15)

Phase 15 adds an external discovery backend path so multiple node agents can share the same dynamic node view over network transport.

### What is now implemented

- `ClusterServiceDiscoveryDaemon` with:
  - node register/upsert
  - heartbeat updates
  - capability updates
  - node remove
  - list/query
  - stale-node expiration
- `ClusterRemoteServiceDiscoveryStoreAdapter` implementing `cluster_service_discovery_store_i`.
- `ClusterNodeAgent` external discovery mode via `discovery.external_daemon`.
- Deterministic discovery protocol validation/error mapping for daemon requests.

### Node Agent External Daemon Config

```typescript
const discovery_daemon = new ClusterServiceDiscoveryDaemon({
  host: '127.0.0.1',
  port: 7101
});

await discovery_daemon.start();

const cluster_node_agent = new ClusterNodeAgent({
  workerprocedurecall,
  node_id: 'node_a',
  worker_start_count: 4,
  discovery: {
    enabled: true,
    external_daemon: {
      host: '127.0.0.1',
      port: 7101,
      request_path: '/wpc/cluster/discovery',
      synchronization_interval_ms: 1_000,
      request_timeout_ms: 3_000
    },
    heartbeat_interval_ms: 2_000,
    lease_ttl_ms: 8_000,
    sync_interval_ms: 2_000
  }
});
```

### Failure/Staleness Model (MVP)

- Node agents keep a local discovery cache and poll daemon snapshots.
- Discovery write failures are retried with bounded backoff.
- If daemon is temporarily unavailable:
  - node agents keep running,
  - routing uses last synchronized cache,
  - discovery update errors are visible in discovery metrics.

### Current boundary

- Built in now:
  - single-daemon external discovery backend path.
- Not yet:
  - cross-region discovery federation,
  - fully managed discovery bootstrap automation.

## High-Availability Discovery Control Plane (Phase 16)

Phase 16 extends external discovery into a replicated multi-daemon HA control plane with leader election and quorum-gated writes.

### What is now implemented

- `ClusterServiceDiscoveryDaemon` HA mode:
  - leader/follower/candidate roles
  - leader election with term tracking
  - follower write redirect (`DISCOVERY_NOT_LEADER`) with leader hint
  - quorum-committed discovery writes
  - replicated append-entries + vote request handling
- deterministic HA protocol errors:
  - `DISCOVERY_NOT_LEADER`
  - `DISCOVERY_QUORUM_UNAVAILABLE`
  - `DISCOVERY_CONFLICT_STALE_TERM`
  - `DISCOVERY_REPLICATION_TIMEOUT`
- multi-endpoint remote adapter failover:
  - `ClusterRemoteServiceDiscoveryStoreAdapter` accepts `endpoint_list`
  - leader-aware retries with redirect handling and endpoint cooldown
- durable daemon HA state primitives:
  - `ClusterServiceDiscoveryDaemonInMemoryStateStore`
  - `ClusterServiceDiscoveryDaemonFileStateStore`
  - persisted term/leader/commit metadata + committed operation log + discovery state snapshot
- Phase 16 tests:
  - 3-daemon election + follower redirect behavior
  - leader failover continuity with multi-endpoint adapter
  - quorum unavailable deterministic failure path

### Node Agent External Daemon HA Config

```typescript
const cluster_node_agent = new ClusterNodeAgent({
  workerprocedurecall,
  node_id: 'node_a',
  discovery: {
    enabled: true,
    external_daemon: {
      endpoint_list: [
        {
          daemon_id: 'discovery_a',
          host: '10.0.0.11',
          port: 7101,
          request_path: '/wpc/cluster/discovery',
          tls_mode: 'disabled'
        },
        {
          daemon_id: 'discovery_b',
          host: '10.0.0.12',
          port: 7101,
          request_path: '/wpc/cluster/discovery',
          tls_mode: 'disabled'
        },
        {
          daemon_id: 'discovery_c',
          host: '10.0.0.13',
          port: 7101,
          request_path: '/wpc/cluster/discovery',
          tls_mode: 'disabled'
        }
      ],
      synchronization_interval_ms: 1_000,
      request_timeout_ms: 3_000
    }
  }
});
```

### HA topology guidance

- Recommended daemon count: `3` (tolerates `1` daemon loss) or `5` (tolerates `2` daemon losses).
- Discovery writes require quorum and are rejected when quorum is unavailable.
- Default reads are leader-consistent; stale follower reads can be requested explicitly with `consistency_mode: 'stale_ok'`.

### Remaining boundary

- Built in now:
  - replicated single-cluster HA discovery with leader failover and quorum writes.
- Not yet:
  - built-in cross-region discovery federation.
  - full Raft log compaction/snapshot streaming across independently restarted clusters.

## Operations Readiness (Phase 13)

Phase 13 adds production-operability surfaces for observability, alerts, and failure validation.

### Observability mapping

- WorkerProcedureCall lifecycle telemetry:
  - `getClusterOperationLifecycleEvents(...)`
  - `clearClusterOperationLifecycleEvents()`
  - `getClusterOperationsObservabilitySnapshot()`
- Call routing and mutation metrics:
  - `getClusterCallRoutingMetrics()`
  - `getClusterAdminMutationMetrics()`
  - `getClusterAdminMutationAuditRecords(...)`
- Transport/session telemetry:
  - `ClusterNodeAgent.getTransportMetrics()`
  - `ClusterNodeAgent.getRecentTransportEvents(...)`
  - `ClusterNodeAgent.getOperationsObservabilitySnapshot()`

Trace correlation guidance:
- Carry and log `trace_id` + `request_id` for every data-plane call.
- Carry and log `trace_id` + `request_id` + `mutation_id` for control-plane mutations.
- Use these identifiers as the primary join keys between transport events, lifecycle events, and audit records.

### Alert conditions (recommended)

- Mutation failure spike:
  - high growth in `admin_mutation_failure_total`.
- Authorization anomaly:
  - sharp increase in transport auth failures and `admin_mutation_authz_denied_total`.
- Timeout/retry storm:
  - rising `dispatch_retryable_total` + repeated `request_failed` reasons tied to timeout/dispatch.
- Rollback frequency:
  - sustained growth in `admin_mutation_rollback_total`.
- Node health degradation:
  - increasing `filtered_unhealthy_node_total` and health state drift from `ready` to `degraded/stopped`.

### Runbooks and checklist

- [Phase13 Operations Runbook](./design_documentation/Operations_Runbook_Phase13.md)
- [Phase13 Staged Deployment Readiness Checklist](./design_documentation/Operations_Readiness_Checklist_Phase13.md)

## Running Benchmarks

```bash
npm run benchmark:worker-vs-single
npm run benchmark:worker-vs-single-sha1
npm run benchmark:worker-vs-single-sha1-heavy
npm run benchmark:shared-vs-ipc
npm run benchmark:shared-vs-ipc-heavy
```

## Running Chaos/Stress Validation

```bash
npm run test:chaos-phase13
```

Benchmark mode note:
- Worker benchmark paths run with bounded parallel RPC calls (`worker_execution_mode=parallel`).
- Local benchmark paths remain single-thread sequential baselines.
