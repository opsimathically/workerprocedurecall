# workerprocedurecall

## Project Overview
`@opsimathically/workerprocedurecall` is a TypeScript/Node.js library for executing dynamically-defined procedures in worker threads, then extending that model across networked nodes with secure transport, routing, discovery, and control-plane coordination. It is designed for high-throughput local parallelism and progressively distributed deployments while preserving typed APIs, deterministic error handling, and operational observability.

## Core Capabilities
- Dynamic worker runtime:
  - define/redefine/undefine functions at runtime
  - define/undefine worker dependencies, constants, and database connections
- Typed worker globals:
  - `wpc_import(...)` with generic/object forms for typed dependency handles
  - `wpc_database_connection(...)` with typed connector/name resolution
- Parent-managed shared memory:
  - create/access/write/release/free with lock-aware semantics
  - lock debug snapshots and automatic call-boundary lock cleanup
- Secure network transport:
  - HTTP/2 over TLS, mTLS-capable, secure-only transport enforcement
  - request authentication hooks, token validation, replay protection support
- Cluster execution path:
  - `ClusterClient` -> transport -> `ClusterNodeAgent` -> `WorkerProcedureCall`
  - deterministic timeout/deadline and retry behavior
- Distributed coordination layers:
  - discovery daemon/store, control plane service, ingress balancer, geo ingress control plane
- Operations tooling:
  - lifecycle events, metrics snapshots, routing/lock/debug state introspection
  - fuzz tests and benchmark scripts included in-repo

## Installation
```bash
npm install @opsimathically/workerprocedurecall
```

## Quick Start
```typescript
import { WorkerProcedureCall } from '@opsimathically/workerprocedurecall';

(async function (): Promise<void> {
  const workerprocedurecall = new WorkerProcedureCall();

  await workerprocedurecall.defineWorkerFunction({
    name: 'SayHello',
    worker_func: async function (params: { name: string }): Promise<string> {
      return `hello ${params.name}`;
    }
  });

  await workerprocedurecall.startWorkers({ count: 2 });

  const value = await workerprocedurecall.call.SayHello({
    name: 'cluster'
  });

  console.log(value);

  await workerprocedurecall.stopWorkers();
})();
```

## Local (In-Process) Worker Usage

### Dynamic Define/Redefine/Undefine
```typescript
import { WorkerProcedureCall } from '@opsimathically/workerprocedurecall';

(async function (): Promise<void> {
  const workerprocedurecall = new WorkerProcedureCall();

  await workerprocedurecall.defineWorkerFunction({
    name: 'TransformValue',
    worker_func: async function (params: { value: number }): Promise<number> {
      return params.value + 1;
    }
  });

  await workerprocedurecall.startWorkers({ count: 1 });
  console.log(await workerprocedurecall.call.TransformValue({ value: 1 })); // 2

  await workerprocedurecall.defineWorkerFunction({
    name: 'TransformValue',
    worker_func: async function (params: { value: number }): Promise<number> {
      return params.value + 10;
    }
  });

  console.log(await workerprocedurecall.call.TransformValue({ value: 1 })); // 11

  await workerprocedurecall.undefineWorkerFunction({
    name: 'TransformValue'
  });

  await workerprocedurecall.stopWorkers();
})();
```

### Typed `wpc_import` Without `as`
```typescript
import { WorkerProcedureCall } from '@opsimathically/workerprocedurecall';

(async function (): Promise<void> {
  const workerprocedurecall = new WorkerProcedureCall();

  await workerprocedurecall.defineWorkerDependency({
    alias: 'path_dep',
    module_specifier: 'node:path'
  });

  await workerprocedurecall.defineWorkerFunction({
    name: 'BaseName',
    worker_func: async function (params: { value: string }): Promise<string> {
      const path_module = await wpc_import<typeof import('node:path')>({
        alias: 'path_dep'
      });

      return path_module.basename(params.value);
    }
  });

  await workerprocedurecall.startWorkers({ count: 1 });
  console.log(await workerprocedurecall.call.BaseName({ value: '/tmp/file.txt' }));
  await workerprocedurecall.stopWorkers();
})();
```

### Typed `wpc_database_connection` Without `as`
```typescript
import { WorkerProcedureCall } from '@opsimathically/workerprocedurecall';

(async function (): Promise<void> {
  const workerprocedurecall = new WorkerProcedureCall();

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
    name: 'InsertAndCount',
    worker_func: async function (params: { value: string }): Promise<number> {
      const sqlite_database = await wpc_database_connection<import('better-sqlite3').Database>({
        name: 'sqlite_main',
        type: 'sqlite'
      });

      sqlite_database.exec('CREATE TABLE IF NOT EXISTS demo (value TEXT NOT NULL)');
      sqlite_database.prepare('INSERT INTO demo (value) VALUES (?)').run(params.value);

      const row = sqlite_database
        .prepare<unknown[], { total: number }>('SELECT COUNT(*) as total FROM demo')
        .get();

      return row?.total ?? 0;
    }
  });

  await workerprocedurecall.startWorkers({ count: 1 });
  console.log(await workerprocedurecall.call.InsertAndCount({ value: 'alpha' }));
  await workerprocedurecall.stopWorkers();
})();
```

## Cluster/Network Usage
The standard network path is:
- `ClusterClient` sends protocol messages over secure HTTP/2
- `ClusterNodeAgent` hosts transport + lifecycle integration
- `WorkerProcedureCall` executes the request in worker threads

```typescript
import fs from 'node:fs';
import {
  ClusterClient,
  ClusterNodeAgent,
  WorkerProcedureCall
} from '@opsimathically/workerprocedurecall';

(async function (): Promise<void> {
  const ca_pem = fs.readFileSync('./certs/ca.pem', 'utf8');
  const server_cert_pem = fs.readFileSync('./certs/node-server-cert.pem', 'utf8');
  const server_key_pem = fs.readFileSync('./certs/node-server-key.pem', 'utf8');
  const client_cert_pem = fs.readFileSync('./certs/client-cert.pem', 'utf8');
  const client_key_pem = fs.readFileSync('./certs/client-key.pem', 'utf8');

  const workerprocedurecall = new WorkerProcedureCall();
  await workerprocedurecall.defineWorkerFunction({
    name: 'Multiply',
    worker_func: async function (params: { left: number; right: number }): Promise<number> {
      return params.left * params.right;
    }
  });

  const node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'node_1',
    worker_start_count: 2,
    transport: {
      host: '127.0.0.1',
      port: 9443,
      request_path: '/wpc/cluster/protocol',
      security: {
        tls: {
          key_pem: server_key_pem,
          cert_pem: server_cert_pem,
          ca_pem_list: [ca_pem]
        }
      },
      authenticate_request: function (params) {
        const authorization_header =
          typeof params.headers.authorization === 'string'
            ? params.headers.authorization
            : Array.isArray(params.headers.authorization)
              ? params.headers.authorization[0]
              : undefined;

        if (authorization_header !== 'Bearer local_demo_token') {
          return {
            ok: false,
            message: 'Unauthorized'
          };
        }

        return {
          ok: true,
          identity: {
            subject: 'demo_client',
            tenant_id: 'tenant_demo',
            scopes: ['rpc.call:*'],
            signed_claims: 'demo_claims'
          }
        };
      }
    }
  });

  await node_agent.start();

  const cluster_client = new ClusterClient({
    host: '127.0.0.1',
    port: 9443,
    request_path: '/wpc/cluster/protocol',
    auth_context: {
      subject: 'demo_client',
      tenant_id: 'tenant_demo',
      scopes: ['rpc.call:*'],
      signed_claims: 'demo_claims'
    },
    auth_headers: {
      authorization: 'Bearer local_demo_token'
    },
    default_call_timeout_ms: 5_000,
    transport_security: {
      ca_pem_list: [ca_pem],
      client_cert_pem,
      client_key_pem,
      servername: 'localhost'
    }
  });

  await cluster_client.connect();

  const result = await cluster_client.call<number, [{ left: number; right: number }]>(
    {
      function_name: 'Multiply',
      args: [{ left: 6, right: 7 }],
      timeout_ms: 2_000,
      deadline_unix_ms: Date.now() + 3_000,
      routing_hint: {
        mode: 'auto'
      }
    }
  );

  console.log(result); // 42

  await cluster_client.close();
  await node_agent.stop();
})();
```

## Shared Memory Usage
Shared memory is parent-managed and lock-aware. Access is exclusive per chunk while locked.

```typescript
import { WorkerProcedureCall } from '@opsimathically/workerprocedurecall';

(async function (): Promise<void> {
  const workerprocedurecall = new WorkerProcedureCall();

  await workerprocedurecall.sharedCreate<{ count: number }>({
    id: 'counter_chunk',
    note: 'demo counter',
    type: 'json',
    content: {
      count: 0
    }
  });

  try {
    const current_value = await workerprocedurecall.sharedAccess<{ count: number }>({
      id: 'counter_chunk',
      timeout_ms: 1_000
    });

    await workerprocedurecall.sharedWrite<{ count: number }>({
      id: 'counter_chunk',
      content: {
        count: current_value.count + 1
      }
    });
  } finally {
    await workerprocedurecall.sharedRelease({
      id: 'counter_chunk'
    });
  }

  const lock_debug_information = await workerprocedurecall.sharedGetLockDebugInfo({
    include_history: true,
    min_held_ms: 1
  });

  console.log(lock_debug_information.active_lock_count);

  await workerprocedurecall.sharedFree({
    id: 'counter_chunk',
    require_unlocked: true
  });
})();
```

Lock behavior notes:
- `sharedAccess` acquires an exclusive lock for the chunk.
- While locked, other accesses to that chunk wait/fail per timeout behavior.
- `sharedRelease` should be used in `finally` blocks.
- Locks are call-scoped in worker RPC execution and are auto-released on call completion, timeout, thrown error, or worker exit.

## Administrative Mutation Plane
Administrative mutations can be issued via `ClusterClient.adminMutate(...)` or by directly handling mutation protocol messages in-process.

```typescript
import { WorkerProcedureCall } from '@opsimathically/workerprocedurecall';

(async function (): Promise<void> {
  const workerprocedurecall = new WorkerProcedureCall();
  const node_id = 'node_local_1';

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'allow_admin_mutations',
        effect: 'allow',
        capabilities: ['rpc.admin.mutate:*']
      }
    ]
  });

  await workerprocedurecall.startWorkers({ count: 1 });

  const dry_run_response = await workerprocedurecall.handleClusterAdminMutationRequest({
    node_id,
    now_unix_ms: Date.now(),
    message: {
      protocol_version: 1,
      message_type: 'cluster_admin_mutation_request',
      timestamp_unix_ms: Date.now(),
      mutation_id: 'mutation_define_function_1',
      request_id: 'request_define_function_1',
      trace_id: 'trace_define_function_1',
      deadline_unix_ms: Date.now() + 10_000,
      target_scope: 'single_node',
      target_selector: {
        node_ids: [node_id]
      },
      mutation_type: 'define_function',
      payload: {
        name: 'HotDefinedFunction',
        worker_func_source: 'async function (params) { return params.value + 1; }'
      },
      dry_run: true,
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

  console.log(dry_run_response.terminal_message);

  await workerprocedurecall.stopWorkers();
})();
```

Privilege requirements:
- Mutation requests require admin mutation capabilities such as:
  - `rpc.admin.mutate:*`
  - or granular mutation capabilities by mutation type
- Call execution permissions and mutation permissions are evaluated separately.

## Routing, Discovery, and Control Plane
This repository contains layered routing/coordination components:
- Routing (in `WorkerProcedureCall`):
  - built-in routing modes and node selection logic (`auto`, `affinity`, `target_node` hints in call requests)
- Discovery:
  - in-memory discovery store
  - external discovery daemon
  - HA discovery control-plane support
- Control plane:
  - gateway registration/heartbeat
  - policy and topology snapshot distribution
- Ingress:
  - built-in ingress balancer service
  - geo ingress control-plane components

Built-in vs external infrastructure:
- Built-in:
  - discovery/control-plane/ingress service classes shipped in this library
- External (recommended in production):
  - edge DNS/Anycast/WAF/CDN front door
  - secrets PKI/token issuance infrastructure
  - durable telemetry backends

## Security Model
Secure-only posture:
- Plaintext transport is not supported.
- TLS configuration is required for listeners and clients on network paths.
- mTLS is supported and expected for service-to-service trust boundaries.

Required secure material:
- Server-side:
  - `security.tls.key_pem`
  - `security.tls.cert_pem`
  - `security.tls.ca_pem_list`
- Client-side:
  - `transport_security.ca_pem_list`
  - `transport_security.client_cert_pem`
  - `transport_security.client_key_pem`

Authentication and replay protections:
- Request auth can be enforced via:
  - `authenticate_request` hooks
  - token validation config
- Replay protection is available for privileged/admin paths.
- Identity used for authorization is derived server-side from validated transport/auth context.

## Observability and Diagnostics
Primary observability surfaces include:
- WorkerProcedureCall:
  - `onWorkerEvent(...)` / `offWorkerEvent(...)`
  - `getWorkerHealthStates()`
  - `getClusterOperationsObservabilitySnapshot()`
  - `sharedGetLockDebugInfo(...)`
- ClusterNodeAgent:
  - `getTransportMetrics()`
  - `getRecentTransportEvents(...)`
  - `getSessionSnapshot()`
  - `getDiscoverySnapshot()` / `getDiscoveryEvents(...)`
  - `getControlPlaneSnapshot()`
  - `getOperationsObservabilitySnapshot()`

Recommended practice:
- Correlate by `request_id`, `trace_id`, and mutation identifiers.
- Monitor timeout/retry/error counters alongside worker and discovery/control snapshots.

## Fuzz, Test, and Benchmark Workflows
Run the full test suite:
```bash
npm test
```

Run fuzz tests:
```bash
npm run test:fuzz:quick
npm run test:fuzz
```

Replay one failing fuzz case:
```bash
FUZZ_SEED=<seed> FUZZ_CASE=<case_index> npm run test:fuzz:replay
```

Run benchmarks:
```bash
npm run benchmark:worker-vs-single
npm run benchmark:worker-vs-single-sha1
npm run benchmark:worker-vs-single-sha1-heavy
npm run benchmark:shared-vs-ipc
npm run benchmark:shared-vs-ipc-heavy
```

## Local Cluster Lab
`./local_cluster_lab/` provides profile-based local orchestration for manual development/testing.

Profiles:
- `minimal`
- `ingress`
- `cluster`
- `geo`

Core commands:
```bash
npm run lab:up:minimal
npm run lab:up:ingress
npm run lab:up:cluster
npm run lab:up:geo
npm run lab:status
npm run lab:logs
npm run lab:snapshot
npm run lab:down
```

Scenarios:
```bash
npm run lab:scenario:happy_path
npm run lab:scenario:ingress_failover
npm run lab:scenario:discovery_membership_and_expiry
npm run lab:scenario:control_plane_policy_sync
npm run lab:scenario:geo_cross_region_failover
npm run lab:scenario:auth_failure
npm run lab:scenario:stale_control_fail_closed
```

## API Reference Pointers
Generate API docs:
```bash
npm run docs
```

Key source modules:
- `src/classes/workerprocedurecall/WorkerProcedureCall.class.ts`
- `src/classes/clusterclient/ClusterClient.class.ts`
- `src/classes/clustertransport/ClusterHttp2Transport.class.ts`
- `src/classes/clustertransport/ClusterNodeAgent.class.ts`
- `src/classes/clusterservicediscovery/ClusterServiceDiscoveryDaemon.class.ts`
- `src/classes/clustercontrolplane/ClusterControlPlaneService.class.ts`
- `src/classes/clusteringress/ClusterIngressBalancerService.class.ts`
- `src/classes/clustergeoingress/ClusterGeoIngressControlPlaneService.class.ts`

## Operational Guidance
- Certificate lifecycle:
  - use short-lived certificates and rotate regularly
  - automate distribution and reload procedures
- Timeout tuning:
  - tune `call_timeout_ms`, request deadlines, and retry policies per workload
  - keep bounded retries with deadline awareness
- Rollout safety:
  - prefer dry-run mutation validation first
  - use staged rollout strategies before cluster-wide changes
- Capacity planning:
  - scale worker counts per node by CPU and workload profile
  - use observability snapshots to tune queue limits and routing weights

## Limitations and Non-Goals
- This library does not provide a managed public edge network (for example, global anycast CDN/WAF); external edge infrastructure is still recommended.
- Operational PKI, token issuance, and secret management are expected to be supplied by your environment.
- Exactly-once delivery across distributed retries is not guaranteed; dedupe/idempotency mechanisms are used where implemented.
- Some advanced deployment topologies remain operator-assembled even when built-in components are available.

## Contributing
- Keep changes implementation-aligned and test-backed.
- Run tests before submitting changes:
  - `npm test`
  - relevant fuzz/benchmark/lab commands for changed surfaces
- Follow project TypeScript naming conventions documented in repository guidance.

## License
See `LICENSE.txt`.
