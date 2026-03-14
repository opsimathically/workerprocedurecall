# High-Level Overview

## TL;DR
This library can run remote procedure calls in worker threads locally, expose those calls over network transport, and apply routing/failover across registered nodes. It also supports an authenticated admin mutation plane and local shared-memory primitives.

> **Implemented Today**
> - Secure-only network posture (TLS + mTLS required; no plaintext fallback).
> - Client SDK (`ClusterClient`) for connect/call/adminMutate/close.
> - Network transport (`ClusterHttp2Transport`) and node agent (`ClusterNodeAgent`).
> - Built-in ingress front door (`ClusterIngressBalancerService`) for multi-gateway dispatch.
> - Worker-thread execution via `WorkerProcedureCall`.
> - Routing modes (`auto`, `affinity`, `target_node`) with failover.
> - Discovery-backed node registration/heartbeat/capability sync/expiry when enabled.
> - External discovery daemon MVP for multi-node shared discovery over network.
> - HA replicated discovery control plane (multi-daemon leader/quorum/failover).
> - Always-on multi-gateway control plane (`ClusterControlPlaneService`).
> - Admin mutation plane (define/redefine/undefine function/dependency/constant/database connection).
> - Local shared-memory + lock lifecycle controls.
> - Observability snapshots/events/metrics and chaos validation tests.
>
> **Not Yet (as built-in defaults)**
> - Geo-distributed anycast/edge ingress fabric.
> - Turnkey cross-region traffic management.

## What This Project Is
`WorkerProcedureCall` started as a local worker-thread RPC system. It now includes distributed building blocks:
- client SDK,
- network ingress,
- call routing,
- mutation orchestration,
- auth/security,
- durability and ops telemetry.

## What You Can Do Right Now
- [x] Run worker procedures locally across multiple threads.
- [x] Expose worker procedures over HTTP/2 transport.
- [x] Enforce encrypted + authenticated network calls only (secure-only transport).
- [x] Connect from remote clients and invoke procedures.
- [x] Route calls through a single built-in ingress endpoint with health/capability-aware dispatch.
- [x] Route to target/affinity/auto-selected nodes.
- [x] Enable node auto-discovery with shared discovery store + heartbeats/leases.
- [x] Run discovery via external daemon (`ClusterServiceDiscoveryDaemon`) for multi-host setups.
- [x] Retry/fail over on retryable conditions.
- [x] Execute remote admin mutations with RBAC/ABAC policy checks.
- [x] Track metrics/events/audit records for operations.

## What This Project Does NOT Do Yet (Out of the Box)
- [ ] Turnkey fleet-wide gateway cluster manager.
- [ ] Built-in global anycast/edge traffic balancing.
- [ ] Cross-node shared memory (shared memory is process-local by design).

## Reading Order
1. [Architecture Overview](./architecture_overview.md)
2. [End-to-End Call Flow](./end_to_end_call_flow.md)
3. [Routing and Balancing](./routing_and_balancing.md)
4. [Mutation Plane Overview](./mutation_plane_overview.md)
5. [Shared Memory Scope](./shared_memory_scope.md)
6. [Operations and Observability](./operations_and_observability.md)
7. [Deployment Playbooks](./deployment_playbooks.md)
8. [Ideal Deployment Topology](./ideal_deployment_topology.md)

## Current Capability Boundary
- The library already handles: client -> network -> node -> worker-thread execution.
- The library also supports: client -> ingress -> node -> worker-thread execution.
- Routing/failover is implemented inside the call handling path when candidate nodes are registered.
- Discovery can run as a multi-daemon HA cluster with leader election and quorum-committed writes.
- For multi-host ingress, you can use built-in ingress directly or place an external LB in front of ingress instances.
- Mutation APIs are live and test-covered; observability and chaos/stress checks are also implemented.
