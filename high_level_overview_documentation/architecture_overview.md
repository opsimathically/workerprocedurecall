# Architecture Overview

## TL;DR
Today you have a working distributed call path with client SDK + built-in ingress + transport + node agent + worker-thread runtime. Routing exists at both ingress and runtime layers.

> **Implemented Today**
> - `ClusterClient` sends protocol messages.
> - `ClusterIngressBalancerService` provides a first-party front door for multi-gateway dispatch/failover.
> - Geo mode in `ClusterIngressBalancerService` supports global region-aware selection + bounded cross-region failover.
> - `ClusterHttp2Transport` receives/authenticates/parses messages.
> - `ClusterNodeAgent` bridges transport to `WorkerProcedureCall`.
> - `ClusterControlPlaneService` provides always-on gateway membership + policy/config distribution.
> - `ClusterGeoIngressControlPlaneService` provides global ingress/region topology + policy snapshots.
> - `ClusterInMemoryServiceDiscoveryStore` enables registration/heartbeat/capability sync/expiry.
> - `ClusterServiceDiscoveryDaemon` enables external shared discovery for multi-node operation.
> - HA discovery mode supports replicated daemons with leader election + quorum writes.
> - `WorkerProcedureCall` executes in local workers and can route/failover across registered call nodes.
>
> **Not Yet (Automatic Platform Layer)**
> - No built-in geo-distributed anycast/edge ingress network.

## Component Map
- `ClusterClient`: remote caller API.
- `ClusterIngressBalancerService`: ingress front door, target resolution, routing choice, bounded retry/failover.
- `ClusterGeoIngressControlPlaneService`: geo ingress registration/heartbeats, global region topology, global routing policy snapshots.
- `ClusterHttp2Transport`: HTTP/2 ingress, request parsing, auth enforcement, request lifecycle events.
- `ClusterNodeAgent`: wraps transport + local runtime.
- `ClusterControlPlaneService`: gateway registration, policy version publishing, topology snapshots.
- `WorkerProcedureCall`: scheduling, execution, routing, mutation-plane logic, audit, telemetry.
- Worker threads: actual execution targets.

```mermaid
flowchart LR
  client[ClusterClient]\n(SDK)
  ingressGlobal[ClusterIngressBalancerService]\n(Global Ingress)
  ingressRegionalA[ClusterIngressBalancerService]\n(Regional Ingress A)
  ingressRegionalB[ClusterIngressBalancerService]\n(Regional Ingress B)
  lb[External Load Balancer]\n(Optional; can sit before ingress)
  agentA[ClusterNodeAgent\nNode A]
  transportA[ClusterHttp2Transport]
  discoveryA[Discovery Daemon A\nLeader/Follower]
  discoveryB[Discovery Daemon B\nLeader/Follower]
  discoveryC[Discovery Daemon C\nLeader/Follower]
  controlPlane[ClusterControlPlaneService\nAlways-on Control Plane]
  geoControlPlane[ClusterGeoIngressControlPlaneService]
  wpcA[WorkerProcedureCall\nNode A Runtime]
  workersA[Worker Threads A]

  agentB[ClusterNodeAgent\nNode B]
  transportB[ClusterHttp2Transport]
  wpcB[WorkerProcedureCall\nNode B Runtime]
  workersB[Worker Threads B]

  client --> ingressGlobal
  lb --> ingressGlobal
  ingressGlobal --> ingressRegionalA
  ingressGlobal --> ingressRegionalB
  ingressRegionalA --> agentA
  ingressRegionalB --> agentB

  agentA --> transportA --> wpcA --> workersA
  agentB --> transportB --> wpcB --> workersB
  agentA <--> controlPlane
  agentB <--> controlPlane
  agentA <--> discoveryA
  agentA <--> discoveryB
  agentA <--> discoveryC
  agentB <--> discoveryA
  agentB <--> discoveryB
  agentB <--> discoveryC

  discoveryA <--> discoveryB
  discoveryB <--> discoveryC
  discoveryC <--> discoveryA
  ingressGlobal <--> geoControlPlane
  ingressRegionalA <--> geoControlPlane
  ingressRegionalB <--> geoControlPlane

  wpcA -. optional routed dispatch via registered call_executor .-> wpcB
```

## Do We Have a Built-In Balancer?
Short answer: **yes, with boundary**.
- Yes: there is a built-in ingress balancer (`ClusterIngressBalancerService`) and runtime routing (`WorkerProcedureCall`).
- Boundary: external/global edge distribution (cross-region anycast, CDN-level ingress, global traffic steering) is still infrastructure territory.

## How Do Multiple Nodes Work Today?
- You run one `ClusterNodeAgent` + `WorkerProcedureCall` per host.
- Clients can connect to:
  - built-in ingress service, or
  - directly to one node, or
  - an external LB in front of ingress or node agents.
- Each gateway can register/heartbeat with `ClusterControlPlaneService` and pull versioned policy snapshots.
- In geo mode, ingress instances can register/heartbeat with `ClusterGeoIngressControlPlaneService` and consume global topology/policy snapshots.
- For auto-discovery, agents can share a replicated discovery daemon set and publish heartbeats/capabilities.
- Inside a runtime, routing/failover targets discovery-synced nodes (or manually registered nodes).

## Practical Interpretation
- Local parallelism: fully built-in.
- Network calls: fully built-in.
- Ingress + routing algorithms: built-in.
- Discovery lifecycle (register/heartbeat/expire): built-in in local + external daemon + HA daemon modes.
- Global edge ingress remains a deployment responsibility.
