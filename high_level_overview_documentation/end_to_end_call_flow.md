# End-to-End Call Flow

## TL;DR
A `ClusterClient.call(...)` request is serialized to protocol JSON, validated/authenticated at ingress, forwarded to a selected gateway/node, validated/routed by `WorkerProcedureCall`, executed on a worker thread (or routed node), and returned to the client.

> **Implemented Today**
> - Full call path from SDK to worker result.
> - Optional ingress front door path (`ClusterIngressBalancerService`) with deterministic dispatch.
> - Optional global ingress geo mode with region-aware selection and bounded cross-region failover.
> - Correlation identifiers (`request_id`, `trace_id`) are carried in protocol messages.
> - Retryable failures can trigger alternate-node dispatch.
> - Discovery-enabled runtimes can auto-sync candidate nodes from local or external discovery backends.
> - Control-plane-enabled gateways can continuously sync topology/policy versions and keep serving on last-known-good config during temporary control-plane outages.
>
> **Not Yet**
> - Global exactly-once semantics.
> - Multi-region globally coordinated call execution semantics.

## Success Path (Sequence)
```mermaid
sequenceDiagram
  participant C as ClusterClient
  participant I as ClusterIngressBalancerService
  participant T as ClusterHttp2Transport
  participant W as WorkerProcedureCall
  participant WT as Worker Thread

  C->>I: cluster_call_request\n(request_id, trace_id, function_name, args)
  I->>I: auth + protocol validation
  I->>I: resolve targets + route + select dispatch endpoint
  I->>T: forward request to selected gateway
  T->>T: auth + protocol validation
  T->>W: handleClusterCallRequest(...)
  W->>W: authorize + route candidate selection
  W->>WT: callWorkerFunction(function_name, args)
  WT-->>W: return_value
  W-->>T: cluster_call_ack + cluster_call_response_success
  T-->>I: response payload
  I-->>C: terminal success response
```

## Retry / Failover Path (Sequence)
```mermaid
sequenceDiagram
  participant C as ClusterClient
  participant I as ClusterIngressBalancerService
  participant G1 as Gateway #1
  participant G2 as Gateway #2

  C->>I: cluster_call_request\n(request_id, trace_id)
  I->>G1: forward attempt #1
  G1-->>I: retryable/transport failure
  I->>I: select alternate eligible target
  I->>G2: forward attempt #2
  G2-->>I: success
  I-->>C: success response
```

## Geo Global Ingress Path (Sequence)
```mermaid
sequenceDiagram
  participant C as ClusterClient
  participant IG as Global Ingress
  participant GCP as Geo Ingress Control Plane
  participant IR1 as Regional Ingress (Region A)
  participant IR2 as Regional Ingress (Region B)

  C->>IG: cluster_call_request(request_id, trace_id, deadline)
  IG->>GCP: fetch cached/synced topology + routing policy
  IG->>IG: select preferred region/ingress
  IG->>IR1: attempt #1 in preferred region
  IR1-->>IG: retryable failure
  IG->>IR2: bounded cross-region failover attempt
  IR2-->>IG: success
  IG-->>C: terminal response + attempt metadata
```

## Step-by-Step Lifecycle
1. Client builds request message with deadline and routing hint.
2. Ingress validates/authenticates request and resolves eligible targets from control-plane/discovery/static snapshots.
3. Ingress routing chooses a target and forwards with preserved correlation/deadline metadata.
4. Transport validates message and authenticates identity.
5. Runtime authorizes function call and builds candidate node list (from local/discovery/control-plane topology depending on mode).
6. Runtime routing picks selected node (+ fallbacks).
7. Runtime executes locally or dispatches to registered remote executor.
8. Terminal success/error is returned and lifecycle telemetry is emitted at both ingress and runtime layers.

## Control-Plane Sync Influence
```mermaid
sequenceDiagram
  participant CP as ClusterControlPlaneService
  participant A as ClusterNodeAgent
  participant W as WorkerProcedureCall

  A->>CP: gateway_register + heartbeat
  A->>CP: subscribe_updates(known_version, known_generation)
  CP-->>A: topology_snapshot + active_policy_version
  A->>W: setClusterCallRoutingPolicy(...)
  A->>W: setClusterAuthorizationPolicyList(...)
  A->>CP: config_version_ack(applied=true/false)
```

## Error + Retry Notes
- Retry behavior depends on error code + retryable flag.
- Unknown-outcome retry behavior requires idempotency awareness.
- Some errors are terminal (`AUTH_FAILED`, `FORBIDDEN_FUNCTION`, etc.).
- In geo mode, ingress retries within selected region first, then performs bounded cross-region attempts if policy allows.

## Correlation Guidance
Use `request_id` + `trace_id` as join keys across:
- transport events,
- runtime lifecycle events,
- client error handling logs.
