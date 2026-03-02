# Operations Runbook (Phase 13)

## Scope

This runbook covers operational triage and remediation for:
- data-plane call execution,
- control-plane mutation operations,
- transport/session/authentication behavior,
- routing and node health degradation.

Use this runbook with:
- `design_documentation/Specification.md`
- `design_documentation/Operations_Readiness_Checklist_Phase13.md`

## 1. Dashboards and Metric Groupings

### 1.1 Data-plane reliability dashboard

Primary metrics:
- `gateway_calls_total` by error code
- `gateway_call_latency_ms` (p50/p95/p99)
- `gateway_attempts_per_call`
- `ClusterNodeAgent.getTransportMetrics().request_failed_count_by_error_code`
- `WorkerProcedureCall.getClusterCallRoutingMetrics()`

Secondary metrics:
- `request_failed_count_by_reason`
- `dispatch_retryable_total`
- `failover_attempt_total`
- `filtered_unhealthy_node_total`

### 1.2 Mutation-plane safety dashboard

Primary metrics:
- `admin_mutation_requests_total`
- `admin_mutation_success_total`
- `admin_mutation_failure_total`
- `admin_mutation_rollback_total`
- `admin_mutation_authz_denied_total`

Secondary telemetry:
- mutation lifecycle events from `getClusterOperationLifecycleEvents(...)`
- immutable audit records from `getClusterAdminMutationAuditRecords(...)`

### 1.3 Transport/auth dashboard

Primary metrics:
- `session_connected_total`
- `session_disconnected_total`
- `request_failed_total`
- `auth_failed_total`
- `replay_rejected_total`
- `active_session_count`
- `active_request_count`

Secondary telemetry:
- `getRecentTransportEvents(...)`
- `getSessionSnapshot()`

## 2. Structured Log and Trace Correlation Rules

Every operational record SHOULD include:
- `trace_id`
- `request_id`
- `mutation_id` (mutation-plane only)
- `node_id` (if known)
- `timestamp_unix_ms`

Correlation process:
1. Start with alert metric spike interval.
2. Filter transport events by `request_id` and `timestamp_unix_ms`.
3. Join lifecycle events by `request_id` or `mutation_id`.
4. Join audit records by `mutation_id`.
5. Build exact event timeline before remediation.

## 3. Alert Conditions and Initial Actions

### 3.1 Mutation failure spikes

Signal:
- rapid increase in `admin_mutation_failure_total`

Actions:
1. Identify failing `mutation_id` values from audit records.
2. Inspect lifecycle event names (`admin_mutation_partially_failed`, `admin_mutation_denied`).
3. Verify auth/policy and rollout strategy constraints.
4. Pause non-essential mutation traffic until root cause is isolated.

### 3.2 Authorization denial anomalies

Signal:
- increase in `auth_failed_total` or `admin_mutation_authz_denied_total`

Actions:
1. Validate token issuer/audience/time and replay windows.
2. Compare denied scopes against policy fixtures.
3. Check recent key/cert rotations.
4. Confirm no client clock skew beyond acceptable bounds.

### 3.3 Timeout/retry storms

Signal:
- rising `dispatch_retryable_total` and timeout-heavy failure reasons

Actions:
1. Inspect `filtered_unhealthy_node_total` and node health mix.
2. Reduce retry pressure and increase backoff.
3. Drain traffic from degraded nodes.
4. Confirm function hash/version alignment.

### 3.4 Rollback frequency anomaly

Signal:
- sustained rise in `admin_mutation_rollback_total`

Actions:
1. Freeze rollout expansion.
2. Review canary behavior and verification thresholds.
3. Validate expected version/CAS inputs.
4. Require change-ticket review before further rollout.

### 3.5 Node health degradation

Signal:
- shift from `ready` to `degraded/stopped`, plus routing unhealthy filters

Actions:
1. Remove impacted nodes from candidate set.
2. Validate heartbeat freshness and capability updates.
3. Restart/recover node agents.
4. Re-admit node only after passing validation checks.

## 4. Incident Triage Flow

1. Confirm alert scope and exact time window.
2. Determine plane impacted (data, mutation, transport, mixed).
3. Pull correlated event timeline (`trace_id`/`request_id`/`mutation_id`).
4. Determine blast radius (tenant, environment, function, node selector).
5. Apply minimal-risk mitigation (drain, rollback, auth gate, traffic reduce).
6. Validate recovery with smoke calls and metric normalization.
7. Record post-incident summary and attach correlated IDs.

## 5. Rollback Procedure

1. Identify target mutation from audit record.
2. Confirm rollback policy and prior node outcomes.
3. Execute rollback mutation with strict target selector.
4. Verify no in-flight destructive mutation remains.
5. Confirm status (`rolled_back` or expected safe terminal state).
6. Capture rollback evidence in immutable audit log.

## 6. Break-Glass Procedure

1. Require explicit incident reason and approver identity.
2. Restrict scope to minimal node/function blast radius.
3. Use break-glass mutation types only when standard path is blocked.
4. Enforce immediate audit tagging and operator acknowledgment.
5. Expire break-glass access once incident is stabilized.

## 7. Key/Certificate Rotation Procedure

1. Prepare new key material and cert chain.
2. Load new token verification keys (`setTokenVerificationKeySet`/`upsertTokenVerificationKey`).
3. Validate auth success on canary traffic.
4. Deprecate old keys after overlap window.
5. Rotate mTLS materials and verify session re-authentication.
6. Monitor auth-denial metrics for regressions.

## 8. Chaos/Stress Validation Expectations

Required scenarios:
- node crash during active load
- network partition simulation
- transport reconnect storms
- mutation rollout during degraded cluster

Pass criteria:
- deterministic terminal outcomes (no hang/no orphan state)
- bounded retries/timeouts
- observable lifecycle and transport events
- no resource leak after scenario completion

