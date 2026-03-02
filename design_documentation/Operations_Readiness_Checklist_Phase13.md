# Phase 13 Staged Deployment Readiness Checklist

Use this checklist before promoting from staging to production.

## 1. Observability Readiness

- [ ] `getClusterOperationsObservabilitySnapshot()` available in runtime diagnostics.
- [ ] `getClusterOperationLifecycleEvents(...)` retention and query verified.
- [ ] `ClusterNodeAgent.getTransportMetrics()` wired to dashboards.
- [ ] `ClusterNodeAgent.getRecentTransportEvents(...)` query path available.
- [ ] Trace correlation fields (`trace_id`, `request_id`, `mutation_id`) present in logs/events.

## 2. Alerting Readiness

- [ ] Alert configured for mutation failure spikes.
- [ ] Alert configured for auth-denial anomalies.
- [ ] Alert configured for timeout/retry storms.
- [ ] Alert configured for rollback frequency increase.
- [ ] Alert configured for node health degradation and unhealthy-routing growth.

## 3. Security and Governance Readiness

- [ ] mTLS mode and trust chain validated for target environment.
- [ ] Token validation issuer/audience/expiry checks validated.
- [ ] Replay protection validated for privileged mutation requests.
- [ ] Change reason/ticket requirements enabled as policy requires.
- [ ] Break-glass access path documented and approval enforced.

## 4. Operational Procedure Readiness

- [ ] Incident triage flow rehearsed with on-call operators.
- [ ] Rollback runbook executed in staging successfully.
- [ ] Key/cert rotation dry run completed.
- [ ] Runbook references accessible to on-call rotation.

## 5. Chaos/Stress Validation Readiness

- [ ] Node crash under load scenario passes deterministically.
- [ ] Network partition simulation passes deterministic recovery checks.
- [ ] Reconnect storm scenario passes with bounded error behavior.
- [ ] Mutation rollout during degraded cluster emits expected failure/rollback signals.
- [ ] No resource leak after all scenarios (`active_session_count` and in-flight metrics return to baseline).

## 6. Promotion Gates

- [ ] `npm test` passes on release candidate.
- [ ] `npm run test:chaos-phase13` passes on release candidate.
- [ ] No unresolved Sev-1/Sev-2 incidents in prior 24 hours.
- [ ] Rollback owner and incident commander are assigned for release window.

