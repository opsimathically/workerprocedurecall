# Local Cluster Lab

## TL;DR
`local_cluster_lab` is a single-machine deployment harness for learning and manually testing the distributed capabilities in this repository.

It provides:
- profile-based startup/shutdown,
- per-service logs,
- service snapshots/health checks,
- guided scenarios with pass/fail output.

## Security-Only Mode

- The lab is secure-only. There is no plaintext transport path.
- All service endpoints run TLS + mTLS.
- Admin health/snapshot/action endpoints also run over HTTPS + mTLS.
- Lab TLS material is loaded from repository fixtures via `local_cluster_lab/lab_security.ts`.
- If TLS material is missing or invalid, service startup fails.

## Quickstart

From repo root:

```bash
npm run lab:up:minimal
npm run lab:status
npm run lab:scenario:happy_path
npm run lab:snapshot
npm run lab:down
```

## Commands

Run through package scripts (recommended) or directly:

```bash
node --require ts-node/register --require tsconfig-paths/register local_cluster_lab/cli.ts <command> [flags]
```

Supported commands:
- `up --profile=<minimal|ingress|cluster|geo>`
- `down`
- `status`
- `logs [--service=<service_id>] [--follow=true]`
- `snapshot [--service=<service_id>]`
- `run-scenario --name=<scenario_name>`

## Profiles

1. `minimal`
- `node_east`
- flow: `ClusterClient -> ClusterNodeAgent -> WorkerProcedureCall`

2. `ingress`
- `node_east`
- `ingress_single`
- flow: `ClusterClient -> ingress_single -> node_east`

3. `cluster`
- `discovery_daemon`
- `control_plane`
- `node_east`
- `node_west`
- `ingress_single`
- flow includes discovery + control-plane sync + ingress failover between nodes.

4. `geo`
- `discovery_daemon`
- `control_plane`
- `geo_control_plane`
- `node_east`
- `node_west`
- `ingress_regional_east`
- `ingress_regional_west`
- `ingress_global`
- flow includes global-to-regional ingress selection and cross-region failover behavior.

## Scenarios

Run:

```bash
npm run lab:scenario:<name>
```

Available:
- `happy_path_call`
- `ingress_failover`
- `discovery_membership_and_expiry`
- `control_plane_policy_sync`
- `geo_cross_region_failover`
- `auth_failure`
- `stale_control_fail_closed`

Each scenario:
- checks required services,
- executes deterministic calls/assertions,
- prints a scenario `trace_id`,
- prints a “where to look next” hint.

## Logs and Snapshots

Logs:
- path: `local_cluster_lab/logs/<service_id>.log`
- view:
  - `npm run lab:logs`
  - `npm run lab:logs:service -- --service=node_east`
  - `npm run lab:logs:follow -- --service=ingress_global --follow=true`

Snapshots:
- `npm run lab:snapshot`
- `npm run lab:snapshot:service -- --service=control_plane`

Snapshot payloads are served by each service process on its local secure admin endpoint (`/snapshot` over HTTPS).

## Troubleshooting

1. Port already in use
- Run `npm run lab:down`
- If needed, set `LOCAL_CLUSTER_LAB_PORT_OFFSET` to move all lab ports.

2. Service unhealthy on startup
- Inspect `local_cluster_lab/logs/<service_id>.log`
- Use `npm run lab:status` to see which service failed readiness.

3. Scenario fails unexpectedly
- Run `npm run lab:snapshot`
- Re-run a simpler scenario (`happy_path_call`) first to validate base connectivity.

## Environment Overrides

- `LOCAL_CLUSTER_LAB_PORT_OFFSET`
- `LOCAL_CLUSTER_LAB_STARTUP_TIMEOUT_MS`
- `LOCAL_CLUSTER_LAB_STARTUP_POLL_INTERVAL_MS`
- `LOCAL_CLUSTER_LAB_SHUTDOWN_TIMEOUT_MS`
- `LOCAL_CLUSTER_LAB_HEALTH_TIMEOUT_MS`
- `LOCAL_CLUSTER_LAB_STALE_WAIT_BUFFER_MS`

## Expected Resource Usage

Approximate on a typical dev machine:
- `minimal`: low CPU/memory
- `ingress`: low-to-medium
- `cluster`: medium
- `geo`: medium-to-high

## Learning Path

1. `minimal`
2. `ingress`
3. `cluster`
4. `geo`

This order helps map behavior from simple local execution to multi-service routing/failover.
