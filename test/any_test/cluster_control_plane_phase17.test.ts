import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';

import {
  ClusterControlPlaneServiceFileStateStore,
  ClusterControlPlaneGatewayAdapter,
  ClusterControlPlaneService,
  ClusterNodeAgent,
  ParseClusterControlPlaneRequestMessage,
  WorkerProcedureCall,
  type cluster_control_plane_request_message_t
} from '../../src/index';
import {
  BuildSecureClientTlsConfig,
  BuildSecureNodeTransportSecurity,
  BuildSecureServerTlsConfig
} from '../fixtures/secure_transport_config';

function Sleep(params: { delay_ms: number }): Promise<void> {
  return new Promise<void>((resolve): void => {
    setTimeout(resolve, params.delay_ms);
  });
}

async function WaitForCondition(params: {
  timeout_ms: number;
  condition_func: () => boolean;
}): Promise<void> {
  const started_unix_ms = Date.now();

  while (Date.now() - started_unix_ms <= params.timeout_ms) {
    if (params.condition_func()) {
      return;
    }

    await Sleep({
      delay_ms: 20
    });
  }

  throw new Error(`Condition was not satisfied within ${params.timeout_ms}ms.`);
}

function BuildAllowAllCallPolicyList(): {
  policy_id: string;
  effect: 'allow';
  capabilities: string[];
}[] {
  return [
    {
      policy_id: 'phase17_allow_all_call_policy',
      effect: 'allow',
      capabilities: ['rpc.call:*']
    }
  ];
}

test('phase17 protocol validator accepts all control-plane request message forms', function () {
  const now_unix_ms = Date.now();

  const request_message_list: cluster_control_plane_request_message_t[] = [
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_gateway_register',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_register',
      gateway_id: 'gateway_1',
      node_reference: {
        node_id: 'node_1'
      },
      address: {
        host: '127.0.0.1',
        port: 8080,
        request_path: '/wpc/cluster/protocol',
        tls_mode: 'required'
      },
      status: 'active',
      gateway_version: '1.0.0'
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_gateway_heartbeat',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_heartbeat',
      gateway_id: 'gateway_1'
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_gateway_deregister',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_deregister',
      gateway_id: 'gateway_1'
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_get_topology_snapshot',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_topology'
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_get_policy_snapshot',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_policy'
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_update_policy_snapshot',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_update',
      actor_subject: 'admin',
      policy_snapshot: {
        routing_policy: {
          heartbeat_ttl_ms: 10_000
        }
      }
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_subscribe_updates',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_subscribe'
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_config_version_announce',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_announce',
      gateway_id: 'gateway_1',
      policy_version_id: 'policy_1'
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_config_version_ack',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_ack',
      gateway_id: 'gateway_1',
      policy_version_id: 'policy_1',
      applied: true
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_get_service_status',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_service_status'
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_mutation_intent_update',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_mutation_intent',
      mutation_id: 'mutation_1',
      status: 'running'
    },
    {
      protocol_version: 1,
      message_type: 'cluster_control_plane_get_mutation_status',
      timestamp_unix_ms: now_unix_ms,
      request_id: 'phase17_validate_mutation_status',
      mutation_id: 'mutation_1'
    }
  ];

  for (const request_message of request_message_list) {
    const parse_result = ParseClusterControlPlaneRequestMessage({
      message: request_message
    });
    assert.equal(parse_result.ok, true);
  }
});

test('phase17 multi-gateway control-plane registration and topology convergence works', async function () {
  const control_plane_service = new ClusterControlPlaneService({
    host: 'localhost',
    port: 0,
    security: {
      tls: BuildSecureServerTlsConfig()
    }
  });

  const workerprocedurecall_a = new WorkerProcedureCall();
  const workerprocedurecall_b = new WorkerProcedureCall();
  let node_agent_a: ClusterNodeAgent | null = null;
  let node_agent_b: ClusterNodeAgent | null = null;

  try {
    const control_plane_address = await control_plane_service.start();

    const endpoint = {
      host: control_plane_address.host,
      port: control_plane_address.port,
      request_path: control_plane_address.request_path,
      tls_mode: 'required' as const
    };

    node_agent_a = new ClusterNodeAgent({
      workerprocedurecall: workerprocedurecall_a,
      node_id: 'phase17_node_a',
      worker_start_count: 1,
      transport: {
        host: 'localhost',
        security: BuildSecureNodeTransportSecurity()
      },
      discovery: {
        enabled: false
      },
      control_plane: {
        enabled: true,
        endpoint,
        transport_security: BuildSecureClientTlsConfig(),
        sync_interval_ms: 100,
        heartbeat_interval_ms: 100
      }
    });

    node_agent_b = new ClusterNodeAgent({
      workerprocedurecall: workerprocedurecall_b,
      node_id: 'phase17_node_b',
      worker_start_count: 1,
      transport: {
        host: 'localhost',
        security: BuildSecureNodeTransportSecurity()
      },
      discovery: {
        enabled: false
      },
      control_plane: {
        enabled: true,
        endpoint,
        transport_security: BuildSecureClientTlsConfig(),
        sync_interval_ms: 100,
        heartbeat_interval_ms: 100
      }
    });

    await node_agent_a.start();
    await node_agent_b.start();

    await WaitForCondition({
      timeout_ms: 4_000,
      condition_func: (): boolean => {
        const snapshot = control_plane_service.getServiceStatusSnapshot();
        return snapshot.active_gateway_count >= 2;
      }
    });

    const topology_snapshot = await control_plane_service.getRecentEvents({
      limit: 50
    });
    assert.equal(
      topology_snapshot.some((event): boolean => {
        return event.event_name === 'control_plane_gateway_registered';
      }),
      true
    );

    await node_agent_a.stop();
    await node_agent_b.stop();
  } finally {
    await node_agent_a?.stop().catch((): void => {});
    await node_agent_b?.stop().catch((): void => {});
    await control_plane_service.stop();
  }
});

test('phase17 policy publish syncs to gateway and applies routing/auth policy versions', async function () {
  const control_plane_service = new ClusterControlPlaneService({
    host: 'localhost',
    port: 0,
    security: {
      tls: BuildSecureServerTlsConfig()
    }
  });

  const workerprocedurecall = new WorkerProcedureCall();
  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });

  let node_agent: ClusterNodeAgent | null = null;

  try {
    const control_plane_address = await control_plane_service.start();

    node_agent = new ClusterNodeAgent({
      workerprocedurecall,
      node_id: 'phase17_policy_node',
      worker_start_count: 1,
      transport: {
        host: 'localhost',
        security: BuildSecureNodeTransportSecurity()
      },
      discovery: {
        enabled: false
      },
      control_plane: {
        enabled: true,
        endpoint: {
          host: control_plane_address.host,
          port: control_plane_address.port,
          request_path: control_plane_address.request_path,
          tls_mode: 'required'
        },
        transport_security: BuildSecureClientTlsConfig(),
        sync_interval_ms: 100,
        heartbeat_interval_ms: 100
      }
    });

    await node_agent.start();

    const published_policy = await control_plane_service.publishPolicySnapshot({
      actor_subject: 'phase17_admin',
      policy_snapshot: {
        routing_policy: {
          heartbeat_ttl_ms: 999_999
        },
        authorization_policy_list: BuildAllowAllCallPolicyList()
      }
    });

    await WaitForCondition({
      timeout_ms: 4_000,
      condition_func: (): boolean => {
        const snapshot = node_agent?.getControlPlaneSnapshot();
        return (
          snapshot?.applied_policy_version_id ===
          published_policy.metadata.version_id
        );
      }
    });

    const updated_routing_policy = workerprocedurecall.getClusterCallRoutingPolicy();
    assert.equal(updated_routing_policy.heartbeat_ttl_ms, 999_999);

    const policy_list = workerprocedurecall.getClusterAuthorizationPolicyList();
    assert.equal(policy_list.length > 0, true);
  } finally {
    await node_agent?.stop();
    await control_plane_service.stop();
  }
});

test('phase17 temporary control-plane outage keeps gateway serving with last-known-good config and exposes staleness', async function () {
  const control_plane_service = new ClusterControlPlaneService({
    host: 'localhost',
    port: 0,
    security: {
      tls: BuildSecureServerTlsConfig()
    }
  });

  const workerprocedurecall = new WorkerProcedureCall();

  let node_agent: ClusterNodeAgent | null = null;

  try {
    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase17OutageCall',
      worker_func: async function (): Promise<string> {
        return 'OK_PHASE17';
      }
    });

    const control_plane_address = await control_plane_service.start();

    node_agent = new ClusterNodeAgent({
      workerprocedurecall,
      node_id: 'phase17_outage_node',
      worker_start_count: 1,
      transport: {
        host: 'localhost',
        security: BuildSecureNodeTransportSecurity()
      },
      discovery: {
        enabled: false
      },
      control_plane: {
        enabled: true,
        endpoint: {
          host: control_plane_address.host,
          port: control_plane_address.port,
          request_path: control_plane_address.request_path,
          tls_mode: 'required'
        },
        transport_security: BuildSecureClientTlsConfig(),
        sync_interval_ms: 100,
        heartbeat_interval_ms: 100
      }
    });

    await node_agent.start();
    await control_plane_service.stop();

    await WaitForCondition({
      timeout_ms: 4_000,
      condition_func: (): boolean => {
        const snapshot = node_agent?.getControlPlaneSnapshot();
        return (
          (snapshot?.stale_since_unix_ms ?? null) !== null &&
          (snapshot?.metrics.sync_error_total ?? 0) > 0
        );
      }
    });

    const return_value = await workerprocedurecall.call.Phase17OutageCall();
    assert.equal(return_value, 'OK_PHASE17');
  } finally {
    await node_agent?.stop().catch((): void => {});
    await control_plane_service.stop().catch((): void => {});
  }
});

test('phase17 control-plane state persists active policy version across service restart', async function () {
  const temp_directory_path = mkdtempSync(
    join(tmpdir(), 'wpc_phase17_control_plane_state_')
  );
  const state_file_path = join(temp_directory_path, 'control_plane_state.json');

  let first_service: ClusterControlPlaneService | null = null;
  let second_service: ClusterControlPlaneService | null = null;

  try {
    const first_state_store = new ClusterControlPlaneServiceFileStateStore({
      file_path: state_file_path
    });

    first_service = new ClusterControlPlaneService({
      host: 'localhost',
      port: 0,
      state_store: first_state_store,
      security: {
        tls: BuildSecureServerTlsConfig()
      }
    });

    const first_address = await first_service.start();
    const gateway_adapter = new ClusterControlPlaneGatewayAdapter({
      endpoint: {
        host: first_address.host,
        port: first_address.port,
        request_path: first_address.request_path,
        tls_mode: 'required'
      },
      transport_security: BuildSecureClientTlsConfig()
    });

    const policy_record = await first_service.publishPolicySnapshot({
      actor_subject: 'phase17_restart_admin',
      policy_snapshot: {
        routing_policy: {
          heartbeat_ttl_ms: 321_000
        }
      }
    });

    const first_policy_snapshot = await gateway_adapter.getPolicySnapshot();
    assert.equal(
      first_policy_snapshot.active_policy_version_id,
      policy_record.metadata.version_id
    );

    await first_service.stop();
    first_service = null;

    const second_state_store = new ClusterControlPlaneServiceFileStateStore({
      file_path: state_file_path
    });

    second_service = new ClusterControlPlaneService({
      host: 'localhost',
      port: 0,
      state_store: second_state_store,
      security: {
        tls: BuildSecureServerTlsConfig()
      }
    });

    const second_address = await second_service.start();
    const second_gateway_adapter = new ClusterControlPlaneGatewayAdapter({
      endpoint: {
        host: second_address.host,
        port: second_address.port,
        request_path: second_address.request_path,
        tls_mode: 'required'
      },
      transport_security: BuildSecureClientTlsConfig()
    });

    const second_policy_snapshot = await second_gateway_adapter.getPolicySnapshot();
    assert.equal(
      second_policy_snapshot.active_policy_version_id,
      policy_record.metadata.version_id
    );
  } finally {
    await first_service?.stop().catch((): void => {});
    await second_service?.stop().catch((): void => {});
    rmSync(temp_directory_path, {
      recursive: true,
      force: true
    });
  }
});

test('phase17 conflicting policy expected_active_policy_version_id is rejected deterministically', async function () {
  const control_plane_service = new ClusterControlPlaneService({
    host: 'localhost',
    port: 0,
    security: {
      tls: BuildSecureServerTlsConfig()
    }
  });

  try {
    await control_plane_service.start();

    const first_policy_record = await control_plane_service.publishPolicySnapshot({
      actor_subject: 'phase17_conflict_admin',
      policy_snapshot: {
        routing_policy: {
          heartbeat_ttl_ms: 10_000
        }
      }
    });

    const second_policy_record = await control_plane_service.publishPolicySnapshot({
      actor_subject: 'phase17_conflict_admin',
      policy_snapshot: {
        routing_policy: {
          heartbeat_ttl_ms: 20_000
        }
      }
    });

    assert.notEqual(
      first_policy_record.metadata.version_id,
      second_policy_record.metadata.version_id
    );

    await assert.rejects(
      async (): Promise<void> => {
        await control_plane_service.publishPolicySnapshot({
          actor_subject: 'phase17_conflict_admin',
          expected_active_policy_version_id: first_policy_record.metadata.version_id,
          policy_snapshot: {
            routing_policy: {
              heartbeat_ttl_ms: 30_000
            }
          }
        });
      },
      (error: unknown): boolean => {
        if (
          typeof error === 'object' &&
          error !== null &&
          'kind' in error &&
          'error' in error
        ) {
          const typed_error = error as {
            kind?: string;
            error?: {
              code?: string;
            };
          };

          return (
            typed_error.kind === 'protocol_error' &&
            typed_error.error?.code === 'CONTROL_PLANE_CONFLICT'
          );
        }

        return false;
      }
    );
  } finally {
    await control_plane_service.stop();
  }
});

test('phase17 gateway lease expiry marks inactive gateways when heartbeats stop', async function () {
  const control_plane_service = new ClusterControlPlaneService({
    host: 'localhost',
    port: 0,
    gateway_expiration_check_interval_ms: 50,
    security: {
      tls: BuildSecureServerTlsConfig()
    }
  });

  try {
    const address = await control_plane_service.start();

    const gateway_adapter = new ClusterControlPlaneGatewayAdapter({
      endpoint: {
        host: address.host,
        port: address.port,
        request_path: address.request_path,
        tls_mode: 'required'
      },
      transport_security: BuildSecureClientTlsConfig()
    });

    await gateway_adapter.registerGateway({
      gateway_id: 'phase17_expiry_gateway',
      node_reference: {
        node_id: 'phase17_expiry_node'
      },
      address: {
        host: '127.0.0.1',
        port: 8181,
        request_path: '/wpc/cluster/protocol',
        tls_mode: 'required'
      },
      status: 'active',
      gateway_version: 'phase17',
      lease_ttl_ms: 100
    });

    await WaitForCondition({
      timeout_ms: 3_000,
      condition_func: (): boolean => {
        const snapshot = control_plane_service.getServiceStatusSnapshot();
        return snapshot.active_gateway_count === 0;
      }
    });

    const recent_event_list = control_plane_service.getRecentEvents({
      limit: 20
    });
    assert.equal(
      recent_event_list.some((event): boolean => {
        return (
          event.gateway_id === 'phase17_expiry_gateway' &&
          event.event_name === 'control_plane_gateway_expired'
        );
      }),
      true
    );
  } finally {
    await control_plane_service.stop();
  }
});

test('phase17 disabled control-plane mode preserves baseline node-agent behavior', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  const node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase17_disabled_mode_node',
    worker_start_count: 1,
    transport: {
      host: 'localhost',
      security: BuildSecureNodeTransportSecurity()
    },
    discovery: {
      enabled: false
    },
    control_plane: {
      enabled: false
    }
  });

  try {
    await node_agent.start();

    const control_plane_snapshot = node_agent.getControlPlaneSnapshot();
    assert.equal(control_plane_snapshot.enabled, false);
  } finally {
    await node_agent.stop();
  }
});
