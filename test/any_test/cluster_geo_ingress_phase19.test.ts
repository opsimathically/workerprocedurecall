import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ClusterClient,
  ClusterClientError,
  ClusterGeoIngressAdapter,
  ClusterGeoIngressControlPlaneService,
  ClusterIngressBalancerService,
  ClusterNodeAgent,
  ParseClusterGeoIngressRequestMessage,
  WorkerProcedureCall
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
  condition_func: () => boolean | Promise<boolean>;
}): Promise<void> {
  const started_unix_ms = Date.now();

  while (Date.now() - started_unix_ms <= params.timeout_ms) {
    if (await params.condition_func()) {
      return;
    }

    await Sleep({
      delay_ms: 25
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
      policy_id: 'phase19_allow_all_calls',
      effect: 'allow',
      capabilities: ['rpc.call:*']
    }
  ];
}

async function BuildNodeAgent(params: {
  node_id: string;
  return_value: string;
  function_name: string;
}): Promise<ClusterNodeAgent> {
  const workerprocedurecall = new WorkerProcedureCall();
  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: BuildAllowAllCallPolicyList()
  });
  await workerprocedurecall.defineWorkerConstant({
    name: 'PHASE19_RETURN_VALUE',
    value: params.return_value
  });

  await workerprocedurecall.defineWorkerFunction({
    name: params.function_name,
    worker_func: async function (): Promise<string> {
      return wpc_constant('PHASE19_RETURN_VALUE') as string;
    }
  });

  return new ClusterNodeAgent({
    workerprocedurecall,
    node_id: params.node_id,
    worker_start_count: 1,
    transport: {
      security: BuildSecureNodeTransportSecurity()
    },
    discovery: {
      enabled: false
    }
  });
}

test('phase19 geo ingress protocol validator accepts valid register payload and rejects malformed payload', function () {
  const valid_request = {
    protocol_version: 1,
    message_type: 'cluster_geo_ingress_ingress_register',
    timestamp_unix_ms: Date.now(),
    request_id: 'phase19_geo_req_1',
    ingress_id: 'ingress_a',
    region_id: 'us-east-1',
    endpoint: {
      host: '127.0.0.1',
      port: 7001,
      request_path: '/wpc/cluster/ingress',
      tls_mode: 'required'
    },
    health_status: 'ready',
    ingress_version: 'phase19'
  };

  const valid_result = ParseClusterGeoIngressRequestMessage({
    message: valid_request
  });
  assert.equal(valid_result.ok, true);

  const invalid_result = ParseClusterGeoIngressRequestMessage({
    message: {
      ...valid_request,
      message_type: 'cluster_geo_ingress_invalid_message_type'
    }
  });
  assert.equal(invalid_result.ok, false);
});

test('phase19 global ingress routes by geo policy and fails over across regions', async function () {
  const geo_control_plane_service = new ClusterGeoIngressControlPlaneService({
    host: '127.0.0.1',
    port: 0,
    ingress_default_lease_ttl_ms: 5_000,
    ingress_expiration_check_interval_ms: 200,
    security: {
      tls: BuildSecureServerTlsConfig()
    }
  });

  let east_node_agent: ClusterNodeAgent | null = null;
  let west_node_agent: ClusterNodeAgent | null = null;
  let east_regional_ingress: ClusterIngressBalancerService | null = null;
  let west_regional_ingress: ClusterIngressBalancerService | null = null;
  let global_ingress: ClusterIngressBalancerService | null = null;
  let cluster_client: ClusterClient | null = null;

  try {
    const geo_address = await geo_control_plane_service.start();

    east_node_agent = await BuildNodeAgent({
      node_id: 'phase19_east_node',
      return_value: 'EAST',
      function_name: 'Phase19GeoEcho'
    });
    west_node_agent = await BuildNodeAgent({
      node_id: 'phase19_west_node',
      return_value: 'WEST',
      function_name: 'Phase19GeoEcho'
    });

    const east_node_address = await east_node_agent.start();
    const west_node_address = await west_node_agent.start();

    east_regional_ingress = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      max_attempts: 1,
      transport_security: BuildSecureClientTlsConfig(),
      security: {
        tls: BuildSecureServerTlsConfig()
      },
      static_target_list: [
        {
          target_id: 'phase19_east_node',
          node_id: 'phase19_east_node',
          endpoint: {
            host: east_node_address.host,
            port: east_node_address.port,
            request_path: east_node_address.request_path,
            tls_mode: east_node_address.tls_mode
          }
        }
      ]
    });

    west_regional_ingress = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      max_attempts: 1,
      transport_security: BuildSecureClientTlsConfig(),
      security: {
        tls: BuildSecureServerTlsConfig()
      },
      static_target_list: [
        {
          target_id: 'phase19_west_node',
          node_id: 'phase19_west_node',
          endpoint: {
            host: west_node_address.host,
            port: west_node_address.port,
            request_path: west_node_address.request_path,
            tls_mode: west_node_address.tls_mode
          }
        }
      ]
    });

    const east_regional_ingress_address = await east_regional_ingress.start();
    const west_regional_ingress_address = await west_regional_ingress.start();

    const geo_ingress_adapter = new ClusterGeoIngressAdapter({
      endpoint: {
        host: geo_address.host,
        port: geo_address.port,
        request_path: geo_address.request_path,
        tls_mode: 'required'
      },
      transport_security: BuildSecureClientTlsConfig()
    });

    await geo_ingress_adapter.registerIngress({
      ingress_id: 'phase19_regional_east',
      region_id: 'us-east-1',
      endpoint: {
        host: east_regional_ingress_address.host,
        port: east_regional_ingress_address.port,
        request_path: east_regional_ingress_address.request_path,
        tls_mode: 'required'
      },
      health_status: 'ready',
      ingress_version: 'phase19',
      lease_ttl_ms: 5_000,
      region_metadata: {
        priority: 1,
        latency_ewma_ms: 15,
        capacity_score: 2,
        status: 'active'
      },
      metadata: {
        ingress_role: 'regional'
      }
    });

    await geo_ingress_adapter.registerIngress({
      ingress_id: 'phase19_regional_west',
      region_id: 'us-west-1',
      endpoint: {
        host: west_regional_ingress_address.host,
        port: west_regional_ingress_address.port,
        request_path: west_regional_ingress_address.request_path,
        tls_mode: 'required'
      },
      health_status: 'ready',
      ingress_version: 'phase19',
      lease_ttl_ms: 5_000,
      region_metadata: {
        priority: 2,
        latency_ewma_ms: 120,
        capacity_score: 1,
        status: 'active'
      },
      metadata: {
        ingress_role: 'regional'
      }
    });

    global_ingress = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      routing_mode: 'least_loaded',
      max_attempts: 4,
      target_refresh_interval_ms: 100,
      stale_snapshot_max_age_ms: 5_000,
      transport_security: BuildSecureClientTlsConfig(),
      security: {
        tls: BuildSecureServerTlsConfig()
      },
      geo_ingress: {
        enabled: true,
        role: 'global',
        region_id: 'global',
        endpoint: {
          host: geo_address.host,
          port: geo_address.port,
          request_path: geo_address.request_path,
          tls_mode: 'required'
        },
        sync_interval_ms: 250,
        stale_snapshot_max_age_ms: 2_000,
        max_cross_region_attempts: 2,
        transport_security: BuildSecureClientTlsConfig()
      }
    });

    const global_address = await global_ingress.start();

    cluster_client = new ClusterClient({
      host: global_address.host,
      port: global_address.port,
      request_path: global_address.request_path,
      transport_security: BuildSecureClientTlsConfig(),
      auth_context: {
        subject: 'phase19_geo_client',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase19_geo_claims'
      }
    });
    await cluster_client.connect();

    await WaitForCondition({
      timeout_ms: 6_000,
      condition_func: (): boolean => {
        const topology_snapshot = geo_control_plane_service.getTopologySnapshot();
        const active_regional_count = topology_snapshot.ingress_instance_record_list.filter(
          (ingress_record): boolean => {
            return (
              ingress_record.health_status !== 'offline' &&
              (ingress_record.region_id === 'us-east-1' ||
                ingress_record.region_id === 'us-west-1')
            );
          }
        ).length;

        return active_regional_count >= 2;
      }
    });

    const east_result = await cluster_client.call<string>({
      function_name: 'Phase19GeoEcho'
    });
    assert.equal(east_result, 'EAST');

    await east_regional_ingress.stop();
    east_regional_ingress = null;

    await WaitForCondition({
      timeout_ms: 6_000,
      condition_func: async (): Promise<boolean> => {
        try {
          const result = await cluster_client?.call<string>({
            function_name: 'Phase19GeoEcho'
          });
          return result === 'WEST';
        } catch {
          return false;
        }
      }
    });

    const global_metrics = global_ingress.getMetrics();
    assert.equal(global_metrics.geo_ingress_requests_total >= 2, true);
    assert.equal(
      global_metrics.geo_ingress_region_selection_count_by_reason.geo_latency_aware_selected >= 1,
      true
    );
  } finally {
    await cluster_client?.close();
    await global_ingress?.stop();
    await east_regional_ingress?.stop().catch((): void => {});
    await west_regional_ingress?.stop().catch((): void => {});
    await east_node_agent?.stop().catch((): void => {});
    await west_node_agent?.stop().catch((): void => {});
    await geo_control_plane_service.stop().catch((): void => {});
  }
});

test('phase19 geo stale-control mode serves briefly from cache then fails closed', async function () {
  const geo_control_plane_service = new ClusterGeoIngressControlPlaneService({
    host: '127.0.0.1',
    port: 0,
    ingress_default_lease_ttl_ms: 5_000,
    ingress_expiration_check_interval_ms: 250,
    security: {
      tls: BuildSecureServerTlsConfig()
    }
  });

  let regional_node_agent: ClusterNodeAgent | null = null;
  let regional_ingress: ClusterIngressBalancerService | null = null;
  let global_ingress: ClusterIngressBalancerService | null = null;
  let cluster_client: ClusterClient | null = null;

  try {
    const geo_address = await geo_control_plane_service.start();

    regional_node_agent = await BuildNodeAgent({
      node_id: 'phase19_stale_node',
      return_value: 'STALE_OK',
      function_name: 'Phase19StaleEcho'
    });
    const node_address = await regional_node_agent.start();

    regional_ingress = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      transport_security: BuildSecureClientTlsConfig(),
      security: {
        tls: BuildSecureServerTlsConfig()
      },
      static_target_list: [
        {
          target_id: 'phase19_stale_node',
          node_id: 'phase19_stale_node',
          endpoint: {
            host: node_address.host,
            port: node_address.port,
            request_path: node_address.request_path,
            tls_mode: node_address.tls_mode
          }
        }
      ]
    });
    const regional_ingress_address = await regional_ingress.start();

    const geo_ingress_adapter = new ClusterGeoIngressAdapter({
      endpoint: {
        host: geo_address.host,
        port: geo_address.port,
        request_path: geo_address.request_path,
        tls_mode: 'required'
      },
      transport_security: BuildSecureClientTlsConfig()
    });

    await geo_ingress_adapter.registerIngress({
      ingress_id: 'phase19_stale_regional_ingress',
      region_id: 'us-central-1',
      endpoint: {
        host: regional_ingress_address.host,
        port: regional_ingress_address.port,
        request_path: regional_ingress_address.request_path,
        tls_mode: 'required'
      },
      health_status: 'ready',
      ingress_version: 'phase19',
      lease_ttl_ms: 5_000,
      region_metadata: {
        priority: 1,
        latency_ewma_ms: 25,
        capacity_score: 1,
        status: 'active'
      },
      metadata: {
        ingress_role: 'regional'
      }
    });

    global_ingress = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      target_refresh_interval_ms: 100,
      stale_snapshot_max_age_ms: 5_000,
      transport_security: BuildSecureClientTlsConfig(),
      security: {
        tls: BuildSecureServerTlsConfig()
      },
      geo_ingress: {
        enabled: true,
        role: 'global',
        region_id: 'global',
        endpoint: {
          host: geo_address.host,
          port: geo_address.port,
          request_path: geo_address.request_path,
          tls_mode: 'required'
        },
        request_timeout_ms: 100,
        max_request_attempts: 1,
        endpoint_cooldown_ms: 50,
        retry_base_delay_ms: 25,
        retry_max_delay_ms: 50,
        sync_interval_ms: 250,
        stale_snapshot_max_age_ms: 450,
        max_cross_region_attempts: 1,
        transport_security: BuildSecureClientTlsConfig()
      }
    });

    const global_address = await global_ingress.start();

    cluster_client = new ClusterClient({
      host: global_address.host,
      port: global_address.port,
      request_path: global_address.request_path,
      transport_security: BuildSecureClientTlsConfig(),
      auth_context: {
        subject: 'phase19_stale_client',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase19_stale_claims'
      }
    });
    await cluster_client.connect();

    await WaitForCondition({
      timeout_ms: 4_000,
      condition_func: (): boolean => {
        return (
          (global_ingress?.getSnapshot().target_snapshot.target_list.length ?? 0) >= 1
        );
      }
    });

    const initial_result = await cluster_client.call<string>({
      function_name: 'Phase19StaleEcho'
    });
    assert.equal(initial_result, 'STALE_OK');

    await geo_control_plane_service.stop();

    const cached_result = await cluster_client.call<string>({
      function_name: 'Phase19StaleEcho'
    });
    assert.equal(cached_result, 'STALE_OK');

    await WaitForCondition({
      timeout_ms: 6_000,
      condition_func: (): boolean => {
        return global_ingress?.getSnapshot().target_snapshot.stale === true;
      }
    });

    await Sleep({ delay_ms: 600 });

    await assert.rejects(
      async (): Promise<void> => {
        await cluster_client?.call<string>({
          function_name: 'Phase19StaleEcho'
        });
      },
      (error: unknown): boolean => {
        return error instanceof ClusterClientError && error.code === 'INGRESS_NO_TARGET';
      }
    );
  } finally {
    await cluster_client?.close();
    await global_ingress?.stop().catch((): void => {});
    await regional_ingress?.stop().catch((): void => {});
    await regional_node_agent?.stop().catch((): void => {});
    await geo_control_plane_service.stop().catch((): void => {});
  }
});

test('phase19 geo disabled mode preserves prior static ingress behavior', async function () {
  const node_agent = await BuildNodeAgent({
    node_id: 'phase19_disabled_node',
    return_value: 'DISABLED_OK',
    function_name: 'Phase19DisabledEcho'
  });

  let ingress_service: ClusterIngressBalancerService | null = null;
  let cluster_client: ClusterClient | null = null;

  try {
    const node_address = await node_agent.start();

    ingress_service = new ClusterIngressBalancerService({
      host: '127.0.0.1',
      port: 0,
      transport_security: BuildSecureClientTlsConfig(),
      security: {
        tls: BuildSecureServerTlsConfig()
      },
      static_target_list: [
        {
          target_id: 'phase19_disabled_node',
          node_id: 'phase19_disabled_node',
          endpoint: {
            host: node_address.host,
            port: node_address.port,
            request_path: node_address.request_path,
            tls_mode: node_address.tls_mode
          }
        }
      ],
      geo_ingress: {
        enabled: false
      }
    });

    const ingress_address = await ingress_service.start();

    cluster_client = new ClusterClient({
      host: ingress_address.host,
      port: ingress_address.port,
      request_path: ingress_address.request_path,
      transport_security: BuildSecureClientTlsConfig(),
      auth_context: {
        subject: 'phase19_disabled_client',
        tenant_id: 'tenant_1',
        scopes: ['rpc.call:*'],
        signed_claims: 'phase19_disabled_claims'
      }
    });
    await cluster_client.connect();

    const result = await cluster_client.call<string>({
      function_name: 'Phase19DisabledEcho'
    });

    assert.equal(result, 'DISABLED_OK');
    assert.equal(ingress_service.getMetrics().geo_ingress_requests_total, 0);
  } finally {
    await cluster_client?.close();
    await ingress_service?.stop().catch((): void => {});
    await node_agent.stop().catch((): void => {});
  }
});
