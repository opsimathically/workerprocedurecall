import assert from 'node:assert/strict';

import { ClusterClient, ClusterClientError } from '../src/index';
import { GetLabServiceDefinition, lab_config } from './lab_config';
import { HttpJsonRequest, WaitForCondition } from './lab_http';
import { BuildLabTlsClientConfig } from './lab_security';
import type {
  lab_scenario_name_t,
  lab_service_endpoint_t,
  lab_service_id_t,
  lab_state_t
} from './lab_types';

type lab_scenario_run_result_t = {
  ok: boolean;
  scenario_name: lab_scenario_name_t;
  trace_id: string;
  detail_message: string;
  where_to_look_next: string;
};

type lab_scenario_context_t = {
  state: lab_state_t;
  stop_service_func: (params: {
    service_id: lab_service_id_t;
    force_kill?: boolean;
  }) => Promise<void>;
  start_service_func: (params: { service_id: lab_service_id_t }) => Promise<void>;
  get_service_snapshot_func: (params: { service_id: lab_service_id_t }) => Promise<unknown>;
  call_service_action_func: (params: {
    service_id: lab_service_id_t;
    action_name: string;
    payload?: Record<string, unknown>;
  }) => Promise<unknown>;
};

function BuildScenarioTraceId(params: {
  scenario_name: lab_scenario_name_t;
}): string {
  return `lab_scenario_${params.scenario_name}_${Date.now()}`;
}

function BuildClient(params: {
  endpoint: lab_service_endpoint_t;
  scopes: string[];
}): ClusterClient {
  return new ClusterClient({
    host: params.endpoint.host,
    port: params.endpoint.port,
    request_path: params.endpoint.request_path,
    transport_security: BuildLabTlsClientConfig(),
    default_call_timeout_ms: 3_000,
    retry_policy: {
      max_attempts: 2,
      base_delay_ms: 30,
      max_delay_ms: 120
    },
    auth_context: {
      subject: 'local_cluster_lab_client',
      tenant_id: 'local_cluster_lab_tenant',
      scopes: [...params.scopes],
      signed_claims: 'local_cluster_lab_signed_claims'
    }
  });
}

function GetEndpointForService(params: { service_id: lab_service_id_t }): lab_service_endpoint_t {
  return GetLabServiceDefinition({
    service_id: params.service_id
  }).endpoint;
}

function RequireService(params: {
  state: lab_state_t;
  service_id: lab_service_id_t;
}): void {
  if (!params.state.service_process_state_by_id[params.service_id]) {
    throw new Error(
      `Scenario requires service "${params.service_id}" to be running in current profile.`
    );
  }
}

async function RunHappyPathCallScenario(params: {
  context: lab_scenario_context_t;
  trace_id: string;
}): Promise<lab_scenario_run_result_t> {
  const preferred_service_id_list: lab_service_id_t[] = [
    'ingress_global',
    'ingress_single',
    'node_east'
  ];

  const selected_service_id = preferred_service_id_list.find((service_id): boolean => {
    return Boolean(params.context.state.service_process_state_by_id[service_id]);
  });

  if (!selected_service_id) {
    throw new Error('No callable service was found for happy_path_call.');
  }

  const client = BuildClient({
    endpoint: GetEndpointForService({
      service_id: selected_service_id
    }),
    scopes: ['rpc.call:*']
  });

  await client.connect();
  try {
    const result = await client.call<string>({
      function_name: 'LabHello',
      args: [
        {
          tag: params.trace_id
        }
      ]
    });

    assert.equal(result.includes('LAB_OK::'), true);

    return {
      ok: true,
      scenario_name: 'happy_path_call',
      trace_id: params.trace_id,
      detail_message: `Call succeeded through "${selected_service_id}" with result "${result}".`,
      where_to_look_next:
        'Run: snapshot --service=node_east (or ingress_global/ingress_single) to inspect metrics and recent events.'
    };
  } finally {
    await client.close();
  }
}

async function RunIngressFailoverScenario(params: {
  context: lab_scenario_context_t;
  trace_id: string;
}): Promise<lab_scenario_run_result_t> {
  RequireService({
    state: params.context.state,
    service_id: 'ingress_single'
  });
  RequireService({
    state: params.context.state,
    service_id: 'node_east'
  });
  RequireService({
    state: params.context.state,
    service_id: 'node_west'
  });

  const client = BuildClient({
    endpoint: GetEndpointForService({
      service_id: 'ingress_single'
    }),
    scopes: ['rpc.call:*']
  });

  await client.connect();
  try {
    const east_result = await client.call<string>({
      function_name: 'LabHello',
      routing_hint: {
        mode: 'target_node',
        target_node_id: 'node_east'
      },
      args: [
        {
          tag: params.trace_id
        }
      ]
    });
    assert.equal(east_result.includes('node_east'), true);

    await params.context.stop_service_func({
      service_id: 'node_east'
    });

    await WaitForCondition({
      timeout_ms: 8_000,
      poll_interval_ms: 250,
      condition_func: async (): Promise<boolean> => {
        try {
          const failover_result = await client.call<string>({
            function_name: 'LabHello',
            args: [
              {
                tag: `${params.trace_id}_failover`
              }
            ]
          });
          return failover_result.includes('node_west');
        } catch {
          return false;
        }
      }
    });

    await params.context.start_service_func({
      service_id: 'node_east'
    });

    return {
      ok: true,
      scenario_name: 'ingress_failover',
      trace_id: params.trace_id,
      detail_message: 'Ingress failed over from node_east to node_west after node_east stop.',
      where_to_look_next:
        'Run: snapshot --service=ingress_single and snapshot --service=node_west to inspect retry/failover counters.'
    };
  } finally {
    await client.close();
  }
}

async function RunDiscoveryMembershipScenario(params: {
  context: lab_scenario_context_t;
  trace_id: string;
}): Promise<lab_scenario_run_result_t> {
  RequireService({
    state: params.context.state,
    service_id: 'node_east'
  });
  RequireService({
    state: params.context.state,
    service_id: 'node_west'
  });

  await WaitForCondition({
    timeout_ms: 10_000,
    poll_interval_ms: 300,
    condition_func: async (): Promise<boolean> => {
      const node_east_snapshot = (await params.context.get_service_snapshot_func({
        service_id: 'node_east'
      })) as {
        snapshot: {
          discovery_snapshot?: {
            known_remote_node_id_list?: string[];
          };
        };
      };

      const known_remote_node_id_list =
        node_east_snapshot.snapshot.discovery_snapshot?.known_remote_node_id_list ?? [];
      return known_remote_node_id_list.includes('node_west');
    }
  });

  await params.context.stop_service_func({
    service_id: 'node_west',
    force_kill: true
  });

  await WaitForCondition({
    timeout_ms: 12_000,
    poll_interval_ms: 300,
    condition_func: async (): Promise<boolean> => {
      const node_east_snapshot = (await params.context.get_service_snapshot_func({
        service_id: 'node_east'
      })) as {
        snapshot: {
          discovery_snapshot?: {
            known_remote_node_id_list?: string[];
          };
        };
      };

      const known_remote_node_id_list =
        node_east_snapshot.snapshot.discovery_snapshot?.known_remote_node_id_list ?? [];
      return !known_remote_node_id_list.includes('node_west');
    }
  });

  await params.context.start_service_func({
    service_id: 'node_west'
  });

  return {
    ok: true,
    scenario_name: 'discovery_membership_and_expiry',
    trace_id: params.trace_id,
    detail_message:
      'Discovery membership converged; node_west disappeared after forced stop and recovered after restart.',
    where_to_look_next:
      'Run: snapshot --service=node_east and snapshot --service=discovery_daemon to inspect discovery events and membership.'
  };
}

async function RunControlPlanePolicySyncScenario(params: {
  context: lab_scenario_context_t;
  trace_id: string;
}): Promise<lab_scenario_run_result_t> {
  RequireService({
    state: params.context.state,
    service_id: 'control_plane'
  });
  RequireService({
    state: params.context.state,
    service_id: 'node_east'
  });

  const action_result = (await params.context.call_service_action_func({
    service_id: 'control_plane',
    action_name: 'publish_default_policy',
    payload: {
      trace_id: params.trace_id
    }
  })) as {
    result?: {
      policy_version_id?: string;
    };
  };

  const published_policy_version_id = String(
    action_result.result?.policy_version_id ?? ''
  );
  if (published_policy_version_id.length === 0) {
    throw new Error('Control-plane policy publish did not return a policy_version_id.');
  }

  await WaitForCondition({
    timeout_ms: 8_000,
    poll_interval_ms: 250,
    condition_func: async (): Promise<boolean> => {
      const node_snapshot = (await params.context.get_service_snapshot_func({
        service_id: 'node_east'
      })) as {
        snapshot: {
          control_plane_snapshot?: {
            applied_policy_version_id?: string | null;
          };
        };
      };

      return (
        node_snapshot.snapshot.control_plane_snapshot?.applied_policy_version_id ===
        published_policy_version_id
      );
    }
  });

  return {
    ok: true,
    scenario_name: 'control_plane_policy_sync',
    trace_id: params.trace_id,
    detail_message: `Policy "${published_policy_version_id}" was published and applied by node_east.`,
    where_to_look_next:
      'Run: snapshot --service=control_plane and snapshot --service=node_east to inspect policy version convergence.'
  };
}

async function RunGeoCrossRegionFailoverScenario(params: {
  context: lab_scenario_context_t;
  trace_id: string;
}): Promise<lab_scenario_run_result_t> {
  RequireService({
    state: params.context.state,
    service_id: 'ingress_global'
  });
  RequireService({
    state: params.context.state,
    service_id: 'ingress_regional_east'
  });
  RequireService({
    state: params.context.state,
    service_id: 'ingress_regional_west'
  });

  const client = BuildClient({
    endpoint: GetEndpointForService({
      service_id: 'ingress_global'
    }),
    scopes: ['rpc.call:*']
  });

  await client.connect();
  try {
    const east_result = await client.call<string>({
      function_name: 'LabHello',
      metadata: {
        target_region_id: 'us-east-1',
        scenario_trace_id: params.trace_id
      }
    });
    assert.equal(east_result.includes('node_east'), true);

    await params.context.stop_service_func({
      service_id: 'ingress_regional_east'
    });

    await WaitForCondition({
      timeout_ms: 8_000,
      poll_interval_ms: 250,
      condition_func: async (): Promise<boolean> => {
        try {
          const west_result = await client.call<string>({
            function_name: 'LabHello',
            metadata: {
              target_region_id: 'us-east-1',
              scenario_trace_id: `${params.trace_id}_failover`
            }
          });
          return west_result.includes('node_west');
        } catch {
          return false;
        }
      }
    });

    await params.context.start_service_func({
      service_id: 'ingress_regional_east'
    });

    return {
      ok: true,
      scenario_name: 'geo_cross_region_failover',
      trace_id: params.trace_id,
      detail_message:
        'Global ingress failed over to west region after east regional ingress stop.',
      where_to_look_next:
        'Run: snapshot --service=ingress_global and snapshot --service=geo_control_plane to inspect region selection/failover events.'
    };
  } finally {
    await client.close();
  }
}

async function RunAuthFailureScenario(params: {
  context: lab_scenario_context_t;
  trace_id: string;
}): Promise<lab_scenario_run_result_t> {
  const preferred_service_id_list: lab_service_id_t[] = [
    'ingress_global',
    'ingress_single',
    'node_east'
  ];

  const selected_service_id = preferred_service_id_list.find((service_id): boolean => {
    return Boolean(params.context.state.service_process_state_by_id[service_id]);
  });

  if (!selected_service_id) {
    throw new Error('No callable service was found for auth_failure.');
  }

  const client = BuildClient({
    endpoint: GetEndpointForService({
      service_id: selected_service_id
    }),
    scopes: []
  });

  await client.connect();
  try {
    await assert.rejects(
      async (): Promise<void> => {
        await client.call<string>({
          function_name: 'LabHello',
          args: [
            {
              tag: params.trace_id
            }
          ]
        });
      },
      (error): boolean => {
        if (!(error instanceof ClusterClientError)) {
          return false;
        }

        return ['AUTH_FAILED', 'INGRESS_FORBIDDEN'].includes(error.code);
      }
    );

    return {
      ok: true,
      scenario_name: 'auth_failure',
      trace_id: params.trace_id,
      detail_message: `Unauthorized call was rejected as expected through "${selected_service_id}".`,
      where_to_look_next:
        'Run: snapshot --service=node_east and inspect transport/auth related events.'
    };
  } finally {
    await client.close();
  }
}

async function RunStaleControlFailClosedScenario(params: {
  context: lab_scenario_context_t;
  trace_id: string;
}): Promise<lab_scenario_run_result_t> {
  RequireService({
    state: params.context.state,
    service_id: 'ingress_global'
  });
  RequireService({
    state: params.context.state,
    service_id: 'geo_control_plane'
  });

  const client = BuildClient({
    endpoint: GetEndpointForService({
      service_id: 'ingress_global'
    }),
    scopes: ['rpc.call:*']
  });

  await client.connect();
  try {
    const first_result = await client.call<string>({
      function_name: 'LabHello',
      metadata: {
        scenario_trace_id: params.trace_id
      }
    });
    assert.equal(first_result.includes('LAB_OK::'), true);

    await params.context.stop_service_func({
      service_id: 'geo_control_plane'
    });

    const cached_result = await client.call<string>({
      function_name: 'LabHello',
      metadata: {
        scenario_trace_id: `${params.trace_id}_cached`
      }
    });
    assert.equal(cached_result.includes('LAB_OK::'), true);

    await WaitForCondition({
      timeout_ms: 8_000,
      poll_interval_ms: 200,
      condition_func: async (): Promise<boolean> => {
        const snapshot = (await params.context.get_service_snapshot_func({
          service_id: 'ingress_global'
        })) as {
          snapshot: {
            snapshot?: {
              target_snapshot?: {
                stale?: boolean;
              };
            };
          };
        };

        return snapshot.snapshot.snapshot?.target_snapshot?.stale === true;
      }
    });

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, lab_config.timing.stale_wait_buffer_ms);
    });

    await assert.rejects(
      async (): Promise<void> => {
        await client.call<string>({
          function_name: 'LabHello',
          metadata: {
            scenario_trace_id: `${params.trace_id}_expect_fail_closed`
          }
        });
      },
      (error): boolean => {
        return error instanceof ClusterClientError && error.code === 'INGRESS_NO_TARGET';
      }
    );

    await params.context.start_service_func({
      service_id: 'geo_control_plane'
    });

    return {
      ok: true,
      scenario_name: 'stale_control_fail_closed',
      trace_id: params.trace_id,
      detail_message: 'Global ingress served from cache briefly, then failed closed after stale-control threshold.',
      where_to_look_next:
        'Run: snapshot --service=ingress_global to inspect geo_ingress_control_stale_total and fail-closed behavior.'
    };
  } finally {
    await client.close();
  }
}

export async function RunLabScenario(params: {
  scenario_name: lab_scenario_name_t;
  context: lab_scenario_context_t;
}): Promise<lab_scenario_run_result_t> {
  const trace_id = BuildScenarioTraceId({
    scenario_name: params.scenario_name
  });

  switch (params.scenario_name) {
    case 'happy_path_call':
      return await RunHappyPathCallScenario({
        context: params.context,
        trace_id
      });
    case 'ingress_failover':
      return await RunIngressFailoverScenario({
        context: params.context,
        trace_id
      });
    case 'discovery_membership_and_expiry':
      return await RunDiscoveryMembershipScenario({
        context: params.context,
        trace_id
      });
    case 'control_plane_policy_sync':
      return await RunControlPlanePolicySyncScenario({
        context: params.context,
        trace_id
      });
    case 'geo_cross_region_failover':
      return await RunGeoCrossRegionFailoverScenario({
        context: params.context,
        trace_id
      });
    case 'auth_failure':
      return await RunAuthFailureScenario({
        context: params.context,
        trace_id
      });
    case 'stale_control_fail_closed':
      return await RunStaleControlFailClosedScenario({
        context: params.context,
        trace_id
      });
    default:
      throw new Error(`Unsupported scenario "${params.scenario_name}".`);
  }
}

export async function GetServiceSnapshotOverAdminHttp(params: {
  service_id: lab_service_id_t;
}): Promise<unknown> {
  const service_definition = GetLabServiceDefinition({
    service_id: params.service_id
  });

  return await HttpJsonRequest({
    method: 'GET',
    host: service_definition.endpoint.host,
    port: service_definition.admin_port,
    path: '/snapshot',
    timeout_ms: lab_config.timing.health_request_timeout_ms
  });
}
