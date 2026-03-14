import http from 'node:http';
import https from 'node:https';

import {
  ClusterControlPlaneService,
  ClusterGeoIngressControlPlaneService,
  ClusterIngressBalancerService,
  ClusterNodeAgent,
  ClusterServiceDiscoveryDaemon,
  WorkerProcedureCall,
  type cluster_node_agent_control_plane_config_t,
  type cluster_node_agent_discovery_config_t
} from '../src/index';
import { GetLabServiceDefinition, lab_config } from './lab_config';
import {
  BuildLabNodeTransportSecurity,
  BuildLabTlsClientConfig,
  BuildLabTlsServerConfig
} from './lab_security';
import type {
  lab_profile_t,
  lab_service_definition_i,
  lab_service_endpoint_t,
  lab_service_id_t
} from './lab_types';

type lab_service_runtime_handle_t = {
  endpoint: lab_service_endpoint_t;
  getSnapshot: () => unknown;
  stop: () => Promise<void>;
  action_handler_by_name: Record<
    string,
    (payload: Record<string, unknown>) => Promise<unknown>
  >;
};

function BuildLogLine(params: {
  service_id: string;
  message: string;
  details?: Record<string, unknown>;
}): string {
  return JSON.stringify({
    timestamp_unix_ms: Date.now(),
    service_id: params.service_id,
    message: params.message,
    details: params.details ?? {}
  });
}

function LogServiceMessage(params: {
  service_id: string;
  message: string;
  details?: Record<string, unknown>;
}): void {
  process.stdout.write(
    `${BuildLogLine({
      service_id: params.service_id,
      message: params.message,
      details: params.details
    })}\n`
  );
}

function ParseRuntimeArgs(params: { argv: string[] }): {
  service_id: lab_service_id_t;
  profile: lab_profile_t;
} {
  let service_id: lab_service_id_t | null = null;
  let profile: lab_profile_t | null = null;

  for (const raw_arg of params.argv) {
    if (!raw_arg.startsWith('--')) {
      continue;
    }

    const [flag_name, flag_value] = raw_arg.slice(2).split('=');
    if (flag_name === 'service') {
      service_id = flag_value as lab_service_id_t;
      continue;
    }

    if (flag_name === 'profile') {
      profile = flag_value as lab_profile_t;
      continue;
    }
  }

  if (!service_id) {
    throw new Error('Missing required --service=<service_id> runtime argument.');
  }

  if (!profile) {
    throw new Error('Missing required --profile=<profile> runtime argument.');
  }

  return {
    service_id,
    profile
  };
}

function BuildNodeDiscoveryConfig(params: {
  profile: lab_profile_t;
}): cluster_node_agent_discovery_config_t {
  if (params.profile !== 'cluster' && params.profile !== 'geo') {
    return {
      enabled: false
    };
  }

  const discovery_definition = GetLabServiceDefinition({
    service_id: 'discovery_daemon'
  });

  return {
    enabled: true,
    external_daemon: {
      host: discovery_definition.endpoint.host,
      port: discovery_definition.endpoint.port,
      request_path: discovery_definition.endpoint.request_path,
      tls_mode: 'required',
      transport_security: BuildLabTlsClientConfig()
    },
    heartbeat_interval_ms: 500,
    lease_ttl_ms: 1_500,
    sync_interval_ms: 500
  };
}

function BuildNodeControlPlaneConfig(params: {
  profile: lab_profile_t;
}): cluster_node_agent_control_plane_config_t {
  if (params.profile !== 'cluster' && params.profile !== 'geo') {
    return {
      enabled: false
    };
  }

  const control_plane_definition = GetLabServiceDefinition({
    service_id: 'control_plane'
  });

  return {
    enabled: true,
    endpoint: {
      host: control_plane_definition.endpoint.host,
      port: control_plane_definition.endpoint.port,
      request_path: control_plane_definition.endpoint.request_path,
      tls_mode: 'required'
    },
    transport_security: BuildLabTlsClientConfig(),
    heartbeat_interval_ms: 500,
    sync_interval_ms: 500,
    use_topology_for_routing: true
  };
}

async function StartNodeService(params: {
  service_definition: lab_service_definition_i;
  profile: lab_profile_t;
}): Promise<lab_service_runtime_handle_t> {
  const workerprocedurecall = new WorkerProcedureCall();
  const region_id = String(params.service_definition.metadata.region_id ?? 'unknown_region');

  workerprocedurecall.setClusterAuthorizationPolicyList({
    policy_list: [
      {
        policy_id: 'local_cluster_lab_allow_all_call_scopes',
        effect: 'allow',
        capabilities: ['rpc.call:*']
      }
    ]
  });

  await workerprocedurecall.defineWorkerFunction({
    name: 'LabHello',
    worker_func: async function (runtime_params?: {
      tag?: string;
    }): Promise<string> {
      const tag_value = runtime_params?.tag ?? 'none';
      const node_id = wpc_constant('LAB_NODE_ID') as string;
      return `LAB_OK::${node_id}::${tag_value}`;
    }
  });

  await workerprocedurecall.defineWorkerFunction({
    name: 'LabRegionEcho',
    worker_func: async function (): Promise<string> {
      return wpc_constant('LAB_REGION_ID') as string;
    }
  });

  await workerprocedurecall.defineWorkerConstant({
    name: 'LAB_REGION_ID',
    value: region_id
  });

  await workerprocedurecall.defineWorkerConstant({
    name: 'LAB_NODE_ID',
    value: params.service_definition.service_id
  });

  const node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: params.service_definition.service_id,
    worker_start_count: 2,
    transport: {
      host: params.service_definition.endpoint.host,
      port: params.service_definition.endpoint.port,
      request_path: params.service_definition.endpoint.request_path,
      security: BuildLabNodeTransportSecurity()
    },
    discovery: BuildNodeDiscoveryConfig({
      profile: params.profile
    }),
    control_plane: BuildNodeControlPlaneConfig({
      profile: params.profile
    })
  });

  const address = await node_agent.start();

  return {
    endpoint: {
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      tls_mode: address.tls_mode
    },
    getSnapshot: (): unknown => {
      return {
        address: node_agent.getAddress(),
        transport_metrics: node_agent.getTransportMetrics(),
        transport_events: node_agent.getRecentTransportEvents({
          limit: 30
        }),
        session_snapshot: node_agent.getSessionSnapshot(),
        operations_snapshot: node_agent.getOperationsObservabilitySnapshot(),
        discovery_snapshot: node_agent.getDiscoverySnapshot(),
        discovery_events: node_agent.getDiscoveryEvents({
          limit: 30
        }),
        control_plane_snapshot: node_agent.getControlPlaneSnapshot()
      };
    },
    stop: async (): Promise<void> => {
      await node_agent.stop();
    },
    action_handler_by_name: {}
  };
}

async function StartDiscoveryDaemonService(params: {
  service_definition: lab_service_definition_i;
}): Promise<lab_service_runtime_handle_t> {
  const daemon = new ClusterServiceDiscoveryDaemon({
    host: params.service_definition.endpoint.host,
    port: params.service_definition.endpoint.port,
    request_path: params.service_definition.endpoint.request_path,
    security: {
      tls: BuildLabTlsServerConfig()
    },
    transport_security: BuildLabTlsClientConfig()
  });

  const address = await daemon.start();

  return {
    endpoint: {
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      tls_mode: 'required'
    },
    getSnapshot: (): unknown => {
      return daemon.getSnapshot();
    },
    stop: async (): Promise<void> => {
      await daemon.stop();
    },
    action_handler_by_name: {}
  };
}

async function StartControlPlaneService(params: {
  service_definition: lab_service_definition_i;
}): Promise<lab_service_runtime_handle_t> {
  const control_plane_service = new ClusterControlPlaneService({
    host: params.service_definition.endpoint.host,
    port: params.service_definition.endpoint.port,
    request_path: params.service_definition.endpoint.request_path,
    security: {
      tls: BuildLabTlsServerConfig()
    }
  });

  const address = await control_plane_service.start();

  return {
    endpoint: {
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      tls_mode: 'required'
    },
    getSnapshot: (): unknown => {
      return {
        address: control_plane_service.getAddress(),
        service_status: control_plane_service.getServiceStatusSnapshot(),
        metrics: control_plane_service.getMetrics(),
        recent_events: control_plane_service.getRecentEvents({
          limit: 100
        })
      };
    },
    stop: async (): Promise<void> => {
      await control_plane_service.stop();
    },
    action_handler_by_name: {
      publish_default_policy: async (): Promise<unknown> => {
        const policy_version = await control_plane_service.publishPolicySnapshot({
          actor_subject: 'local_cluster_lab',
          policy_snapshot: {
            routing_policy: {},
            authorization_policy_list: [
              {
                policy_id: 'local_cluster_lab_published_allow_all_calls',
                effect: 'allow',
                capabilities: ['rpc.call:*']
              }
            ],
            metadata: {
              source: 'local_cluster_lab_action'
            }
          },
          activate_immediately: true
        });

        return {
          policy_version_id: policy_version.metadata.version_id
        };
      }
    }
  };
}

async function StartGeoControlPlaneService(params: {
  service_definition: lab_service_definition_i;
}): Promise<lab_service_runtime_handle_t> {
  const geo_control_plane_service = new ClusterGeoIngressControlPlaneService({
    host: params.service_definition.endpoint.host,
    port: params.service_definition.endpoint.port,
    request_path: params.service_definition.endpoint.request_path,
    ingress_default_lease_ttl_ms: 1_500,
    ingress_expiration_check_interval_ms: 300,
    security: {
      tls: BuildLabTlsServerConfig()
    }
  });

  const address = await geo_control_plane_service.start();

  return {
    endpoint: {
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      tls_mode: 'required'
    },
    getSnapshot: (): unknown => {
      return {
        address: geo_control_plane_service.getAddress(),
        service_status: geo_control_plane_service.getServiceStatusSnapshot(),
        topology_snapshot: geo_control_plane_service.getTopologySnapshot(),
        metrics: geo_control_plane_service.getMetrics(),
        recent_events: geo_control_plane_service.getRecentEvents({
          limit: 100
        })
      };
    },
    stop: async (): Promise<void> => {
      await geo_control_plane_service.stop();
    },
    action_handler_by_name: {
      publish_default_geo_policy: async (): Promise<unknown> => {
        const policy_version = await geo_control_plane_service.publishGlobalRoutingPolicy({
          actor_subject: 'local_cluster_lab',
          policy_snapshot: {
            default_mode: 'latency_aware',
            retry_within_region_first: true,
            max_cross_region_attempts: 2,
            stickiness_scope: 'none'
          }
        });

        return {
          policy_version_id: policy_version.version_id
        };
      }
    }
  };
}

function BuildIngressTarget(params: {
  service_id: 'node_east' | 'node_west';
}): {
  target_id: string;
  node_id: string;
  endpoint: lab_service_endpoint_t;
} {
  const service_definition = GetLabServiceDefinition({
    service_id: params.service_id
  });

  return {
    target_id: service_definition.service_id,
    node_id: service_definition.service_id,
    endpoint: service_definition.endpoint
  };
}

async function StartIngressService(params: {
  service_definition: lab_service_definition_i;
  profile: lab_profile_t;
}): Promise<lab_service_runtime_handle_t> {
  const service_id = params.service_definition.service_id;

  const base_constructor_params: ConstructorParameters<
    typeof ClusterIngressBalancerService
  >[0] = {
    ingress_id: service_id,
    host: params.service_definition.endpoint.host,
    port: params.service_definition.endpoint.port,
    request_path: params.service_definition.endpoint.request_path,
    transport_security: BuildLabTlsClientConfig(),
    security: {
      tls: BuildLabTlsServerConfig()
    }
  };

  if (service_id === 'ingress_single') {
    const static_target_list =
      params.profile === 'cluster'
        ? [BuildIngressTarget({ service_id: 'node_east' }), BuildIngressTarget({ service_id: 'node_west' })]
        : [BuildIngressTarget({ service_id: 'node_east' })];

    base_constructor_params.static_target_list = static_target_list;
    base_constructor_params.max_attempts = 3;
    base_constructor_params.request_timeout_ms = 2_000;
  } else if (service_id === 'ingress_regional_east') {
    const geo_definition = GetLabServiceDefinition({
      service_id: 'geo_control_plane'
    });

    base_constructor_params.static_target_list = [
      BuildIngressTarget({
        service_id: 'node_east'
      })
    ];
    base_constructor_params.geo_ingress = {
      enabled: true,
      role: 'regional',
      region_id: 'us-east-1',
      endpoint: {
        host: geo_definition.endpoint.host,
        port: geo_definition.endpoint.port,
        request_path: geo_definition.endpoint.request_path,
        tls_mode: 'required'
      },
      transport_security: BuildLabTlsClientConfig(),
      sync_interval_ms: 300,
      lease_ttl_ms: 1_500,
      region_priority: 1,
      region_latency_ewma_ms: 15,
      region_capacity_score: 2
    };
  } else if (service_id === 'ingress_regional_west') {
    const geo_definition = GetLabServiceDefinition({
      service_id: 'geo_control_plane'
    });

    base_constructor_params.static_target_list = [
      BuildIngressTarget({
        service_id: 'node_west'
      })
    ];
    base_constructor_params.geo_ingress = {
      enabled: true,
      role: 'regional',
      region_id: 'us-west-1',
      endpoint: {
        host: geo_definition.endpoint.host,
        port: geo_definition.endpoint.port,
        request_path: geo_definition.endpoint.request_path,
        tls_mode: 'required'
      },
      transport_security: BuildLabTlsClientConfig(),
      sync_interval_ms: 300,
      lease_ttl_ms: 1_500,
      region_priority: 2,
      region_latency_ewma_ms: 90,
      region_capacity_score: 1
    };
  } else if (service_id === 'ingress_global') {
    const geo_definition = GetLabServiceDefinition({
      service_id: 'geo_control_plane'
    });

    base_constructor_params.max_attempts = 4;
    base_constructor_params.request_timeout_ms = 1_500;
    base_constructor_params.target_refresh_interval_ms = 150;
    base_constructor_params.geo_ingress = {
      enabled: true,
      role: 'global',
      region_id: 'global',
      endpoint: {
        host: geo_definition.endpoint.host,
        port: geo_definition.endpoint.port,
        request_path: geo_definition.endpoint.request_path,
        tls_mode: 'required'
      },
      transport_security: BuildLabTlsClientConfig(),
      request_timeout_ms: 120,
      max_request_attempts: 1,
      endpoint_cooldown_ms: 80,
      retry_base_delay_ms: 30,
      retry_max_delay_ms: 60,
      sync_interval_ms: 250,
      stale_snapshot_max_age_ms: 700,
      max_cross_region_attempts: 2
    };
  }

  const ingress_service = new ClusterIngressBalancerService(base_constructor_params);
  const address = await ingress_service.start();

  return {
    endpoint: {
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      tls_mode: 'required'
    },
    getSnapshot: (): unknown => {
      return {
        address: ingress_service.getAddress(),
        metrics: ingress_service.getMetrics(),
        snapshot: ingress_service.getSnapshot(),
        recent_events: ingress_service.getRecentEvents({
          limit: 120
        })
      };
    },
    stop: async (): Promise<void> => {
      await ingress_service.stop();
    },
    action_handler_by_name: {}
  };
}

async function StartServiceRuntime(params: {
  service_definition: lab_service_definition_i;
  profile: lab_profile_t;
}): Promise<lab_service_runtime_handle_t> {
  switch (params.service_definition.role) {
    case 'discovery_daemon':
      return await StartDiscoveryDaemonService({
        service_definition: params.service_definition
      });
    case 'control_plane':
      return await StartControlPlaneService({
        service_definition: params.service_definition
      });
    case 'geo_control_plane':
      return await StartGeoControlPlaneService({
        service_definition: params.service_definition
      });
    case 'node_agent':
      return await StartNodeService({
        service_definition: params.service_definition,
        profile: params.profile
      });
    case 'ingress':
      return await StartIngressService({
        service_definition: params.service_definition,
        profile: params.profile
      });
    default:
      throw new Error(`Unsupported service role: ${params.service_definition.role}`);
  }
}

function ReadJsonBody(params: {
  request: http.IncomingMessage;
}): Promise<Record<string, unknown>> {
  return new Promise<Record<string, unknown>>((resolve, reject): void => {
    const chunk_list: Buffer[] = [];

    params.request.on('data', (chunk): void => {
      if (typeof chunk === 'string') {
        chunk_list.push(Buffer.from(chunk));
        return;
      }

      chunk_list.push(chunk as Buffer);
    });

    params.request.on('end', (): void => {
      if (chunk_list.length === 0) {
        resolve({});
        return;
      }

      try {
        const parsed_value = JSON.parse(Buffer.concat(chunk_list).toString('utf8')) as Record<
          string,
          unknown
        >;
        resolve(parsed_value);
      } catch (error) {
        reject(error);
      }
    });

    params.request.on('error', (error): void => {
      reject(error);
    });
  });
}

function WriteJsonResponse(params: {
  response: http.ServerResponse;
  status_code: number;
  body: Record<string, unknown>;
}): void {
  params.response.statusCode = params.status_code;
  params.response.setHeader('content-type', 'application/json');
  params.response.end(JSON.stringify(params.body));
}

async function Run(): Promise<void> {
  const runtime_args = ParseRuntimeArgs({
    argv: process.argv.slice(2)
  });

  const service_definition = GetLabServiceDefinition({
    service_id: runtime_args.service_id
  });

  if (!service_definition.supported_profile_list.includes(runtime_args.profile)) {
    throw new Error(
      `Service "${service_definition.service_id}" is not supported by profile "${runtime_args.profile}".`
    );
  }

  const service_runtime = await StartServiceRuntime({
    service_definition,
    profile: runtime_args.profile
  });

  let shutting_down = false;
  const started_unix_ms = Date.now();

  const admin_server = https.createServer(
    {
      key: BuildLabTlsServerConfig().key_pem,
      cert: BuildLabTlsServerConfig().cert_pem,
      ca: BuildLabTlsServerConfig().ca_pem_list,
      requestCert: BuildLabTlsServerConfig().request_client_cert ?? true,
      rejectUnauthorized:
        BuildLabTlsServerConfig().reject_unauthorized_client_cert ?? true,
      minVersion: BuildLabTlsServerConfig().min_tls_version
    },
    async (request, response): Promise<void> => {
      try {
        if (request.method === 'GET' && request.url === '/health') {
          WriteJsonResponse({
            response,
            status_code: 200,
            body: {
              ok: true,
              service_id: runtime_args.service_id,
              profile: runtime_args.profile,
              started_unix_ms,
              endpoint: service_runtime.endpoint
            }
          });
          return;
        }

        if (request.method === 'GET' && request.url === '/snapshot') {
          WriteJsonResponse({
            response,
            status_code: 200,
            body: {
              ok: true,
              service_id: runtime_args.service_id,
              profile: runtime_args.profile,
              endpoint: service_runtime.endpoint,
              snapshot: service_runtime.getSnapshot()
            }
          });
          return;
        }

        if (request.method === 'POST' && request.url === '/action') {
          const payload = await ReadJsonBody({
            request
          });
          const action_name = String(payload.action_name ?? '');
          const action_handler = service_runtime.action_handler_by_name[action_name];

          if (!action_handler) {
            WriteJsonResponse({
              response,
              status_code: 404,
              body: {
                ok: false,
                error: `Unknown action "${action_name}".`
              }
            });
            return;
          }

          const action_result = await action_handler(payload);
          WriteJsonResponse({
            response,
            status_code: 200,
            body: {
              ok: true,
              action_name,
              result: action_result
            }
          });
          return;
        }

        WriteJsonResponse({
          response,
          status_code: 404,
          body: {
            ok: false,
            error: 'Unknown route.'
          }
        });
      } catch (error) {
        WriteJsonResponse({
          response,
          status_code: 500,
          body: {
            ok: false,
            error: error instanceof Error ? error.message : String(error)
          }
        });
      }
    }
  );

  await new Promise<void>((resolve, reject): void => {
    admin_server.once('error', reject);
    admin_server.listen(service_definition.admin_port, service_definition.endpoint.host, (): void => {
      admin_server.off('error', reject);
      resolve();
    });
  });

  LogServiceMessage({
    service_id: runtime_args.service_id,
    message: 'service_started',
    details: {
      profile: runtime_args.profile,
      endpoint: service_runtime.endpoint,
      admin_port: service_definition.admin_port
    }
  });

  async function Shutdown(): Promise<void> {
    if (shutting_down) {
      return;
    }

    shutting_down = true;
    LogServiceMessage({
      service_id: runtime_args.service_id,
      message: 'service_stopping'
    });

    await new Promise<void>((resolve): void => {
      admin_server.close((): void => {
        resolve();
      });
    }).catch((): void => {
      // ignore close failures.
    });

    await service_runtime.stop().catch((error): void => {
      LogServiceMessage({
        service_id: runtime_args.service_id,
        message: 'service_stop_error',
        details: {
          error: error instanceof Error ? error.message : String(error)
        }
      });
    });

    LogServiceMessage({
      service_id: runtime_args.service_id,
      message: 'service_stopped'
    });

    process.exit(0);
  }

  process.on('SIGTERM', (): void => {
    void Shutdown();
  });

  process.on('SIGINT', (): void => {
    void Shutdown();
  });
}

void Run().catch((error): void => {
  const service_arg = process.argv.find((arg): boolean => {
    return arg.startsWith('--service=');
  });
  const service_id = service_arg ? service_arg.replace('--service=', '') : 'unknown_service';

  process.stderr.write(
    `${BuildLogLine({
      service_id,
      message: 'service_start_failed',
      details: {
        error: error instanceof Error ? error.message : String(error)
      }
    })}\n`
  );
  process.exit(1);
});
