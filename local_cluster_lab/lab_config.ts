import type {
  lab_config_t,
  lab_profile_t,
  lab_service_definition_i,
  lab_service_id_t
} from './lab_types';

function ReadEnvInteger(params: {
  name: string;
  default_value: number;
  minimum_value?: number;
}): number {
  const raw_value = process.env[params.name];
  if (typeof raw_value !== 'string' || raw_value.length === 0) {
    return params.default_value;
  }

  const parsed_value = Number.parseInt(raw_value, 10);
  if (!Number.isFinite(parsed_value)) {
    return params.default_value;
  }

  if (
    typeof params.minimum_value === 'number' &&
    parsed_value < params.minimum_value
  ) {
    return params.default_value;
  }

  return parsed_value;
}

function ApplyPortOffset(params: {
  base_port: number;
  port_offset: number;
}): number {
  return params.base_port + params.port_offset;
}

function BuildServiceDefinition(params: {
  service_id: lab_service_id_t;
  role: lab_service_definition_i['role'];
  startup_order_index: number;
  supported_profile_list: lab_profile_t[];
  port: number;
  request_path: string;
  admin_port: number;
  metadata?: Record<string, unknown>;
}): lab_service_definition_i {
  return {
    service_id: params.service_id,
    role: params.role,
    startup_order_index: params.startup_order_index,
    supported_profile_list: [...params.supported_profile_list],
    endpoint: {
      host: 'localhost',
      port: params.port,
      request_path: params.request_path,
      tls_mode: 'required'
    },
    admin_port: params.admin_port,
    metadata: { ...(params.metadata ?? {}) }
  };
}

function BuildLabConfig(): lab_config_t {
  const port_offset = ReadEnvInteger({
    name: 'LOCAL_CLUSTER_LAB_PORT_OFFSET',
    default_value: 0,
    minimum_value: 0
  });

  const service_definition_by_id: Record<lab_service_id_t, lab_service_definition_i> = {
    discovery_daemon: BuildServiceDefinition({
      service_id: 'discovery_daemon',
      role: 'discovery_daemon',
      startup_order_index: 10,
      supported_profile_list: ['cluster', 'geo'],
      port: ApplyPortOffset({ base_port: 7103, port_offset }),
      request_path: '/wpc/cluster/discovery',
      admin_port: ApplyPortOffset({ base_port: 8103, port_offset })
    }),
    control_plane: BuildServiceDefinition({
      service_id: 'control_plane',
      role: 'control_plane',
      startup_order_index: 20,
      supported_profile_list: ['cluster', 'geo'],
      port: ApplyPortOffset({ base_port: 7101, port_offset }),
      request_path: '/wpc/cluster/control-plane',
      admin_port: ApplyPortOffset({ base_port: 8101, port_offset })
    }),
    geo_control_plane: BuildServiceDefinition({
      service_id: 'geo_control_plane',
      role: 'geo_control_plane',
      startup_order_index: 30,
      supported_profile_list: ['geo'],
      port: ApplyPortOffset({ base_port: 7102, port_offset }),
      request_path: '/wpc/cluster/geo-ingress',
      admin_port: ApplyPortOffset({ base_port: 8102, port_offset })
    }),
    node_east: BuildServiceDefinition({
      service_id: 'node_east',
      role: 'node_agent',
      startup_order_index: 40,
      supported_profile_list: ['minimal', 'ingress', 'cluster', 'geo'],
      port: ApplyPortOffset({ base_port: 7201, port_offset }),
      request_path: '/wpc/cluster/protocol',
      admin_port: ApplyPortOffset({ base_port: 8201, port_offset }),
      metadata: {
        region_id: 'us-east-1',
        zone: 'east-a'
      }
    }),
    node_west: BuildServiceDefinition({
      service_id: 'node_west',
      role: 'node_agent',
      startup_order_index: 50,
      supported_profile_list: ['cluster', 'geo'],
      port: ApplyPortOffset({ base_port: 7202, port_offset }),
      request_path: '/wpc/cluster/protocol',
      admin_port: ApplyPortOffset({ base_port: 8202, port_offset }),
      metadata: {
        region_id: 'us-west-1',
        zone: 'west-a'
      }
    }),
    ingress_single: BuildServiceDefinition({
      service_id: 'ingress_single',
      role: 'ingress',
      startup_order_index: 60,
      supported_profile_list: ['ingress', 'cluster'],
      port: ApplyPortOffset({ base_port: 7301, port_offset }),
      request_path: '/wpc/cluster/ingress',
      admin_port: ApplyPortOffset({ base_port: 8301, port_offset })
    }),
    ingress_regional_east: BuildServiceDefinition({
      service_id: 'ingress_regional_east',
      role: 'ingress',
      startup_order_index: 70,
      supported_profile_list: ['geo'],
      port: ApplyPortOffset({ base_port: 7311, port_offset }),
      request_path: '/wpc/cluster/ingress',
      admin_port: ApplyPortOffset({ base_port: 8311, port_offset }),
      metadata: {
        region_id: 'us-east-1'
      }
    }),
    ingress_regional_west: BuildServiceDefinition({
      service_id: 'ingress_regional_west',
      role: 'ingress',
      startup_order_index: 80,
      supported_profile_list: ['geo'],
      port: ApplyPortOffset({ base_port: 7312, port_offset }),
      request_path: '/wpc/cluster/ingress',
      admin_port: ApplyPortOffset({ base_port: 8312, port_offset }),
      metadata: {
        region_id: 'us-west-1'
      }
    }),
    ingress_global: BuildServiceDefinition({
      service_id: 'ingress_global',
      role: 'ingress',
      startup_order_index: 90,
      supported_profile_list: ['geo'],
      port: ApplyPortOffset({ base_port: 7320, port_offset }),
      request_path: '/wpc/cluster/ingress',
      admin_port: ApplyPortOffset({ base_port: 8320, port_offset }),
      metadata: {
        region_id: 'global'
      }
    })
  };

  return {
    host: 'localhost',
    profile_service_id_list_by_profile: {
      minimal: ['node_east'],
      ingress: ['node_east', 'ingress_single'],
      cluster: [
        'discovery_daemon',
        'control_plane',
        'node_east',
        'node_west',
        'ingress_single'
      ],
      geo: [
        'discovery_daemon',
        'control_plane',
        'geo_control_plane',
        'node_east',
        'node_west',
        'ingress_regional_east',
        'ingress_regional_west',
        'ingress_global'
      ]
    },
    service_definition_by_id,
    timing: {
      startup_timeout_ms: ReadEnvInteger({
        name: 'LOCAL_CLUSTER_LAB_STARTUP_TIMEOUT_MS',
        default_value: 10_000,
        minimum_value: 1_000
      }),
      startup_poll_interval_ms: ReadEnvInteger({
        name: 'LOCAL_CLUSTER_LAB_STARTUP_POLL_INTERVAL_MS',
        default_value: 120,
        minimum_value: 20
      }),
      shutdown_timeout_ms: ReadEnvInteger({
        name: 'LOCAL_CLUSTER_LAB_SHUTDOWN_TIMEOUT_MS',
        default_value: 6_000,
        minimum_value: 500
      }),
      health_request_timeout_ms: ReadEnvInteger({
        name: 'LOCAL_CLUSTER_LAB_HEALTH_TIMEOUT_MS',
        default_value: 1_500,
        minimum_value: 100
      }),
      stale_wait_buffer_ms: ReadEnvInteger({
        name: 'LOCAL_CLUSTER_LAB_STALE_WAIT_BUFFER_MS',
        default_value: 800,
        minimum_value: 100
      })
    }
  };
}

export const lab_config = BuildLabConfig();

export function GetLabServiceDefinition(params: {
  service_id: lab_service_id_t;
}): lab_service_definition_i {
  const service_definition = lab_config.service_definition_by_id[params.service_id];
  if (!service_definition) {
    throw new Error(`Unknown lab service_id: ${params.service_id}`);
  }

  return service_definition;
}

export function GetLabProfileServiceIdList(params: {
  profile: lab_profile_t;
}): lab_service_id_t[] {
  const service_id_list = lab_config.profile_service_id_list_by_profile[params.profile];
  if (!service_id_list) {
    throw new Error(`Unknown lab profile: ${params.profile}`);
  }

  const sorted_list = [...service_id_list].sort((left_service_id, right_service_id): number => {
    const left_order =
      lab_config.service_definition_by_id[left_service_id].startup_order_index;
    const right_order =
      lab_config.service_definition_by_id[right_service_id].startup_order_index;
    return left_order - right_order;
  });

  return sorted_list;
}
