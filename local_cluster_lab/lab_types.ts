export type lab_profile_t = 'minimal' | 'ingress' | 'cluster' | 'geo';

export type lab_service_id_t =
  | 'discovery_daemon'
  | 'control_plane'
  | 'geo_control_plane'
  | 'node_east'
  | 'node_west'
  | 'ingress_single'
  | 'ingress_regional_east'
  | 'ingress_regional_west'
  | 'ingress_global';

export type lab_service_role_t =
  | 'discovery_daemon'
  | 'control_plane'
  | 'geo_control_plane'
  | 'node_agent'
  | 'ingress';

export type lab_service_endpoint_t = {
  host: string;
  port: number;
  request_path: string;
  tls_mode: 'required';
};

export interface lab_service_definition_i {
  service_id: lab_service_id_t;
  role: lab_service_role_t;
  startup_order_index: number;
  supported_profile_list: lab_profile_t[];
  endpoint: lab_service_endpoint_t;
  admin_port: number;
  metadata: Record<string, unknown>;
}

export type lab_timing_config_t = {
  startup_timeout_ms: number;
  startup_poll_interval_ms: number;
  shutdown_timeout_ms: number;
  health_request_timeout_ms: number;
  stale_wait_buffer_ms: number;
};

export type lab_config_t = {
  host: string;
  profile_service_id_list_by_profile: Record<lab_profile_t, lab_service_id_t[]>;
  service_definition_by_id: Record<lab_service_id_t, lab_service_definition_i>;
  timing: lab_timing_config_t;
};

export type lab_service_process_state_t = {
  service_id: lab_service_id_t;
  role: lab_service_role_t;
  profile: lab_profile_t;
  pid: number;
  admin_base_url: string;
  log_file_path: string;
  endpoint: lab_service_endpoint_t;
  started_unix_ms: number;
};

export type lab_state_t = {
  profile: lab_profile_t;
  started_unix_ms: number;
  service_process_state_by_id: Partial<
    Record<lab_service_id_t, lab_service_process_state_t>
  >;
};

export type lab_cli_command_t =
  | 'up'
  | 'down'
  | 'status'
  | 'logs'
  | 'snapshot'
  | 'run-scenario';

export type lab_scenario_name_t =
  | 'happy_path_call'
  | 'ingress_failover'
  | 'discovery_membership_and_expiry'
  | 'control_plane_policy_sync'
  | 'geo_cross_region_failover'
  | 'auth_failure'
  | 'stale_control_fail_closed';

export type lab_cli_args_t = {
  command: lab_cli_command_t;
  profile?: lab_profile_t;
  service_id?: lab_service_id_t;
  follow?: boolean;
  scenario_name?: lab_scenario_name_t;
};
