export type cluster_geo_ingress_protocol_supported_version_t = 1;

export type cluster_geo_ingress_region_status_t =
  | 'active'
  | 'degraded'
  | 'draining'
  | 'offline';

export type cluster_geo_ingress_instance_health_status_t =
  | 'ready'
  | 'overloaded'
  | 'degraded'
  | 'offline';

export type cluster_geo_ingress_policy_mode_t =
  | 'latency_aware'
  | 'capacity_aware'
  | 'affinity';

export type cluster_geo_ingress_stickiness_scope_t =
  | 'none'
  | 'session'
  | 'tenant';

export type cluster_geo_ingress_policy_version_status_t =
  | 'staged'
  | 'active'
  | 'superseded';

export type cluster_geo_ingress_protocol_request_message_type_t =
  | 'cluster_geo_ingress_ingress_register'
  | 'cluster_geo_ingress_ingress_heartbeat'
  | 'cluster_geo_ingress_ingress_deregister'
  | 'cluster_geo_ingress_publish_global_routing_policy'
  | 'cluster_geo_ingress_get_global_routing_policy'
  | 'cluster_geo_ingress_get_global_topology_snapshot'
  | 'cluster_geo_ingress_report_metrics_summary'
  | 'cluster_geo_ingress_subscribe_updates'
  | 'cluster_geo_ingress_get_service_status';

export type cluster_geo_ingress_protocol_response_message_type_t =
  | 'cluster_geo_ingress_response_success'
  | 'cluster_geo_ingress_response_error';

export type cluster_geo_ingress_protocol_message_type_t =
  | cluster_geo_ingress_protocol_request_message_type_t
  | cluster_geo_ingress_protocol_response_message_type_t;

export type cluster_geo_ingress_protocol_error_code_t =
  | 'GEO_INGRESS_VALIDATION_FAILED'
  | 'GEO_INGRESS_AUTH_FAILED'
  | 'GEO_INGRESS_FORBIDDEN'
  | 'GEO_INGRESS_CONFLICT'
  | 'GEO_INGRESS_NO_ELIGIBLE_REGION'
  | 'GEO_INGRESS_UNAVAILABLE'
  | 'GEO_INGRESS_TIMEOUT'
  | 'GEO_INGRESS_INTERNAL';

export type cluster_geo_ingress_protocol_error_t = {
  code: cluster_geo_ingress_protocol_error_code_t;
  message: string;
  retryable: boolean;
  details?: Record<string, unknown>;
};

export type cluster_geo_ingress_validation_result_t<value_t> =
  | {
      ok: true;
      value: value_t;
    }
  | {
      ok: false;
      error: cluster_geo_ingress_protocol_error_t;
    };

export type cluster_geo_ingress_endpoint_t = {
  host: string;
  port: number;
  request_path: string;
  tls_mode: 'disabled' | 'required' | 'terminated_upstream';
};

export type cluster_geo_ingress_region_record_t = {
  region_id: string;
  status: cluster_geo_ingress_region_status_t;
  priority: number;
  latency_slo_ms: number;
  capacity_score: number;
  latency_ewma_ms: number;
  updated_unix_ms: number;
};

export type cluster_geo_ingress_instance_record_t = {
  ingress_id: string;
  region_id: string;
  endpoint: cluster_geo_ingress_endpoint_t;
  health_status: cluster_geo_ingress_instance_health_status_t;
  inflight_calls: number;
  pending_calls: number;
  success_rate_1m: number;
  timeout_rate_1m: number;
  ewma_latency_ms: number;
  ingress_version: string;
  policy_version_id: string | null;
  lease_expires_unix_ms: number;
  last_heartbeat_unix_ms: number;
  registered_unix_ms: number;
  metadata?: Record<string, unknown>;
};

export type cluster_geo_ingress_global_routing_policy_snapshot_t = {
  default_mode: cluster_geo_ingress_policy_mode_t;
  retry_within_region_first: boolean;
  max_cross_region_attempts: number;
  stickiness_scope: cluster_geo_ingress_stickiness_scope_t;
  region_priority_list?: string[];
  region_allow_by_tenant?: Record<string, string[]>;
  region_deny_by_tenant?: Record<string, string[]>;
  environment_region_allow_list?: Record<string, string[]>;
  metadata?: Record<string, unknown>;
};

export type cluster_geo_ingress_policy_version_record_t = {
  version_id: string;
  created_unix_ms: number;
  actor_subject: string;
  checksum_sha1: string;
  status: cluster_geo_ingress_policy_version_status_t;
  snapshot: cluster_geo_ingress_global_routing_policy_snapshot_t;
};

export type cluster_geo_ingress_region_failover_state_t = {
  active_region_id_list: string[];
  degraded_region_id_list: string[];
  failover_reason: string | null;
  last_transition_unix_ms: number | null;
};

export type cluster_geo_ingress_topology_snapshot_t = {
  captured_at_unix_ms: number;
  service_generation: number;
  active_policy_version_id: string | null;
  region_record_list: cluster_geo_ingress_region_record_t[];
  ingress_instance_record_list: cluster_geo_ingress_instance_record_t[];
  region_failover_state: cluster_geo_ingress_region_failover_state_t;
};

export type cluster_geo_ingress_metrics_t = {
  geo_ingress_requests_total: number;
  geo_ingress_success_total: number;
  geo_ingress_failure_total: number;
  geo_ingress_region_failover_total: number;
  geo_ingress_region_selection_count_by_reason: Record<string, number>;
  geo_ingress_control_stale_total: number;
  geo_ingress_dispatch_latency_ms: {
    count: number;
    total: number;
    min: number;
    max: number;
    avg: number;
  };
  geo_ingress_request_failure_count_by_reason: Record<string, number>;
};

export type cluster_geo_ingress_event_name_t =
  | 'geo_ingress_request_received'
  | 'geo_ingress_region_selected'
  | 'geo_ingress_failover_attempted'
  | 'geo_ingress_region_degraded'
  | 'geo_ingress_region_restored'
  | 'geo_ingress_request_completed';

export type cluster_geo_ingress_event_t = {
  event_id: string;
  event_name: cluster_geo_ingress_event_name_t;
  timestamp_unix_ms: number;
  ingress_id?: string;
  region_id?: string;
  request_id?: string;
  trace_id?: string;
  details?: Record<string, unknown>;
};

export interface cluster_geo_ingress_protocol_envelope_i {
  protocol_version: cluster_geo_ingress_protocol_supported_version_t;
  message_type: cluster_geo_ingress_protocol_message_type_t;
  timestamp_unix_ms: number;
  request_id: string;
  trace_id?: string;
}

export interface cluster_geo_ingress_register_request_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_ingress_register';
  ingress_id: string;
  region_id: string;
  endpoint: cluster_geo_ingress_endpoint_t;
  health_status: Exclude<cluster_geo_ingress_instance_health_status_t, 'offline'>;
  ingress_version: string;
  lease_ttl_ms?: number;
  policy_version_id?: string;
  region_metadata?: {
    priority?: number;
    latency_slo_ms?: number;
    capacity_score?: number;
    latency_ewma_ms?: number;
    status?: cluster_geo_ingress_region_status_t;
  };
  metrics_summary?: {
    inflight_calls?: number;
    pending_calls?: number;
    success_rate_1m?: number;
    timeout_rate_1m?: number;
    ewma_latency_ms?: number;
  };
  metadata?: Record<string, unknown>;
}

export interface cluster_geo_ingress_heartbeat_request_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_ingress_heartbeat';
  ingress_id: string;
  region_id?: string;
  health_status?: cluster_geo_ingress_instance_health_status_t;
  ingress_version?: string;
  lease_ttl_ms?: number;
  policy_version_id?: string;
  region_metadata?: {
    priority?: number;
    latency_slo_ms?: number;
    capacity_score?: number;
    latency_ewma_ms?: number;
    status?: cluster_geo_ingress_region_status_t;
  };
  metrics_summary?: {
    inflight_calls?: number;
    pending_calls?: number;
    success_rate_1m?: number;
    timeout_rate_1m?: number;
    ewma_latency_ms?: number;
  };
  metadata?: Record<string, unknown>;
}

export interface cluster_geo_ingress_deregister_request_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_ingress_deregister';
  ingress_id: string;
  reason?: string;
}

export interface cluster_geo_ingress_publish_global_routing_policy_request_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_publish_global_routing_policy';
  actor_subject: string;
  policy_snapshot: cluster_geo_ingress_global_routing_policy_snapshot_t;
  requested_version_id?: string;
  expected_active_policy_version_id?: string;
  activate_immediately?: boolean;
}

export interface cluster_geo_ingress_get_global_routing_policy_request_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_get_global_routing_policy';
}

export interface cluster_geo_ingress_get_global_topology_snapshot_request_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_get_global_topology_snapshot';
}

export interface cluster_geo_ingress_report_metrics_summary_request_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_report_metrics_summary';
  ingress_id: string;
  metrics_summary: {
    inflight_calls?: number;
    pending_calls?: number;
    success_rate_1m?: number;
    timeout_rate_1m?: number;
    ewma_latency_ms?: number;
  };
}

export interface cluster_geo_ingress_subscribe_updates_request_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_subscribe_updates';
  ingress_id?: string;
  known_service_generation?: number;
  known_policy_version_id?: string;
  include_topology?: boolean;
}

export interface cluster_geo_ingress_get_service_status_request_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_get_service_status';
  include_events_limit?: number;
}

export type cluster_geo_ingress_request_message_t =
  | cluster_geo_ingress_register_request_message_i
  | cluster_geo_ingress_heartbeat_request_message_i
  | cluster_geo_ingress_deregister_request_message_i
  | cluster_geo_ingress_publish_global_routing_policy_request_message_i
  | cluster_geo_ingress_get_global_routing_policy_request_message_i
  | cluster_geo_ingress_get_global_topology_snapshot_request_message_i
  | cluster_geo_ingress_report_metrics_summary_request_message_i
  | cluster_geo_ingress_subscribe_updates_request_message_i
  | cluster_geo_ingress_get_service_status_request_message_i;

export type cluster_geo_ingress_response_data_t =
  | {
      ingress_instance_record: cluster_geo_ingress_instance_record_t;
      topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
    }
  | {
      deregistered: boolean;
      topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
    }
  | {
      topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
    }
  | {
      active_policy_version_id: string | null;
      active_policy_version_record: cluster_geo_ingress_policy_version_record_t | null;
    }
  | {
      policy_version_record: cluster_geo_ingress_policy_version_record_t;
      active_policy_version_id: string | null;
    }
  | {
      changed: boolean;
      topology_snapshot?: cluster_geo_ingress_topology_snapshot_t;
      active_policy_version_id: string | null;
      active_policy_version_record: cluster_geo_ingress_policy_version_record_t | null;
      service_generation: number;
    }
  | {
      acknowledged: boolean;
      ingress_instance_record: cluster_geo_ingress_instance_record_t | null;
    }
  | {
      service_generation: number;
      active_policy_version_id: string | null;
      region_count: number;
      ingress_instance_count: number;
      metrics: cluster_geo_ingress_metrics_t;
      event_list?: cluster_geo_ingress_event_t[];
    };

export interface cluster_geo_ingress_response_success_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_response_success';
  operation_message_type: cluster_geo_ingress_protocol_request_message_type_t;
  data: cluster_geo_ingress_response_data_t;
}

export interface cluster_geo_ingress_response_error_message_i
  extends cluster_geo_ingress_protocol_envelope_i {
  message_type: 'cluster_geo_ingress_response_error';
  operation_message_type?: cluster_geo_ingress_protocol_request_message_type_t;
  error: cluster_geo_ingress_protocol_error_t;
}

export type cluster_geo_ingress_response_message_t =
  | cluster_geo_ingress_response_success_message_i
  | cluster_geo_ingress_response_error_message_i;
