import type { cluster_authorization_policy_t } from '../clusterauthorization/ClusterAuthorizationPolicyEngine.class';
import type { cluster_call_routing_policy_t } from '../workerprocedurecall/WorkerProcedureCall.class';

export type cluster_control_plane_protocol_supported_version_t = 1;

export type cluster_control_plane_gateway_status_t =
  | 'active'
  | 'degraded'
  | 'draining'
  | 'offline';

export type cluster_control_plane_policy_version_status_t =
  | 'staged'
  | 'active'
  | 'superseded';

export type cluster_control_plane_mutation_status_t =
  | 'pending'
  | 'running'
  | 'completed'
  | 'failed'
  | 'cancelled';

export type cluster_control_plane_gateway_address_t = {
  host: string;
  port: number;
  request_path: string;
  tls_mode: 'disabled' | 'required' | 'terminated_upstream';
};

export type cluster_control_plane_node_reference_t = {
  node_id: string;
  discovery_reference?: {
    discovery_daemon_id?: string;
    lease_revision?: number;
    last_heartbeat_unix_ms?: number;
  };
  health_summary?: {
    worker_count?: number;
    pending_call_count?: number;
    lifecycle_state?: 'stopped' | 'starting' | 'running' | 'stopping';
  };
};

export type cluster_control_plane_gateway_record_t = {
  gateway_id: string;
  node_reference: cluster_control_plane_node_reference_t;
  address: cluster_control_plane_gateway_address_t;
  status: cluster_control_plane_gateway_status_t;
  gateway_version: string;
  registered_unix_ms: number;
  last_heartbeat_unix_ms: number;
  lease_expires_unix_ms: number;
  applied_policy_version_id?: string;
  pending_policy_version_id?: string;
  metadata?: Record<string, unknown>;
};

export type cluster_control_plane_policy_snapshot_t = {
  routing_policy: Partial<cluster_call_routing_policy_t>;
  authorization_policy_list?: cluster_authorization_policy_t[];
  metadata?: Record<string, unknown>;
};

export type cluster_control_plane_policy_version_metadata_t = {
  version_id: string;
  created_unix_ms: number;
  actor_subject: string;
  checksum_sha1: string;
  status: cluster_control_plane_policy_version_status_t;
};

export type cluster_control_plane_policy_version_record_t = {
  metadata: cluster_control_plane_policy_version_metadata_t;
  snapshot: cluster_control_plane_policy_snapshot_t;
};

export type cluster_control_plane_mutation_gateway_progress_t = {
  gateway_id: string;
  status: cluster_control_plane_mutation_status_t;
  updated_unix_ms: number;
  details?: Record<string, unknown>;
};

export type cluster_control_plane_mutation_tracking_record_t = {
  mutation_id: string;
  status: cluster_control_plane_mutation_status_t;
  created_unix_ms: number;
  updated_unix_ms: number;
  dispatch_intent?: Record<string, unknown>;
  gateway_progress_by_id: Record<string, cluster_control_plane_mutation_gateway_progress_t>;
};

export type cluster_control_plane_event_name_t =
  | 'control_plane_gateway_registered'
  | 'control_plane_gateway_expired'
  | 'control_plane_policy_published'
  | 'control_plane_policy_applied'
  | 'control_plane_policy_apply_failed'
  | 'control_plane_sync_degraded'
  | 'control_plane_sync_restored';

export type cluster_control_plane_event_t = {
  event_id: string;
  event_name: cluster_control_plane_event_name_t;
  timestamp_unix_ms: number;
  gateway_id?: string;
  policy_version_id?: string;
  mutation_id?: string;
  details?: Record<string, unknown>;
};

export type cluster_control_plane_topology_snapshot_t = {
  captured_at_unix_ms: number;
  gateway_list: cluster_control_plane_gateway_record_t[];
  active_policy_version_id: string | null;
  service_generation: number;
};

export type cluster_control_plane_service_status_snapshot_t = {
  captured_at_unix_ms: number;
  service_generation: number;
  active_gateway_count: number;
  gateway_count: number;
  active_policy_version_id: string | null;
  policy_version_count: number;
  mutation_tracking_count: number;
};

export type cluster_control_plane_metrics_t = {
  control_plane_gateway_registered_total: number;
  control_plane_gateway_active_count: number;
  control_plane_policy_version_total: number;
  control_plane_policy_apply_failures_total: number;
  control_plane_sync_lag_ms: number;
  control_plane_request_failure_total: number;
  control_plane_request_failure_count_by_reason: Record<string, number>;
};

export type cluster_control_plane_protocol_request_message_type_t =
  | 'cluster_control_plane_gateway_register'
  | 'cluster_control_plane_gateway_heartbeat'
  | 'cluster_control_plane_gateway_deregister'
  | 'cluster_control_plane_get_topology_snapshot'
  | 'cluster_control_plane_get_policy_snapshot'
  | 'cluster_control_plane_update_policy_snapshot'
  | 'cluster_control_plane_subscribe_updates'
  | 'cluster_control_plane_config_version_announce'
  | 'cluster_control_plane_config_version_ack'
  | 'cluster_control_plane_get_service_status'
  | 'cluster_control_plane_mutation_intent_update'
  | 'cluster_control_plane_get_mutation_status';

export type cluster_control_plane_protocol_response_message_type_t =
  | 'cluster_control_plane_response_success'
  | 'cluster_control_plane_response_error';

export type cluster_control_plane_protocol_message_type_t =
  | cluster_control_plane_protocol_request_message_type_t
  | cluster_control_plane_protocol_response_message_type_t;

export type cluster_control_plane_protocol_error_code_t =
  | 'CONTROL_PLANE_VALIDATION_FAILED'
  | 'CONTROL_PLANE_AUTH_FAILED'
  | 'CONTROL_PLANE_FORBIDDEN'
  | 'CONTROL_PLANE_CONFLICT'
  | 'CONTROL_PLANE_UNAVAILABLE'
  | 'CONTROL_PLANE_TIMEOUT'
  | 'CONTROL_PLANE_INTERNAL';

export type cluster_control_plane_protocol_error_t = {
  code: cluster_control_plane_protocol_error_code_t;
  message: string;
  retryable: boolean;
  details?: Record<string, unknown>;
};

export interface cluster_control_plane_protocol_envelope_i {
  protocol_version: cluster_control_plane_protocol_supported_version_t;
  message_type: cluster_control_plane_protocol_message_type_t;
  timestamp_unix_ms: number;
  request_id: string;
  trace_id?: string;
}

export interface cluster_control_plane_gateway_register_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_gateway_register';
  gateway_id: string;
  node_reference: cluster_control_plane_node_reference_t;
  address: cluster_control_plane_gateway_address_t;
  status: Exclude<cluster_control_plane_gateway_status_t, 'offline'>;
  gateway_version: string;
  lease_ttl_ms?: number;
  metadata?: Record<string, unknown>;
}

export interface cluster_control_plane_gateway_heartbeat_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_gateway_heartbeat';
  gateway_id: string;
  node_reference?: cluster_control_plane_node_reference_t;
  status?: cluster_control_plane_gateway_status_t;
  gateway_version?: string;
  lease_ttl_ms?: number;
  metadata?: Record<string, unknown>;
}

export interface cluster_control_plane_gateway_deregister_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_gateway_deregister';
  gateway_id: string;
  reason?: string;
}

export interface cluster_control_plane_get_topology_snapshot_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_get_topology_snapshot';
}

export interface cluster_control_plane_get_policy_snapshot_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_get_policy_snapshot';
}

export interface cluster_control_plane_update_policy_snapshot_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_update_policy_snapshot';
  actor_subject: string;
  expected_active_policy_version_id?: string;
  activate_immediately?: boolean;
  requested_version_id?: string;
  policy_snapshot: cluster_control_plane_policy_snapshot_t;
}

export interface cluster_control_plane_subscribe_updates_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_subscribe_updates';
  gateway_id?: string;
  known_policy_version_id?: string;
  known_service_generation?: number;
  include_topology?: boolean;
}

export interface cluster_control_plane_config_version_announce_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_config_version_announce';
  gateway_id: string;
  policy_version_id: string;
}

export interface cluster_control_plane_config_version_ack_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_config_version_ack';
  gateway_id: string;
  policy_version_id: string;
  applied: boolean;
  details?: Record<string, unknown>;
}

export interface cluster_control_plane_get_service_status_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_get_service_status';
  include_events_limit?: number;
}

export interface cluster_control_plane_mutation_intent_update_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_mutation_intent_update';
  mutation_id: string;
  status: cluster_control_plane_mutation_status_t;
  gateway_id?: string;
  dispatch_intent?: Record<string, unknown>;
  details?: Record<string, unknown>;
}

export interface cluster_control_plane_get_mutation_status_request_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_get_mutation_status';
  mutation_id: string;
}

export type cluster_control_plane_request_message_t =
  | cluster_control_plane_gateway_register_request_message_i
  | cluster_control_plane_gateway_heartbeat_request_message_i
  | cluster_control_plane_gateway_deregister_request_message_i
  | cluster_control_plane_get_topology_snapshot_request_message_i
  | cluster_control_plane_get_policy_snapshot_request_message_i
  | cluster_control_plane_update_policy_snapshot_request_message_i
  | cluster_control_plane_subscribe_updates_request_message_i
  | cluster_control_plane_config_version_announce_request_message_i
  | cluster_control_plane_config_version_ack_request_message_i
  | cluster_control_plane_get_service_status_request_message_i
  | cluster_control_plane_mutation_intent_update_request_message_i
  | cluster_control_plane_get_mutation_status_request_message_i;

export type cluster_control_plane_response_data_t =
  | {
      gateway_record: cluster_control_plane_gateway_record_t;
      topology_snapshot: cluster_control_plane_topology_snapshot_t;
    }
  | {
      deregistered: boolean;
      topology_snapshot: cluster_control_plane_topology_snapshot_t;
    }
  | {
      topology_snapshot: cluster_control_plane_topology_snapshot_t;
    }
  | {
      active_policy_version_id: string | null;
      active_policy_version: cluster_control_plane_policy_version_record_t | null;
    }
  | {
      policy_version: cluster_control_plane_policy_version_record_t;
      active_policy_version_id: string | null;
    }
  | {
      changed: boolean;
      topology_snapshot?: cluster_control_plane_topology_snapshot_t;
      active_policy_version_id: string | null;
      active_policy_version: cluster_control_plane_policy_version_record_t | null;
      service_generation: number;
    }
  | {
      acknowledged: boolean;
      gateway_record: cluster_control_plane_gateway_record_t | null;
    }
  | {
      service_status: cluster_control_plane_service_status_snapshot_t;
      metrics: cluster_control_plane_metrics_t;
      event_list?: cluster_control_plane_event_t[];
    }
  | {
      mutation_tracking_record: cluster_control_plane_mutation_tracking_record_t;
    }
  | {
      mutation_tracking_record: cluster_control_plane_mutation_tracking_record_t | null;
    };

export interface cluster_control_plane_response_success_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_response_success';
  operation_message_type: cluster_control_plane_protocol_request_message_type_t;
  data: cluster_control_plane_response_data_t;
}

export interface cluster_control_plane_response_error_message_i
  extends cluster_control_plane_protocol_envelope_i {
  message_type: 'cluster_control_plane_response_error';
  operation_message_type?: cluster_control_plane_protocol_request_message_type_t;
  error: cluster_control_plane_protocol_error_t;
}

export type cluster_control_plane_response_message_t =
  | cluster_control_plane_response_success_message_i
  | cluster_control_plane_response_error_message_i;

export type cluster_control_plane_validation_result_t<value_t> =
  | {
      ok: true;
      value: value_t;
    }
  | {
      ok: false;
      error: cluster_control_plane_protocol_error_t;
    };
