import type {
  cluster_service_discovery_event_name_t,
  cluster_service_discovery_node_capability_t,
  cluster_service_discovery_node_identity_t,
  cluster_service_discovery_node_metrics_t,
  cluster_service_discovery_node_record_t,
  cluster_service_discovery_node_status_t,
  cluster_service_discovery_metrics_t,
  cluster_service_discovery_event_t
} from './ClusterServiceDiscoveryStore.class';

export type cluster_service_discovery_protocol_supported_version_t = 1;

export type cluster_service_discovery_consistency_mode_t =
  | 'leader_linearizable'
  | 'stale_ok';

export type cluster_service_discovery_consensus_role_t =
  | 'leader'
  | 'follower'
  | 'candidate';

export type cluster_service_discovery_redirect_endpoint_t = {
  daemon_id?: string;
  host: string;
  port: number;
  request_path: string;
  tls_mode: 'required';
};

export type cluster_service_discovery_protocol_request_message_type_t =
  | 'cluster_service_discovery_register_node'
  | 'cluster_service_discovery_heartbeat_node'
  | 'cluster_service_discovery_update_capability'
  | 'cluster_service_discovery_remove_node'
  | 'cluster_service_discovery_list_nodes'
  | 'cluster_service_discovery_query_nodes'
  | 'cluster_service_discovery_get_metrics'
  | 'cluster_service_discovery_get_events'
  | 'cluster_service_discovery_expire_stale_nodes'
  | 'cluster_service_discovery_consensus_request_vote'
  | 'cluster_service_discovery_consensus_append_entries'
  | 'cluster_service_discovery_get_ha_status';

export type cluster_service_discovery_protocol_response_message_type_t =
  | 'cluster_service_discovery_response_success'
  | 'cluster_service_discovery_response_error';

export type cluster_service_discovery_protocol_message_type_t =
  | cluster_service_discovery_protocol_request_message_type_t
  | cluster_service_discovery_protocol_response_message_type_t;

export type cluster_service_discovery_protocol_error_code_t =
  | 'DISCOVERY_VALIDATION_FAILED'
  | 'DISCOVERY_AUTH_FAILED'
  | 'DISCOVERY_STATE_FAILED'
  | 'DISCOVERY_INTERNAL'
  | 'DISCOVERY_UNAVAILABLE'
  | 'DISCOVERY_NOT_LEADER'
  | 'DISCOVERY_QUORUM_UNAVAILABLE'
  | 'DISCOVERY_CONFLICT_STALE_TERM'
  | 'DISCOVERY_REPLICATION_TIMEOUT';

export type cluster_service_discovery_ha_response_metadata_t = {
  term?: number;
  leader_id?: string;
  commit_index?: number;
  daemon_id?: string;
  role?: cluster_service_discovery_consensus_role_t;
  redirect_endpoint?: cluster_service_discovery_redirect_endpoint_t;
};

export interface cluster_service_discovery_protocol_envelope_i {
  protocol_version: cluster_service_discovery_protocol_supported_version_t;
  message_type: cluster_service_discovery_protocol_message_type_t;
  timestamp_unix_ms: number;
  request_id: string;
  trace_id?: string;
  idempotency_key?: string;
  ha_metadata?: cluster_service_discovery_ha_response_metadata_t;
}

export interface cluster_service_discovery_register_node_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_register_node';
  node_identity: cluster_service_discovery_node_identity_t;
  status: Exclude<cluster_service_discovery_node_status_t, 'expired'>;
  metrics?: Partial<cluster_service_discovery_node_metrics_t>;
  capability_list?: cluster_service_discovery_node_capability_t[];
  lease_ttl_ms?: number;
}

export interface cluster_service_discovery_heartbeat_node_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_heartbeat_node';
  node_id: string;
  status: Exclude<cluster_service_discovery_node_status_t, 'expired'>;
  metrics?: Partial<cluster_service_discovery_node_metrics_t>;
  lease_ttl_ms?: number;
  lease_revision?: number;
}

export interface cluster_service_discovery_update_capability_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_update_capability';
  node_id: string;
  capability_list: cluster_service_discovery_node_capability_t[];
}

export interface cluster_service_discovery_remove_node_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_remove_node';
  node_id: string;
  reason?: string;
}

export interface cluster_service_discovery_list_nodes_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_list_nodes';
  include_expired?: boolean;
  consistency_mode?: cluster_service_discovery_consistency_mode_t;
}

export interface cluster_service_discovery_query_nodes_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_query_nodes';
  include_expired?: boolean;
  status_list?: cluster_service_discovery_node_status_t[];
  label_match?: Record<string, string>;
  zone?: string;
  capability_function_name?: string;
  capability_function_hash_sha1?: string;
  consistency_mode?: cluster_service_discovery_consistency_mode_t;
}

export interface cluster_service_discovery_get_metrics_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_get_metrics';
  consistency_mode?: cluster_service_discovery_consistency_mode_t;
}

export interface cluster_service_discovery_get_events_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_get_events';
  limit?: number;
  event_name?: cluster_service_discovery_event_name_t;
  node_id?: string;
  consistency_mode?: cluster_service_discovery_consistency_mode_t;
}

export interface cluster_service_discovery_expire_stale_nodes_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_expire_stale_nodes';
  now_unix_ms?: number;
}

export type cluster_service_discovery_write_request_message_t =
  | cluster_service_discovery_register_node_request_message_i
  | cluster_service_discovery_heartbeat_node_request_message_i
  | cluster_service_discovery_update_capability_request_message_i
  | cluster_service_discovery_remove_node_request_message_i
  | cluster_service_discovery_expire_stale_nodes_request_message_i;

export type cluster_service_discovery_consensus_log_entry_t = {
  index: number;
  term: number;
  request_id: string;
  request_message: cluster_service_discovery_write_request_message_t;
};

export interface cluster_service_discovery_consensus_request_vote_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_consensus_request_vote';
  candidate_id: string;
  term: number;
  last_log_index: number;
  last_log_term: number;
}

export interface cluster_service_discovery_consensus_append_entries_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_consensus_append_entries';
  leader_id: string;
  term: number;
  prev_log_index: number;
  prev_log_term: number;
  entry_list: cluster_service_discovery_consensus_log_entry_t[];
  leader_commit_index: number;
}

export interface cluster_service_discovery_get_ha_status_request_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_get_ha_status';
}

export type cluster_service_discovery_request_message_t =
  | cluster_service_discovery_register_node_request_message_i
  | cluster_service_discovery_heartbeat_node_request_message_i
  | cluster_service_discovery_update_capability_request_message_i
  | cluster_service_discovery_remove_node_request_message_i
  | cluster_service_discovery_list_nodes_request_message_i
  | cluster_service_discovery_query_nodes_request_message_i
  | cluster_service_discovery_get_metrics_request_message_i
  | cluster_service_discovery_get_events_request_message_i
  | cluster_service_discovery_expire_stale_nodes_request_message_i
  | cluster_service_discovery_consensus_request_vote_request_message_i
  | cluster_service_discovery_consensus_append_entries_request_message_i
  | cluster_service_discovery_get_ha_status_request_message_i;

export type cluster_service_discovery_ha_status_data_t = {
  daemon_id: string;
  role: cluster_service_discovery_consensus_role_t;
  term: number;
  leader_id: string | null;
  commit_index: number;
  applied_index: number;
  peer_daemon_id_list: string[];
  quorum_size: number;
  known_endpoint_by_daemon_id: Record<string, cluster_service_discovery_redirect_endpoint_t>;
};

export type cluster_service_discovery_response_data_t =
  | {
      node_record: cluster_service_discovery_node_record_t;
      lease_revision?: number;
    }
  | {
      removed: boolean;
    }
  | {
      node_list: cluster_service_discovery_node_record_t[];
    }
  | {
      metrics: cluster_service_discovery_metrics_t;
    }
  | {
      event_list: cluster_service_discovery_event_t[];
    }
  | {
      expired_node_id_list: string[];
    }
  | {
      vote_granted: boolean;
      term: number;
      daemon_id: string;
    }
  | {
      success: boolean;
      term: number;
      match_index: number;
      daemon_id: string;
      conflict_index?: number;
    }
  | {
      ha_status: cluster_service_discovery_ha_status_data_t;
    };

export type cluster_service_discovery_protocol_error_t = {
  code: cluster_service_discovery_protocol_error_code_t;
  message: string;
  retryable: boolean;
  details?: Record<string, unknown>;
};

export interface cluster_service_discovery_response_success_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_response_success';
  operation_message_type: cluster_service_discovery_protocol_request_message_type_t;
  data: cluster_service_discovery_response_data_t;
}

export interface cluster_service_discovery_response_error_message_i
  extends cluster_service_discovery_protocol_envelope_i {
  message_type: 'cluster_service_discovery_response_error';
  operation_message_type?: cluster_service_discovery_protocol_request_message_type_t;
  error: cluster_service_discovery_protocol_error_t;
}

export type cluster_service_discovery_response_message_t =
  | cluster_service_discovery_response_success_message_i
  | cluster_service_discovery_response_error_message_i;

export type cluster_service_discovery_validation_result_t<value_t> =
  | {
      ok: true;
      value: value_t;
    }
  | {
      ok: false;
      error: cluster_service_discovery_protocol_error_t;
    };
