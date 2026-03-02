import type { cluster_call_request_message_i } from '../clusterprotocol/ClusterProtocolTypes';

export type ingress_balancer_protocol_supported_version_t = 1;

export type ingress_balancer_error_code_t =
  | 'INGRESS_VALIDATION_FAILED'
  | 'INGRESS_AUTH_FAILED'
  | 'INGRESS_FORBIDDEN'
  | 'INGRESS_NO_TARGET'
  | 'INGRESS_TARGET_UNAVAILABLE'
  | 'INGRESS_DISPATCH_RETRYABLE'
  | 'INGRESS_TIMEOUT'
  | 'INGRESS_INTERNAL';

export type ingress_balancer_error_t = {
  code: ingress_balancer_error_code_t;
  message: string;
  retryable: boolean;
  unknown_outcome: boolean;
  details: Record<string, unknown>;
};

export type ingress_balancer_target_health_state_t =
  | 'ready'
  | 'degraded'
  | 'restarting'
  | 'stopped';

export type ingress_balancer_target_source_t =
  | 'control_plane'
  | 'discovery'
  | 'static'
  | 'geo_control_plane';

export type ingress_balancer_target_record_t = {
  target_id: string;
  gateway_id?: string;
  node_id: string;
  endpoint: {
    host: string;
    port: number;
    request_path: string;
    tls_mode: 'disabled' | 'required' | 'terminated_upstream';
  };
  source: ingress_balancer_target_source_t;
  health_state: ingress_balancer_target_health_state_t;
  inflight_calls: number;
  pending_calls: number;
  success_rate_1m: number;
  timeout_rate_1m: number;
  ewma_latency_ms: number;
  capability_hash_by_function_name: Record<string, string>;
  zones?: string[];
  labels?: Record<string, string>;
  tenant_allow_list?: string[];
  environment_allow_list?: string[];
  policy_version_id?: string | null;
  updated_unix_ms: number;
  metadata?: Record<string, unknown>;
  region_id?: string;
  region_status?: 'active' | 'degraded' | 'draining' | 'offline';
  region_priority?: number;
  region_capacity_score?: number;
  region_latency_ewma_ms?: number;
};

export type ingress_balancer_instance_identity_t = {
  ingress_id: string;
  zone?: string;
  version: string;
  status: 'active' | 'degraded' | 'draining' | 'offline';
};

export type ingress_balancer_request_context_t = {
  request_id: string;
  trace_id: string;
  deadline_unix_ms: number;
  call_request: cluster_call_request_message_i;
  caller_identity_summary: {
    subject: string;
    tenant_id: string;
    scopes: string[];
  };
};

export type ingress_balancer_routing_decision_t = {
  request_id: string;
  trace_id: string;
  mode: 'least_loaded' | 'weighted_rr' | 'affinity' | 'target_node';
  selected_target_id: string | null;
  candidate_target_id_list: string[];
  reason: string;
  timestamp_unix_ms: number;
};

export type ingress_balancer_forwarding_attempt_t = {
  request_id: string;
  trace_id: string;
  attempt_index: number;
  target_id: string;
  started_unix_ms: number;
  finished_unix_ms: number;
  duration_ms: number;
  outcome:
    | 'success'
    | 'retryable_error'
    | 'non_retryable_error'
    | 'transport_error'
    | 'timeout';
  error?: ingress_balancer_error_t;
};

export type ingress_balancer_target_snapshot_t = {
  refreshed_unix_ms: number;
  stale_since_unix_ms: number | null;
  stale: boolean;
  source_success_by_name: Record<
    'control_plane' | 'discovery' | 'static' | 'geo_control_plane',
    boolean
  >;
  target_list: ingress_balancer_target_record_t[];
};

export type ingress_balancer_event_name_t =
  | 'ingress_request_received'
  | 'ingress_target_selected'
  | 'ingress_dispatch_failed'
  | 'ingress_failover_attempted'
  | 'ingress_request_completed'
  | 'geo_ingress_region_selected'
  | 'geo_ingress_failover_attempted'
  | 'geo_ingress_region_degraded'
  | 'geo_ingress_region_restored';

export type ingress_balancer_event_t = {
  event_id: string;
  timestamp_unix_ms: number;
  event_name: ingress_balancer_event_name_t;
  request_id?: string;
  trace_id?: string;
  target_id?: string;
  details?: Record<string, unknown>;
};

export type ingress_balancer_metrics_t = {
  ingress_requests_total: number;
  ingress_success_total: number;
  ingress_failure_total: number;
  ingress_retry_total: number;
  ingress_failover_total: number;
  ingress_no_target_total: number;
  ingress_dispatch_latency_ms: {
    count: number;
    min: number;
    max: number;
    avg: number;
    total: number;
  };
  ingress_target_selection_count_by_reason: Record<string, number>;
  geo_ingress_requests_total: number;
  geo_ingress_success_total: number;
  geo_ingress_failure_total: number;
  geo_ingress_region_failover_total: number;
  geo_ingress_region_selection_count_by_reason: Record<string, number>;
  geo_ingress_control_stale_total: number;
  geo_ingress_dispatch_latency_ms: {
    count: number;
    min: number;
    max: number;
    avg: number;
    total: number;
  };
};

export type ingress_balancer_validation_result_t<value_t> =
  | {
      ok: true;
      value: value_t;
    }
  | {
      ok: false;
      error: ingress_balancer_error_t;
    };
