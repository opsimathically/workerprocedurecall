export type cluster_protocol_supported_version_t = 1;

export type cluster_protocol_error_code_t =
  | 'PROTOCOL_VALIDATION_FAILED'
  | 'AUTH_FAILED'
  | 'FORBIDDEN_FUNCTION'
  | 'NO_CAPABLE_NODE'
  | 'NODE_OVERLOADED_RETRYABLE'
  | 'DISPATCH_FAILED_RETRYABLE'
  | 'UNKNOWN_OUTCOME_RETRYABLE_WITH_IDEMPOTENCY'
  | 'REMOTE_FUNCTION_ERROR'
  | 'DEADLINE_EXCEEDED'
  | 'CANCELLED'
  | 'INTERNAL_SUPERVISOR_ERROR'
  | 'ADMIN_AUTH_FAILED'
  | 'ADMIN_FORBIDDEN'
  | 'ADMIN_VALIDATION_FAILED'
  | 'ADMIN_TARGET_EMPTY'
  | 'ADMIN_CONFLICT'
  | 'ADMIN_DISPATCH_RETRYABLE'
  | 'ADMIN_TIMEOUT'
  | 'ADMIN_PARTIAL_FAILURE'
  | 'ADMIN_ROLLBACK_FAILED'
  | 'ADMIN_INTERNAL';

export type cluster_protocol_error_t = {
  code: cluster_protocol_error_code_t;
  message: string;
  retryable: boolean;
  unknown_outcome: boolean;
  details: Record<string, unknown>;
};

export type cluster_protocol_validation_result_t<value_t> =
  | {
      ok: true;
      value: value_t;
    }
  | {
      ok: false;
      error: cluster_protocol_error_t;
    };

export interface cluster_protocol_envelope_i {
  protocol_version: cluster_protocol_supported_version_t;
  message_type: cluster_protocol_message_type_t;
  timestamp_unix_ms: number;
}

export type cluster_protocol_routing_mode_t = 'auto' | 'target_node' | 'affinity';

export interface cluster_call_request_message_i extends cluster_protocol_envelope_i {
  message_type: 'cluster_call_request';
  request_id: string;
  trace_id: string;
  span_id: string;
  attempt_index: number;
  max_attempts: number;
  deadline_unix_ms: number;
  function_name: string;
  function_hash_sha1?: string;
  args: unknown[];
  routing_hint: {
    mode: cluster_protocol_routing_mode_t;
    target_node_id?: string;
    affinity_key?: string;
    zone?: string;
  };
  idempotency_key?: string;
  caller_identity: {
    subject: string;
    tenant_id: string;
    scopes: string[];
    signed_claims: string;
  };
  metadata?: Record<string, unknown>;
}

export interface cluster_call_ack_message_i extends cluster_protocol_envelope_i {
  message_type: 'cluster_call_ack';
  request_id: string;
  attempt_index: number;
  node_id: string;
  accepted: boolean;
  queue_position: number;
  estimated_start_delay_ms: number;
}

export interface cluster_call_response_success_message_i
  extends cluster_protocol_envelope_i {
  message_type: 'cluster_call_response_success';
  request_id: string;
  attempt_index: number;
  node_id: string;
  function_name: string;
  function_hash_sha1: string;
  return_value: unknown;
  timing: {
    gateway_received_unix_ms: number;
    node_received_unix_ms: number;
    worker_started_unix_ms: number;
    worker_finished_unix_ms: number;
  };
}

export interface cluster_call_response_error_message_i
  extends cluster_protocol_envelope_i {
  message_type: 'cluster_call_response_error';
  request_id: string;
  attempt_index: number;
  node_id?: string;
  error: {
    code: string;
    message: string;
    retryable: boolean;
    unknown_outcome: boolean;
    details?: Record<string, unknown>;
  };
  timing: {
    gateway_received_unix_ms: number;
    last_attempt_started_unix_ms: number;
  };
}

export interface cluster_call_cancel_message_i extends cluster_protocol_envelope_i {
  message_type: 'cluster_call_cancel';
  request_id: string;
  reason: 'client_cancelled' | 'deadline_exceeded' | 'gateway_shutdown';
}

export interface cluster_call_cancel_ack_message_i extends cluster_protocol_envelope_i {
  message_type: 'cluster_call_cancel_ack';
  request_id: string;
  cancelled: boolean;
  best_effort_only: boolean;
}

export interface node_heartbeat_message_i extends cluster_protocol_envelope_i {
  message_type: 'node_heartbeat';
  node_id: string;
  health_state: 'ready' | 'degraded' | 'restarting' | 'stopped';
  metrics: {
    inflight_calls: number;
    pending_calls: number;
    success_rate_1m: number;
    timeout_rate_1m: number;
    ewma_latency_ms: number;
  };
}

export interface node_capability_announce_message_i extends cluster_protocol_envelope_i {
  message_type: 'node_capability_announce';
  node_id: string;
  capabilities: {
    function_name: string;
    function_hash_sha1: string;
    installed: boolean;
  }[];
}

export type cluster_admin_target_scope_t =
  | 'single_node'
  | 'node_selector'
  | 'cluster_wide';

export type cluster_admin_mutation_type_t =
  | 'define_function'
  | 'redefine_function'
  | 'undefine_function'
  | 'define_dependency'
  | 'undefine_dependency'
  | 'define_constant'
  | 'undefine_constant'
  | 'define_database_connection'
  | 'undefine_database_connection'
  | 'shared_create_chunk'
  | 'shared_free_chunk'
  | 'shared_reconfigure_limits'
  | 'shared_clear_all_chunks';

export type cluster_admin_rollout_mode_t =
  | 'all_at_once'
  | 'rolling_percent'
  | 'canary_then_expand'
  | 'single_node';

export type cluster_admin_mutation_status_t =
  | 'completed'
  | 'partially_failed'
  | 'rolled_back'
  | 'dry_run_completed';

export interface cluster_admin_mutation_request_message_i
  extends cluster_protocol_envelope_i {
  message_type: 'cluster_admin_mutation_request';
  mutation_id: string;
  request_id: string;
  trace_id: string;
  deadline_unix_ms: number;
  target_scope: cluster_admin_target_scope_t;
  target_selector?: {
    node_ids?: string[];
    labels?: Record<string, string>;
    zones?: string[];
    version_constraints?: {
      node_agent_semver?: string;
      runtime_semver?: string;
    };
  };
  mutation_type: cluster_admin_mutation_type_t;
  payload: Record<string, unknown>;
  expected_version?: {
    entity_name?: string;
    entity_version?: string;
    compare_mode?: 'exact' | 'at_least';
  };
  dry_run: boolean;
  rollout_strategy: {
    mode: cluster_admin_rollout_mode_t;
    min_success_percent?: number;
    batch_percent?: number;
    canary_node_count?: number;
    inter_batch_delay_ms?: number;
    apply_timeout_ms?: number;
    verify_timeout_ms?: number;
    rollback_policy?: {
      auto_rollback?: boolean;
      rollback_on_partial_failure?: boolean;
      rollback_on_verification_failure?: boolean;
    };
  };
  in_flight_policy?: 'no_interruption' | 'drain_and_swap';
  change_context?: {
    reason?: string;
    change_ticket_id?: string;
    requested_by?: string;
    dual_authorization?: {
      required?: boolean;
      approver_subject?: string;
    };
  };
  artifact?: {
    source_hash_sha256?: string;
    signature?: string;
    signature_key_id?: string;
  };
  auth_context: {
    subject: string;
    tenant_id: string;
    environment?: string;
    capability_claims: string[];
    signed_claims: string;
  };
  idempotency_key?: string;
}

export interface cluster_admin_mutation_ack_message_i
  extends cluster_protocol_envelope_i {
  message_type: 'cluster_admin_mutation_ack';
  mutation_id: string;
  request_id: string;
  accepted: boolean;
  planner_id: string;
  target_node_count: number;
  dry_run: boolean;
}

export interface cluster_admin_mutation_result_message_i
  extends cluster_protocol_envelope_i {
  message_type: 'cluster_admin_mutation_result';
  mutation_id: string;
  request_id: string;
  status: cluster_admin_mutation_status_t;
  rollout_strategy: cluster_admin_rollout_mode_t;
  summary: {
    target_node_count: number;
    applied_node_count: number;
    failed_node_count: number;
    rolled_back_node_count: number;
    min_success_percent: number;
    achieved_success_percent: number;
  };
  per_node_results: {
    node_id: string;
    status: 'applied' | 'failed' | 'rolled_back' | 'skipped';
    error_code?: string;
    error_message?: string;
    applied_version?: string;
  }[];
  timing?: {
    planning_started_unix_ms?: number;
    apply_started_unix_ms?: number;
    verify_finished_unix_ms?: number;
  };
  audit_record_id: string;
}

export interface cluster_admin_mutation_error_message_i
  extends cluster_protocol_envelope_i {
  message_type: 'cluster_admin_mutation_error';
  mutation_id: string;
  request_id: string;
  error: cluster_protocol_error_t;
}

export type cluster_protocol_message_type_t =
  | 'cluster_call_request'
  | 'cluster_call_ack'
  | 'cluster_call_response_success'
  | 'cluster_call_response_error'
  | 'cluster_call_cancel'
  | 'cluster_call_cancel_ack'
  | 'node_heartbeat'
  | 'node_capability_announce'
  | 'cluster_admin_mutation_request'
  | 'cluster_admin_mutation_ack'
  | 'cluster_admin_mutation_result'
  | 'cluster_admin_mutation_error';

export type cluster_protocol_message_t =
  | cluster_call_request_message_i
  | cluster_call_ack_message_i
  | cluster_call_response_success_message_i
  | cluster_call_response_error_message_i
  | cluster_call_cancel_message_i
  | cluster_call_cancel_ack_message_i
  | node_heartbeat_message_i
  | node_capability_announce_message_i
  | cluster_admin_mutation_request_message_i
  | cluster_admin_mutation_ack_message_i
  | cluster_admin_mutation_result_message_i
  | cluster_admin_mutation_error_message_i;
