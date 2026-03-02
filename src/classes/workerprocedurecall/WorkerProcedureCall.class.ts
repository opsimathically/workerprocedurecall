import { createHash } from 'node:crypto';
import { join as JoinPath } from 'node:path';
import { Worker } from 'node:worker_threads';
import type { MongoClientOptions as mongodb_client_options_t } from 'mongodb';
import type {
  ConnectionOptions as mysql_connection_options_t,
  PoolOptions as mysql_pool_options_t
} from 'mysql2/promise';
import {
  WorkerProcedureCallSharedMemoryStore,
  type shared_lock_auto_release_reason_t,
  type shared_chunk_access_params_t,
  type shared_chunk_create_params_t,
  type shared_chunk_free_params_t,
  type shared_chunk_release_params_t,
  type shared_chunk_type_t,
  type shared_chunk_write_params_t,
  type shared_lock_debug_info_params_t,
  type shared_lock_debug_information_t,
  type shared_lock_owner_context_t
} from './SharedMemoryStore.class';
import {
  ParseClusterAdminMutationRequestMessage,
  ParseClusterCallRequestMessage
} from '../clusterprotocol/ClusterProtocolValidators';
import type {
  cluster_admin_mutation_ack_message_i,
  cluster_admin_mutation_error_message_i,
  cluster_admin_mutation_request_message_i,
  cluster_admin_mutation_result_message_i,
  cluster_admin_mutation_status_t,
  cluster_admin_rollout_mode_t,
  cluster_call_ack_message_i,
  cluster_call_request_message_i,
  cluster_call_response_error_message_i,
  cluster_call_response_success_message_i,
  node_capability_announce_message_i,
  node_heartbeat_message_i,
  cluster_protocol_error_code_t,
  cluster_protocol_error_t,
  cluster_protocol_validation_result_t
} from '../clusterprotocol/ClusterProtocolTypes';
import {
  ClusterAuthorizationPolicyEngine,
  type cluster_authorization_evaluation_result_t,
  type cluster_authorization_policy_t,
  type cluster_authorization_target_selector_t
} from '../clusterauthorization/ClusterAuthorizationPolicyEngine.class';
import {
  ClusterControlPlaneFileStateStore,
  ClusterControlPlaneInMemoryStateStore,
  type cluster_control_plane_audit_record_t,
  type cluster_control_plane_dedupe_record_t,
  type cluster_control_plane_retention_policy_t,
  type cluster_control_plane_state_snapshot_t,
  type cluster_control_plane_state_store_i
} from './ClusterControlPlaneStateStore.class';
import {
  ClusterCallRoutingStrategy,
  type cluster_call_node_capability_t as cluster_call_node_capability_base_t,
  type cluster_call_node_health_state_t as cluster_call_node_health_state_base_t,
  type cluster_call_node_metrics_t as cluster_call_node_metrics_base_t,
  type cluster_call_routing_candidate_node_t as cluster_call_routing_candidate_node_base_t
} from '../clusterrouting/ClusterCallRouting.class';

type lifecycle_state_t = 'stopped' | 'starting' | 'running' | 'stopping';
type control_command_t =
  | 'define_function'
  | 'undefine_function'
  | 'define_dependency'
  | 'undefine_dependency'
  | 'define_constant'
  | 'undefine_constant'
  | 'define_database_connection'
  | 'undefine_database_connection'
  | 'shutdown';

type runtime_options_t = {
  call_timeout_ms?: number;
  control_timeout_ms?: number;
  start_timeout_ms?: number;
  stop_timeout_ms?: number;
  restart_on_failure?: boolean;
  max_restarts_per_worker?: number;
  max_pending_calls_per_worker?: number;
  restart_base_delay_ms?: number;
  restart_max_delay_ms?: number;
  restart_jitter_ms?: number;
  mutation_dedupe_retention_ms?: number;
  mutation_retry_max_attempts?: number;
  mutation_retry_base_delay_ms?: number;
  mutation_retry_max_delay_ms?: number;
  call_routing_heartbeat_ttl_ms?: number;
  call_routing_sticky_ttl_ms?: number;
  call_routing_allow_degraded_node_routing?: boolean;
  call_routing_inflight_weight?: number;
  call_routing_latency_weight?: number;
  call_routing_error_weight?: number;
  call_routing_degraded_penalty?: number;
  operations_event_history_max_count?: number;
  control_plane_durable_state_enabled?: boolean;
  control_plane_state_file_path?: string;
  control_plane_audit_retention_ms?: number;
  control_plane_audit_max_record_count?: number;
  control_plane_state_store?: cluster_control_plane_state_store_i;
};

export type workerprocedurecall_constructor_params_t = runtime_options_t;

export type worker_function_handler_t<
  args_t extends unknown[] = [unknown],
  return_t = unknown
> = (...args: args_t) => return_t | Promise<return_t>;

export type define_worker_function_params_t<
  args_t extends unknown[] = [unknown],
  return_t = unknown
> = {
  name: string;
  worker_func: worker_function_handler_t<args_t, return_t>;
};

export type undefine_worker_function_params_t = {
  name: string;
};

export type define_worker_dependency_params_t = {
  alias: string;
  module_specifier: string;
  export_name?: string;
  is_default_export?: boolean;
};

export type undefine_worker_dependency_params_t = {
  alias: string;
};

export type define_worker_constant_params_t = {
  name: string;
  value: unknown;
};

export type undefine_worker_constant_params_t = {
  name: string;
};

export type database_connector_type_t =
  | 'mongodb'
  | 'postgresql'
  | 'mariadb'
  | 'mysql'
  | 'sqlite';

export type mongodb_connector_semantics_t = {
  uri: string;
  database_name?: string;
  client_options?: mongodb_client_options_t;
};

export type postgresql_connector_semantics_t = {
  connection_string?: string;
  use_pool?: boolean;
  pool_options?: Record<string, unknown>;
  client_options?: Record<string, unknown>;
};

export type mysql_connector_semantics_t = {
  use_pool?: boolean;
  pool_options?: mysql_pool_options_t;
  connection_options?: mysql_connection_options_t;
};

export type mariadb_connector_semantics_t = mysql_connector_semantics_t;

export type sqlite_connector_driver_t = 'sqlite3' | 'better-sqlite3';

export type sqlite_connector_semantics_t = {
  filename?: string;
  driver?: sqlite_connector_driver_t;
  mode?: number;
  options?: Record<string, unknown>;
};

export type database_connector_semantics_by_type_t = {
  mongodb: mongodb_connector_semantics_t;
  postgresql: postgresql_connector_semantics_t;
  mariadb: mariadb_connector_semantics_t;
  mysql: mysql_connector_semantics_t;
  sqlite: sqlite_connector_semantics_t;
};

export type database_connector_definition_for_type_t<
  connector_type_t extends database_connector_type_t
> = connector_type_t extends database_connector_type_t
  ? {
      type: connector_type_t;
      semantics: database_connector_semantics_by_type_t[connector_type_t];
    }
  : never;

export type database_connector_definition_t = {
  [connector_type in database_connector_type_t]:
    database_connector_definition_for_type_t<connector_type>;
}[database_connector_type_t];

export type define_database_connection_params_t<
  connector_type_t extends database_connector_type_t = database_connector_type_t
> = {
  name: string;
  connector: database_connector_definition_for_type_t<connector_type_t>;
};

export type undefine_database_connection_params_t = {
  name: string;
};

export type start_workers_params_t = runtime_options_t & {
  count: number;
};

export type remote_error_t = {
  name: string;
  message: string;
  stack?: string;
};

export type remote_function_information_t = {
  name: string;
  parameter_signature: string | null;
  function_hash_sha1: string;
  installed_worker_count: number;
};

export type remote_dependency_information_t = {
  alias: string;
  module_specifier: string;
  export_name: string | null;
  is_default_export: boolean;
  installed_worker_count: number;
};

export type remote_constant_information_t = {
  name: string;
  installed_worker_count: number;
};

export type remote_database_connection_information_t = {
  name: string;
  connector_type: database_connector_type_t;
  installed_worker_count: number;
};

export type wpc_default_database_connector_handle_by_type_t = {
  mongodb: {
    client: import('mongodb').MongoClient;
    db: import('mongodb').Db | null;
  };
  postgresql: import('pg').Pool | import('pg').Client;
  mariadb:
    | import('mysql2/promise').Pool
    | import('mysql2/promise').Connection;
  mysql:
    | import('mysql2/promise').Pool
    | import('mysql2/promise').Connection;
  sqlite: import('better-sqlite3').Database | import('sqlite3').Database;
};

export type mongodb_database_connection_handle_t =
  wpc_default_database_connector_handle_by_type_t['mongodb'];

export type postgresql_database_connection_handle_t =
  wpc_default_database_connector_handle_by_type_t['postgresql'];

export type mysql_database_connection_handle_t =
  wpc_default_database_connector_handle_by_type_t['mysql'];

export type mariadb_database_connection_handle_t =
  wpc_default_database_connector_handle_by_type_t['mariadb'];

export type better_sqlite3_database_connection_handle_t =
  import('better-sqlite3').Database;

export type better_sqlite3_statement_handle_t<
  bind_parameters_t extends unknown[] | {} = unknown[],
  row_t = unknown
> = import('better-sqlite3').Statement<bind_parameters_t, row_t>;

export type sqlite_database_connection_handle_t =
  wpc_default_database_connector_handle_by_type_t['sqlite'];

declare global {
  interface wpc_dependency_by_alias_i {}
  interface wpc_database_connector_handle_overrides_i {}
  interface wpc_database_connection_type_by_name_i {}
  interface wpc_database_connection_handle_by_name_i {}
}

type wpc_dependency_alias_key_t = Extract<
  keyof wpc_dependency_by_alias_i,
  string
>;

type wpc_database_connector_handle_override_key_t = Extract<
  keyof wpc_database_connector_handle_overrides_i,
  database_connector_type_t
>;

type wpc_database_connection_type_name_key_t = Extract<
  keyof wpc_database_connection_type_by_name_i,
  string
>;

type wpc_database_connection_handle_name_key_t = Extract<
  keyof wpc_database_connection_handle_by_name_i,
  string
>;

export type wpc_database_connector_handle_by_type_t = Omit<
  wpc_default_database_connector_handle_by_type_t,
  wpc_database_connector_handle_override_key_t
> &
  Pick<
    wpc_database_connector_handle_overrides_i,
    wpc_database_connector_handle_override_key_t
  >;

export type wpc_database_connection_handle_from_type_t<
  connector_type_t extends database_connector_type_t
> = connector_type_t extends keyof wpc_database_connector_handle_by_type_t
  ? wpc_database_connector_handle_by_type_t[connector_type_t]
  : unknown;

type wpc_database_connection_handle_from_name_type_t<
  connection_name_t extends wpc_database_connection_type_name_key_t
> = wpc_database_connection_type_by_name_i[connection_name_t] extends database_connector_type_t
  ? wpc_database_connection_handle_from_type_t<
      wpc_database_connection_type_by_name_i[connection_name_t]
    >
  : unknown;

export type wpc_database_connection_handle_by_name_t<
  connection_name_t extends string
> = connection_name_t extends wpc_database_connection_handle_name_key_t
  ? wpc_database_connection_handle_by_name_i[connection_name_t]
  : connection_name_t extends wpc_database_connection_type_name_key_t
    ? wpc_database_connection_handle_from_name_type_t<connection_name_t>
    : unknown;

export type wpc_dependency_from_alias_t<alias_t extends string> =
  alias_t extends wpc_dependency_alias_key_t
    ? wpc_dependency_by_alias_i[alias_t]
    : unknown;

export type wpc_dependency_lookup_by_alias_params_t<
  alias_t extends string = string
> = {
  alias: alias_t;
};

export type wpc_dependency_lookup_params_t =
  wpc_dependency_lookup_by_alias_params_t;

export type wpc_database_connection_lookup_by_type_params_t<
  connector_type_t extends database_connector_type_t = database_connector_type_t
> = {
  name: string;
  type: connector_type_t;
};

export type wpc_database_connection_lookup_params_t = {
  name: string;
  type?: database_connector_type_t;
};

export type shared_memory_chunk_type_t = shared_chunk_type_t;
export type shared_create_params_t<content_t = unknown> =
  shared_chunk_create_params_t<content_t>;
export type shared_access_params_t = shared_chunk_access_params_t;
export type shared_write_params_t<content_t = unknown> =
  shared_chunk_write_params_t<content_t>;
export type shared_release_params_t = shared_chunk_release_params_t;
export type shared_free_params_t = shared_chunk_free_params_t;
export type shared_get_lock_debug_info_params_t =
  shared_lock_debug_info_params_t;
export type shared_lock_debug_information_result_t =
  shared_lock_debug_information_t;

export type worker_health_state_t =
  | 'starting'
  | 'ready'
  | 'degraded'
  | 'restarting'
  | 'stopped';

export type worker_event_severity_t = 'info' | 'warn' | 'error';

export type worker_event_t = {
  event_id: string;
  worker_id: number;
  source: 'worker' | 'parent';
  event_name: string;
  severity: worker_event_severity_t;
  timestamp: string;
  correlation_id?: string;
  error?: remote_error_t;
  details?: Record<string, unknown>;
};

export type worker_event_listener_t = (worker_event: worker_event_t) => void;

export type worker_health_information_t = {
  worker_id: number;
  health_state: worker_health_state_t;
  pending_call_count: number;
  pending_control_count: number;
  restart_attempt: number;
};

export type worker_call_proxy_t = {
  [function_name: string]: (...args: unknown[]) => Promise<unknown>;
};

export type handle_cluster_admin_mutation_request_params_t = {
  message: unknown;
  node_id: string;
  now_unix_ms?: number;
};

export type handle_cluster_admin_mutation_response_t = {
  ack: cluster_admin_mutation_ack_message_i;
  terminal_message:
    | cluster_admin_mutation_result_message_i
    | cluster_admin_mutation_error_message_i;
};

export type handle_cluster_call_request_params_t = {
  message: unknown;
  node_id: string;
  now_unix_ms?: number;
  gateway_received_unix_ms?: number;
};

export type handle_cluster_call_response_t = {
  ack: cluster_call_ack_message_i;
  terminal_message:
    | cluster_call_response_success_message_i
    | cluster_call_response_error_message_i;
};

export type cluster_call_node_health_state_t = cluster_call_node_health_state_base_t;
export type cluster_call_node_metrics_t = cluster_call_node_metrics_base_t;
export type cluster_call_node_capability_t = cluster_call_node_capability_base_t;

export type cluster_call_node_descriptor_t = {
  node_id: string;
  labels?: Record<string, string>;
  zones?: string[];
  node_agent_semver?: string;
  runtime_semver?: string;
};

export type cluster_call_node_executor_t = (params: {
  request: cluster_call_request_message_i;
  node: cluster_call_node_descriptor_t;
  now_unix_ms: number;
  gateway_received_unix_ms: number;
}) => Promise<handle_cluster_call_response_t>;

export type register_cluster_call_node_params_t = {
  node: cluster_call_node_descriptor_t;
  call_executor: cluster_call_node_executor_t;
  initial_health_state?: cluster_call_node_health_state_t;
  initial_metrics?: Partial<cluster_call_node_metrics_t>;
  capability_list?: cluster_call_node_capability_t[];
  last_health_update_unix_ms?: number;
};

export type cluster_call_routing_policy_t = {
  heartbeat_ttl_ms: number;
  sticky_ttl_ms: number;
  allow_degraded_node_routing: boolean;
  inflight_weight: number;
  latency_weight: number;
  error_weight: number;
  degraded_penalty: number;
};

export type cluster_call_routing_metrics_t = {
  call_requests_total: number;
  routed_local_total: number;
  routed_remote_total: number;
  routing_mode_auto_total: number;
  routing_mode_target_node_total: number;
  routing_mode_affinity_total: number;
  filtered_unhealthy_node_total: number;
  filtered_capability_node_total: number;
  filtered_zone_node_total: number;
  filtered_authorization_node_total: number;
  failover_attempt_total: number;
  no_capable_node_total: number;
  dispatch_retryable_total: number;
};

export type cluster_operation_lifecycle_event_name_t =
  | 'cluster_call_received'
  | 'cluster_call_routed'
  | 'cluster_call_retried'
  | 'cluster_call_completed'
  | 'cluster_call_failed'
  | 'admin_mutation_requested'
  | 'admin_mutation_authorized'
  | 'admin_mutation_applied'
  | 'admin_mutation_partially_failed'
  | 'admin_mutation_rolled_back'
  | 'admin_mutation_denied';

export type cluster_operation_lifecycle_event_t = {
  timestamp_unix_ms: number;
  event_name: cluster_operation_lifecycle_event_name_t;
  severity: worker_event_severity_t;
  request_id?: string;
  trace_id?: string;
  mutation_id?: string;
  function_name?: string;
  node_id?: string;
  error_code?: string;
  retryable?: boolean;
  details?: Record<string, unknown>;
};

export type cluster_operations_observability_snapshot_t = {
  captured_at_unix_ms: number;
  call_routing_metrics: cluster_call_routing_metrics_t;
  mutation_metrics: cluster_admin_mutation_metrics_t;
  worker_health_summary: {
    lifecycle_state: 'stopped' | 'starting' | 'running' | 'stopping';
    worker_count: number;
    worker_health_count_by_state: Record<worker_health_state_t, number>;
    pending_call_count: number;
    pending_control_count: number;
  };
  cluster_call_node_summary: {
    registered_node_count: number;
    health_count_by_state: Record<cluster_call_node_health_state_t, number>;
  };
  mutation_node_count: number;
};

export type cluster_mutation_node_descriptor_t = {
  node_id: string;
  labels?: Record<string, string>;
  zones?: string[];
  node_agent_semver?: string;
  runtime_semver?: string;
};

export type cluster_mutation_node_executor_result_t = {
  applied_version?: string;
  verification_passed?: boolean;
  snapshot?: unknown;
};

export type cluster_mutation_node_executor_t = (params: {
  mode: 'apply' | 'rollback';
  request: cluster_admin_mutation_request_message_i;
  node: cluster_mutation_node_descriptor_t;
  apply_timeout_ms: number;
  verify_timeout_ms: number;
  in_flight_policy?: 'no_interruption' | 'drain_and_swap';
  snapshot?: unknown;
}) => Promise<cluster_mutation_node_executor_result_t>;

export type register_cluster_mutation_node_params_t = {
  node: cluster_mutation_node_descriptor_t;
  mutation_executor: cluster_mutation_node_executor_t;
};

export type mutation_governance_policy_t = {
  require_change_reason: boolean;
  require_change_ticket_id: boolean;
  enforce_dual_authorization: boolean;
  break_glass_mutation_type_list: string[];
};

export type cluster_admin_mutation_metrics_t = {
  admin_mutation_requests_total: number;
  admin_mutation_success_total: number;
  admin_mutation_failure_total: number;
  admin_mutation_rollout_duration_ms: number[];
  admin_mutation_rollback_total: number;
  admin_mutation_authz_denied_total: number;
};

export type cluster_admin_mutation_audit_record_t = {
  audit_record_id: string;
  mutation_id: string;
  request_id: string;
  timestamp_unix_ms: number;
  actor_identity: {
    subject?: string;
    tenant_id?: string;
    environment?: string;
  };
  evaluated_privileges: {
    capability_claims: string[];
    matched_allow_policy_id_list: string[];
    matched_deny_policy_id_list: string[];
    authorization_reason?: string;
  };
  payload_hash_sha256: string | null;
  target_scope: string | null;
  resolved_node_id_list: string[];
  per_node_outcomes: cluster_admin_mutation_result_message_i['per_node_results'];
  timing: {
    received_unix_ms: number;
    completed_unix_ms: number;
    duration_ms: number;
  };
  rollback_metadata: {
    attempted: boolean;
    rolled_back_node_count: number;
    rollback_failed: boolean;
  };
  result: {
    kind: 'result' | 'error';
    status?: cluster_admin_mutation_status_t;
    error_code?: string;
    error_message?: string;
  };
  governance: {
    change_reason?: string;
    change_ticket_id?: string;
    dual_authorization_required: boolean;
    dual_authorization_approver_subject?: string;
    break_glass: boolean;
  };
  immutable: true;
};

interface admin_mutation_error_with_code_i extends Error {
  code: cluster_protocol_error_code_t;
  details?: Record<string, unknown>;
}

type worker_function_definition_t = {
  name: string;
  worker_func: worker_function_handler_t<any, any>;
  function_source: string;
  normalized_function_source: string;
  function_hash_sha1: string;
  parameter_signature: string | null;
  required_dependency_aliases: Set<string>;
  required_constant_names: Set<string>;
  required_database_connection_names: Set<string>;
  installed_worker_ids: Set<number>;
};

type worker_dependency_definition_t = {
  alias: string;
  module_specifier: string;
  export_name: string | null;
  is_default_export: boolean;
  installed_worker_ids: Set<number>;
};

type worker_constant_definition_t = {
  name: string;
  value: unknown;
  installed_worker_ids: Set<number>;
};

type worker_database_connection_definition_t = {
  name: string;
  connector_type: database_connector_type_t;
  semantics: Record<string, unknown>;
  installed_worker_ids: Set<number>;
};

type worker_state_t = {
  worker_id: number;
  worker_instance: Worker;
  health_state: worker_health_state_t;
  ready: boolean;
  shutting_down: boolean;
  restart_attempt: number;
  pending_call_request_ids: Set<string>;
  pending_control_request_ids: Set<string>;
};

type cluster_call_node_registration_t = cluster_call_node_descriptor_t & {
  call_executor: cluster_call_node_executor_t;
  health_state: cluster_call_node_health_state_t;
  last_health_update_unix_ms: number;
  metrics: cluster_call_node_metrics_t;
  capability_list: cluster_call_node_capability_t[];
  runtime_inflight_call_count: number;
};

type cluster_mutation_node_registration_t = cluster_mutation_node_descriptor_t & {
  mutation_executor: cluster_mutation_node_executor_t;
};

type cluster_mutation_target_node_t = cluster_mutation_node_descriptor_t & {
  is_local_node: boolean;
  mutation_executor?: cluster_mutation_node_executor_t;
};

type cluster_mutation_node_state_t = {
  node_id: string;
  status: 'applied' | 'failed' | 'rolled_back' | 'skipped';
  error_code?: string;
  error_message?: string;
  applied_version?: string;
  apply_succeeded: boolean;
  verification_succeeded: boolean;
  rollback_succeeded: boolean;
  snapshot?: unknown;
};

type cluster_local_mutation_snapshot_t =
  | {
      kind: 'function';
      name: string;
      existed: boolean;
      worker_func?: worker_function_handler_t<any, any>;
    }
  | {
      kind: 'dependency';
      alias: string;
      existed: boolean;
      definition?: {
        module_specifier: string;
        export_name?: string;
        is_default_export?: boolean;
      };
    }
  | {
      kind: 'constant';
      name: string;
      existed: boolean;
      value?: unknown;
    }
  | {
      kind: 'database_connection';
      name: string;
      existed: boolean;
      connector?: database_connector_definition_t;
    }
  | {
      kind: 'unsupported';
      mutation_type: string;
    };

type cluster_mutation_dedupe_record_t = cluster_control_plane_dedupe_record_t;

type pending_call_record_t = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout_handle: NodeJS.Timeout;
  worker_id: number;
  function_name: string;
};

type pending_control_record_t = {
  resolve: () => void;
  reject: (error: Error) => void;
  timeout_handle: NodeJS.Timeout;
  worker_id: number;
  command: control_command_t;
};

type worker_ready_waiter_t = {
  resolve: () => void;
  reject: (error: Error) => void;
  timeout_handle: NodeJS.Timeout;
};

type worker_exit_waiter_t = {
  resolve: () => void;
  timeout_handle: NodeJS.Timeout;
};

type call_request_message_t = {
  message_type: 'call_request';
  request_id: string;
  function_name: string;
  call_args: unknown[];
};

type control_request_message_t = {
  message_type: 'control_request';
  control_request_id: string;
  command: control_command_t;
  payload?: Record<string, unknown>;
};

type worker_shared_command_t =
  | 'shared_create'
  | 'shared_access'
  | 'shared_write'
  | 'shared_release'
  | 'shared_free';

type shared_response_message_t = {
  message_type: 'shared_response';
  shared_request_id: string;
  ok: boolean;
  return_value?: unknown;
  error?: remote_error_t;
};

type parent_to_worker_message_t =
  | call_request_message_t
  | control_request_message_t
  | shared_response_message_t;

type ready_message_t = {
  message_type: 'ready';
};

type call_response_message_t = {
  message_type: 'call_response';
  request_id: string;
  ok: boolean;
  return_value?: unknown;
  error?: remote_error_t;
};

type control_response_message_t = {
  message_type: 'control_response';
  control_request_id: string;
  ok: boolean;
  error?: remote_error_t;
};

type worker_event_message_t = {
  message_type: 'worker_event';
  event_id: string;
  worker_id: number;
  event_name: string;
  severity: worker_event_severity_t;
  timestamp: string;
  correlation_id?: string;
  error?: remote_error_t;
  details?: Record<string, unknown>;
};

type shared_request_message_t = {
  message_type: 'shared_request';
  shared_request_id: string;
  command: worker_shared_command_t;
  call_request_id: string;
  payload?: Record<string, unknown>;
};

type worker_to_parent_message_t =
  | ready_message_t
  | call_response_message_t
  | control_response_message_t
  | worker_event_message_t
  | shared_request_message_t;

declare global {
  function wpc_import<alias_t extends string>(
    alias: alias_t
  ): Promise<wpc_dependency_from_alias_t<alias_t>>;
  function wpc_import<alias_t extends string>(
    params: wpc_dependency_lookup_by_alias_params_t<alias_t>
  ): Promise<wpc_dependency_from_alias_t<alias_t>>;
  function wpc_import<dependency_t = unknown>(
    params: wpc_dependency_lookup_params_t
  ): Promise<dependency_t>;
  function wpc_constant(name: string): unknown;
  function wpc_database_connection<connection_name_t extends string>(
    name: connection_name_t
  ): Promise<wpc_database_connection_handle_by_name_t<connection_name_t>>;
  function wpc_database_connection<
    connector_type_t extends database_connector_type_t
  >(
    params: wpc_database_connection_lookup_by_type_params_t<connector_type_t>
  ): Promise<wpc_database_connection_handle_from_type_t<connector_type_t>>;
  function wpc_database_connection<connection_handle_t = unknown>(
    params: wpc_database_connection_lookup_params_t
  ): Promise<connection_handle_t>;
  function wpc_shared_create<content_t = unknown>(
    params: shared_create_params_t<content_t>
  ): Promise<void>;
  function wpc_shared_access<content_t = unknown>(
    params: shared_access_params_t
  ): Promise<content_t>;
  function wpc_shared_write<content_t = unknown>(
    params: shared_write_params_t<content_t>
  ): Promise<void>;
  function wpc_shared_release(params: shared_release_params_t): Promise<void>;
  function wpc_shared_free(params: shared_free_params_t): Promise<void>;
}

function ValidatePositiveInteger(params: { value: number; label: string }): void {
  const { value, label } = params;
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive integer.`);
  }
}

function ValidateNonNegativeInteger(params: {
  value: number;
  label: string;
}): void {
  const { value, label } = params;
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${label} must be a non-negative integer.`);
  }
}

function GetErrorMessage(params: { error: unknown }): string {
  const { error } = params;

  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === 'string') {
    return error;
  }

  return String(error);
}

function ToRemoteError(params: { error: unknown }): remote_error_t {
  const { error } = params;

  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack
    };
  }

  return {
    name: 'Error',
    message: GetErrorMessage({ error })
  };
}

function ToError(params: {
  remote_error: remote_error_t | undefined;
  fallback_message: string;
}): Error {
  const { remote_error, fallback_message } = params;

  if (!remote_error) {
    return new Error(fallback_message);
  }

  const error_to_return = new Error(remote_error.message);
  error_to_return.name = remote_error.name;

  if (typeof remote_error.stack === 'string' && remote_error.stack.length > 0) {
    error_to_return.stack = remote_error.stack;
  }

  return error_to_return;
}

function BuildWorkerSharedOwnerId(params: {
  worker_id: number;
  call_request_id: string;
}): string {
  const { worker_id, call_request_id } = params;
  return `worker:${worker_id}:${call_request_id}`;
}

function EnsureSerializable(params: { value: unknown; label: string }): void {
  const { value, label } = params;

  try {
    structuredClone(value);
  } catch (error) {
    throw new Error(
      `${label} is not serializable via structured clone: ${GetErrorMessage({ error })}`
    );
  }
}

function ParseParameterSignature(params: {
  function_source: string;
}): string | null {
  const { function_source } = params;

  const standard_parameter_match = function_source.match(/^[\s\S]*?\(([^)]*)\)/);
  if (standard_parameter_match) {
    const full_parameter_group = standard_parameter_match[1].trim();
    if (full_parameter_group.length === 0) {
      return null;
    }

    const parameter_name = full_parameter_group.split(',')[0]?.trim();
    if (parameter_name && parameter_name.length > 0) {
      return parameter_name;
    }
  }

  const arrow_parameter_match = function_source.match(
    /^\s*(?:async\s*)?([^\s=()]+)\s*=>/
  );

  if (!arrow_parameter_match) {
    return null;
  }

  return arrow_parameter_match[1].trim();
}

function NormalizeFunctionSourceForHash(params: { function_source: string }): string {
  const { function_source } = params;

  // Normalize line endings and boundary whitespace for deterministic hashing.
  return function_source.replace(/\r\n?/g, '\n').trim();
}

function ComputeFunctionHashSha1(params: { function_source: string }): string {
  const { function_source } = params;

  const normalized_function_source = NormalizeFunctionSourceForHash({
    function_source
  });

  return createHash('sha1').update(normalized_function_source, 'utf8').digest('hex');
}

function ValidateIdentifier(params: { value: string; label: string }): void {
  const { value, label } = params;
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`${label} must be a valid JavaScript identifier.`);
  }
}

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function CapabilityPatternMatches(params: {
  policy_capability: string;
  requested_capability: string;
}): boolean {
  const { policy_capability, requested_capability } = params;
  if (policy_capability === requested_capability) {
    return true;
  }

  if (!policy_capability.endsWith('*')) {
    return false;
  }

  const wildcard_prefix = policy_capability.slice(0, -1);
  return requested_capability.startsWith(wildcard_prefix);
}

function CapabilityListAllowsAnyRequestedCapability(params: {
  granted_capability_list: string[];
  required_capability_list: string[];
}): boolean {
  const { granted_capability_list, required_capability_list } = params;
  for (const granted_capability of granted_capability_list) {
    for (const required_capability of required_capability_list) {
      if (
        CapabilityPatternMatches({
          policy_capability: granted_capability,
          requested_capability: required_capability
        })
      ) {
        return true;
      }
    }
  }

  return false;
}

function BuildRequiredMutationCapabilityList(params: {
  mutation_type: string;
}): string[] {
  const { mutation_type } = params;
  const prefix = 'rpc.admin.mutate';

  if (mutation_type === 'define_function') {
    return [`${prefix}:function:define`, `${prefix}:*`];
  }

  if (mutation_type === 'redefine_function') {
    return [`${prefix}:function:redefine`, `${prefix}:*`];
  }

  if (mutation_type === 'undefine_function') {
    return [`${prefix}:function:undefine`, `${prefix}:*`];
  }

  if (mutation_type === 'define_dependency') {
    return [`${prefix}:dependency:define`, `${prefix}:*`];
  }

  if (mutation_type === 'undefine_dependency') {
    return [`${prefix}:dependency:undefine`, `${prefix}:*`];
  }

  if (mutation_type === 'define_constant') {
    return [`${prefix}:constant:define`, `${prefix}:*`];
  }

  if (mutation_type === 'undefine_constant') {
    return [`${prefix}:constant:undefine`, `${prefix}:*`];
  }

  if (mutation_type === 'define_database_connection') {
    return [`${prefix}:database_connection:define`, `${prefix}:*`];
  }

  if (mutation_type === 'undefine_database_connection') {
    return [`${prefix}:database_connection:undefine`, `${prefix}:*`];
  }

  if (mutation_type.startsWith('shared_')) {
    return [`${prefix}:shared:*`, `${prefix}:*`];
  }

  return [`${prefix}:*`];
}

function NormalizeDatabaseConnectorType(params: {
  value: string;
  label: string;
}): database_connector_type_t {
  const { value, label } = params;
  const valid_connector_types: database_connector_type_t[] = [
    'mongodb',
    'postgresql',
    'mariadb',
    'mysql',
    'sqlite'
  ];

  if (!valid_connector_types.includes(value as database_connector_type_t)) {
    throw new Error(
      `${label} must be one of: ${valid_connector_types.join(', ')}.`
    );
  }

  return value as database_connector_type_t;
}

function ParseStringLiteralDependencies(params: {
  function_source: string;
}): Set<string> {
  const { function_source } = params;

  const dependency_aliases = new Set<string>();
  const import_call_pattern = /wpc_import\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)/g;
  const import_object_call_pattern =
    /wpc_import\(\s*\{[^}]*\balias\s*:\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"][^}]*\}\s*\)/g;
  const context_dependency_pattern =
    /context\.dependencies\.([A-Za-z_][A-Za-z0-9_]*)/g;

  for (const pattern of [
    import_call_pattern,
    import_object_call_pattern,
    context_dependency_pattern
  ]) {
    let match_result = pattern.exec(function_source);
    while (match_result) {
      dependency_aliases.add(match_result[1]);
      match_result = pattern.exec(function_source);
    }
  }

  return dependency_aliases;
}

function ParseStringLiteralConstants(params: {
  function_source: string;
}): Set<string> {
  const { function_source } = params;

  const constant_names = new Set<string>();
  const constant_call_pattern = /wpc_constant\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)/g;
  const context_constant_pattern = /context\.constants\.([A-Za-z_][A-Za-z0-9_]*)/g;

  for (const pattern of [constant_call_pattern, context_constant_pattern]) {
    let match_result = pattern.exec(function_source);
    while (match_result) {
      constant_names.add(match_result[1]);
      match_result = pattern.exec(function_source);
    }
  }

  return constant_names;
}

function ParseStringLiteralDatabaseConnections(params: {
  function_source: string;
}): Set<string> {
  const { function_source } = params;

  const database_connection_names = new Set<string>();
  const database_connection_call_pattern =
    /wpc_database_connection\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)/g;
  const database_connection_object_name_pattern =
    /wpc_database_connection\(\s*\{[^}]*\bname\s*:\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"][^}]*\}\s*\)/g;
  const context_database_connection_pattern =
    /context\.database_connections\.([A-Za-z_][A-Za-z0-9_]*)/g;

  for (const pattern of [
    database_connection_call_pattern,
    database_connection_object_name_pattern,
    context_database_connection_pattern
  ]) {
    let match_result = pattern.exec(function_source);
    while (match_result) {
      database_connection_names.add(match_result[1]);
      match_result = pattern.exec(function_source);
    }
  }

  return database_connection_names;
}

function BuildWorkerRuntimeScript(): string {
  return String.raw`
const { parentPort, threadId } = require('node:worker_threads');
const { AsyncLocalStorage } = require('node:async_hooks');

if (!parentPort) {
  throw new Error('Worker runtime started without parentPort.');
}

const worker_function_registry = new Map();
const worker_dependency_registry = new Map();
const worker_constant_registry = new Map();
const worker_database_connection_definition_registry = new Map();
const worker_database_connection_registry = new Map();
const worker_database_connection_connect_promise_by_name = new Map();
const shared_request_waiter_by_id = new Map();
const shared_call_context_storage = new AsyncLocalStorage();
let next_worker_event_id = 1;
let next_shared_request_id = 1;

function GetErrorMessage(error) {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === 'string') {
    return error;
  }

  return String(error);
}

function ToRemoteError(error) {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack
    };
  }

  return {
    name: 'Error',
    message: GetErrorMessage(error)
  };
}

function EmitWorkerEvent(params) {
  const event_message = {
    message_type: 'worker_event',
    event_id: 'worker_' + String(threadId) + '_' + String(next_worker_event_id),
    worker_id: threadId,
    event_name: params.event_name,
    severity: params.severity,
    timestamp: new Date().toISOString(),
    correlation_id: params.correlation_id,
    error: params.error,
    details: params.details
  };

  next_worker_event_id += 1;

  try {
    parentPort.postMessage(event_message);
  } catch {
    // Ignore - cannot safely report event transport failures.
  }
}

function SafePostMessage(message) {
  try {
    parentPort.postMessage(message);
    return true;
  } catch (error) {
    EmitWorkerEvent({
      event_name: 'post_message_failed',
      severity: 'error',
      error: ToRemoteError(error),
      details: {
        message_type: message && message.message_type ? message.message_type : null
      }
    });

    return false;
  }
}

function EnsureSerializable(value, label) {
  try {
    structuredClone(value);
  } catch (error) {
    throw new Error(label + ' is not serializable via structured clone: ' + GetErrorMessage(error));
  }
}

function ValidateIdentifier(value, label) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(label + ' must be a valid JavaScript identifier.');
  }
}

function InstallFunction(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Define payload must be an object.');
  }

  const name = payload.name;
  const function_source = payload.function_source;

  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('Function name must be a non-empty string.');
  }

  if (typeof function_source !== 'string' || function_source.length === 0) {
    throw new Error('Function source must be a non-empty string.');
  }

  const installed_function = new Function('"use strict"; return (' + function_source + ');')();

  if (typeof installed_function !== 'function') {
    throw new Error('Provided worker function source does not evaluate to a function.');
  }

  worker_function_registry.set(name, installed_function);
}

async function ResolveModule(module_specifier) {
  try {
    return require(module_specifier);
  } catch (require_error) {
    try {
      return await import(module_specifier);
    } catch (import_error) {
      throw new Error(
        'Failed to load module "' + module_specifier + '". require error: ' + GetErrorMessage(require_error) + '. import error: ' + GetErrorMessage(import_error)
      );
    }
  }
}

async function InstallDependency(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Dependency payload must be an object.');
  }

  const alias = payload.alias;
  const module_specifier = payload.module_specifier;
  const export_name = payload.export_name;
  const is_default_export = payload.is_default_export === true;

  if (typeof alias !== 'string' || alias.length === 0) {
    throw new Error('Dependency alias must be a non-empty string.');
  }

  ValidateIdentifier(alias, 'Dependency alias');

  if (typeof module_specifier !== 'string' || module_specifier.length === 0) {
    throw new Error('Dependency module_specifier must be a non-empty string.');
  }

  if (typeof export_name !== 'undefined' && typeof export_name !== 'string') {
    throw new Error('Dependency export_name must be a string when provided.');
  }

  const resolved_module = await ResolveModule(module_specifier);
  let resolved_dependency;

  if (is_default_export) {
    resolved_dependency = resolved_module && resolved_module.default;
  } else if (typeof export_name === 'string' && export_name.length > 0) {
    resolved_dependency = resolved_module && resolved_module[export_name];
  } else {
    resolved_dependency = resolved_module;
  }

  if (typeof resolved_dependency === 'undefined') {
    throw new Error(
      'Dependency alias "' + alias + '" could not resolve requested export from "' + module_specifier + '".'
    );
  }

  worker_dependency_registry.set(alias, resolved_dependency);
}

function InstallConstant(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Constant payload must be an object.');
  }

  const name = payload.name;
  const value = payload.value;

  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('Constant name must be a non-empty string.');
  }

  ValidateIdentifier(name, 'Constant name');
  EnsureSerializable(value, 'Constant value');

  worker_constant_registry.set(name, value);
}

function ParseDependencyLookupInput(value) {
  if (typeof value === 'string') {
    if (value.length === 0) {
      throw new Error(
        'wpc_import(alias_or_params) requires a non-empty string alias.'
      );
    }

    return {
      alias: value
    };
  }

  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(
      'wpc_import(alias_or_params) requires either a string alias or an object with { alias }.'
    );
  }

  const alias = value.alias;
  if (typeof alias !== 'string' || alias.length === 0) {
    throw new Error(
      'wpc_import(alias_or_params) object input requires a non-empty string alias.'
    );
  }

  return {
    alias
  };
}

function ValidateDatabaseConnectorType(connector_type) {
  const supported_connector_types = new Set([
    'mongodb',
    'postgresql',
    'mariadb',
    'mysql',
    'sqlite'
  ]);

  if (!supported_connector_types.has(connector_type)) {
    throw new Error(
      'Database connector type must be one of: mongodb, postgresql, mariadb, mysql, sqlite.'
    );
  }
}

function ParseDatabaseConnectionLookupInput(value) {
  if (typeof value === 'string') {
    if (value.length === 0) {
      throw new Error(
        'wpc_database_connection(name_or_params) requires a non-empty string name.'
      );
    }

    return {
      name: value,
      connector_type_hint: null
    };
  }

  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(
      'wpc_database_connection(name_or_params) requires either a string name or an object with { name, type? }.'
    );
  }

  const name = value.name;
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error(
      'wpc_database_connection(name_or_params) object input requires a non-empty string name.'
    );
  }

  const connector_type_hint =
    typeof value.type === 'string' && value.type.length > 0
      ? value.type
      : null;

  if (connector_type_hint) {
    ValidateDatabaseConnectorType(connector_type_hint);
  }

  return {
    name,
    connector_type_hint
  };
}

function ParseSharedCreateInput(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('wpc_shared_create(params) requires an object params argument.');
  }

  if (typeof value.id !== 'string' || value.id.length === 0) {
    throw new Error('wpc_shared_create(params) requires params.id to be a non-empty string.');
  }

  if (
    value.type !== 'json' &&
    value.type !== 'text' &&
    value.type !== 'number' &&
    value.type !== 'binary'
  ) {
    throw new Error(
      'wpc_shared_create(params) requires params.type to be one of: json, text, number, binary.'
    );
  }

  if (typeof value.note !== 'undefined' && typeof value.note !== 'string') {
    throw new Error('wpc_shared_create(params) requires params.note to be a string when provided.');
  }

  return value;
}

function ParseSharedAccessInput(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('wpc_shared_access(params) requires an object params argument.');
  }

  if (typeof value.id !== 'string' || value.id.length === 0) {
    throw new Error('wpc_shared_access(params) requires params.id to be a non-empty string.');
  }

  if (
    typeof value.timeout_ms !== 'undefined' &&
    (!Number.isInteger(value.timeout_ms) || value.timeout_ms <= 0)
  ) {
    throw new Error('wpc_shared_access(params) requires params.timeout_ms to be a positive integer when provided.');
  }

  return value;
}

function ParseSharedWriteInput(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('wpc_shared_write(params) requires an object params argument.');
  }

  if (typeof value.id !== 'string' || value.id.length === 0) {
    throw new Error('wpc_shared_write(params) requires params.id to be a non-empty string.');
  }

  if (!Object.prototype.hasOwnProperty.call(value, 'content')) {
    throw new Error('wpc_shared_write(params) requires params.content.');
  }

  return value;
}

function ParseSharedReleaseInput(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('wpc_shared_release(params) requires an object params argument.');
  }

  if (typeof value.id !== 'string' || value.id.length === 0) {
    throw new Error('wpc_shared_release(params) requires params.id to be a non-empty string.');
  }

  return value;
}

function ParseSharedFreeInput(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('wpc_shared_free(params) requires an object params argument.');
  }

  if (typeof value.id !== 'string' || value.id.length === 0) {
    throw new Error('wpc_shared_free(params) requires params.id to be a non-empty string.');
  }

  if (
    typeof value.require_unlocked !== 'undefined' &&
    typeof value.require_unlocked !== 'boolean'
  ) {
    throw new Error('wpc_shared_free(params) requires params.require_unlocked to be a boolean when provided.');
  }

  return value;
}

function GetCurrentSharedCallRequestId() {
  const active_call_context = shared_call_context_storage.getStore();
  if (
    !active_call_context ||
    typeof active_call_context.call_request_id !== 'string' ||
    active_call_context.call_request_id.length === 0
  ) {
    throw new Error(
      'wpc_shared_* functions can only be used while handling an active worker call request.'
    );
  }

  return active_call_context.call_request_id;
}

async function SendSharedRequest(params) {
  const call_request_id = GetCurrentSharedCallRequestId();
  const shared_request_id = 'shared_' + String(next_shared_request_id);
  next_shared_request_id += 1;
  const timeout_ms =
    typeof params.timeout_ms === 'number' && Number.isInteger(params.timeout_ms)
      ? params.timeout_ms
      : 30000;

  if (timeout_ms <= 0) {
    throw new Error('Shared request timeout_ms must be a positive integer.');
  }

  return await new Promise(function(resolve, reject) {
    const timeout_handle = setTimeout(function() {
      const pending_waiter = shared_request_waiter_by_id.get(shared_request_id);
      if (!pending_waiter) {
        return;
      }

      shared_request_waiter_by_id.delete(shared_request_id);
      reject(
        new Error(
          'Shared request "' +
            params.command +
            '" timed out after ' +
            String(timeout_ms) +
            'ms.'
        )
      );
    }, timeout_ms);

    shared_request_waiter_by_id.set(shared_request_id, {
      resolve,
      reject,
      timeout_handle
    });

    const sent = SafePostMessage({
      message_type: 'shared_request',
      shared_request_id,
      command: params.command,
      call_request_id,
      payload: params.payload
    });

    if (!sent) {
      clearTimeout(timeout_handle);
      shared_request_waiter_by_id.delete(shared_request_id);
      reject(
        new Error(
          'Shared request "' + params.command + '" could not be posted to parent.'
        )
      );
    }
  });
}

function ToRecordOrEmpty(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }

  return value;
}

function BuildDatabaseConnectionContext() {
  const database_connections = {};

  for (const [connection_name, connection_entry] of worker_database_connection_registry.entries()) {
    database_connections[connection_name] = connection_entry.value;
  }

  return database_connections;
}

function TryAttachDatabaseConnectionLifecycleHooks(params) {
  const { name, connection_entry } = params;

  const handle_disconnect = function(event_name) {
    if (!worker_database_connection_registry.has(name)) {
      return;
    }

    worker_database_connection_registry.delete(name);

    EmitWorkerEvent({
      event_name: 'database_connection_lost',
      severity: 'warn',
      details: {
        name,
        connector_type: connection_entry.connector_type,
        lifecycle_event: event_name
      }
    });
  };

  const possible_event_sources = [];
  if (connection_entry.value && typeof connection_entry.value === 'object') {
    possible_event_sources.push(connection_entry.value);
  }

  if (
    connection_entry.value &&
    typeof connection_entry.value === 'object' &&
    connection_entry.value.client &&
    typeof connection_entry.value.client === 'object'
  ) {
    possible_event_sources.push(connection_entry.value.client);
  }

  for (const event_source of possible_event_sources) {
    if (!event_source || typeof event_source.on !== 'function') {
      continue;
    }

    event_source.on('close', function() {
      handle_disconnect('close');
    });

    event_source.on('end', function() {
      handle_disconnect('end');
    });

    event_source.on('error', function() {
      handle_disconnect('error');
    });
  }
}

async function CreateMongodbConnection(params) {
  const { semantics } = params;

  const uri = semantics.uri;
  if (typeof uri !== 'string' || uri.length === 0) {
    throw new Error('mongodb semantics.uri must be a non-empty string.');
  }

  const client_options = ToRecordOrEmpty(semantics.client_options);
  const database_name =
    typeof semantics.database_name === 'string' && semantics.database_name.length > 0
      ? semantics.database_name
      : null;

  const mongodb_module = await ResolveModule('mongodb');
  const MongoClient =
    mongodb_module.MongoClient ||
    (mongodb_module.default && mongodb_module.default.MongoClient);

  if (typeof MongoClient !== 'function') {
    throw new Error('mongodb module does not export MongoClient.');
  }

  const mongo_client = new MongoClient(uri, client_options);
  await mongo_client.connect();

  return {
    value: {
      client: mongo_client,
      db: database_name ? mongo_client.db(database_name) : null
    },
    close: async function() {
      await mongo_client.close();
    }
  };
}

async function CreatePostgresqlConnection(params) {
  const { semantics } = params;

  const postgresql_module = await ResolveModule('pg');
  const PoolClass =
    postgresql_module.Pool || (postgresql_module.default && postgresql_module.default.Pool);
  const ClientClass =
    postgresql_module.Client ||
    (postgresql_module.default && postgresql_module.default.Client);

  const connection_string =
    typeof semantics.connection_string === 'string' && semantics.connection_string.length > 0
      ? semantics.connection_string
      : null;
  const use_pool = semantics.use_pool !== false;

  if (use_pool) {
    const pool_options = ToRecordOrEmpty(semantics.pool_options);
    if (connection_string) {
      pool_options.connectionString = connection_string;
    }

    if (typeof PoolClass !== 'function') {
      throw new Error('pg module does not export Pool.');
    }

    const postgresql_pool = new PoolClass(pool_options);
    await postgresql_pool.query('SELECT 1');

    return {
      value: postgresql_pool,
      close: async function() {
        await postgresql_pool.end();
      }
    };
  }

  const client_options = ToRecordOrEmpty(semantics.client_options);
  if (connection_string) {
    client_options.connectionString = connection_string;
  }

  if (typeof ClientClass !== 'function') {
    throw new Error('pg module does not export Client.');
  }

  const postgresql_client = new ClientClass(client_options);
  await postgresql_client.connect();
  await postgresql_client.query('SELECT 1');

  return {
    value: postgresql_client,
    close: async function() {
      await postgresql_client.end();
    }
  };
}

async function CreateMysqlConnection(params) {
  const { semantics } = params;

  const mysql_module = await ResolveModule('mysql2/promise');
  const use_pool = semantics.use_pool !== false;

  if (use_pool) {
    const pool_options =
      Object.keys(ToRecordOrEmpty(semantics.pool_options)).length > 0
        ? ToRecordOrEmpty(semantics.pool_options)
        : ToRecordOrEmpty(semantics.connection_options);

    if (typeof mysql_module.createPool !== 'function') {
      throw new Error('mysql2/promise does not export createPool.');
    }

    const mysql_pool = mysql_module.createPool(pool_options);
    await mysql_pool.query('SELECT 1');

    return {
      value: mysql_pool,
      close: async function() {
        await mysql_pool.end();
      }
    };
  }

  const connection_options = ToRecordOrEmpty(semantics.connection_options);

  if (typeof mysql_module.createConnection !== 'function') {
    throw new Error('mysql2/promise does not export createConnection.');
  }

  const mysql_connection = await mysql_module.createConnection(connection_options);
  await mysql_connection.query('SELECT 1');

  return {
    value: mysql_connection,
    close: async function() {
      await mysql_connection.end();
    }
  };
}

async function CreateSqliteConnection(params) {
  const { semantics } = params;

  const filename =
    typeof semantics.filename === 'string' && semantics.filename.length > 0
      ? semantics.filename
      : ':memory:';
  const driver =
    typeof semantics.driver === 'string' && semantics.driver === 'sqlite3'
      ? 'sqlite3'
      : 'better-sqlite3';

  if (driver === 'sqlite3') {
    const sqlite3_module = await ResolveModule('sqlite3');
    const sqlite3_export = typeof sqlite3_module.verbose === 'function'
      ? sqlite3_module.verbose()
      : sqlite3_module;
    const SqliteDatabase =
      sqlite3_export.Database ||
      (sqlite3_export.default && sqlite3_export.default.Database);

    if (typeof SqliteDatabase !== 'function') {
      throw new Error('sqlite3 module does not export Database.');
    }

    const mode = typeof semantics.mode === 'number' ? semantics.mode : null;
    const sqlite_database = await new Promise(function(resolve, reject) {
      let opened_database;
      const open_callback = function(error) {
        if (error) {
          reject(error);
          return;
        }

        resolve(opened_database);
      };

      if (typeof mode === 'number') {
        opened_database = new SqliteDatabase(filename, mode, open_callback);
        return;
      }

      opened_database = new SqliteDatabase(filename, open_callback);
    });

    return {
      value: sqlite_database,
      close: async function() {
        await new Promise(function(resolve, reject) {
          sqlite_database.close(function(error) {
            if (error) {
              reject(error);
              return;
            }

            resolve();
          });
        });
      }
    };
  }

  const better_sqlite3_module = await ResolveModule('better-sqlite3');
  const BetterSqliteDatabase =
    better_sqlite3_module.default || better_sqlite3_module;

  if (typeof BetterSqliteDatabase !== 'function') {
    throw new Error('better-sqlite3 module did not resolve to a constructor.');
  }

  const sqlite_options = ToRecordOrEmpty(semantics.options);
  const better_sqlite_database = new BetterSqliteDatabase(filename, sqlite_options);

  return {
    value: better_sqlite_database,
    close: async function() {
      better_sqlite_database.close();
    }
  };
}

async function CreateDatabaseConnectionEntry(params) {
  const { name, connector_type, semantics } = params;

  if (connector_type === 'mongodb') {
    const mongodb_connection = await CreateMongodbConnection({ semantics });
    return {
      name,
      connector_type,
      semantics,
      value: mongodb_connection.value,
      close: mongodb_connection.close
    };
  }

  if (connector_type === 'postgresql') {
    const postgresql_connection = await CreatePostgresqlConnection({ semantics });
    return {
      name,
      connector_type,
      semantics,
      value: postgresql_connection.value,
      close: postgresql_connection.close
    };
  }

  if (connector_type === 'mariadb' || connector_type === 'mysql') {
    const mysql_connection = await CreateMysqlConnection({ semantics });
    return {
      name,
      connector_type,
      semantics,
      value: mysql_connection.value,
      close: mysql_connection.close
    };
  }

  if (connector_type === 'sqlite') {
    const sqlite_connection = await CreateSqliteConnection({ semantics });
    return {
      name,
      connector_type,
      semantics,
      value: sqlite_connection.value,
      close: sqlite_connection.close
    };
  }

  throw new Error('Unsupported database connector type: ' + connector_type);
}

async function CloseDatabaseConnectionEntry(params) {
  const { name, swallow_error } = params;
  const existing_entry = worker_database_connection_registry.get(name);
  if (!existing_entry) {
    return;
  }

  worker_database_connection_registry.delete(name);

  try {
    await existing_entry.close();
  } catch (error) {
    if (swallow_error) {
      EmitWorkerEvent({
        event_name: 'database_connection_close_failed',
        severity: 'warn',
        error: ToRemoteError(error),
        details: {
          name,
          connector_type: existing_entry.connector_type
        }
      });
      return;
    }

    throw new Error(
      'Database connection "' +
        name +
        '" (' +
        existing_entry.connector_type +
        ') failed to close: ' +
        GetErrorMessage(error)
    );
  }
}

async function ConnectDatabaseConnection(params) {
  const { name } = params;

  const existing_entry = worker_database_connection_registry.get(name);
  if (existing_entry) {
    return existing_entry.value;
  }

  const existing_connect_promise =
    worker_database_connection_connect_promise_by_name.get(name);
  if (existing_connect_promise) {
    return await existing_connect_promise;
  }

  const connect_promise = (async function() {
    const definition = worker_database_connection_definition_registry.get(name);
    if (!definition) {
      throw new Error('Database connection "' + name + '" is not defined on this worker.');
    }

    try {
      const connection_entry = await CreateDatabaseConnectionEntry({
        name,
        connector_type: definition.connector_type,
        semantics: definition.semantics
      });

      worker_database_connection_registry.set(name, connection_entry);
      TryAttachDatabaseConnectionLifecycleHooks({
        name,
        connection_entry
      });

      return connection_entry.value;
    } catch (error) {
      worker_database_connection_registry.delete(name);
      throw new Error(
        'Database connection "' +
          name +
          '" (' +
          definition.connector_type +
          ') failed: ' +
          GetErrorMessage(error)
      );
    }
  })();

  worker_database_connection_connect_promise_by_name.set(name, connect_promise);

  try {
    return await connect_promise;
  } finally {
    worker_database_connection_connect_promise_by_name.delete(name);
  }
}

async function InstallDatabaseConnection(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Database connection payload must be an object.');
  }

  const name = payload.name;
  const connector = payload.connector;

  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('Database connection name must be a non-empty string.');
  }

  ValidateIdentifier(name, 'Database connection name');

  if (!connector || typeof connector !== 'object') {
    throw new Error('Database connector must be an object.');
  }

  const connector_type = connector.type;
  if (typeof connector_type !== 'string') {
    throw new Error('Database connector.type must be a string.');
  }

  ValidateDatabaseConnectorType(connector_type);

  const semantics = connector.semantics;
  if (!semantics || typeof semantics !== 'object' || Array.isArray(semantics)) {
    throw new Error('Database connector.semantics must be an object.');
  }

  worker_database_connection_definition_registry.set(name, {
    name,
    connector_type,
    semantics
  });

  await CloseDatabaseConnectionEntry({
    name,
    swallow_error: true
  });
  await ConnectDatabaseConnection({ name });
}

async function RemoveDatabaseConnection(params) {
  const { name } = params;

  if (worker_database_connection_connect_promise_by_name.has(name)) {
    try {
      await worker_database_connection_connect_promise_by_name.get(name);
    } catch {
      // Ignore failed in-flight connect attempts during removal.
    }
  }

  worker_database_connection_definition_registry.delete(name);
  await CloseDatabaseConnectionEntry({
    name,
    swallow_error: true
  });
}

async function CloseAllDatabaseConnections() {
  const connection_names = Array.from(worker_database_connection_registry.keys());
  for (const connection_name of connection_names) {
    await CloseDatabaseConnectionEntry({
      name: connection_name,
      swallow_error: true
    });
  }
}

globalThis.wpc_import = async function(alias_or_params) {
  const lookup_input = ParseDependencyLookupInput(alias_or_params);
  const alias = lookup_input.alias;

  if (!worker_dependency_registry.has(alias)) {
    throw new Error('Dependency alias "' + alias + '" is not defined on this worker.');
  }

  return worker_dependency_registry.get(alias);
};

globalThis.wpc_constant = function(name) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('wpc_constant(name) requires a non-empty string name.');
  }

  if (!worker_constant_registry.has(name)) {
    throw new Error('Constant "' + name + '" is not defined on this worker.');
  }

  return worker_constant_registry.get(name);
};

globalThis.wpc_database_connection = async function(name_or_params) {
  const lookup_input = ParseDatabaseConnectionLookupInput(name_or_params);
  const name = lookup_input.name;

  const definition = worker_database_connection_definition_registry.get(name);
  if (!definition) {
    throw new Error('Database connection "' + name + '" is not defined on this worker.');
  }

  if (
    lookup_input.connector_type_hint &&
    definition.connector_type !== lookup_input.connector_type_hint
  ) {
    throw new Error(
      'Database connection "' +
        name +
        '" is defined as type "' +
        definition.connector_type +
        '" but lookup requested type "' +
        lookup_input.connector_type_hint +
        '".'
    );
  }

  return await ConnectDatabaseConnection({ name });
};

globalThis.wpc_shared_create = async function(params) {
  const parsed_params = ParseSharedCreateInput(params);
  await SendSharedRequest({
    command: 'shared_create',
    payload: parsed_params
  });
};

globalThis.wpc_shared_access = async function(params) {
  const parsed_params = ParseSharedAccessInput(params);
  return await SendSharedRequest({
    command: 'shared_access',
    payload: parsed_params,
    timeout_ms: parsed_params.timeout_ms
  });
};

globalThis.wpc_shared_write = async function(params) {
  const parsed_params = ParseSharedWriteInput(params);
  await SendSharedRequest({
    command: 'shared_write',
    payload: parsed_params
  });
};

globalThis.wpc_shared_release = async function(params) {
  const parsed_params = ParseSharedReleaseInput(params);
  await SendSharedRequest({
    command: 'shared_release',
    payload: parsed_params
  });
};

globalThis.wpc_shared_free = async function(params) {
  const parsed_params = ParseSharedFreeInput(params);
  await SendSharedRequest({
    command: 'shared_free',
    payload: parsed_params
  });
};

async function HandleControlRequest(message) {
  const control_request_id = message.control_request_id;

  if (typeof control_request_id !== 'string' || control_request_id.length === 0) {
    EmitWorkerEvent({
      event_name: 'malformed_control_request',
      severity: 'warn',
      details: {
        reason: 'control_request_id missing or invalid'
      }
    });

    return;
  }

  try {
    if (message.command === 'define_function') {
      InstallFunction(message.payload);
    } else if (message.command === 'undefine_function') {
      const function_name = message.payload && message.payload.name;
      if (typeof function_name !== 'string' || function_name.length === 0) {
        throw new Error('Function name is required for undefine.');
      }

      worker_function_registry.delete(function_name);
    } else if (message.command === 'define_dependency') {
      await InstallDependency(message.payload);
    } else if (message.command === 'undefine_dependency') {
      const dependency_alias = message.payload && message.payload.alias;
      if (typeof dependency_alias !== 'string' || dependency_alias.length === 0) {
        throw new Error('Dependency alias is required for undefine.');
      }

      worker_dependency_registry.delete(dependency_alias);
    } else if (message.command === 'define_constant') {
      InstallConstant(message.payload);
    } else if (message.command === 'undefine_constant') {
      const constant_name = message.payload && message.payload.name;
      if (typeof constant_name !== 'string' || constant_name.length === 0) {
        throw new Error('Constant name is required for undefine.');
      }

      worker_constant_registry.delete(constant_name);
    } else if (message.command === 'define_database_connection') {
      await InstallDatabaseConnection(message.payload);
    } else if (message.command === 'undefine_database_connection') {
      const database_connection_name = message.payload && message.payload.name;
      if (
        typeof database_connection_name !== 'string' ||
        database_connection_name.length === 0
      ) {
        throw new Error('Database connection name is required for undefine.');
      }

      await RemoveDatabaseConnection({
        name: database_connection_name
      });
    } else if (message.command === 'shutdown') {
      SafePostMessage({
        message_type: 'control_response',
        control_request_id,
        ok: true
      });

      EmitWorkerEvent({
        event_name: 'worker_shutdown_requested',
        severity: 'info',
        correlation_id: control_request_id
      });

      setImmediate(function() {
        for (const [shared_request_id, pending_waiter] of shared_request_waiter_by_id.entries()) {
          clearTimeout(pending_waiter.timeout_handle);
          pending_waiter.reject(
            new Error(
              'Worker shutting down before shared request "' +
                shared_request_id +
                '" could complete.'
            )
          );
          shared_request_waiter_by_id.delete(shared_request_id);
        }

        void CloseAllDatabaseConnections().finally(function() {
          process.exit(0);
        });
      });

      return;
    } else {
      throw new Error('Unknown control command: ' + String(message.command));
    }

    SafePostMessage({
      message_type: 'control_response',
      control_request_id,
      ok: true
    });
  } catch (error) {
    EmitWorkerEvent({
      event_name: 'control_command_failed',
      severity: 'error',
      correlation_id: control_request_id,
      error: ToRemoteError(error),
      details: {
        command: message.command
      }
    });

    SafePostMessage({
      message_type: 'control_response',
      control_request_id,
      ok: false,
      error: ToRemoteError(error)
    });
  }
}

async function HandleCallRequest(message) {
  const request_id = message.request_id;
  const function_name = message.function_name;

  if (typeof request_id !== 'string' || request_id.length === 0) {
    EmitWorkerEvent({
      event_name: 'malformed_call_request',
      severity: 'warn',
      details: {
        reason: 'request_id missing or invalid'
      }
    });

    return;
  }

  if (typeof function_name !== 'string' || function_name.length === 0) {
    EmitWorkerEvent({
      event_name: 'malformed_call_request',
      severity: 'warn',
      correlation_id: request_id,
      details: {
        reason: 'function_name missing or invalid'
      }
    });

    SafePostMessage({
      message_type: 'call_response',
      request_id,
      ok: false,
      error: {
        name: 'Error',
        message: 'Malformed call request: function_name is required.'
      }
    });

    return;
  }

  const worker_function = worker_function_registry.get(function_name);

  if (!worker_function) {
    EmitWorkerEvent({
      event_name: 'call_target_missing',
      severity: 'warn',
      correlation_id: request_id,
      details: {
        function_name
      }
    });

    SafePostMessage({
      message_type: 'call_response',
      request_id,
      ok: false,
      error: {
        name: 'Error',
        message: 'Worker function "' + function_name + '" is not defined.'
      }
    });

    return;
  }

  try {
    const call_args = Array.isArray(message.call_args)
      ? message.call_args
      : [message.params];

    const function_context = {
      dependencies: Object.fromEntries(worker_dependency_registry.entries()),
      constants: Object.fromEntries(worker_constant_registry.entries()),
      database_connections: BuildDatabaseConnectionContext()
    };

    const return_value = await shared_call_context_storage.run(
      {
        call_request_id: request_id
      },
      async function() {
        return await worker_function(...call_args, function_context);
      }
    );
    EnsureSerializable(return_value, 'Worker return value');

    SafePostMessage({
      message_type: 'call_response',
      request_id,
      ok: true,
      return_value
    });
  } catch (error) {
    EmitWorkerEvent({
      event_name: 'call_execution_failed',
      severity: 'error',
      correlation_id: request_id,
      error: ToRemoteError(error),
      details: {
        function_name
      }
    });

    SafePostMessage({
      message_type: 'call_response',
      request_id,
      ok: false,
      error: ToRemoteError(error)
    });
  }
}

function HandleSharedResponseMessage(message) {
  const shared_request_id = message.shared_request_id;
  if (typeof shared_request_id !== 'string' || shared_request_id.length === 0) {
    EmitWorkerEvent({
      event_name: 'malformed_shared_response',
      severity: 'warn',
      details: {
        reason: 'shared_request_id missing or invalid'
      }
    });
    return;
  }

  const pending_waiter = shared_request_waiter_by_id.get(shared_request_id);
  if (!pending_waiter) {
    return;
  }

  shared_request_waiter_by_id.delete(shared_request_id);
  clearTimeout(pending_waiter.timeout_handle);

  if (message.ok) {
    pending_waiter.resolve(message.return_value);
    return;
  }

  const remote_error = message.error;
  const shared_request_error = new Error(
    remote_error && typeof remote_error.message === 'string'
      ? remote_error.message
      : 'Shared request failed.'
  );

  if (remote_error && typeof remote_error.name === 'string') {
    shared_request_error.name = remote_error.name;
  }

  if (remote_error && typeof remote_error.stack === 'string') {
    shared_request_error.stack = remote_error.stack;
  }

  pending_waiter.reject(shared_request_error);
}

async function HandleParentMessage(message) {
  if (!message || typeof message !== 'object') {
    EmitWorkerEvent({
      event_name: 'malformed_parent_message',
      severity: 'warn',
      details: {
        reason: 'message is not an object'
      }
    });

    return;
  }

  const message_type = message.message_type;

  if (message_type === 'control_request') {
    await HandleControlRequest(message);
    return;
  }

  if (message_type === 'call_request') {
    await HandleCallRequest(message);
    return;
  }

  if (message_type === 'shared_response') {
    HandleSharedResponseMessage(message);
    return;
  }

  EmitWorkerEvent({
    event_name: 'unknown_message_type',
    severity: 'warn',
    details: {
      message_type: typeof message_type === 'string' ? message_type : null
    }
  });
}

parentPort.on('message', function(message) {
  void HandleParentMessage(message).catch(function(error) {
    EmitWorkerEvent({
      event_name: 'message_handler_failure',
      severity: 'error',
      error: ToRemoteError(error)
    });
  });
});

process.on('uncaughtException', function(error) {
  EmitWorkerEvent({
    event_name: 'uncaught_exception',
    severity: 'error',
    error: ToRemoteError(error)
  });
});

process.on('unhandledRejection', function(reason) {
  EmitWorkerEvent({
    event_name: 'unhandled_rejection',
    severity: 'error',
    error: ToRemoteError(reason)
  });
});

SafePostMessage({ message_type: 'ready' });
EmitWorkerEvent({
  event_name: 'worker_ready',
  severity: 'info',
  details: {
    pid: process.pid
  }
});
`;
}

export class WorkerProcedureCall {
  private lifecycle_state: lifecycle_state_t = 'stopped';
  private target_worker_count = 0;
  private shared_memory_store = new WorkerProcedureCallSharedMemoryStore();
  private cluster_authorization_policy_engine = new ClusterAuthorizationPolicyEngine();
  private cluster_call_routing_strategy = new ClusterCallRoutingStrategy();
  private cluster_call_node_registration_by_id = new Map<
    string,
    cluster_call_node_registration_t
  >();
  private cluster_call_routing_metrics: cluster_call_routing_metrics_t = {
    call_requests_total: 0,
    routed_local_total: 0,
    routed_remote_total: 0,
    routing_mode_auto_total: 0,
    routing_mode_target_node_total: 0,
    routing_mode_affinity_total: 0,
    filtered_unhealthy_node_total: 0,
    filtered_capability_node_total: 0,
    filtered_zone_node_total: 0,
    filtered_authorization_node_total: 0,
    failover_attempt_total: 0,
    no_capable_node_total: 0,
    dispatch_retryable_total: 0
  };
  private cluster_operation_lifecycle_event_history: cluster_operation_lifecycle_event_t[] =
    [];
  private operations_event_history_max_count = 2_000;
  private control_plane_state_store: cluster_control_plane_state_store_i =
    new ClusterControlPlaneInMemoryStateStore();
  private control_plane_durable_state_enabled = false;
  private control_plane_state_file_path: string | null = null;
  private control_plane_audit_retention_ms: number | null = null;
  private control_plane_audit_max_record_count: number | null = null;
  private cluster_mutation_node_registration_by_id = new Map<
    string,
    cluster_mutation_node_registration_t
  >();
  private cluster_entity_version_by_name = new Map<string, string>();
  private cluster_mutation_dedupe_record_by_id = new Map<
    string,
    cluster_mutation_dedupe_record_t
  >();
  private cluster_admin_mutation_audit_record_by_mutation_id = new Map<
    string,
    cluster_admin_mutation_audit_record_t
  >();
  private cluster_admin_mutation_metrics: cluster_admin_mutation_metrics_t = {
    admin_mutation_requests_total: 0,
    admin_mutation_success_total: 0,
    admin_mutation_failure_total: 0,
    admin_mutation_rollout_duration_ms: [],
    admin_mutation_rollback_total: 0,
    admin_mutation_authz_denied_total: 0
  };
  private mutation_governance_policy: mutation_governance_policy_t = {
    require_change_reason: false,
    require_change_ticket_id: false,
    enforce_dual_authorization: false,
    break_glass_mutation_type_list: ['shared_clear_all_chunks']
  };

  private worker_state_by_id = new Map<number, worker_state_t>();
  private function_definition_by_name = new Map<string, worker_function_definition_t>();
  private dependency_definition_by_alias = new Map<
    string,
    worker_dependency_definition_t
  >();
  private constant_definition_by_name = new Map<string, worker_constant_definition_t>();
  private database_connection_definition_by_name = new Map<
    string,
    worker_database_connection_definition_t
  >();

  private pending_call_by_request_id = new Map<string, pending_call_record_t>();
  private pending_control_by_request_id = new Map<
    string,
    pending_control_record_t
  >();

  private ready_waiter_by_worker_id = new Map<number, worker_ready_waiter_t>();
  private exit_waiter_by_worker_id = new Map<number, worker_exit_waiter_t>();
  private last_error_by_worker_id = new Map<number, Error>();
  private worker_event_listener_by_id = new Map<number, worker_event_listener_t>();

  private next_worker_id = 1;
  private next_call_request_id = 1;
  private next_control_request_id = 1;
  private next_worker_event_id = 1;
  private next_worker_event_listener_id = 1;
  private round_robin_index = 0;

  private default_call_timeout_ms = 30_000;
  private default_control_timeout_ms = 10_000;
  private default_start_timeout_ms = 10_000;
  private default_stop_timeout_ms = 10_000;
  private restart_on_failure = true;
  private max_restarts_per_worker = 3;
  private max_pending_calls_per_worker = 10_000;
  private restart_base_delay_ms = 100;
  private restart_max_delay_ms = 5_000;
  private restart_jitter_ms = 100;
  private mutation_dedupe_retention_ms = 300_000;
  private mutation_retry_max_attempts = 3;
  private mutation_retry_base_delay_ms = 25;
  private mutation_retry_max_delay_ms = 500;

  private stop_in_progress_promise: Promise<void> | null = null;

  public readonly call: worker_call_proxy_t;

  constructor(params: workerprocedurecall_constructor_params_t = {}) {
    this.applyRuntimeOptions({ options: params });
    this.configureControlPlaneStatePersistence({ options: params });
    this.call = this.createCallProxy();
  }

  setClusterAuthorizationPolicyList(params: {
    policy_list: cluster_authorization_policy_t[];
  }): void {
    this.cluster_authorization_policy_engine.setPolicyList({
      policy_list: params.policy_list
    });
  }

  getClusterAuthorizationPolicyList(): cluster_authorization_policy_t[] {
    return this.cluster_authorization_policy_engine.getPolicyList();
  }

  registerClusterCallNode(params: register_cluster_call_node_params_t): void {
    const {
      node,
      call_executor,
      initial_health_state = 'ready',
      initial_metrics,
      capability_list,
      last_health_update_unix_ms = Date.now()
    } = params;

    if (!IsRecordObject(node)) {
      throw new Error('node must be an object.');
    }

    if (typeof node.node_id !== 'string' || node.node_id.length === 0) {
      throw new Error('node.node_id must be a non-empty string.');
    }

    if (typeof call_executor !== 'function') {
      throw new Error('call_executor must be a function.');
    }

    const normalized_capability_list = Array.isArray(capability_list)
      ? capability_list.filter((capability): capability is cluster_call_node_capability_t => {
          return (
            IsRecordObject(capability) &&
            typeof capability.function_name === 'string' &&
            capability.function_name.length > 0 &&
            typeof capability.function_hash_sha1 === 'string' &&
            capability.function_hash_sha1.length > 0 &&
            typeof capability.installed === 'boolean'
          );
        })
      : [];

    const normalized_metrics = this.normalizeClusterCallNodeMetrics({
      metrics: initial_metrics
    });

    this.cluster_call_node_registration_by_id.set(node.node_id, {
      node_id: node.node_id,
      labels: node.labels ? { ...node.labels } : undefined,
      zones: Array.isArray(node.zones) ? [...node.zones] : undefined,
      node_agent_semver: node.node_agent_semver,
      runtime_semver: node.runtime_semver,
      call_executor,
      health_state: initial_health_state,
      last_health_update_unix_ms,
      metrics: normalized_metrics,
      capability_list: normalized_capability_list,
      runtime_inflight_call_count: 0
    });
  }

  unregisterClusterCallNode(params: { node_id: string }): void {
    this.cluster_call_node_registration_by_id.delete(params.node_id);
  }

  clearClusterCallNodes(): void {
    this.cluster_call_node_registration_by_id.clear();
  }

  getClusterCallNodes(): (cluster_call_node_descriptor_t & {
    health_state: cluster_call_node_health_state_t;
    last_health_update_unix_ms: number;
    metrics: cluster_call_node_metrics_t;
    capability_list: cluster_call_node_capability_t[];
  })[] {
    return Array.from(this.cluster_call_node_registration_by_id.values())
      .map((node) => {
        return {
          node_id: node.node_id,
          labels: node.labels ? { ...node.labels } : undefined,
          zones: node.zones ? [...node.zones] : undefined,
          node_agent_semver: node.node_agent_semver,
          runtime_semver: node.runtime_semver,
          health_state: node.health_state,
          last_health_update_unix_ms: node.last_health_update_unix_ms,
          metrics: { ...node.metrics },
          capability_list: node.capability_list.map((capability) => {
            return { ...capability };
          })
        };
      })
      .sort((left_node, right_node): number => {
        return left_node.node_id.localeCompare(right_node.node_id);
      });
  }

  recordClusterCallNodeHeartbeat(params: {
    heartbeat: node_heartbeat_message_i;
  }): void {
    const { heartbeat } = params;
    const registration = this.cluster_call_node_registration_by_id.get(heartbeat.node_id);
    if (!registration) {
      return;
    }

    registration.health_state = heartbeat.health_state;
    registration.last_health_update_unix_ms = heartbeat.timestamp_unix_ms;
    registration.metrics = this.normalizeClusterCallNodeMetrics({
      metrics: heartbeat.metrics
    });
  }

  recordClusterCallNodeCapabilities(params: {
    announcement: node_capability_announce_message_i;
  }): void {
    const { announcement } = params;
    const registration = this.cluster_call_node_registration_by_id.get(announcement.node_id);
    if (!registration) {
      return;
    }

    registration.capability_list = announcement.capabilities
      .filter((capability): capability is cluster_call_node_capability_t => {
        return (
          typeof capability.function_name === 'string' &&
          capability.function_name.length > 0 &&
          typeof capability.function_hash_sha1 === 'string' &&
          capability.function_hash_sha1.length > 0 &&
          typeof capability.installed === 'boolean'
        );
      })
      .map((capability) => {
        return {
          function_name: capability.function_name,
          function_hash_sha1: capability.function_hash_sha1,
          installed: capability.installed
        };
      });
  }

  setClusterCallRoutingPolicy(params: {
    policy: Partial<cluster_call_routing_policy_t>;
  }): void {
    this.cluster_call_routing_strategy.setRoutingConfig({
      routing_config: params.policy
    });
  }

  getClusterCallRoutingPolicy(): cluster_call_routing_policy_t {
    return this.cluster_call_routing_strategy.getRoutingConfig();
  }

  getClusterCallRoutingMetrics(): cluster_call_routing_metrics_t {
    return {
      ...this.cluster_call_routing_metrics
    };
  }

  getClusterOperationLifecycleEvents(params?: {
    limit?: number;
    event_name?: cluster_operation_lifecycle_event_name_t;
    request_id?: string;
    mutation_id?: string;
  }): cluster_operation_lifecycle_event_t[] {
    let event_list = [...this.cluster_operation_lifecycle_event_history];

    if (typeof params?.event_name === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.event_name === params.event_name;
      });
    }

    if (typeof params?.request_id === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.request_id === params.request_id;
      });
    }

    if (typeof params?.mutation_id === 'string') {
      event_list = event_list.filter((event): boolean => {
        return event.mutation_id === params.mutation_id;
      });
    }

    if (typeof params?.limit === 'number' && params.limit >= 0) {
      event_list = event_list.slice(Math.max(0, event_list.length - params.limit));
    }

    return event_list.map((event): cluster_operation_lifecycle_event_t => {
      return {
        ...event,
        details: event.details ? { ...event.details } : undefined
      };
    });
  }

  clearClusterOperationLifecycleEvents(): void {
    this.cluster_operation_lifecycle_event_history = [];
  }

  getClusterOperationsObservabilitySnapshot(): cluster_operations_observability_snapshot_t {
    const worker_health_count_by_state: Record<worker_health_state_t, number> = {
      starting: 0,
      ready: 0,
      degraded: 0,
      restarting: 0,
      stopped: 0
    };
    const health_state_count_by_node: Record<cluster_call_node_health_state_t, number> = {
      ready: 0,
      degraded: 0,
      restarting: 0,
      stopped: 0
    };

    let pending_call_count = 0;
    let pending_control_count = 0;

    for (const worker_state of this.worker_state_by_id.values()) {
      worker_health_count_by_state[worker_state.health_state] += 1;
      pending_call_count += worker_state.pending_call_request_ids.size;
      pending_control_count += worker_state.pending_control_request_ids.size;
    }

    for (const call_node of this.cluster_call_node_registration_by_id.values()) {
      health_state_count_by_node[call_node.health_state] += 1;
    }

    return {
      captured_at_unix_ms: Date.now(),
      call_routing_metrics: this.getClusterCallRoutingMetrics(),
      mutation_metrics: this.getClusterAdminMutationMetrics(),
      worker_health_summary: {
        lifecycle_state: this.lifecycle_state,
        worker_count: this.worker_state_by_id.size,
        worker_health_count_by_state,
        pending_call_count,
        pending_control_count
      },
      cluster_call_node_summary: {
        registered_node_count: this.cluster_call_node_registration_by_id.size,
        health_count_by_state: health_state_count_by_node
      },
      mutation_node_count: this.cluster_mutation_node_registration_by_id.size
    };
  }

  registerClusterMutationNode(
    params: register_cluster_mutation_node_params_t
  ): void {
    const { node, mutation_executor } = params;
    if (!IsRecordObject(node)) {
      throw new Error('node must be an object.');
    }

    if (typeof node.node_id !== 'string' || node.node_id.length === 0) {
      throw new Error('node.node_id must be a non-empty string.');
    }

    if (typeof mutation_executor !== 'function') {
      throw new Error('mutation_executor must be a function.');
    }

    this.cluster_mutation_node_registration_by_id.set(node.node_id, {
      node_id: node.node_id,
      labels: node.labels ? { ...node.labels } : undefined,
      zones: Array.isArray(node.zones) ? [...node.zones] : undefined,
      node_agent_semver: node.node_agent_semver,
      runtime_semver: node.runtime_semver,
      mutation_executor
    });
  }

  unregisterClusterMutationNode(params: { node_id: string }): void {
    const { node_id } = params;
    this.cluster_mutation_node_registration_by_id.delete(node_id);
  }

  clearClusterMutationNodes(): void {
    this.cluster_mutation_node_registration_by_id.clear();
  }

  getClusterMutationNodes(): cluster_mutation_node_descriptor_t[] {
    return Array.from(this.cluster_mutation_node_registration_by_id.values())
      .map((node): cluster_mutation_node_descriptor_t => {
        return {
          node_id: node.node_id,
          labels: node.labels ? { ...node.labels } : undefined,
          zones: node.zones ? [...node.zones] : undefined,
          node_agent_semver: node.node_agent_semver,
          runtime_semver: node.runtime_semver
        };
      })
      .sort((left_node, right_node): number => {
        return left_node.node_id.localeCompare(right_node.node_id);
      });
  }

  setMutationGovernancePolicy(params: {
    policy: Partial<mutation_governance_policy_t>;
  }): void {
    const { policy } = params;
    const next_break_glass_mutation_type_list = Array.isArray(
      policy.break_glass_mutation_type_list
    )
      ? policy.break_glass_mutation_type_list.filter((item): item is string => {
          return typeof item === 'string' && item.length > 0;
        })
      : this.mutation_governance_policy.break_glass_mutation_type_list;

    this.mutation_governance_policy = {
      require_change_reason:
        typeof policy.require_change_reason === 'boolean'
          ? policy.require_change_reason
          : this.mutation_governance_policy.require_change_reason,
      require_change_ticket_id:
        typeof policy.require_change_ticket_id === 'boolean'
          ? policy.require_change_ticket_id
          : this.mutation_governance_policy.require_change_ticket_id,
      enforce_dual_authorization:
        typeof policy.enforce_dual_authorization === 'boolean'
          ? policy.enforce_dual_authorization
          : this.mutation_governance_policy.enforce_dual_authorization,
      break_glass_mutation_type_list: next_break_glass_mutation_type_list
    };
  }

  getMutationGovernancePolicy(): mutation_governance_policy_t {
    return {
      require_change_reason: this.mutation_governance_policy.require_change_reason,
      require_change_ticket_id: this.mutation_governance_policy.require_change_ticket_id,
      enforce_dual_authorization:
        this.mutation_governance_policy.enforce_dual_authorization,
      break_glass_mutation_type_list: [
        ...this.mutation_governance_policy.break_glass_mutation_type_list
      ]
    };
  }

  getClusterAdminMutationMetrics(): cluster_admin_mutation_metrics_t {
    return {
      admin_mutation_requests_total:
        this.cluster_admin_mutation_metrics.admin_mutation_requests_total,
      admin_mutation_success_total:
        this.cluster_admin_mutation_metrics.admin_mutation_success_total,
      admin_mutation_failure_total:
        this.cluster_admin_mutation_metrics.admin_mutation_failure_total,
      admin_mutation_rollout_duration_ms: [
        ...this.cluster_admin_mutation_metrics.admin_mutation_rollout_duration_ms
      ],
      admin_mutation_rollback_total:
        this.cluster_admin_mutation_metrics.admin_mutation_rollback_total,
      admin_mutation_authz_denied_total:
        this.cluster_admin_mutation_metrics.admin_mutation_authz_denied_total
    };
  }

  getClusterAdminMutationAuditRecords(params?: {
    mutation_id?: string;
    limit?: number;
  }): cluster_admin_mutation_audit_record_t[] {
    if (params?.mutation_id) {
      const audit_record = this.cluster_admin_mutation_audit_record_by_mutation_id.get(
        params.mutation_id
      );
      if (!audit_record) {
        return [];
      }
      return [structuredClone(audit_record)];
    }

    const sorted_record_list = Array.from(
      this.cluster_admin_mutation_audit_record_by_mutation_id.values()
    ).sort((left_record, right_record): number => {
      return right_record.timestamp_unix_ms - left_record.timestamp_unix_ms;
    });

    const limited_record_list =
      typeof params?.limit === 'number' && params.limit >= 0
        ? sorted_record_list.slice(0, params.limit)
        : sorted_record_list;

    return limited_record_list.map((record): cluster_admin_mutation_audit_record_t => {
      return structuredClone(record);
    });
  }

  getControlPlaneStatePersistenceInfo(): {
    durable_state_enabled: boolean;
    state_file_path: string | null;
    dedupe_retention_ms: number;
    audit_retention_ms: number | null;
    audit_max_record_count: number | null;
  } {
    return {
      durable_state_enabled: this.control_plane_durable_state_enabled,
      state_file_path: this.control_plane_state_file_path,
      dedupe_retention_ms: this.mutation_dedupe_retention_ms,
      audit_retention_ms: this.control_plane_audit_retention_ms,
      audit_max_record_count: this.control_plane_audit_max_record_count
    };
  }

  compactControlPlaneState(params: {
    now_unix_ms?: number;
  } = {}): {
    removed_dedupe_record_count: number;
    removed_audit_record_count: number;
  } {
    const now_unix_ms = params.now_unix_ms ?? Date.now();
    const previous_dedupe_record_count = this.cluster_mutation_dedupe_record_by_id.size;
    const previous_audit_record_count =
      this.cluster_admin_mutation_audit_record_by_mutation_id.size;

    this.runControlPlaneStateCompaction({
      now_unix_ms
    });

    return {
      removed_dedupe_record_count:
        previous_dedupe_record_count - this.cluster_mutation_dedupe_record_by_id.size,
      removed_audit_record_count:
        previous_audit_record_count -
        this.cluster_admin_mutation_audit_record_by_mutation_id.size
    };
  }

  private configureControlPlaneStatePersistence(params: {
    options: runtime_options_t;
  }): void {
    const { options } = params;

    if (
      typeof options.control_plane_state_file_path === 'string' &&
      options.control_plane_state_file_path.length === 0
    ) {
      throw new Error('control_plane_state_file_path must be a non-empty string.');
    }

    if (typeof options.control_plane_audit_retention_ms === 'number') {
      ValidateNonNegativeInteger({
        value: options.control_plane_audit_retention_ms,
        label: 'control_plane_audit_retention_ms'
      });
      this.control_plane_audit_retention_ms = options.control_plane_audit_retention_ms;
    }

    if (typeof options.control_plane_audit_max_record_count === 'number') {
      ValidatePositiveInteger({
        value: options.control_plane_audit_max_record_count,
        label: 'control_plane_audit_max_record_count'
      });
      this.control_plane_audit_max_record_count =
        options.control_plane_audit_max_record_count;
    }

    if (options.control_plane_state_store) {
      this.control_plane_state_store = options.control_plane_state_store;
      this.control_plane_durable_state_enabled = true;
    } else {
      const durable_state_enabled =
        options.control_plane_durable_state_enabled === true ||
        typeof options.control_plane_state_file_path === 'string';

      if (durable_state_enabled) {
        const file_path =
          typeof options.control_plane_state_file_path === 'string' &&
          options.control_plane_state_file_path.length > 0
            ? options.control_plane_state_file_path
            : JoinPath(
                process.cwd(),
                '.workerprocedurecall',
                'control_plane_state.json'
              );

        this.control_plane_state_store = new ClusterControlPlaneFileStateStore({
          file_path
        });
        this.control_plane_durable_state_enabled = true;
        this.control_plane_state_file_path = file_path;
      }
    }

    const loaded_state = this.control_plane_state_store.loadState();
    this.hydrateControlPlaneStateFromSnapshot({
      state: loaded_state
    });

    this.runControlPlaneStateCompaction({
      now_unix_ms: Date.now()
    });
  }

  private buildControlPlaneRetentionPolicy(): cluster_control_plane_retention_policy_t {
    const retention_policy: cluster_control_plane_retention_policy_t = {
      dedupe_retention_ms: this.mutation_dedupe_retention_ms
    };

    if (typeof this.control_plane_audit_retention_ms === 'number') {
      retention_policy.audit_retention_ms = this.control_plane_audit_retention_ms;
    }

    if (typeof this.control_plane_audit_max_record_count === 'number') {
      retention_policy.audit_max_record_count = this.control_plane_audit_max_record_count;
    }

    return retention_policy;
  }

  private buildControlPlaneStateSnapshot(params?: {
    now_unix_ms?: number;
  }): cluster_control_plane_state_snapshot_t {
    const dedupe_record_by_id: Record<string, cluster_control_plane_dedupe_record_t> = {};
    for (const [mutation_id, dedupe_record] of this.cluster_mutation_dedupe_record_by_id) {
      dedupe_record_by_id[mutation_id] = structuredClone(dedupe_record);
    }

    const entity_version_by_name: Record<string, string> = {};
    for (const [entity_name, entity_version] of this.cluster_entity_version_by_name) {
      entity_version_by_name[entity_name] = entity_version;
    }

    const audit_record_by_mutation_id: Record<
      string,
      cluster_control_plane_audit_record_t
    > = {};
    for (const [mutation_id, audit_record] of this
      .cluster_admin_mutation_audit_record_by_mutation_id) {
      audit_record_by_mutation_id[mutation_id] = structuredClone(
        audit_record
      ) as cluster_control_plane_audit_record_t;
    }

    return {
      schema_version: 1,
      updated_unix_ms: params?.now_unix_ms ?? Date.now(),
      dedupe_record_by_id,
      entity_version_by_name,
      audit_record_by_mutation_id
    };
  }

  private hydrateControlPlaneStateFromSnapshot(params: {
    state: cluster_control_plane_state_snapshot_t;
  }): void {
    const { state } = params;

    this.cluster_mutation_dedupe_record_by_id.clear();
    for (const [mutation_id, dedupe_record] of Object.entries(state.dedupe_record_by_id)) {
      if (
        typeof mutation_id !== 'string' ||
        mutation_id.length === 0 ||
        typeof dedupe_record.created_unix_ms !== 'number' ||
        typeof dedupe_record.expires_unix_ms !== 'number' ||
        (dedupe_record.status !== 'in_progress' && dedupe_record.status !== 'completed')
      ) {
        continue;
      }

      this.cluster_mutation_dedupe_record_by_id.set(mutation_id, {
        mutation_id,
        created_unix_ms: dedupe_record.created_unix_ms,
        expires_unix_ms: dedupe_record.expires_unix_ms,
        status: dedupe_record.status,
        response: dedupe_record.response
          ? (structuredClone(
              dedupe_record.response
            ) as handle_cluster_admin_mutation_response_t)
          : undefined
      });
    }

    this.cluster_entity_version_by_name.clear();
    for (const [entity_name, entity_version] of Object.entries(state.entity_version_by_name)) {
      if (typeof entity_version === 'string' && entity_version.length > 0) {
        this.cluster_entity_version_by_name.set(entity_name, entity_version);
      }
    }

    this.cluster_admin_mutation_audit_record_by_mutation_id.clear();
    for (const [mutation_id, audit_record] of Object.entries(
      state.audit_record_by_mutation_id
    )) {
      if (
        typeof mutation_id !== 'string' ||
        mutation_id.length === 0 ||
        !IsRecordObject(audit_record)
      ) {
        continue;
      }

      const typed_audit_record = audit_record as cluster_admin_mutation_audit_record_t;
      if (
        typeof typed_audit_record.mutation_id !== 'string' ||
        typeof typed_audit_record.timestamp_unix_ms !== 'number'
      ) {
        continue;
      }

      const immutable_audit_record = this.deepFreezeObject({
        value: structuredClone(typed_audit_record)
      });
      this.cluster_admin_mutation_audit_record_by_mutation_id.set(
        mutation_id,
        immutable_audit_record
      );
    }
  }

  private persistControlPlaneState(params?: {
    now_unix_ms?: number;
  }): void {
    const state_snapshot = this.buildControlPlaneStateSnapshot({
      now_unix_ms: params?.now_unix_ms
    });

    this.control_plane_state_store.saveState({
      state: state_snapshot
    });
  }

  private runControlPlaneStateCompaction(params: { now_unix_ms: number }): void {
    const { now_unix_ms } = params;
    const compacted_state = this.control_plane_state_store.compactState({
      state: this.buildControlPlaneStateSnapshot({
        now_unix_ms
      }),
      now_unix_ms,
      retention_policy: this.buildControlPlaneRetentionPolicy()
    });

    this.hydrateControlPlaneStateFromSnapshot({
      state: compacted_state
    });
  }

  private isBreakGlassMutationRequest(params: {
    request: cluster_admin_mutation_request_message_i;
  }): boolean {
    const { request } = params;
    if (
      this.mutation_governance_policy.break_glass_mutation_type_list.includes(
        request.mutation_type
      )
    ) {
      return true;
    }

    const reason = request.change_context?.reason;
    if (typeof reason === 'string' && /break[_\s-]?glass/i.test(reason)) {
      return true;
    }

    return false;
  }

  private validateMutationGovernancePolicy(params: {
    request: cluster_admin_mutation_request_message_i;
  }): cluster_protocol_validation_result_t<{}> {
    const { request } = params;
    const change_context = request.change_context;

    if (
      this.mutation_governance_policy.require_change_reason &&
      (typeof change_context?.reason !== 'string' || change_context.reason.length === 0)
    ) {
      return {
        ok: false,
        error: {
          code: 'ADMIN_VALIDATION_FAILED',
          message:
            'Governance policy requires change_context.reason for admin mutations.',
          retryable: false,
          unknown_outcome: false,
          details: {
            policy_requirement: 'require_change_reason'
          }
        }
      };
    }

    if (
      this.mutation_governance_policy.require_change_ticket_id &&
      (typeof change_context?.change_ticket_id !== 'string' ||
        change_context.change_ticket_id.length === 0)
    ) {
      return {
        ok: false,
        error: {
          code: 'ADMIN_VALIDATION_FAILED',
          message:
            'Governance policy requires change_context.change_ticket_id for admin mutations.',
          retryable: false,
          unknown_outcome: false,
          details: {
            policy_requirement: 'require_change_ticket_id'
          }
        }
      };
    }

    if (this.mutation_governance_policy.enforce_dual_authorization) {
      const dual_authorization = change_context?.dual_authorization;
      if (dual_authorization?.required !== true) {
        return {
          ok: false,
          error: {
            code: 'ADMIN_FORBIDDEN',
            message:
              'Governance policy requires dual authorization for admin mutations.',
            retryable: false,
            unknown_outcome: false,
            details: {
              policy_requirement: 'enforce_dual_authorization'
            }
          }
        };
      }

      if (
        typeof dual_authorization.approver_subject !== 'string' ||
        dual_authorization.approver_subject.length === 0
      ) {
        return {
          ok: false,
          error: {
            code: 'ADMIN_FORBIDDEN',
            message:
              'Dual authorization requires change_context.dual_authorization.approver_subject.',
            retryable: false,
            unknown_outcome: false,
            details: {
              policy_requirement: 'dual_authorization.approver_subject'
            }
          }
        };
      }
    }

    return {
      ok: true,
      value: {}
    };
  }

  private removeExpiredMutationDedupeRecords(params: {
    now_unix_ms: number;
  }): void {
    this.runControlPlaneStateCompaction({
      now_unix_ms: params.now_unix_ms
    });
  }

  private lookupClusterMutationDedupe(params: {
    mutation_id: string;
    now_unix_ms: number;
  }): {
    record_status: 'none' | 'in_progress' | 'completed';
    response?: handle_cluster_admin_mutation_response_t;
  } {
    const { mutation_id, now_unix_ms } = params;
    const dedupe_record = this.cluster_mutation_dedupe_record_by_id.get(mutation_id);
    if (!dedupe_record) {
      return {
        record_status: 'none'
      };
    }

    if (dedupe_record.expires_unix_ms <= now_unix_ms) {
      this.cluster_mutation_dedupe_record_by_id.delete(mutation_id);
      this.persistControlPlaneState({
        now_unix_ms
      });
      return {
        record_status: 'none'
      };
    }

    if (dedupe_record.status === 'completed' && dedupe_record.response) {
      return {
        record_status: 'completed',
        response: structuredClone(dedupe_record.response)
      };
    }

    return {
      record_status: 'in_progress'
    };
  }

  private registerClusterMutationDedupeInProgress(params: {
    mutation_id: string;
    now_unix_ms: number;
  }): void {
    const { mutation_id, now_unix_ms } = params;
    this.cluster_mutation_dedupe_record_by_id.set(mutation_id, {
      mutation_id,
      created_unix_ms: now_unix_ms,
      expires_unix_ms: now_unix_ms + this.mutation_dedupe_retention_ms,
      status: 'in_progress'
    });
    this.persistControlPlaneState({
      now_unix_ms
    });
  }

  private recordClusterAdminMutationCompletion(params: {
    response: handle_cluster_admin_mutation_response_t;
    mutation_id: string;
    request_id: string;
    request?: cluster_admin_mutation_request_message_i;
    resolved_node_id_list: string[];
    received_unix_ms: number;
    completed_unix_ms: number;
    matched_allow_policy_id_list: string[];
    matched_deny_policy_id_list: string[];
    authorization_reason?: string;
    break_glass: boolean;
    skip_dedupe_update?: boolean;
  }): void {
    const {
      response,
      mutation_id,
      request_id,
      request,
      resolved_node_id_list,
      received_unix_ms,
      completed_unix_ms,
      matched_allow_policy_id_list,
      matched_deny_policy_id_list,
      authorization_reason,
      break_glass,
      skip_dedupe_update = false
    } = params;

    const duration_ms = Math.max(0, completed_unix_ms - received_unix_ms);
    this.cluster_admin_mutation_metrics.admin_mutation_rollout_duration_ms.push(duration_ms);

    const terminal_message = response.terminal_message;
    if (terminal_message.message_type === 'cluster_admin_mutation_result') {
      if (
        terminal_message.status === 'completed' ||
        terminal_message.status === 'dry_run_completed'
      ) {
        this.cluster_admin_mutation_metrics.admin_mutation_success_total += 1;
      } else {
        this.cluster_admin_mutation_metrics.admin_mutation_failure_total += 1;
      }

      if (terminal_message.status === 'rolled_back') {
        this.cluster_admin_mutation_metrics.admin_mutation_rollback_total += 1;
      }
    } else {
      this.cluster_admin_mutation_metrics.admin_mutation_failure_total += 1;
      if (
        terminal_message.error.code === 'ADMIN_AUTH_FAILED' ||
        terminal_message.error.code === 'ADMIN_FORBIDDEN'
      ) {
        this.cluster_admin_mutation_metrics.admin_mutation_authz_denied_total += 1;
      }
    }

    if (!skip_dedupe_update && mutation_id !== 'unknown_mutation') {
      this.cluster_mutation_dedupe_record_by_id.set(mutation_id, {
        mutation_id,
        created_unix_ms: received_unix_ms,
        expires_unix_ms: completed_unix_ms + this.mutation_dedupe_retention_ms,
        status: 'completed',
        response: structuredClone(response)
      });
    }

    const payload_hash_sha256 = request
      ? createHash('sha256')
          .update(JSON.stringify(request.payload), 'utf8')
          .digest('hex')
      : null;

    const per_node_outcomes =
      terminal_message.message_type === 'cluster_admin_mutation_result'
        ? terminal_message.per_node_results
        : [];
    const rolled_back_node_count =
      terminal_message.message_type === 'cluster_admin_mutation_result'
        ? terminal_message.summary.rolled_back_node_count
        : 0;
    const rollback_failed =
      terminal_message.message_type === 'cluster_admin_mutation_error' &&
      terminal_message.error.code === 'ADMIN_ROLLBACK_FAILED';

    const audit_record: cluster_admin_mutation_audit_record_t = {
      audit_record_id:
        terminal_message.message_type === 'cluster_admin_mutation_result'
          ? terminal_message.audit_record_id
          : `audit_${mutation_id}`,
      mutation_id,
      request_id,
      timestamp_unix_ms: completed_unix_ms,
      actor_identity: {
        subject: request?.auth_context.subject,
        tenant_id: request?.auth_context.tenant_id,
        environment: request?.auth_context.environment
      },
      evaluated_privileges: {
        capability_claims: request?.auth_context.capability_claims ?? [],
        matched_allow_policy_id_list,
        matched_deny_policy_id_list,
        authorization_reason
      },
      payload_hash_sha256,
      target_scope: request?.target_scope ?? null,
      resolved_node_id_list: [...resolved_node_id_list],
      per_node_outcomes,
      timing: {
        received_unix_ms,
        completed_unix_ms,
        duration_ms
      },
      rollback_metadata: {
        attempted: rolled_back_node_count > 0 || rollback_failed,
        rolled_back_node_count,
        rollback_failed
      },
      result:
        terminal_message.message_type === 'cluster_admin_mutation_result'
          ? {
              kind: 'result',
              status: terminal_message.status
            }
          : {
              kind: 'error',
              error_code: terminal_message.error.code,
              error_message: terminal_message.error.message
            },
      governance: {
        change_reason: request?.change_context?.reason,
        change_ticket_id: request?.change_context?.change_ticket_id,
        dual_authorization_required:
          request?.change_context?.dual_authorization?.required === true,
        dual_authorization_approver_subject:
          request?.change_context?.dual_authorization?.approver_subject,
        break_glass
      },
      immutable: true
    };

    const immutable_audit_record = this.deepFreezeObject({
      value: structuredClone(audit_record)
    });
    this.cluster_admin_mutation_audit_record_by_mutation_id.set(
      mutation_id,
      immutable_audit_record
    );

    this.runControlPlaneStateCompaction({
      now_unix_ms: completed_unix_ms
    });
  }

  private deepFreezeObject<value_t>(params: { value: value_t }): value_t {
    const { value } = params;
    if (
      typeof value !== 'object' ||
      value === null ||
      Object.isFrozen(value)
    ) {
      return value;
    }

    Object.freeze(value);
    for (const nested_value of Object.values(value as Record<string, unknown>)) {
      if (typeof nested_value === 'object' && nested_value !== null) {
        this.deepFreezeObject({
          value: nested_value
        });
      }
    }

    return value;
  }

  onWorkerEvent(params: { listener: worker_event_listener_t }): number {
    const { listener } = params;

    const listener_id = this.next_worker_event_listener_id;
    this.next_worker_event_listener_id += 1;

    this.worker_event_listener_by_id.set(listener_id, listener);
    return listener_id;
  }

  offWorkerEvent(params: { listener_id: number }): void {
    const { listener_id } = params;
    this.worker_event_listener_by_id.delete(listener_id);
  }

  getWorkerHealthStates(): worker_health_information_t[] {
    return Array.from(this.worker_state_by_id.values())
      .sort((left_worker_state, right_worker_state): number => {
        return left_worker_state.worker_id - right_worker_state.worker_id;
      })
      .map((worker_state): worker_health_information_t => {
        return {
          worker_id: worker_state.worker_id,
          health_state: worker_state.health_state,
          pending_call_count: worker_state.pending_call_request_ids.size,
          pending_control_count: worker_state.pending_control_request_ids.size,
          restart_attempt: worker_state.restart_attempt
        };
      });
  }

  async handleClusterCallRequest(
    params: handle_cluster_call_request_params_t
  ): Promise<handle_cluster_call_response_t> {
    const {
      message,
      node_id,
      now_unix_ms = Date.now(),
      gateway_received_unix_ms = now_unix_ms
    } = params;

    if (typeof node_id !== 'string' || node_id.length === 0) {
      throw new Error('node_id must be a non-empty string.');
    }

    this.cluster_call_routing_metrics.call_requests_total += 1;

    let request_id = 'unknown_request';
    let attempt_index = 0;
    let function_name = 'unknown_function';

    if (IsRecordObject(message)) {
      if (typeof message.request_id === 'string' && message.request_id.length > 0) {
        request_id = message.request_id;
      }
      if (
        typeof message.attempt_index === 'number' &&
        Number.isInteger(message.attempt_index)
      ) {
        attempt_index = message.attempt_index;
      }
      if (
        typeof message.function_name === 'string' &&
        message.function_name.length > 0
      ) {
        function_name = message.function_name;
      }
    }

    const parsed_request_result = ParseClusterCallRequestMessage({
      message,
      now_unix_ms
    });

    if (!parsed_request_result.ok) {
      this.appendClusterOperationLifecycleEvent({
        event: {
          timestamp_unix_ms: now_unix_ms,
          event_name: 'cluster_call_failed',
          severity: 'warn',
          request_id,
          function_name,
          node_id,
          error_code: parsed_request_result.error.code,
          retryable: parsed_request_result.error.retryable,
          details: {
            reason: 'cluster_call_request_validation_failed'
          }
        }
      });

      return this.buildClusterCallRejectedResponse({
        request_id,
        attempt_index,
        node_id,
        gateway_received_unix_ms,
        now_unix_ms,
        error: parsed_request_result.error
      });
    }

    const parsed_request = parsed_request_result.value;
    request_id = parsed_request.request_id;
    attempt_index = parsed_request.attempt_index;
    function_name = parsed_request.function_name;

    this.appendClusterOperationLifecycleEvent({
      event: {
        timestamp_unix_ms: now_unix_ms,
        event_name: 'cluster_call_received',
        severity: 'info',
        request_id: parsed_request.request_id,
        trace_id: parsed_request.trace_id,
        function_name: parsed_request.function_name,
        node_id,
        details: {
          attempt_index: parsed_request.attempt_index,
          routing_mode: parsed_request.routing_hint.mode
        }
      }
    });

    if (parsed_request.routing_hint.mode === 'auto') {
      this.cluster_call_routing_metrics.routing_mode_auto_total += 1;
    } else if (parsed_request.routing_hint.mode === 'target_node') {
      this.cluster_call_routing_metrics.routing_mode_target_node_total += 1;
    } else {
      this.cluster_call_routing_metrics.routing_mode_affinity_total += 1;
    }

    const call_environment = this.extractClusterCallEnvironmentFromMetadata({
      request: parsed_request
    });
    const call_cluster = this.extractClusterCallClusterFromMetadata({
      request: parsed_request
    });

    const routed_candidate_result = this.resolveRoutedClusterCallCandidate({
      request: parsed_request,
      local_node_id: node_id,
      call_environment,
      call_cluster,
      now_unix_ms
    });

    this.cluster_call_routing_metrics.filtered_unhealthy_node_total +=
      routed_candidate_result.filtered_unhealthy_node_count;
    this.cluster_call_routing_metrics.filtered_capability_node_total +=
      routed_candidate_result.filtered_capability_node_count;
    this.cluster_call_routing_metrics.filtered_zone_node_total +=
      routed_candidate_result.filtered_zone_node_count;
    this.cluster_call_routing_metrics.filtered_authorization_node_total +=
      routed_candidate_result.filtered_authorization_node_count;

    if (!routed_candidate_result.ok) {
      this.cluster_call_routing_metrics.no_capable_node_total += 1;

      this.appendClusterOperationLifecycleEvent({
        event: {
          timestamp_unix_ms: now_unix_ms,
          event_name: 'cluster_call_failed',
          severity: 'warn',
          request_id: parsed_request.request_id,
          trace_id: parsed_request.trace_id,
          function_name: parsed_request.function_name,
          node_id,
          error_code: routed_candidate_result.error.code,
          retryable: routed_candidate_result.error.retryable,
          details: {
            reason: 'cluster_call_routing_failed'
          }
        }
      });

      return this.buildClusterCallRejectedResponse({
        request_id,
        attempt_index,
        node_id,
        gateway_received_unix_ms,
        now_unix_ms,
        error: routed_candidate_result.error
      });
    }

    const ordered_candidate_node_list = [
      routed_candidate_result.selected_node,
      ...routed_candidate_result.fallback_node_list
    ];

    this.appendClusterOperationLifecycleEvent({
      event: {
        timestamp_unix_ms: now_unix_ms,
        event_name: 'cluster_call_routed',
        severity: 'info',
        request_id: parsed_request.request_id,
        trace_id: parsed_request.trace_id,
        function_name: parsed_request.function_name,
        node_id: routed_candidate_result.selected_node.node_id,
        details: {
          routing_mode: parsed_request.routing_hint.mode,
          candidate_node_count: ordered_candidate_node_list.length,
          fallback_node_count: routed_candidate_result.fallback_node_list.length
        }
      }
    });

    let last_response: handle_cluster_call_response_t | null = null;
    for (
      let candidate_index = 0;
      candidate_index < ordered_candidate_node_list.length;
      candidate_index += 1
    ) {
      const candidate_node = ordered_candidate_node_list[candidate_index];
      const has_fallback_candidate =
        candidate_index < ordered_candidate_node_list.length - 1;

      const dispatch_response = await this.dispatchClusterCallToCandidateNode({
        request: parsed_request,
        candidate_node,
        local_node_id: node_id,
        gateway_received_unix_ms,
        node_received_unix_ms: now_unix_ms
      });

      last_response = dispatch_response;

      if (dispatch_response.terminal_message.message_type === 'cluster_call_response_success') {
        if (candidate_node.node_id === node_id) {
          this.cluster_call_routing_metrics.routed_local_total += 1;
        } else {
          this.cluster_call_routing_metrics.routed_remote_total += 1;
        }

        this.appendClusterOperationLifecycleEvent({
          event: {
            timestamp_unix_ms: dispatch_response.terminal_message.timestamp_unix_ms,
            event_name: 'cluster_call_completed',
            severity: 'info',
            request_id: parsed_request.request_id,
            trace_id: parsed_request.trace_id,
            function_name: parsed_request.function_name,
            node_id: candidate_node.node_id,
            details: {
              attempt_index: dispatch_response.terminal_message.attempt_index
            }
          }
        });
        return dispatch_response;
      }

      const should_failover = this.shouldRetryClusterCallOnAlternateNode({
        response: dispatch_response,
        idempotency_key: parsed_request.idempotency_key,
        has_fallback_candidate
      });

      if (!should_failover) {
        this.appendClusterOperationLifecycleEvent({
          event: {
            timestamp_unix_ms: dispatch_response.terminal_message.timestamp_unix_ms,
            event_name: 'cluster_call_failed',
            severity: 'warn',
            request_id: parsed_request.request_id,
            trace_id: parsed_request.trace_id,
            function_name: parsed_request.function_name,
            node_id: candidate_node.node_id,
            error_code: dispatch_response.terminal_message.error.code,
            retryable: dispatch_response.terminal_message.error.retryable,
            details: {
              reason: 'cluster_call_dispatch_terminal_failure',
              attempt_index: dispatch_response.terminal_message.attempt_index
            }
          }
        });
        return dispatch_response;
      }

      this.cluster_call_routing_metrics.dispatch_retryable_total += 1;
      this.cluster_call_routing_metrics.failover_attempt_total += 1;

      this.appendClusterOperationLifecycleEvent({
        event: {
          timestamp_unix_ms: dispatch_response.terminal_message.timestamp_unix_ms,
          event_name: 'cluster_call_retried',
          severity: 'warn',
          request_id: parsed_request.request_id,
          trace_id: parsed_request.trace_id,
          function_name: parsed_request.function_name,
          node_id: candidate_node.node_id,
          error_code: dispatch_response.terminal_message.error.code,
          retryable: dispatch_response.terminal_message.error.retryable,
          details: {
            reason: 'cluster_call_dispatch_retryable_failure',
            attempt_index: dispatch_response.terminal_message.attempt_index
          }
        }
      });
    }

    if (last_response) {
      return last_response;
    }

    return this.buildClusterCallRejectedResponse({
      request_id,
      attempt_index,
      node_id,
      gateway_received_unix_ms,
      now_unix_ms,
      error: {
        code: 'INTERNAL_SUPERVISOR_ERROR',
        message: 'No cluster call candidate could be selected.',
        retryable: false,
        unknown_outcome: false,
        details: {
          reason: 'empty_candidate_dispatch_list'
        }
      }
    });
  }

  private buildClusterCallRejectedResponse(params: {
    request_id: string;
    attempt_index: number;
    node_id: string;
    gateway_received_unix_ms: number;
    now_unix_ms: number;
    error: cluster_protocol_error_t;
  }): handle_cluster_call_response_t {
    const { request_id, attempt_index, node_id, gateway_received_unix_ms, now_unix_ms, error } =
      params;

    const ack = this.buildClusterCallAckMessage({
      request_id,
      attempt_index,
      node_id,
      accepted: false,
      timestamp_unix_ms: now_unix_ms
    });

    const terminal_message = this.buildClusterCallErrorMessage({
      request_id,
      attempt_index,
      node_id,
      code: error.code,
      message: error.message,
      retryable: error.retryable,
      unknown_outcome: error.unknown_outcome,
      details: error.details,
      gateway_received_unix_ms,
      last_attempt_started_unix_ms: now_unix_ms,
      timestamp_unix_ms: now_unix_ms
    });

    return {
      ack,
      terminal_message
    };
  }

  private extractClusterCallEnvironmentFromMetadata(params: {
    request: cluster_call_request_message_i;
  }): string | undefined {
    const metadata = params.request.metadata;
    if (!IsRecordObject(metadata)) {
      return undefined;
    }

    const environment = metadata.environment;
    if (typeof environment === 'string' && environment.length > 0) {
      return environment;
    }

    return undefined;
  }

  private extractClusterCallClusterFromMetadata(params: {
    request: cluster_call_request_message_i;
  }): string | undefined {
    const metadata = params.request.metadata;
    if (!IsRecordObject(metadata)) {
      return undefined;
    }

    const cluster = metadata.cluster;
    if (typeof cluster === 'string' && cluster.length > 0) {
      return cluster;
    }

    return undefined;
  }

  private buildClusterCallCandidateNodeList(params: {
    local_node_id: string;
    now_unix_ms: number;
  }): cluster_call_routing_candidate_node_base_t[] {
    const { local_node_id, now_unix_ms } = params;
    const candidate_node_by_id = new Map<string, cluster_call_routing_candidate_node_base_t>();

    const local_candidate = this.buildLocalClusterCallCandidateNode({
      local_node_id,
      now_unix_ms
    });
    candidate_node_by_id.set(local_candidate.node_id, local_candidate);

    for (const registration of this.cluster_call_node_registration_by_id.values()) {
      if (registration.node_id === local_node_id) {
        continue;
      }

      const inflight_calls =
        registration.metrics.inflight_calls + registration.runtime_inflight_call_count;

      candidate_node_by_id.set(registration.node_id, {
        node_id: registration.node_id,
        labels: registration.labels ? { ...registration.labels } : undefined,
        zones: registration.zones ? [...registration.zones] : undefined,
        health_state: registration.health_state,
        last_health_update_unix_ms: registration.last_health_update_unix_ms,
        metrics: {
          inflight_calls,
          pending_calls: registration.metrics.pending_calls,
          success_rate_1m: registration.metrics.success_rate_1m,
          timeout_rate_1m: registration.metrics.timeout_rate_1m,
          ewma_latency_ms: registration.metrics.ewma_latency_ms
        },
        capability_list: registration.capability_list.map((capability) => {
          return { ...capability };
        })
      });
    }

    return Array.from(candidate_node_by_id.values());
  }

  private buildLocalClusterCallCandidateNode(params: {
    local_node_id: string;
    now_unix_ms: number;
  }): cluster_call_routing_candidate_node_base_t {
    const { local_node_id, now_unix_ms } = params;
    let pending_call_count = 0;
    let ready_worker_count = 0;

    for (const worker_state of this.worker_state_by_id.values()) {
      pending_call_count += worker_state.pending_call_request_ids.size;
      if (worker_state.ready && worker_state.health_state === 'ready') {
        ready_worker_count += 1;
      }
    }

    const local_health_state: cluster_call_node_health_state_t =
      this.lifecycle_state !== 'running'
        ? 'stopped'
        : ready_worker_count > 0
          ? 'ready'
          : 'degraded';

    const capability_list: cluster_call_node_capability_t[] = Array.from(
      this.function_definition_by_name.values()
    ).map((definition) => {
      return {
        function_name: definition.name,
        function_hash_sha1: definition.function_hash_sha1,
        installed: true
      };
    });

    return {
      node_id: local_node_id,
      health_state: local_health_state,
      last_health_update_unix_ms: now_unix_ms,
      metrics: {
        inflight_calls: pending_call_count,
        pending_calls: pending_call_count,
        success_rate_1m: 1,
        timeout_rate_1m: 0,
        ewma_latency_ms: pending_call_count > 0 ? pending_call_count : 0
      },
      capability_list
    };
  }

  private resolveRoutedClusterCallCandidate(params: {
    request: cluster_call_request_message_i;
    local_node_id: string;
    call_environment?: string;
    call_cluster?: string;
    now_unix_ms: number;
  }):
    | {
        ok: true;
        selected_node: cluster_call_routing_candidate_node_base_t;
        fallback_node_list: cluster_call_routing_candidate_node_base_t[];
        filtered_unhealthy_node_count: number;
        filtered_capability_node_count: number;
        filtered_zone_node_count: number;
        filtered_authorization_node_count: number;
      }
    | {
        ok: false;
        error: cluster_protocol_error_t;
        filtered_unhealthy_node_count: number;
        filtered_capability_node_count: number;
        filtered_zone_node_count: number;
        filtered_authorization_node_count: number;
      } {
    const { request, local_node_id, call_environment, call_cluster, now_unix_ms } = params;

    const raw_candidate_node_list = this.buildClusterCallCandidateNodeList({
      local_node_id,
      now_unix_ms
    });

    const authorized_candidate_node_list: cluster_call_routing_candidate_node_base_t[] = [];
    let filtered_authorization_node_count = 0;
    let last_authorization_error: cluster_protocol_error_t | null = null;

    for (const candidate_node of raw_candidate_node_list) {
      const authorization_result = this.authorizeClusterCallRequest({
        request,
        cluster: call_cluster,
        environment: call_environment,
        target_selector: this.buildCallTargetSelectorForCandidateNode({
          candidate_node
        })
      });

      if (authorization_result.ok) {
        authorized_candidate_node_list.push(candidate_node);
      } else {
        filtered_authorization_node_count += 1;
        last_authorization_error = authorization_result.error;
      }
    }

    if (authorized_candidate_node_list.length === 0) {
      return {
        ok: false,
        error:
          last_authorization_error ??
          {
            code: 'FORBIDDEN_FUNCTION',
            message: 'No candidate nodes are authorized for this call request.',
            retryable: false,
            unknown_outcome: false,
            details: {
              function_name: request.function_name
            }
          },
        filtered_unhealthy_node_count: 0,
        filtered_capability_node_count: 0,
        filtered_zone_node_count: 0,
        filtered_authorization_node_count
      };
    }

    const routing_result = this.cluster_call_routing_strategy.selectNode({
      request_id: request.request_id,
      function_name: request.function_name,
      function_hash_sha1: request.function_hash_sha1,
      routing_hint: request.routing_hint,
      now_unix_ms,
      candidate_node_list: authorized_candidate_node_list
    });

    if (!routing_result.ok) {
      if (
        routing_result.reason === 'no_capable_nodes' &&
        authorized_candidate_node_list.length === 1 &&
        authorized_candidate_node_list[0].node_id === local_node_id
      ) {
        return {
          ok: true,
          selected_node: authorized_candidate_node_list[0],
          fallback_node_list: [],
          filtered_unhealthy_node_count: routing_result.filtered_unhealthy_node_count,
          filtered_capability_node_count: routing_result.filtered_capability_node_count,
          filtered_zone_node_count: routing_result.filtered_zone_node_count,
          filtered_authorization_node_count
        };
      }

      const retryable =
        request.routing_hint.mode !== 'target_node' &&
        (routing_result.reason === 'no_healthy_nodes' ||
          routing_result.reason === 'no_capable_nodes');

      return {
        ok: false,
        error: {
          code: 'NO_CAPABLE_NODE',
          message: 'No candidate node is currently available for this call request.',
          retryable,
          unknown_outcome: false,
          details: {
            routing_mode: request.routing_hint.mode,
            routing_reason: routing_result.reason,
            routing_details: routing_result.details
          }
        },
        filtered_unhealthy_node_count: routing_result.filtered_unhealthy_node_count,
        filtered_capability_node_count: routing_result.filtered_capability_node_count,
        filtered_zone_node_count: routing_result.filtered_zone_node_count,
        filtered_authorization_node_count
      };
    }

    return {
      ok: true,
      selected_node: routing_result.selected_node,
      fallback_node_list: routing_result.fallback_node_list,
      filtered_unhealthy_node_count: routing_result.filtered_unhealthy_node_count,
      filtered_capability_node_count: routing_result.filtered_capability_node_count,
      filtered_zone_node_count: routing_result.filtered_zone_node_count,
      filtered_authorization_node_count
    };
  }

  private buildCallTargetSelectorForCandidateNode(params: {
    candidate_node: cluster_call_routing_candidate_node_base_t;
  }): cluster_authorization_target_selector_t {
    const { candidate_node } = params;
    return {
      node_ids: [candidate_node.node_id],
      labels: candidate_node.labels ? { ...candidate_node.labels } : undefined,
      zones: candidate_node.zones ? [...candidate_node.zones] : undefined
    };
  }

  private async dispatchClusterCallToCandidateNode(params: {
    request: cluster_call_request_message_i;
    candidate_node: cluster_call_routing_candidate_node_base_t;
    local_node_id: string;
    gateway_received_unix_ms: number;
    node_received_unix_ms: number;
  }): Promise<handle_cluster_call_response_t> {
    const {
      request,
      candidate_node,
      local_node_id,
      gateway_received_unix_ms,
      node_received_unix_ms
    } = params;

    if (candidate_node.node_id === local_node_id) {
      return await this.executeClusterCallOnLocalNode({
        request,
        node_id: local_node_id,
        gateway_received_unix_ms,
        node_received_unix_ms
      });
    }

    const registration = this.cluster_call_node_registration_by_id.get(candidate_node.node_id);
    if (!registration) {
      return this.buildClusterCallRejectedResponse({
        request_id: request.request_id,
        attempt_index: request.attempt_index,
        node_id: candidate_node.node_id,
        gateway_received_unix_ms,
        now_unix_ms: node_received_unix_ms,
        error: {
          code: 'DISPATCH_FAILED_RETRYABLE',
          message: `Call node "${candidate_node.node_id}" is not registered.`,
          retryable: true,
          unknown_outcome: false,
          details: {
            node_id: candidate_node.node_id
          }
        }
      });
    }

    registration.runtime_inflight_call_count += 1;
    try {
      return await registration.call_executor({
        request,
        node: {
          node_id: registration.node_id,
          labels: registration.labels ? { ...registration.labels } : undefined,
          zones: registration.zones ? [...registration.zones] : undefined,
          node_agent_semver: registration.node_agent_semver,
          runtime_semver: registration.runtime_semver
        },
        now_unix_ms: node_received_unix_ms,
        gateway_received_unix_ms
      });
    } catch (error) {
      return this.buildClusterCallRejectedResponse({
        request_id: request.request_id,
        attempt_index: request.attempt_index,
        node_id: candidate_node.node_id,
        gateway_received_unix_ms,
        now_unix_ms: Date.now(),
        error: {
          code: 'DISPATCH_FAILED_RETRYABLE',
          message: GetErrorMessage({ error }),
          retryable: true,
          unknown_outcome: false,
          details: {
            node_id: candidate_node.node_id
          }
        }
      });
    } finally {
      registration.runtime_inflight_call_count = Math.max(
        0,
        registration.runtime_inflight_call_count - 1
      );
    }
  }

  private async executeClusterCallOnLocalNode(params: {
    request: cluster_call_request_message_i;
    node_id: string;
    gateway_received_unix_ms: number;
    node_received_unix_ms: number;
  }): Promise<handle_cluster_call_response_t> {
    const { request, node_id, gateway_received_unix_ms, node_received_unix_ms } = params;

    if (typeof request.function_hash_sha1 === 'string') {
      const current_function_definition = this.function_definition_by_name.get(
        request.function_name
      );
      if (
        current_function_definition &&
        current_function_definition.function_hash_sha1 !== request.function_hash_sha1
      ) {
        return this.buildClusterCallRejectedResponse({
          request_id: request.request_id,
          attempt_index: request.attempt_index,
          node_id,
          gateway_received_unix_ms,
          now_unix_ms: node_received_unix_ms,
          error: {
            code: 'FORBIDDEN_FUNCTION',
            message:
              'Requested function_hash_sha1 does not match currently installed function definition.',
            retryable: false,
            unknown_outcome: false,
            details: {
              function_name: request.function_name,
              requested_function_hash_sha1: request.function_hash_sha1,
              current_function_hash_sha1:
                current_function_definition.function_hash_sha1
            }
          }
        });
      }
    }

    const ack = this.buildClusterCallAckMessage({
      request_id: request.request_id,
      attempt_index: request.attempt_index,
      node_id,
      accepted: true,
      timestamp_unix_ms: node_received_unix_ms
    });

    const worker_started_unix_ms = Date.now();
    try {
      const return_value = await this.callWorkerFunction({
        function_name: request.function_name,
        call_args: request.args
      });
      const worker_finished_unix_ms = Date.now();
      const function_definition = this.function_definition_by_name.get(request.function_name);

      return {
        ack,
        terminal_message: this.buildClusterCallSuccessMessage({
          request_id: request.request_id,
          attempt_index: request.attempt_index,
          node_id,
          function_name: request.function_name,
          function_hash_sha1:
            function_definition?.function_hash_sha1 ??
            request.function_hash_sha1 ??
            'unknown',
          return_value,
          gateway_received_unix_ms,
          node_received_unix_ms,
          worker_started_unix_ms,
          worker_finished_unix_ms,
          timestamp_unix_ms: worker_finished_unix_ms
        })
      };
    } catch (error) {
      const worker_finished_unix_ms = Date.now();
      const error_message = GetErrorMessage({ error });
      const is_saturation_error = error_message.includes('saturated');
      return {
        ack,
        terminal_message: this.buildClusterCallErrorMessage({
          request_id: request.request_id,
          attempt_index: request.attempt_index,
          node_id,
          code: is_saturation_error
            ? 'NODE_OVERLOADED_RETRYABLE'
            : 'REMOTE_FUNCTION_ERROR',
          message: error_message,
          retryable: is_saturation_error,
          unknown_outcome: false,
          details: {
            function_name: request.function_name
          },
          gateway_received_unix_ms,
          last_attempt_started_unix_ms: worker_started_unix_ms,
          timestamp_unix_ms: worker_finished_unix_ms
        })
      };
    }
  }

  private shouldRetryClusterCallOnAlternateNode(params: {
    response: handle_cluster_call_response_t;
    idempotency_key?: string;
    has_fallback_candidate: boolean;
  }): boolean {
    const { response, idempotency_key, has_fallback_candidate } = params;
    if (!has_fallback_candidate) {
      return false;
    }

    if (response.terminal_message.message_type !== 'cluster_call_response_error') {
      return false;
    }

    const terminal_error = response.terminal_message.error;
    if (!terminal_error.retryable) {
      return false;
    }

    if (
      terminal_error.code === 'UNKNOWN_OUTCOME_RETRYABLE_WITH_IDEMPOTENCY' &&
      (typeof idempotency_key !== 'string' || idempotency_key.length === 0)
    ) {
      return false;
    }

    if (
      terminal_error.unknown_outcome &&
      (typeof idempotency_key !== 'string' || idempotency_key.length === 0)
    ) {
      return false;
    }

    return true;
  }

  private normalizeClusterCallNodeMetrics(params: {
    metrics?: Partial<cluster_call_node_metrics_t>;
  }): cluster_call_node_metrics_t {
    const metrics = params.metrics ?? {};

    const inflight_calls =
      typeof metrics.inflight_calls === 'number' && Number.isFinite(metrics.inflight_calls)
        ? Math.max(0, metrics.inflight_calls)
        : 0;
    const pending_calls =
      typeof metrics.pending_calls === 'number' && Number.isFinite(metrics.pending_calls)
        ? Math.max(0, metrics.pending_calls)
        : 0;
    const success_rate_1m =
      typeof metrics.success_rate_1m === 'number' && Number.isFinite(metrics.success_rate_1m)
        ? Math.max(0, Math.min(1, metrics.success_rate_1m))
        : 1;
    const timeout_rate_1m =
      typeof metrics.timeout_rate_1m === 'number' && Number.isFinite(metrics.timeout_rate_1m)
        ? Math.max(0, Math.min(1, metrics.timeout_rate_1m))
        : 0;
    const ewma_latency_ms =
      typeof metrics.ewma_latency_ms === 'number' && Number.isFinite(metrics.ewma_latency_ms)
        ? Math.max(0, metrics.ewma_latency_ms)
        : 0;

    return {
      inflight_calls,
      pending_calls,
      success_rate_1m,
      timeout_rate_1m,
      ewma_latency_ms
    };
  }

  async handleClusterAdminMutationRequest(
    params: handle_cluster_admin_mutation_request_params_t
  ): Promise<handle_cluster_admin_mutation_response_t> {
    const { message, node_id, now_unix_ms = Date.now() } = params;
    const mutation_request_started_unix_ms = Date.now();

    this.cluster_admin_mutation_metrics.admin_mutation_requests_total += 1;
    this.removeExpiredMutationDedupeRecords({
      now_unix_ms: mutation_request_started_unix_ms
    });

    if (typeof node_id !== 'string' || node_id.length === 0) {
      throw new Error('node_id must be a non-empty string.');
    }

    let mutation_id = 'unknown_mutation';
    let request_id = 'unknown_request';

    if (IsRecordObject(message)) {
      if (typeof message.mutation_id === 'string' && message.mutation_id.length > 0) {
        mutation_id = message.mutation_id;
      }
      if (typeof message.request_id === 'string' && message.request_id.length > 0) {
        request_id = message.request_id;
      }
    }

    const parsed_request_result = ParseClusterAdminMutationRequestMessage({
      message,
      now_unix_ms
    });

    if (!parsed_request_result.ok) {
      this.emitAdminMutationLifecycleEvent({
        event_name: 'admin_mutation_denied',
        mutation_id,
        request_id,
        severity: 'warn',
        details: {
          reason: parsed_request_result.error.code,
          validation_details: parsed_request_result.error.details
        }
      });

      const ack = this.buildAdminMutationAckMessage({
        mutation_id,
        request_id,
        accepted: false,
        target_node_count: 0,
        dry_run: false,
        timestamp_unix_ms: now_unix_ms
      });

      const terminal_message = this.buildAdminMutationErrorMessage({
        mutation_id,
        request_id,
        code: parsed_request_result.error.code,
        message: parsed_request_result.error.message,
        details: parsed_request_result.error.details,
        timestamp_unix_ms: now_unix_ms
      });

      const response: handle_cluster_admin_mutation_response_t = {
        ack,
        terminal_message
      };
      this.recordClusterAdminMutationCompletion({
        response,
        mutation_id,
        request_id,
        resolved_node_id_list: [],
        received_unix_ms: mutation_request_started_unix_ms,
        completed_unix_ms: Date.now(),
        matched_allow_policy_id_list: [],
        matched_deny_policy_id_list: [],
        authorization_reason: 'parse_failed',
        break_glass: false
      });
      return response;
    }

    const parsed_request = parsed_request_result.value;
    mutation_id = parsed_request.mutation_id;
    request_id = parsed_request.request_id;
    const is_break_glass_mutation = this.isBreakGlassMutationRequest({
      request: parsed_request
    });

    const dedupe_lookup_result = this.lookupClusterMutationDedupe({
      mutation_id,
      now_unix_ms
    });
    if (dedupe_lookup_result.record_status === 'completed' && dedupe_lookup_result.response) {
      return structuredClone(dedupe_lookup_result.response);
    }

    if (dedupe_lookup_result.record_status === 'in_progress') {
      const ack = this.buildAdminMutationAckMessage({
        mutation_id,
        request_id,
        accepted: true,
        target_node_count: 0,
        dry_run: parsed_request.dry_run,
        timestamp_unix_ms: now_unix_ms
      });

      const terminal_message = this.buildAdminMutationErrorMessage({
        mutation_id,
        request_id,
        code: 'ADMIN_DISPATCH_RETRYABLE',
        message:
          'Mutation with this mutation_id is already in progress. Query status or retry later.',
        details: {
          mutation_id,
          dedupe_status: 'in_progress'
        },
        timestamp_unix_ms: now_unix_ms
      });

      const response: handle_cluster_admin_mutation_response_t = {
        ack,
        terminal_message
      };
      this.recordClusterAdminMutationCompletion({
        response,
        mutation_id,
        request_id,
        request: parsed_request,
        resolved_node_id_list: [],
        received_unix_ms: mutation_request_started_unix_ms,
        completed_unix_ms: Date.now(),
        matched_allow_policy_id_list: [],
        matched_deny_policy_id_list: [],
        authorization_reason: 'dedupe_in_progress',
        break_glass: is_break_glass_mutation,
        skip_dedupe_update: true
      });
      return response;
    }

    this.registerClusterMutationDedupeInProgress({
      mutation_id,
      now_unix_ms
    });

    let matched_allow_policy_id_list: string[] = [];
    let matched_deny_policy_id_list: string[] = [];
    let authorization_reason: string | undefined;

    this.emitAdminMutationLifecycleEvent({
      event_name: 'admin_mutation_requested',
      mutation_id,
      request_id,
      details: {
        mutation_type: parsed_request.mutation_type,
        target_scope: parsed_request.target_scope,
        dry_run: parsed_request.dry_run,
        break_glass: is_break_glass_mutation
      }
    });

    const governance_validation_result = this.validateMutationGovernancePolicy({
      request: parsed_request
    });
    if (!governance_validation_result.ok) {
      this.emitAdminMutationLifecycleEvent({
        event_name: 'admin_mutation_denied',
        mutation_id,
        request_id,
        severity: 'warn',
        details: {
          reason: governance_validation_result.error.code,
          governance_details: governance_validation_result.error.details,
          break_glass: is_break_glass_mutation
        }
      });

      const ack = this.buildAdminMutationAckMessage({
        mutation_id,
        request_id,
        accepted: false,
        target_node_count: 0,
        dry_run: parsed_request.dry_run,
        timestamp_unix_ms: now_unix_ms
      });

      const terminal_message = this.buildAdminMutationErrorMessage({
        mutation_id,
        request_id,
        code: governance_validation_result.error.code,
        message: governance_validation_result.error.message,
        details: governance_validation_result.error.details,
        timestamp_unix_ms: now_unix_ms
      });

      const response: handle_cluster_admin_mutation_response_t = {
        ack,
        terminal_message
      };
      this.recordClusterAdminMutationCompletion({
        response,
        mutation_id,
        request_id,
        request: parsed_request,
        resolved_node_id_list: [],
        received_unix_ms: mutation_request_started_unix_ms,
        completed_unix_ms: Date.now(),
        matched_allow_policy_id_list: [],
        matched_deny_policy_id_list: [],
        authorization_reason: 'governance_denied',
        break_glass: is_break_glass_mutation
      });
      return response;
    }

    const target_resolution_result = this.resolveMutationTargetNodeList({
      request: parsed_request,
      node_id
    });

    if (!target_resolution_result.ok) {
      this.emitAdminMutationLifecycleEvent({
        event_name: 'admin_mutation_denied',
        mutation_id,
        request_id,
        severity: 'warn',
        details: {
          reason: target_resolution_result.error.code,
          resolution_details: target_resolution_result.error.details,
          break_glass: is_break_glass_mutation
        }
      });

      const ack = this.buildAdminMutationAckMessage({
        mutation_id,
        request_id,
        accepted: false,
        target_node_count: 0,
        dry_run: parsed_request.dry_run,
        timestamp_unix_ms: now_unix_ms
      });

      const terminal_message = this.buildAdminMutationErrorMessage({
        mutation_id,
        request_id,
        code: target_resolution_result.error.code,
        message: target_resolution_result.error.message,
        details: target_resolution_result.error.details,
        timestamp_unix_ms: now_unix_ms
      });

      const response: handle_cluster_admin_mutation_response_t = {
        ack,
        terminal_message
      };
      this.recordClusterAdminMutationCompletion({
        response,
        mutation_id,
        request_id,
        request: parsed_request,
        resolved_node_id_list: [],
        received_unix_ms: mutation_request_started_unix_ms,
        completed_unix_ms: Date.now(),
        matched_allow_policy_id_list: [],
        matched_deny_policy_id_list: [],
        authorization_reason: 'target_resolution_failed',
        break_glass: is_break_glass_mutation
      });
      return response;
    }

    const authorization_result = this.authorizeClusterAdminMutationRequest({
      request: parsed_request,
      node_id
    });

    if (!authorization_result.ok) {
      authorization_reason = authorization_result.error.code;
      this.emitAdminMutationLifecycleEvent({
        event_name: 'admin_mutation_denied',
        mutation_id,
        request_id,
        severity: 'warn',
        details: {
          reason: authorization_result.error.code,
          authorization_details: authorization_result.error.details,
          break_glass: is_break_glass_mutation
        }
      });

      const ack = this.buildAdminMutationAckMessage({
        mutation_id,
        request_id,
        accepted: false,
        target_node_count: 0,
        dry_run: parsed_request.dry_run,
        timestamp_unix_ms: now_unix_ms
      });

      const terminal_message = this.buildAdminMutationErrorMessage({
        mutation_id,
        request_id,
        code: authorization_result.error.code,
        message: authorization_result.error.message,
        details: authorization_result.error.details,
        timestamp_unix_ms: now_unix_ms
      });

      const response: handle_cluster_admin_mutation_response_t = {
        ack,
        terminal_message
      };
      this.recordClusterAdminMutationCompletion({
        response,
        mutation_id,
        request_id,
        request: parsed_request,
        resolved_node_id_list: [],
        received_unix_ms: mutation_request_started_unix_ms,
        completed_unix_ms: Date.now(),
        matched_allow_policy_id_list: [],
        matched_deny_policy_id_list: [],
        authorization_reason,
        break_glass: is_break_glass_mutation
      });
      return response;
    }

    const planning_started_unix_ms = Date.now();
    const resolved_target_node_list = target_resolution_result.value.target_node_list;
    matched_allow_policy_id_list = [
      ...authorization_result.value.evaluation_result.matched_allow_policy_id_list
    ];
    matched_deny_policy_id_list = [
      ...authorization_result.value.evaluation_result.matched_deny_policy_id_list
    ];
    authorization_reason = authorization_result.value.evaluation_result.reason;

    this.emitAdminMutationLifecycleEvent({
      event_name: 'admin_mutation_authorized',
      mutation_id,
      request_id,
      details: {
        target_node_id_list: resolved_target_node_list.map((node): string => {
          return node.node_id;
        }),
        matched_allow_policy_id_list:
          authorization_result.value.evaluation_result.matched_allow_policy_id_list,
        matched_deny_policy_id_list:
          authorization_result.value.evaluation_result.matched_deny_policy_id_list,
        break_glass: is_break_glass_mutation
      }
    });

    const ack = this.buildAdminMutationAckMessage({
      mutation_id,
      request_id,
      accepted: true,
      target_node_count: resolved_target_node_list.length,
      dry_run: parsed_request.dry_run,
      timestamp_unix_ms: now_unix_ms
    });

    const expected_version_validation_result =
      this.validateExpectedVersionForMutationRequest({
        request: parsed_request
      });
    if (!expected_version_validation_result.ok) {
      this.emitAdminMutationLifecycleEvent({
        event_name: 'admin_mutation_denied',
        mutation_id,
        request_id,
        severity: 'warn',
        details: {
          reason: expected_version_validation_result.error.code,
          conflict_details: expected_version_validation_result.error.details,
          break_glass: is_break_glass_mutation
        }
      });

      const terminal_message = this.buildAdminMutationErrorMessage({
        mutation_id,
        request_id,
        code: expected_version_validation_result.error.code,
        message: expected_version_validation_result.error.message,
        details: expected_version_validation_result.error.details,
        timestamp_unix_ms: now_unix_ms
      });

      const response: handle_cluster_admin_mutation_response_t = {
        ack,
        terminal_message
      };
      this.recordClusterAdminMutationCompletion({
        response,
        mutation_id,
        request_id,
        request: parsed_request,
        resolved_node_id_list: resolved_target_node_list.map((node): string => {
          return node.node_id;
        }),
        received_unix_ms: mutation_request_started_unix_ms,
        completed_unix_ms: Date.now(),
        matched_allow_policy_id_list,
        matched_deny_policy_id_list,
        authorization_reason,
        break_glass: is_break_glass_mutation
      });
      return response;
    }

    if (parsed_request.dry_run) {
      const per_node_state_list = this.buildDryRunNodeStateList({
        target_node_list: resolved_target_node_list
      });

      this.emitAdminMutationLifecycleEvent({
        event_name: 'admin_mutation_applied',
        mutation_id,
        request_id,
        details: {
          dry_run: true,
          target_node_count: resolved_target_node_list.length,
          break_glass: is_break_glass_mutation
        }
      });

      const terminal_message = this.buildAdminMutationResultMessage({
        mutation_id,
        request_id,
        status: 'dry_run_completed',
        rollout_strategy: parsed_request.rollout_strategy.mode,
        min_success_percent:
          parsed_request.rollout_strategy.min_success_percent ?? 100,
        per_node_state_list,
        planning_started_unix_ms,
        apply_started_unix_ms: planning_started_unix_ms,
        verify_finished_unix_ms: Date.now(),
        timestamp_unix_ms: now_unix_ms
      });

      const response: handle_cluster_admin_mutation_response_t = {
        ack,
        terminal_message
      };
      this.recordClusterAdminMutationCompletion({
        response,
        mutation_id,
        request_id,
        request: parsed_request,
        resolved_node_id_list: resolved_target_node_list.map((node): string => {
          return node.node_id;
        }),
        received_unix_ms: mutation_request_started_unix_ms,
        completed_unix_ms: Date.now(),
        matched_allow_policy_id_list,
        matched_deny_policy_id_list,
        authorization_reason,
        break_glass: is_break_glass_mutation
      });
      return response;
    }

    try {
      const apply_started_unix_ms = Date.now();
      const retry_execution_result = await this.executeMutationRolloutWithRetries({
        request: parsed_request,
        target_node_list: resolved_target_node_list,
        deadline_unix_ms: parsed_request.deadline_unix_ms
      });
      const rollout_execution_result = retry_execution_result.rollout_execution_result;
      const verify_finished_unix_ms = Date.now();

      const retryable_terminal_error_message =
        this.buildRetryableMutationFailureTerminalError({
          request: parsed_request,
          rollout_execution_result
        });
      if (retryable_terminal_error_message) {
        this.emitAdminMutationLifecycleEvent({
          event_name: 'admin_mutation_denied',
          mutation_id,
          request_id,
          severity: 'warn',
          details: {
            reason: retryable_terminal_error_message.error.code,
            retry_attempt_count: retry_execution_result.retry_attempt_count,
            break_glass: is_break_glass_mutation
          }
        });

        const response: handle_cluster_admin_mutation_response_t = {
          ack,
          terminal_message: retryable_terminal_error_message
        };
        this.recordClusterAdminMutationCompletion({
          response,
          mutation_id,
          request_id,
          request: parsed_request,
          resolved_node_id_list: resolved_target_node_list.map((node): string => {
            return node.node_id;
          }),
          received_unix_ms: mutation_request_started_unix_ms,
          completed_unix_ms: verify_finished_unix_ms,
          matched_allow_policy_id_list,
          matched_deny_policy_id_list,
          authorization_reason,
          break_glass: is_break_glass_mutation
        });
        return response;
      }

      if (rollout_execution_result.rollback_failed_error) {
        this.emitAdminMutationLifecycleEvent({
          event_name: 'admin_mutation_denied',
          mutation_id,
          request_id,
          severity: 'error',
          details: {
            reason: rollout_execution_result.rollback_failed_error.code,
            rollback_failure_details:
              rollout_execution_result.rollback_failed_error.details,
            break_glass: is_break_glass_mutation
          }
        });

        const terminal_message = this.buildAdminMutationErrorMessage({
          mutation_id,
          request_id,
          code: rollout_execution_result.rollback_failed_error.code,
          message: rollout_execution_result.rollback_failed_error.message,
          details: rollout_execution_result.rollback_failed_error.details ?? {},
          timestamp_unix_ms: verify_finished_unix_ms
        });

        const response: handle_cluster_admin_mutation_response_t = {
          ack,
          terminal_message
        };
        this.recordClusterAdminMutationCompletion({
          response,
          mutation_id,
          request_id,
          request: parsed_request,
          resolved_node_id_list: resolved_target_node_list.map((node): string => {
            return node.node_id;
          }),
          received_unix_ms: mutation_request_started_unix_ms,
          completed_unix_ms: verify_finished_unix_ms,
          matched_allow_policy_id_list,
          matched_deny_policy_id_list,
          authorization_reason,
          break_glass: is_break_glass_mutation
        });
        return response;
      }

      if (rollout_execution_result.final_status === 'partially_failed') {
        this.emitAdminMutationLifecycleEvent({
          event_name: 'admin_mutation_partially_failed',
          mutation_id,
          request_id,
          severity: 'warn',
          details: {
            mutation_type: parsed_request.mutation_type,
            target_node_count: resolved_target_node_list.length,
            break_glass: is_break_glass_mutation
          }
        });
      } else if (rollout_execution_result.final_status === 'rolled_back') {
        this.emitAdminMutationLifecycleEvent({
          event_name: 'admin_mutation_rolled_back',
          mutation_id,
          request_id,
          severity: 'warn',
          details: {
            mutation_type: parsed_request.mutation_type,
            rolled_back_node_count: rollout_execution_result.rolled_back_node_count,
            break_glass: is_break_glass_mutation
          }
        });
      } else {
        this.emitAdminMutationLifecycleEvent({
          event_name: 'admin_mutation_applied',
          mutation_id,
          request_id,
          details: {
            dry_run: false,
            mutation_type: parsed_request.mutation_type,
            target_node_count: resolved_target_node_list.length,
            break_glass: is_break_glass_mutation
          }
        });
      }

      if (rollout_execution_result.final_status === 'completed') {
        this.updateEntityVersionForMutationRequest({
          request: parsed_request,
          applied_version:
            rollout_execution_result.first_applied_version ?? parsed_request.mutation_id
        });
      }

      const terminal_message = this.buildAdminMutationResultMessage({
        mutation_id,
        request_id,
        status: rollout_execution_result.final_status,
        rollout_strategy: parsed_request.rollout_strategy.mode,
        min_success_percent:
          parsed_request.rollout_strategy.min_success_percent ?? 100,
        per_node_state_list: rollout_execution_result.per_node_state_list,
        planning_started_unix_ms,
        apply_started_unix_ms,
        verify_finished_unix_ms,
        timestamp_unix_ms: verify_finished_unix_ms
      });

      const response: handle_cluster_admin_mutation_response_t = {
        ack,
        terminal_message
      };
      this.recordClusterAdminMutationCompletion({
        response,
        mutation_id,
        request_id,
        request: parsed_request,
        resolved_node_id_list: resolved_target_node_list.map((node): string => {
          return node.node_id;
        }),
        received_unix_ms: mutation_request_started_unix_ms,
        completed_unix_ms: verify_finished_unix_ms,
        matched_allow_policy_id_list,
        matched_deny_policy_id_list,
        authorization_reason,
        break_glass: is_break_glass_mutation
      });
      return response;
    } catch (error) {
      const mutation_error = this.toAdminMutationError({
        error
      });

      this.emitAdminMutationLifecycleEvent({
        event_name: 'admin_mutation_denied',
        mutation_id,
        request_id,
        severity: 'warn',
        error: ToRemoteError({ error }),
        details: {
          reason: mutation_error.code,
          mutation_error_details: mutation_error.details ?? {},
          break_glass: is_break_glass_mutation
        }
      });

      const terminal_message = this.buildAdminMutationErrorMessage({
        mutation_id,
        request_id,
        code: mutation_error.code,
        message: mutation_error.message,
        details: mutation_error.details ?? {},
        timestamp_unix_ms: now_unix_ms
      });

      const response: handle_cluster_admin_mutation_response_t = {
        ack,
        terminal_message
      };
      this.recordClusterAdminMutationCompletion({
        response,
        mutation_id,
        request_id,
        request: parsed_request,
        resolved_node_id_list: resolved_target_node_list.map((node): string => {
          return node.node_id;
        }),
        received_unix_ms: mutation_request_started_unix_ms,
        completed_unix_ms: Date.now(),
        matched_allow_policy_id_list,
        matched_deny_policy_id_list,
        authorization_reason,
        break_glass: is_break_glass_mutation
      });
      return response;
    }
  }

  async defineWorkerFunction<args_t extends unknown[] = [unknown], return_t = unknown>(
    params: define_worker_function_params_t<args_t, return_t>
  ): Promise<void> {
    const { name, worker_func } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Function name must be a non-empty string.');
    }

    ValidateIdentifier({ value: name, label: 'Function name' });

    if (typeof worker_func !== 'function') {
      throw new Error('worker_func must be a function.');
    }

    const function_source = worker_func.toString();
    if (function_source.length === 0) {
      throw new Error(`Function "${name}" could not be serialized.`);
    }

    const normalized_function_source = NormalizeFunctionSourceForHash({
      function_source
    });
    const function_hash_sha1 = ComputeFunctionHashSha1({ function_source });
    const parameter_signature = ParseParameterSignature({ function_source });
    const required_dependency_aliases = ParseStringLiteralDependencies({
      function_source
    });
    const required_constant_names = ParseStringLiteralConstants({
      function_source
    });
    const required_database_connection_names =
      ParseStringLiteralDatabaseConnections({
        function_source
      });

    const existing_definition = this.function_definition_by_name.get(name);
    if (
      existing_definition &&
      existing_definition.normalized_function_source === normalized_function_source &&
      existing_definition.function_hash_sha1 === function_hash_sha1
    ) {
      return;
    }

    const updated_definition: worker_function_definition_t =
      existing_definition ?? {
        name,
        worker_func,
        function_source,
        normalized_function_source,
        function_hash_sha1,
        parameter_signature,
        required_dependency_aliases,
        required_constant_names,
        required_database_connection_names,
        installed_worker_ids: new Set<number>()
      };

    updated_definition.worker_func = worker_func;
    updated_definition.function_source = function_source;
    updated_definition.normalized_function_source = normalized_function_source;
    updated_definition.function_hash_sha1 = function_hash_sha1;
    updated_definition.parameter_signature = parameter_signature;
    updated_definition.required_dependency_aliases = required_dependency_aliases;
    updated_definition.required_constant_names = required_constant_names;
    updated_definition.required_database_connection_names =
      required_database_connection_names;
    updated_definition.installed_worker_ids.clear();

    this.function_definition_by_name.set(name, updated_definition);

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const installation_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.installFunctionOnWorker({
            worker_id: worker_state.worker_id,
            function_definition: updated_definition
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = installation_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to install function "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  async defineWorkerDependency(
    params: define_worker_dependency_params_t
  ): Promise<void> {
    const { alias, module_specifier } = params;
    const export_name =
      typeof params.export_name === 'string' && params.export_name.length > 0
        ? params.export_name
        : null;
    const is_default_export = params.is_default_export === true;

    if (typeof alias !== 'string' || alias.length === 0) {
      throw new Error('Dependency alias must be a non-empty string.');
    }

    ValidateIdentifier({ value: alias, label: 'Dependency alias' });

    if (typeof module_specifier !== 'string' || module_specifier.length === 0) {
      throw new Error('Dependency module_specifier must be a non-empty string.');
    }

    if (is_default_export && export_name !== null) {
      throw new Error(
        'Dependency cannot set both is_default_export and export_name simultaneously.'
      );
    }

    const existing_definition = this.dependency_definition_by_alias.get(alias);
    if (
      existing_definition &&
      existing_definition.module_specifier === module_specifier &&
      existing_definition.export_name === export_name &&
      existing_definition.is_default_export === is_default_export
    ) {
      if (this.lifecycle_state === 'running') {
        await this.installDependencyAcrossRunningWorkers({
          alias,
          dependency_definition: existing_definition
        });
      }
      return;
    }

    const dependency_definition: worker_dependency_definition_t =
      existing_definition ?? {
        alias,
        module_specifier,
        export_name,
        is_default_export,
        installed_worker_ids: new Set<number>()
      };

    dependency_definition.alias = alias;
    dependency_definition.module_specifier = module_specifier;
    dependency_definition.export_name = export_name;
    dependency_definition.is_default_export = is_default_export;
    dependency_definition.installed_worker_ids.clear();

    this.dependency_definition_by_alias.set(alias, dependency_definition);
    await this.installDependencyAcrossRunningWorkers({
      alias,
      dependency_definition
    });
  }

  async undefineWorkerDependency(
    params: undefine_worker_dependency_params_t
  ): Promise<void> {
    const { alias } = params;

    if (typeof alias !== 'string' || alias.length === 0) {
      throw new Error('Dependency alias must be a non-empty string.');
    }

    const existing_definition = this.dependency_definition_by_alias.get(alias);
    if (!existing_definition) {
      return;
    }

    this.dependency_definition_by_alias.delete(alias);

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const removal_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.removeDependencyOnWorker({
            worker_id: worker_state.worker_id,
            alias
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = removal_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to remove dependency "${alias}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  async getWorkerDependencies(): Promise<remote_dependency_information_t[]> {
    return Array.from(this.dependency_definition_by_alias.values())
      .sort((left_definition, right_definition): number => {
        return left_definition.alias.localeCompare(right_definition.alias);
      })
      .map((dependency_definition): remote_dependency_information_t => {
        return {
          alias: dependency_definition.alias,
          module_specifier: dependency_definition.module_specifier,
          export_name: dependency_definition.export_name,
          is_default_export: dependency_definition.is_default_export,
          installed_worker_count: dependency_definition.installed_worker_ids.size
        };
      });
  }

  async defineWorkerConstant(params: define_worker_constant_params_t): Promise<void> {
    const { name, value } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Constant name must be a non-empty string.');
    }

    ValidateIdentifier({ value: name, label: 'Constant name' });
    EnsureSerializable({ value, label: `Constant "${name}" value` });

    const existing_definition = this.constant_definition_by_name.get(name);
    if (existing_definition) {
      try {
        if (JSON.stringify(existing_definition.value) === JSON.stringify(value)) {
          if (this.lifecycle_state === 'running') {
            await this.installConstantAcrossRunningWorkers({
              name,
              constant_definition: existing_definition
            });
          }
          return;
        }
      } catch {
        // fall through to redefinition path when equality cannot be determined.
      }
    }

    const constant_definition: worker_constant_definition_t =
      existing_definition ?? {
        name,
        value,
        installed_worker_ids: new Set<number>()
      };

    constant_definition.name = name;
    constant_definition.value = value;
    constant_definition.installed_worker_ids.clear();

    this.constant_definition_by_name.set(name, constant_definition);
    await this.installConstantAcrossRunningWorkers({
      name,
      constant_definition
    });
  }

  async undefineWorkerConstant(
    params: undefine_worker_constant_params_t
  ): Promise<void> {
    const { name } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Constant name must be a non-empty string.');
    }

    const existing_definition = this.constant_definition_by_name.get(name);
    if (!existing_definition) {
      return;
    }

    this.constant_definition_by_name.delete(name);

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const removal_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.removeConstantOnWorker({
            worker_id: worker_state.worker_id,
            name
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = removal_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to remove constant "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  async getWorkerConstants(): Promise<remote_constant_information_t[]> {
    return Array.from(this.constant_definition_by_name.values())
      .sort((left_definition, right_definition): number => {
        return left_definition.name.localeCompare(right_definition.name);
      })
      .map((constant_definition): remote_constant_information_t => {
        return {
          name: constant_definition.name,
          installed_worker_count: constant_definition.installed_worker_ids.size
        };
      });
  }

  async sharedCreate<content_t = unknown>(
    params: shared_create_params_t<content_t>
  ): Promise<void> {
    await this.shared_memory_store.createChunk({
      chunk: params
    });
  }

  async sharedAccess<content_t = unknown>(
    params: shared_access_params_t
  ): Promise<content_t> {
    return await this.shared_memory_store.accessChunk<content_t>({
      access: params,
      owner_context: this.getParentSharedOwnerContext()
    });
  }

  async sharedWrite<content_t = unknown>(
    params: shared_write_params_t<content_t>
  ): Promise<void> {
    await this.shared_memory_store.writeChunk({
      write: params,
      owner_context: this.getParentSharedOwnerContext()
    });
  }

  async sharedRelease(params: shared_release_params_t): Promise<void> {
    await this.shared_memory_store.releaseChunk({
      release: params,
      owner_context: this.getParentSharedOwnerContext()
    });
  }

  async sharedFree(params: shared_free_params_t): Promise<void> {
    await this.shared_memory_store.freeChunk({
      free: params,
      owner_context: this.getParentSharedOwnerContext()
    });
  }

  async sharedGetLockDebugInfo(
    params: shared_get_lock_debug_info_params_t = {}
  ): Promise<shared_lock_debug_information_result_t> {
    return this.shared_memory_store.getLockDebugInfo({
      options: params
    });
  }

  async defineDatabaseConnection(
    params: define_database_connection_params_t
  ): Promise<void> {
    const { name, connector } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Database connection name must be a non-empty string.');
    }

    ValidateIdentifier({ value: name, label: 'Database connection name' });

    if (!connector || !IsRecordObject(connector)) {
      throw new Error('Database connector must be an object.');
    }

    if (this.database_connection_definition_by_name.has(name)) {
      throw new Error(`Database connection "${name}" is already defined.`);
    }

    if (typeof connector.type !== 'string' || connector.type.length === 0) {
      throw new Error('Database connector.type must be a non-empty string.');
    }

    const connector_type = NormalizeDatabaseConnectorType({
      value: connector.type,
      label: 'Database connector.type'
    });

    if (!IsRecordObject(connector.semantics)) {
      throw new Error('Database connector.semantics must be an object.');
    }

    let semantics: Record<string, unknown>;
    try {
      semantics = structuredClone(connector.semantics);
    } catch (error) {
      throw new Error(
        `Database connection "${name}" semantics must be serializable: ${GetErrorMessage({ error })}`
      );
    }

    const database_connection_definition: worker_database_connection_definition_t = {
      name,
      connector_type,
      semantics,
      installed_worker_ids: new Set<number>()
    };

    this.database_connection_definition_by_name.set(name, database_connection_definition);

    try {
      await this.installDatabaseConnectionAcrossRunningWorkers({
        name,
        database_connection_definition
      });
    } catch (error) {
      this.database_connection_definition_by_name.delete(name);
      throw error;
    }
  }

  async undefineDatabaseConnection(
    params: undefine_database_connection_params_t
  ): Promise<void> {
    const { name } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Database connection name must be a non-empty string.');
    }

    const existing_definition = this.database_connection_definition_by_name.get(name);
    if (!existing_definition) {
      return;
    }

    this.database_connection_definition_by_name.delete(name);

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const removal_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.removeDatabaseConnectionOnWorker({
            worker_id: worker_state.worker_id,
            name
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = removal_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to remove database connection "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  async getWorkerDatabaseConnections(): Promise<
    remote_database_connection_information_t[]
  > {
    return Array.from(this.database_connection_definition_by_name.values())
      .sort((left_definition, right_definition): number => {
        return left_definition.name.localeCompare(right_definition.name);
      })
      .map((database_connection_definition): remote_database_connection_information_t => {
        return {
          name: database_connection_definition.name,
          connector_type: database_connection_definition.connector_type,
          installed_worker_count:
            database_connection_definition.installed_worker_ids.size
        };
      });
  }

  async startWorkers(params: start_workers_params_t): Promise<void> {
    const { count } = params;

    if (this.lifecycle_state !== 'stopped') {
      throw new Error('Workers are already started or currently transitioning.');
    }

    ValidatePositiveInteger({ value: count, label: 'count' });
    this.applyRuntimeOptions({ options: params });

    this.lifecycle_state = 'starting';
    this.target_worker_count = count;

    try {
      for (let index = 0; index < count; index += 1) {
        await this.createWorker({ restart_attempt: 0 });
      }

      this.lifecycle_state = 'running';
    } catch (error) {
      await this.stopWorkers();
      throw new Error(`Failed to start workers: ${GetErrorMessage({ error })}`);
    }
  }

  async getRemoteFunctions(): Promise<remote_function_information_t[]> {
    return Array.from(this.function_definition_by_name.values())
      .sort((left_definition, right_definition): number => {
        return left_definition.name.localeCompare(right_definition.name);
      })
      .map((function_definition): remote_function_information_t => {
        return {
          name: function_definition.name,
          parameter_signature: function_definition.parameter_signature,
          function_hash_sha1: function_definition.function_hash_sha1,
          installed_worker_count: function_definition.installed_worker_ids.size
        };
      });
  }

  async undefineWokerFunction(
    params: undefine_worker_function_params_t
  ): Promise<void> {
    const { name } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Function name must be a non-empty string.');
    }

    const existing_definition = this.function_definition_by_name.get(name);
    if (!existing_definition) {
      return;
    }

    this.function_definition_by_name.delete(name);

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const removal_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.removeFunctionOnWorker({
            worker_id: worker_state.worker_id,
            function_name: name
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = removal_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to remove function "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  async undefineWorkerFunction(
    params: undefine_worker_function_params_t
  ): Promise<void> {
    await this.undefineWokerFunction(params);
  }

  async stopWorkers(): Promise<void> {
    if (this.stop_in_progress_promise) {
      await this.stop_in_progress_promise;
      return;
    }

    if (this.lifecycle_state === 'stopped') {
      return;
    }

    this.stop_in_progress_promise = this.stopWorkersInternal();

    try {
      await this.stop_in_progress_promise;
    } finally {
      this.stop_in_progress_promise = null;
    }
  }

  private async stopWorkersInternal(): Promise<void> {
    this.lifecycle_state = 'stopping';

    const worker_states = Array.from(this.worker_state_by_id.values());
    for (const worker_state of worker_states) {
      worker_state.shutting_down = true;
      this.setWorkerHealthState({
        worker_id: worker_state.worker_id,
        health_state: 'stopped',
        reason: 'stopWorkers requested'
      });
    }

    await Promise.all(
      worker_states.map(async (worker_state): Promise<void> => {
        if (worker_state.ready) {
          try {
            await this.sendControlCommand({
              worker_id: worker_state.worker_id,
              command: 'shutdown',
              payload: {},
              timeout_ms: Math.min(2_000, this.default_control_timeout_ms)
            });
          } catch {
            // Ignore and continue into forced termination path.
          }
        }

        await this.waitForWorkerExitOrTerminate({
          worker_id: worker_state.worker_id,
          timeout_ms: this.default_stop_timeout_ms
        });
      })
    );

    this.rejectAllPendingCalls({
      reason: new Error('Workers were stopped before call completion.')
    });

    this.rejectAllPendingControls({
      reason: new Error('Workers were stopped before control completion.')
    });

    for (const function_definition of this.function_definition_by_name.values()) {
      function_definition.installed_worker_ids.clear();
    }

    for (const dependency_definition of this.dependency_definition_by_alias.values()) {
      dependency_definition.installed_worker_ids.clear();
    }

    for (const constant_definition of this.constant_definition_by_name.values()) {
      constant_definition.installed_worker_ids.clear();
    }

    for (const database_connection_definition of this.database_connection_definition_by_name.values()) {
      database_connection_definition.installed_worker_ids.clear();
    }

    this.worker_state_by_id.clear();
    this.ready_waiter_by_worker_id.clear();
    this.exit_waiter_by_worker_id.clear();
    this.last_error_by_worker_id.clear();

    this.round_robin_index = 0;
    this.target_worker_count = 0;
    this.lifecycle_state = 'stopped';
  }

  private async callWorkerFunction(params: {
    function_name: string;
    call_args: unknown[];
  }): Promise<unknown> {
    const { function_name, call_args } = params;

    if (this.lifecycle_state !== 'running') {
      throw new Error('Workers are not running. Start workers before invoking calls.');
    }

    const function_definition = this.function_definition_by_name.get(function_name);
    if (!function_definition) {
      throw new Error(`Function "${function_name}" is not defined.`);
    }

    EnsureSerializable({ value: call_args, label: 'Call args' });

    const worker_state = this.selectWorkerForCall({
      function_definition,
      function_name
    });
    const request_id = this.generateCallRequestId();

    return await new Promise<unknown>((resolve, reject): void => {
      const timeout_handle = setTimeout((): void => {
        const pending_call = this.pending_call_by_request_id.get(request_id);
        if (!pending_call) {
          return;
        }

        this.autoReleaseSharedLocksByOwnerId({
          worker_id: worker_state.worker_id,
          call_request_id: request_id,
          release_reason: 'call_timeout_auto_release'
        });

        this.pending_call_by_request_id.delete(request_id);
        const current_worker_state = this.worker_state_by_id.get(worker_state.worker_id);
        current_worker_state?.pending_call_request_ids.delete(request_id);
        if (current_worker_state) {
          this.setWorkerHealthState({
            worker_id: current_worker_state.worker_id,
            health_state: 'degraded',
            reason: `call timeout for ${function_name}`
          });
        }

        reject(
          new Error(
            `Call to "${function_name}" timed out after ${this.default_call_timeout_ms}ms.`
          )
        );
      }, this.default_call_timeout_ms);

      this.pending_call_by_request_id.set(request_id, {
        resolve,
        reject,
        timeout_handle,
        worker_id: worker_state.worker_id,
        function_name
      });

      worker_state.pending_call_request_ids.add(request_id);

      const message: call_request_message_t = {
        message_type: 'call_request',
        request_id,
        function_name,
        call_args
      };

      try {
        worker_state.worker_instance.postMessage(message);
      } catch (error) {
        clearTimeout(timeout_handle);
        this.autoReleaseSharedLocksByOwnerId({
          worker_id: worker_state.worker_id,
          call_request_id: request_id,
          release_reason: 'call_rejection_auto_release'
        });
        worker_state.pending_call_request_ids.delete(request_id);
        this.pending_call_by_request_id.delete(request_id);
        this.setWorkerHealthState({
          worker_id: worker_state.worker_id,
          health_state: 'degraded',
          reason: `call dispatch failed for ${function_name}`
        });

        reject(
          new Error(
            `Failed to dispatch call "${function_name}": ${GetErrorMessage({ error })}`
          )
        );
      }
    });
  }

  private createCallProxy(): worker_call_proxy_t {
    const call_proxy_target: Record<string, unknown> = {};

    const call_proxy = new Proxy(call_proxy_target, {
      get: (_target, property_name: string | symbol): unknown => {
        if (typeof property_name !== 'string') {
          return undefined;
        }

        if (
          property_name === 'then' ||
          property_name === 'catch' ||
          property_name === 'finally'
        ) {
          return undefined;
        }

        return async (...call_args: unknown[]): Promise<unknown> => {
          return await this.callWorkerFunction({
            function_name: property_name,
            call_args
          });
        };
      }
    });

    return call_proxy as worker_call_proxy_t;
  }

  private authorizeClusterCallRequest(params: {
    request: cluster_call_request_message_i;
    cluster?: string;
    environment?: string;
    target_selector?: cluster_authorization_target_selector_t;
  }): cluster_protocol_validation_result_t<{
    evaluation_result: cluster_authorization_evaluation_result_t;
  }> {
    const { request, cluster, environment, target_selector } = params;
    const required_capability_list = [
      `rpc.call:function:${request.function_name}`,
      'rpc.call:*'
    ];

    if (
      !CapabilityListAllowsAnyRequestedCapability({
        granted_capability_list: request.caller_identity.scopes,
        required_capability_list
      })
    ) {
      return {
        ok: false,
        error: {
          code: 'AUTH_FAILED',
          message:
            'Caller identity does not include required rpc.call capability claims.',
          retryable: false,
          unknown_outcome: false,
          details: {
            function_name: request.function_name,
            required_capability_list
          }
        }
      };
    }

    const evaluation_result = this.cluster_authorization_policy_engine.authorizeCall({
      function_name: request.function_name,
      subject: request.caller_identity.subject,
      tenant: request.caller_identity.tenant_id,
      environment,
      cluster,
      target_selector
    });

    if (!evaluation_result.authorized) {
      return {
        ok: false,
        error: {
          code: 'FORBIDDEN_FUNCTION',
          message: `Caller is not authorized to execute function "${request.function_name}".`,
          retryable: false,
          unknown_outcome: false,
          details: {
            function_name: request.function_name,
            authorization_reason: evaluation_result.reason,
            matched_allow_policy_id_list:
              evaluation_result.matched_allow_policy_id_list,
            matched_deny_policy_id_list:
              evaluation_result.matched_deny_policy_id_list
          }
        }
      };
    }

    return {
      ok: true,
      value: {
        evaluation_result
      }
    };
  }

  private authorizeClusterAdminMutationRequest(params: {
    request: cluster_admin_mutation_request_message_i;
    node_id: string;
  }): cluster_protocol_validation_result_t<{
    evaluation_result: cluster_authorization_evaluation_result_t;
  }> {
    const { request, node_id } = params;
    const required_capability_list = BuildRequiredMutationCapabilityList({
      mutation_type: request.mutation_type
    });

    if (
      !CapabilityListAllowsAnyRequestedCapability({
        granted_capability_list: request.auth_context.capability_claims,
        required_capability_list
      })
    ) {
      return {
        ok: false,
        error: {
          code: 'ADMIN_AUTH_FAILED',
          message:
            'Mutation auth_context capability_claims do not include required mutation capability.',
          retryable: false,
          unknown_outcome: false,
          details: {
            mutation_type: request.mutation_type,
            required_capability_list
          }
        }
      };
    }

    const mutation_function_name = this.extractMutationFunctionNameFromPayload({
      request
    });

    const evaluation_result = this.cluster_authorization_policy_engine.authorizeMutation({
      mutation_type: request.mutation_type,
      function_name: mutation_function_name,
      target_selector: request.target_selector,
      subject: request.auth_context.subject,
      tenant: request.auth_context.tenant_id,
      environment: request.auth_context.environment,
      cluster: node_id
    });

    if (!evaluation_result.authorized) {
      return {
        ok: false,
        error: {
          code: 'ADMIN_FORBIDDEN',
          message: `Mutation "${request.mutation_type}" is forbidden for subject "${request.auth_context.subject}".`,
          retryable: false,
          unknown_outcome: false,
          details: {
            mutation_type: request.mutation_type,
            function_name: mutation_function_name,
            authorization_reason: evaluation_result.reason,
            matched_allow_policy_id_list:
              evaluation_result.matched_allow_policy_id_list,
            matched_deny_policy_id_list:
              evaluation_result.matched_deny_policy_id_list
          }
        }
      };
    }

    return {
      ok: true,
      value: {
        evaluation_result
      }
    };
  }

  private extractMutationFunctionNameFromPayload(params: {
    request: cluster_admin_mutation_request_message_i;
  }): string | undefined {
    const { request } = params;
    if (!IsRecordObject(request.payload)) {
      return undefined;
    }

    const name = request.payload.name;
    if (typeof name !== 'string' || name.length === 0) {
      return undefined;
    }

    if (
      request.mutation_type === 'define_function' ||
      request.mutation_type === 'redefine_function' ||
      request.mutation_type === 'undefine_function'
    ) {
      return name;
    }

    return undefined;
  }

  private buildClusterCallAckMessage(params: {
    request_id: string;
    attempt_index: number;
    node_id: string;
    accepted: boolean;
    timestamp_unix_ms: number;
  }): cluster_call_ack_message_i {
    const { request_id, attempt_index, node_id, accepted, timestamp_unix_ms } =
      params;

    return {
      protocol_version: 1,
      message_type: 'cluster_call_ack',
      timestamp_unix_ms,
      request_id,
      attempt_index,
      node_id,
      accepted,
      queue_position: 0,
      estimated_start_delay_ms: 0
    };
  }

  private buildClusterCallSuccessMessage(params: {
    request_id: string;
    attempt_index: number;
    node_id: string;
    function_name: string;
    function_hash_sha1?: string;
    return_value: unknown;
    gateway_received_unix_ms: number;
    node_received_unix_ms: number;
    worker_started_unix_ms: number;
    worker_finished_unix_ms: number;
    timestamp_unix_ms: number;
  }): cluster_call_response_success_message_i {
    const {
      request_id,
      attempt_index,
      node_id,
      function_name,
      function_hash_sha1,
      return_value,
      gateway_received_unix_ms,
      node_received_unix_ms,
      worker_started_unix_ms,
      worker_finished_unix_ms,
      timestamp_unix_ms
    } = params;

    return {
      protocol_version: 1,
      message_type: 'cluster_call_response_success',
      timestamp_unix_ms,
      request_id,
      attempt_index,
      node_id,
      function_name,
      function_hash_sha1: function_hash_sha1 ?? '',
      return_value,
      timing: {
        gateway_received_unix_ms,
        node_received_unix_ms,
        worker_started_unix_ms,
        worker_finished_unix_ms
      }
    };
  }

  private buildClusterCallErrorMessage(params: {
    request_id: string;
    attempt_index: number;
    node_id?: string;
    code: string;
    message: string;
    retryable: boolean;
    unknown_outcome: boolean;
    details?: Record<string, unknown>;
    gateway_received_unix_ms: number;
    last_attempt_started_unix_ms: number;
    timestamp_unix_ms: number;
  }): cluster_call_response_error_message_i {
    const {
      request_id,
      attempt_index,
      node_id,
      code,
      message,
      retryable,
      unknown_outcome,
      details,
      gateway_received_unix_ms,
      last_attempt_started_unix_ms,
      timestamp_unix_ms
    } = params;

    return {
      protocol_version: 1,
      message_type: 'cluster_call_response_error',
      timestamp_unix_ms,
      request_id,
      attempt_index,
      node_id,
      error: {
        code,
        message,
        retryable,
        unknown_outcome,
        details
      },
      timing: {
        gateway_received_unix_ms,
        last_attempt_started_unix_ms
      }
    };
  }

  private emitAdminMutationLifecycleEvent(params: {
    event_name:
      | 'admin_mutation_requested'
      | 'admin_mutation_authorized'
      | 'admin_mutation_applied'
      | 'admin_mutation_partially_failed'
      | 'admin_mutation_rolled_back'
      | 'admin_mutation_denied';
    mutation_id: string;
    request_id: string;
    severity?: worker_event_severity_t;
    error?: remote_error_t;
    details?: Record<string, unknown>;
  }): void {
    const {
      event_name,
      mutation_id,
      request_id,
      severity = 'info',
      error,
      details
    } = params;

    this.emitWorkerEvent({
      worker_event: {
        event_id: this.generateWorkerEventId(),
        worker_id: 0,
        source: 'parent',
        event_name,
        severity,
        timestamp: new Date().toISOString(),
        correlation_id: mutation_id,
        error,
        details: {
          request_id,
          ...details
        }
      }
    });

    this.appendClusterOperationLifecycleEvent({
      event: {
        timestamp_unix_ms: Date.now(),
        event_name,
        severity,
        request_id,
        mutation_id,
        error_code: error?.name,
        details
      }
    });
  }

  private buildAdminMutationAckMessage(params: {
    mutation_id: string;
    request_id: string;
    accepted: boolean;
    target_node_count: number;
    dry_run: boolean;
    timestamp_unix_ms: number;
  }): cluster_admin_mutation_ack_message_i {
    const {
      mutation_id,
      request_id,
      accepted,
      target_node_count,
      dry_run,
      timestamp_unix_ms
    } = params;

    return {
      protocol_version: 1,
      message_type: 'cluster_admin_mutation_ack',
      timestamp_unix_ms,
      mutation_id,
      request_id,
      accepted,
      planner_id: 'single_node_workerprocedurecall',
      target_node_count,
      dry_run
    };
  }

  private buildAdminMutationResultMessage(params: {
    mutation_id: string;
    request_id: string;
    status: cluster_admin_mutation_status_t;
    rollout_strategy: cluster_admin_rollout_mode_t;
    min_success_percent: number;
    per_node_state_list: cluster_mutation_node_state_t[];
    planning_started_unix_ms: number;
    apply_started_unix_ms: number;
    verify_finished_unix_ms: number;
    timestamp_unix_ms: number;
  }): cluster_admin_mutation_result_message_i {
    const {
      mutation_id,
      request_id,
      status,
      rollout_strategy,
      min_success_percent,
      per_node_state_list,
      planning_started_unix_ms,
      apply_started_unix_ms,
      verify_finished_unix_ms,
      timestamp_unix_ms
    } = params;
    const applied_node_count = per_node_state_list.filter((node_state): boolean => {
      return node_state.status === 'applied';
    }).length;
    const failed_node_count = per_node_state_list.filter((node_state): boolean => {
      return node_state.status === 'failed';
    }).length;
    const rolled_back_node_count = per_node_state_list.filter((node_state): boolean => {
      return node_state.status === 'rolled_back';
    }).length;
    const target_node_count = per_node_state_list.length;
    const achieved_success_percent =
      target_node_count === 0 ? 0 : (applied_node_count / target_node_count) * 100;

    return {
      protocol_version: 1,
      message_type: 'cluster_admin_mutation_result',
      timestamp_unix_ms,
      mutation_id,
      request_id,
      status,
      rollout_strategy,
      summary: {
        target_node_count,
        applied_node_count,
        failed_node_count,
        rolled_back_node_count,
        min_success_percent,
        achieved_success_percent
      },
      per_node_results: per_node_state_list.map((node_state) => {
        return {
          node_id: node_state.node_id,
          status: node_state.status,
          error_code: node_state.error_code,
          error_message: node_state.error_message,
          applied_version: node_state.applied_version
        };
      }),
      timing: {
        planning_started_unix_ms,
        apply_started_unix_ms,
        verify_finished_unix_ms
      },
      audit_record_id: `audit_${mutation_id}`
    };
  }

  private buildAdminMutationErrorMessage(params: {
    mutation_id: string;
    request_id: string;
    code: cluster_protocol_error_code_t;
    message: string;
    details: Record<string, unknown>;
    timestamp_unix_ms: number;
  }): cluster_admin_mutation_error_message_i {
    const { mutation_id, request_id, code, message, details, timestamp_unix_ms } = params;
    const retryable_error_code_set = new Set<cluster_protocol_error_code_t>([
      'ADMIN_CONFLICT',
      'ADMIN_DISPATCH_RETRYABLE',
      'ADMIN_TIMEOUT',
      'ADMIN_INTERNAL'
    ]);
    const unknown_outcome_code_set = new Set<cluster_protocol_error_code_t>([
      'ADMIN_TIMEOUT',
      'ADMIN_INTERNAL',
      'ADMIN_ROLLBACK_FAILED'
    ]);

    return {
      protocol_version: 1,
      message_type: 'cluster_admin_mutation_error',
      timestamp_unix_ms,
      mutation_id,
      request_id,
      error: {
        code,
        message,
        retryable: retryable_error_code_set.has(code),
        unknown_outcome: unknown_outcome_code_set.has(code),
        details
      }
    };
  }

  private resolveMutationTargetNodeList(params: {
    request: cluster_admin_mutation_request_message_i;
    node_id: string;
  }): cluster_protocol_validation_result_t<{
    target_node_list: cluster_mutation_target_node_t[];
  }> {
    const { request, node_id } = params;

    const available_node_by_id = new Map<string, cluster_mutation_target_node_t>();
    for (const registered_node of this.cluster_mutation_node_registration_by_id.values()) {
      available_node_by_id.set(registered_node.node_id, {
        node_id: registered_node.node_id,
        labels: registered_node.labels ? { ...registered_node.labels } : undefined,
        zones: registered_node.zones ? [...registered_node.zones] : undefined,
        node_agent_semver: registered_node.node_agent_semver,
        runtime_semver: registered_node.runtime_semver,
        is_local_node: registered_node.node_id === node_id,
        mutation_executor: registered_node.mutation_executor
      });
    }

    if (!available_node_by_id.has(node_id)) {
      available_node_by_id.set(node_id, {
        node_id,
        is_local_node: true
      });
    }

    const all_available_nodes = Array.from(available_node_by_id.values()).sort(
      (left_node, right_node): number => {
        return left_node.node_id.localeCompare(right_node.node_id);
      }
    );

    if (request.target_scope === 'single_node') {
      const target_node_id = request.target_selector?.node_ids?.[0];
      if (typeof target_node_id !== 'string' || target_node_id.length === 0) {
        return {
          ok: false,
          error: {
            code: 'ADMIN_VALIDATION_FAILED',
            message:
              'target_scope "single_node" requires target_selector.node_ids[0] to be a non-empty string.',
            retryable: false,
            unknown_outcome: false,
            details: {
              target_scope: request.target_scope
            }
          }
        };
      }

      const single_target_node = available_node_by_id.get(target_node_id);
      if (!single_target_node) {
        return {
          ok: false,
          error: {
            code: 'ADMIN_TARGET_EMPTY',
            message: `Mutation target "${target_node_id}" was not found in available node registry.`,
            retryable: false,
            unknown_outcome: false,
            details: {
              target_node_id
            }
          }
        };
      }

      return {
        ok: true,
        value: {
          target_node_list: [single_target_node]
        }
      };
    }

    if (request.target_scope === 'cluster_wide') {
      if (all_available_nodes.length === 0) {
        return {
          ok: false,
          error: {
            code: 'ADMIN_TARGET_EMPTY',
            message: 'cluster_wide mutation resolved zero target nodes.',
            retryable: false,
            unknown_outcome: false,
            details: {
              target_scope: request.target_scope
            }
          }
        };
      }

      return {
        ok: true,
        value: {
          target_node_list: all_available_nodes
        }
      };
    }

    const selected_node_list = all_available_nodes.filter(
      (candidate_node): boolean => {
        return this.doesMutationTargetSelectorMatchNode({
          request,
          candidate_node
        });
      }
    );

    if (selected_node_list.length === 0) {
      return {
        ok: false,
        error: {
          code: 'ADMIN_TARGET_EMPTY',
          message: 'node_selector mutation resolved zero target nodes.',
          retryable: false,
          unknown_outcome: false,
          details: {
            target_scope: request.target_scope
          }
        }
      };
    }

    return {
      ok: true,
      value: {
        target_node_list: selected_node_list
      }
    };
  }

  private doesMutationTargetSelectorMatchNode(params: {
    request: cluster_admin_mutation_request_message_i;
    candidate_node: cluster_mutation_target_node_t;
  }): boolean {
    const { request, candidate_node } = params;
    const selector = request.target_selector;
    if (!selector) {
      return true;
    }

    if (Array.isArray(selector.node_ids) && selector.node_ids.length > 0) {
      if (!selector.node_ids.includes(candidate_node.node_id)) {
        return false;
      }
    }

    if (Array.isArray(selector.zones) && selector.zones.length > 0) {
      if (!Array.isArray(candidate_node.zones) || candidate_node.zones.length === 0) {
        return false;
      }
      const has_matching_zone = candidate_node.zones.some((candidate_zone): boolean => {
        return selector.zones?.includes(candidate_zone) ?? false;
      });
      if (!has_matching_zone) {
        return false;
      }
    }

    if (selector.labels && Object.keys(selector.labels).length > 0) {
      if (!candidate_node.labels) {
        return false;
      }
      for (const [label_key, label_value] of Object.entries(selector.labels)) {
        if (candidate_node.labels[label_key] !== label_value) {
          return false;
        }
      }
    }

    if (selector.version_constraints) {
      if (
        typeof selector.version_constraints.node_agent_semver === 'string' &&
        !this.doesSemverConstraintMatchValue({
          constraint: selector.version_constraints.node_agent_semver,
          value: candidate_node.node_agent_semver
        })
      ) {
        return false;
      }

      if (
        typeof selector.version_constraints.runtime_semver === 'string' &&
        !this.doesSemverConstraintMatchValue({
          constraint: selector.version_constraints.runtime_semver,
          value: candidate_node.runtime_semver
        })
      ) {
        return false;
      }
    }

    return true;
  }

  private doesSemverConstraintMatchValue(params: {
    constraint: string;
    value: string | undefined;
  }): boolean {
    const { constraint, value } = params;
    if (typeof value !== 'string' || value.length === 0) {
      return false;
    }

    if (!constraint.startsWith('>=')) {
      return constraint === value;
    }

    const minimum_value = constraint.slice(2);
    const parsed_value = this.parseSemverToIntList({
      value
    });
    const parsed_minimum_value = this.parseSemverToIntList({
      value: minimum_value
    });
    if (!parsed_value || !parsed_minimum_value) {
      return value === minimum_value;
    }

    const max_length = Math.max(parsed_value.length, parsed_minimum_value.length);
    for (let index = 0; index < max_length; index += 1) {
      const current_component = parsed_value[index] ?? 0;
      const minimum_component = parsed_minimum_value[index] ?? 0;
      if (current_component > minimum_component) {
        return true;
      }
      if (current_component < minimum_component) {
        return false;
      }
    }

    return true;
  }

  private parseSemverToIntList(params: { value: string }): number[] | null {
    const { value } = params;
    if (!/^\d+(?:\.\d+)*$/.test(value)) {
      return null;
    }
    return value.split('.').map((component): number => {
      return Number.parseInt(component, 10);
    });
  }

  private validateExpectedVersionForMutationRequest(params: {
    request: cluster_admin_mutation_request_message_i;
  }): cluster_protocol_validation_result_t<{
    entity_name: string;
    current_entity_version: string | null;
  }> {
    const { request } = params;
    const derived_entity_name = this.getMutationEntityName({
      request
    });
    const expected_version = request.expected_version;

    if (!expected_version) {
      return {
        ok: true,
        value: {
          entity_name: derived_entity_name,
          current_entity_version: this.cluster_entity_version_by_name.get(derived_entity_name) ?? null
        }
      };
    }

    const entity_name = expected_version.entity_name ?? derived_entity_name;
    const expected_entity_version = expected_version.entity_version;
    const compare_mode = expected_version.compare_mode ?? 'exact';
    const current_entity_version = this.cluster_entity_version_by_name.get(entity_name) ?? null;

    if (typeof expected_entity_version !== 'string' || expected_entity_version.length === 0) {
      return {
        ok: false,
        error: {
          code: 'ADMIN_CONFLICT',
          message: 'expected_version.entity_version must be a non-empty string.',
          retryable: false,
          unknown_outcome: false,
          details: {
            entity_name,
            compare_mode
          }
        }
      };
    }

    if (compare_mode === 'exact') {
      if (current_entity_version !== expected_entity_version) {
        return {
          ok: false,
          error: {
            code: 'ADMIN_CONFLICT',
            message: 'CAS exact comparison failed for expected_version.',
            retryable: false,
            unknown_outcome: false,
            details: {
              entity_name,
              compare_mode,
              expected_entity_version,
              current_entity_version
            }
          }
        };
      }
    } else {
      const at_least_match = this.isVersionAtLeast({
        current_entity_version,
        minimum_entity_version: expected_entity_version
      });
      if (!at_least_match) {
        return {
          ok: false,
          error: {
            code: 'ADMIN_CONFLICT',
            message: 'CAS at_least comparison failed for expected_version.',
            retryable: false,
            unknown_outcome: false,
            details: {
              entity_name,
              compare_mode,
              expected_entity_version,
              current_entity_version
            }
          }
        };
      }
    }

    return {
      ok: true,
      value: {
        entity_name,
        current_entity_version
      }
    };
  }

  private isVersionAtLeast(params: {
    current_entity_version: string | null;
    minimum_entity_version: string;
  }): boolean {
    const { current_entity_version, minimum_entity_version } = params;
    if (typeof current_entity_version !== 'string' || current_entity_version.length === 0) {
      return false;
    }

    const current_semver = this.parseSemverToIntList({
      value: current_entity_version
    });
    const minimum_semver = this.parseSemverToIntList({
      value: minimum_entity_version
    });
    if (current_semver && minimum_semver) {
      const max_length = Math.max(current_semver.length, minimum_semver.length);
      for (let index = 0; index < max_length; index += 1) {
        const current_component = current_semver[index] ?? 0;
        const minimum_component = minimum_semver[index] ?? 0;
        if (current_component > minimum_component) {
          return true;
        }
        if (current_component < minimum_component) {
          return false;
        }
      }
      return true;
    }

    if (
      /^\d+$/.test(current_entity_version) &&
      /^\d+$/.test(minimum_entity_version)
    ) {
      return Number.parseInt(current_entity_version, 10) >= Number.parseInt(minimum_entity_version, 10);
    }

    return current_entity_version === minimum_entity_version;
  }

  private getMutationEntityName(params: {
    request: cluster_admin_mutation_request_message_i;
  }): string {
    const { request } = params;
    const payload = IsRecordObject(request.payload) ? request.payload : {};

    if (typeof payload.name === 'string' && payload.name.length > 0) {
      return `${request.mutation_type}:${payload.name}`;
    }
    if (typeof payload.alias === 'string' && payload.alias.length > 0) {
      return `${request.mutation_type}:${payload.alias}`;
    }

    return `${request.mutation_type}:__cluster_scope__`;
  }

  private updateEntityVersionForMutationRequest(params: {
    request: cluster_admin_mutation_request_message_i;
    applied_version: string;
  }): void {
    const { request, applied_version } = params;
    const entity_name = this.getMutationEntityName({
      request
    });
    this.cluster_entity_version_by_name.set(entity_name, applied_version);
  }

  private buildDryRunNodeStateList(params: {
    target_node_list: cluster_mutation_target_node_t[];
  }): cluster_mutation_node_state_t[] {
    const { target_node_list } = params;
    return target_node_list.map((target_node): cluster_mutation_node_state_t => {
      return {
        node_id: target_node.node_id,
        status: 'skipped',
        apply_succeeded: false,
        verification_succeeded: false,
        rollback_succeeded: false
      };
    });
  }

  private async executeMutationRolloutWithRetries(params: {
    request: cluster_admin_mutation_request_message_i;
    target_node_list: cluster_mutation_target_node_t[];
    deadline_unix_ms: number;
  }): Promise<{
    rollout_execution_result: {
      final_status: cluster_admin_mutation_status_t;
      per_node_state_list: cluster_mutation_node_state_t[];
      rolled_back_node_count: number;
      first_applied_version: string | null;
      rollback_failed_error?: {
        code: cluster_protocol_error_code_t;
        message: string;
        details?: Record<string, unknown>;
      };
    };
    retry_attempt_count: number;
  }> {
    const { request, target_node_list, deadline_unix_ms } = params;

    let attempt_index = 1;
    let retry_attempt_count = 0;
    let rollout_execution_result = await this.executeMutationRolloutPlan({
      request,
      target_node_list
    });

    while (
      attempt_index < this.mutation_retry_max_attempts &&
      Date.now() < deadline_unix_ms &&
      this.shouldRetryMutationRolloutResult({
        rollout_execution_result
      })
    ) {
      retry_attempt_count += 1;
      const retry_delay_ms = Math.min(
        this.mutation_retry_max_delay_ms,
        this.mutation_retry_base_delay_ms * 2 ** (attempt_index - 1)
      );
      const now_unix_ms = Date.now();
      if (now_unix_ms + retry_delay_ms >= deadline_unix_ms) {
        break;
      }

      await this.waitForMs({
        delay_ms: retry_delay_ms
      });

      attempt_index += 1;
      rollout_execution_result = await this.executeMutationRolloutPlan({
        request,
        target_node_list
      });
    }

    return {
      rollout_execution_result,
      retry_attempt_count
    };
  }

  private shouldRetryMutationRolloutResult(params: {
    rollout_execution_result: {
      final_status: cluster_admin_mutation_status_t;
      per_node_state_list: cluster_mutation_node_state_t[];
      rollback_failed_error?: {
        code: cluster_protocol_error_code_t;
      };
    };
  }): boolean {
    const { rollout_execution_result } = params;
    if (rollout_execution_result.rollback_failed_error) {
      return false;
    }

    if (rollout_execution_result.final_status !== 'partially_failed') {
      return false;
    }

    const has_applied_node = rollout_execution_result.per_node_state_list.some(
      (node_state): boolean => {
        return node_state.status === 'applied';
      }
    );
    if (has_applied_node) {
      return false;
    }

    const failed_node_state_list = rollout_execution_result.per_node_state_list.filter(
      (node_state): boolean => {
        return node_state.status === 'failed';
      }
    );
    if (failed_node_state_list.length === 0) {
      return false;
    }

    return failed_node_state_list.every((node_state): boolean => {
      return (
        node_state.error_code === 'ADMIN_DISPATCH_RETRYABLE' ||
        node_state.error_code === 'ADMIN_TIMEOUT'
      );
    });
  }

  private buildRetryableMutationFailureTerminalError(params: {
    request: cluster_admin_mutation_request_message_i;
    rollout_execution_result: {
      final_status: cluster_admin_mutation_status_t;
      per_node_state_list: cluster_mutation_node_state_t[];
      rollback_failed_error?: {
        code: cluster_protocol_error_code_t;
      };
    };
  }): cluster_admin_mutation_error_message_i | null {
    const { request, rollout_execution_result } = params;
    if (rollout_execution_result.rollback_failed_error) {
      return null;
    }

    if (rollout_execution_result.final_status !== 'partially_failed') {
      return null;
    }

    const has_applied_node = rollout_execution_result.per_node_state_list.some(
      (node_state): boolean => {
        return node_state.status === 'applied';
      }
    );
    if (has_applied_node) {
      return null;
    }

    const failed_node_state_list = rollout_execution_result.per_node_state_list.filter(
      (node_state): boolean => {
        return node_state.status === 'failed';
      }
    );
    if (failed_node_state_list.length === 0) {
      return null;
    }

    const all_timeouts = failed_node_state_list.every((node_state): boolean => {
      return node_state.error_code === 'ADMIN_TIMEOUT';
    });
    const all_retryable_dispatch = failed_node_state_list.every((node_state): boolean => {
      return (
        node_state.error_code === 'ADMIN_DISPATCH_RETRYABLE' ||
        node_state.error_code === 'ADMIN_TIMEOUT'
      );
    });

    if (!all_retryable_dispatch) {
      return null;
    }

    const code: cluster_protocol_error_code_t = all_timeouts
      ? 'ADMIN_TIMEOUT'
      : 'ADMIN_DISPATCH_RETRYABLE';

    return this.buildAdminMutationErrorMessage({
      mutation_id: request.mutation_id,
      request_id: request.request_id,
      code,
      message:
        code === 'ADMIN_TIMEOUT'
          ? 'Mutation rollout timed out before any node could be applied successfully.'
          : 'Mutation rollout dispatch failed on all targets; retry is safe with same mutation_id.',
      details: {
        failed_node_count: failed_node_state_list.length
      },
      timestamp_unix_ms: Date.now()
    });
  }

  private async executeMutationRolloutPlan(params: {
    request: cluster_admin_mutation_request_message_i;
    target_node_list: cluster_mutation_target_node_t[];
  }): Promise<{
    final_status: cluster_admin_mutation_status_t;
    per_node_state_list: cluster_mutation_node_state_t[];
    rolled_back_node_count: number;
    first_applied_version: string | null;
    rollback_failed_error?: {
      code: cluster_protocol_error_code_t;
      message: string;
      details?: Record<string, unknown>;
    };
  }> {
    const { request, target_node_list } = params;

    const sorted_target_node_list = [...target_node_list].sort(
      (left_node, right_node): number => {
        return left_node.node_id.localeCompare(right_node.node_id);
      }
    );
    const node_state_by_id = new Map<string, cluster_mutation_node_state_t>();
    for (const target_node of sorted_target_node_list) {
      node_state_by_id.set(target_node.node_id, {
        node_id: target_node.node_id,
        status: 'skipped',
        apply_succeeded: false,
        verification_succeeded: false,
        rollback_succeeded: false
      });
    }

    const rollout_wave_node_list = this.planRolloutWaveNodeList({
      request,
      target_node_list: sorted_target_node_list
    });

    const apply_timeout_ms = request.rollout_strategy.apply_timeout_ms ?? 30_000;
    const verify_timeout_ms = request.rollout_strategy.verify_timeout_ms ?? 30_000;
    const inter_batch_delay_ms = request.rollout_strategy.inter_batch_delay_ms ?? 0;
    const min_success_percent = request.rollout_strategy.min_success_percent ?? 100;

    let saw_verification_failure = false;
    let first_applied_version: string | null = null;

    for (let wave_index = 0; wave_index < rollout_wave_node_list.length; wave_index += 1) {
      const wave_target_node_list = rollout_wave_node_list[wave_index];
      const wave_result_list = await Promise.all(
        wave_target_node_list.map(async (target_node): Promise<cluster_mutation_node_state_t> => {
          return await this.executeMutationApplyOnNode({
            request,
            target_node,
            apply_timeout_ms,
            verify_timeout_ms
          });
        })
      );

      for (const wave_result of wave_result_list) {
        node_state_by_id.set(wave_result.node_id, wave_result);
        if (
          wave_result.apply_succeeded &&
          wave_result.verification_succeeded &&
          !first_applied_version &&
          typeof wave_result.applied_version === 'string'
        ) {
          first_applied_version = wave_result.applied_version;
        }
        if (wave_result.apply_succeeded && !wave_result.verification_succeeded) {
          saw_verification_failure = true;
        }
      }

      const current_node_state_list = Array.from(node_state_by_id.values());
      const current_success_percent = this.calculateMutationSuccessPercent({
        per_node_state_list: current_node_state_list
      });

      if (request.rollout_strategy.mode === 'canary_then_expand' && wave_index === 0) {
        const canary_wave_failed = wave_result_list.some((node_state): boolean => {
          return node_state.status !== 'applied';
        });
        if (canary_wave_failed) {
          break;
        }
      }

      if (
        request.rollout_strategy.mode === 'rolling_percent' &&
        current_success_percent < min_success_percent
      ) {
        const remaining_waves = rollout_wave_node_list.length - (wave_index + 1);
        if (remaining_waves <= 0) {
          break;
        }
      }

      if (inter_batch_delay_ms > 0 && wave_index < rollout_wave_node_list.length - 1) {
        await this.waitForMs({
          delay_ms: inter_batch_delay_ms
        });
      }
    }

    const per_node_state_list = Array.from(node_state_by_id.values()).sort(
      (left_node_state, right_node_state): number => {
        return left_node_state.node_id.localeCompare(right_node_state.node_id);
      }
    );
    const achieved_success_percent = this.calculateMutationSuccessPercent({
      per_node_state_list
    });
    const has_failure = per_node_state_list.some((node_state): boolean => {
      return node_state.status === 'failed';
    });

    const rollback_policy = request.rollout_strategy.rollback_policy;
    const should_auto_rollback =
      rollback_policy?.auto_rollback === true &&
      ((rollback_policy.rollback_on_partial_failure === true &&
        (has_failure || achieved_success_percent < min_success_percent)) ||
        (rollback_policy.rollback_on_verification_failure === true &&
          saw_verification_failure));

    if (should_auto_rollback) {
      const rollback_result = await this.rollbackMutationAcrossAppliedNodes({
        request,
        target_node_list: sorted_target_node_list,
        per_node_state_list
      });
      if (!rollback_result.ok) {
        return {
          final_status: 'partially_failed',
          per_node_state_list: rollback_result.per_node_state_list,
          rolled_back_node_count: rollback_result.rolled_back_node_count,
          first_applied_version,
          rollback_failed_error: {
            code: 'ADMIN_ROLLBACK_FAILED',
            message: rollback_result.error_message,
            details: {
              per_node_results: rollback_result.per_node_state_list
            }
          }
        };
      }

      return {
        final_status: 'rolled_back',
        per_node_state_list: rollback_result.per_node_state_list,
        rolled_back_node_count: rollback_result.rolled_back_node_count,
        first_applied_version
      };
    }

    return {
      final_status: has_failure || achieved_success_percent < min_success_percent
        ? 'partially_failed'
        : 'completed',
      per_node_state_list,
      rolled_back_node_count: 0,
      first_applied_version
    };
  }

  private planRolloutWaveNodeList(params: {
    request: cluster_admin_mutation_request_message_i;
    target_node_list: cluster_mutation_target_node_t[];
  }): cluster_mutation_target_node_t[][] {
    const { request, target_node_list } = params;
    const sorted_target_node_list = [...target_node_list].sort(
      (left_node, right_node): number => {
        return left_node.node_id.localeCompare(right_node.node_id);
      }
    );

    if (request.rollout_strategy.mode === 'single_node') {
      return [sorted_target_node_list.slice(0, 1)];
    }

    if (request.rollout_strategy.mode === 'all_at_once') {
      return [sorted_target_node_list];
    }

    if (request.rollout_strategy.mode === 'rolling_percent') {
      const batch_percent = request.rollout_strategy.batch_percent ?? 10;
      const batch_size = Math.max(
        1,
        Math.ceil((sorted_target_node_list.length * batch_percent) / 100)
      );
      const wave_node_list: cluster_mutation_target_node_t[][] = [];
      for (let start_index = 0; start_index < sorted_target_node_list.length; start_index += batch_size) {
        wave_node_list.push(
          sorted_target_node_list.slice(start_index, start_index + batch_size)
        );
      }
      return wave_node_list;
    }

    const canary_node_count = Math.min(
      Math.max(1, request.rollout_strategy.canary_node_count ?? 1),
      sorted_target_node_list.length
    );
    const canary_node_list = sorted_target_node_list.slice(0, canary_node_count);
    const remaining_node_list = sorted_target_node_list.slice(canary_node_count);
    if (remaining_node_list.length === 0) {
      return [canary_node_list];
    }

    const batch_percent = request.rollout_strategy.batch_percent ?? 100;
    const batch_size = Math.max(
      1,
      Math.ceil((remaining_node_list.length * batch_percent) / 100)
    );
    const wave_node_list: cluster_mutation_target_node_t[][] = [canary_node_list];
    for (let start_index = 0; start_index < remaining_node_list.length; start_index += batch_size) {
      wave_node_list.push(remaining_node_list.slice(start_index, start_index + batch_size));
    }
    return wave_node_list;
  }

  private async executeMutationApplyOnNode(params: {
    request: cluster_admin_mutation_request_message_i;
    target_node: cluster_mutation_target_node_t;
    apply_timeout_ms: number;
    verify_timeout_ms: number;
  }): Promise<cluster_mutation_node_state_t> {
    const { request, target_node, apply_timeout_ms, verify_timeout_ms } = params;
    const base_state: cluster_mutation_node_state_t = {
      node_id: target_node.node_id,
      status: 'failed',
      apply_succeeded: false,
      verification_succeeded: false,
      rollback_succeeded: false
    };

    try {
      if (target_node.is_local_node) {
        const snapshot = this.captureLocalMutationRollbackSnapshot({
          request
        });
        if (request.in_flight_policy === 'drain_and_swap') {
          const drained = await this.waitForLocalWorkersToDrain({
            timeout_ms: apply_timeout_ms
          });
          if (!drained) {
            return {
              ...base_state,
              error_code: 'ADMIN_TIMEOUT',
              error_message: 'Local drain_and_swap timed out waiting for in-flight calls to drain.'
            };
          }
        }

        await this.runWithTimeout({
          timeout_ms: apply_timeout_ms,
          timeout_message: `Mutation apply timed out on node "${target_node.node_id}".`,
          promise_factory: async (): Promise<void> => {
            await this.applySingleNodeMutationRequest({
              request
            });
          }
        });

        const verification_succeeded = await this.runWithTimeout({
          timeout_ms: verify_timeout_ms,
          timeout_message: `Mutation verification timed out on node "${target_node.node_id}".`,
          promise_factory: async (): Promise<boolean> => {
            return this.verifyLocalMutationRequestApplied({
              request
            });
          }
        });

        const applied_version = this.resolveLocalAppliedVersionForMutation({
          request
        });

        return {
          ...base_state,
          status: verification_succeeded ? 'applied' : 'failed',
          applied_version,
          apply_succeeded: true,
          verification_succeeded,
          snapshot,
          error_code: verification_succeeded ? undefined : 'ADMIN_PARTIAL_FAILURE',
          error_message: verification_succeeded
            ? undefined
            : 'Verification failed after apply.'
        };
      }

      if (typeof target_node.mutation_executor !== 'function') {
        return {
          ...base_state,
          error_code: 'ADMIN_DISPATCH_RETRYABLE',
          error_message: `No mutation executor registered for node "${target_node.node_id}".`
        };
      }

      const executor_result = await this.runWithTimeout({
        timeout_ms: apply_timeout_ms,
        timeout_message: `Mutation apply timed out on node "${target_node.node_id}".`,
        promise_factory: async (): Promise<cluster_mutation_node_executor_result_t> => {
          return await target_node.mutation_executor!({
            mode: 'apply',
            request,
            node: target_node,
            apply_timeout_ms,
            verify_timeout_ms,
            in_flight_policy: request.in_flight_policy
          });
        }
      });

      const verification_succeeded = executor_result.verification_passed !== false;

      return {
        ...base_state,
        status: verification_succeeded ? 'applied' : 'failed',
        applied_version: executor_result.applied_version,
        apply_succeeded: true,
        verification_succeeded,
        snapshot: executor_result.snapshot,
        error_code: verification_succeeded ? undefined : 'ADMIN_PARTIAL_FAILURE',
        error_message: verification_succeeded
          ? undefined
          : `Remote executor verification failed on node "${target_node.node_id}".`
      };
    } catch (error) {
      if (
        error instanceof Error &&
        'code' in error &&
        typeof (error as { code?: unknown }).code === 'string'
      ) {
        const error_code = (error as { code: string }).code;
        if (
          error_code === 'ADMIN_VALIDATION_FAILED' ||
          error_code === 'ADMIN_CONFLICT' ||
          error_code === 'ADMIN_FORBIDDEN' ||
          error_code === 'ADMIN_AUTH_FAILED'
        ) {
          throw error;
        }
      }

      return {
        ...base_state,
        error_code:
          GetErrorMessage({ error }).toLowerCase().includes('timed out')
            ? 'ADMIN_TIMEOUT'
            : 'ADMIN_DISPATCH_RETRYABLE',
        error_message: GetErrorMessage({ error })
      };
    }
  }

  private async rollbackMutationAcrossAppliedNodes(params: {
    request: cluster_admin_mutation_request_message_i;
    target_node_list: cluster_mutation_target_node_t[];
    per_node_state_list: cluster_mutation_node_state_t[];
  }): Promise<
    | {
        ok: true;
        per_node_state_list: cluster_mutation_node_state_t[];
        rolled_back_node_count: number;
      }
    | {
        ok: false;
        per_node_state_list: cluster_mutation_node_state_t[];
        rolled_back_node_count: number;
        error_message: string;
      }
  > {
    const { request, target_node_list, per_node_state_list } = params;
    const node_by_id = new Map<string, cluster_mutation_target_node_t>();
    for (const node of target_node_list) {
      node_by_id.set(node.node_id, node);
    }

    const rolled_back_state_list = per_node_state_list.map((node_state): cluster_mutation_node_state_t => {
      return {
        ...node_state
      };
    });

    let rolled_back_node_count = 0;
    for (const node_state of rolled_back_state_list) {
      if (!node_state.apply_succeeded) {
        continue;
      }

      const target_node = node_by_id.get(node_state.node_id);
      if (!target_node) {
        node_state.status = 'failed';
        node_state.error_code = 'ADMIN_ROLLBACK_FAILED';
        node_state.error_message = `Rollback target node "${node_state.node_id}" no longer exists.`;
        return {
          ok: false,
          per_node_state_list: rolled_back_state_list,
          rolled_back_node_count,
          error_message: node_state.error_message
        };
      }

      try {
        if (target_node.is_local_node) {
          await this.rollbackLocalMutationSnapshot({
            request,
            snapshot: node_state.snapshot
          });
        } else {
          if (typeof target_node.mutation_executor !== 'function') {
            throw new Error(
              `No mutation executor registered for rollback on node "${target_node.node_id}".`
            );
          }

          await target_node.mutation_executor({
            mode: 'rollback',
            request,
            node: target_node,
            apply_timeout_ms: request.rollout_strategy.apply_timeout_ms ?? 30_000,
            verify_timeout_ms: request.rollout_strategy.verify_timeout_ms ?? 30_000,
            in_flight_policy: request.in_flight_policy,
            snapshot: node_state.snapshot
          });
        }

        node_state.status = 'rolled_back';
        node_state.rollback_succeeded = true;
        rolled_back_node_count += 1;
      } catch (error) {
        node_state.status = 'failed';
        node_state.error_code = 'ADMIN_ROLLBACK_FAILED';
        node_state.error_message = GetErrorMessage({ error });
        return {
          ok: false,
          per_node_state_list: rolled_back_state_list,
          rolled_back_node_count,
          error_message: node_state.error_message
        };
      }
    }

    return {
      ok: true,
      per_node_state_list: rolled_back_state_list,
      rolled_back_node_count
    };
  }

  private calculateMutationSuccessPercent(params: {
    per_node_state_list: cluster_mutation_node_state_t[];
  }): number {
    const { per_node_state_list } = params;
    if (per_node_state_list.length === 0) {
      return 0;
    }
    const successful_node_count = per_node_state_list.filter((node_state): boolean => {
      return node_state.status === 'applied';
    }).length;
    return (successful_node_count / per_node_state_list.length) * 100;
  }

  private async runWithTimeout<return_t>(params: {
    timeout_ms: number;
    timeout_message: string;
    promise_factory: () => Promise<return_t>;
  }): Promise<return_t> {
    const { timeout_ms, timeout_message, promise_factory } = params;
    return await new Promise<return_t>((resolve, reject): void => {
      const timeout_handle = setTimeout((): void => {
        reject(new Error(timeout_message));
      }, timeout_ms);

      promise_factory()
        .then((result): void => {
          clearTimeout(timeout_handle);
          resolve(result);
        })
        .catch((error): void => {
          clearTimeout(timeout_handle);
          reject(error);
        });
    });
  }

  private async waitForLocalWorkersToDrain(params: {
    timeout_ms: number;
  }): Promise<boolean> {
    const { timeout_ms } = params;
    const timeout_at = Date.now() + timeout_ms;
    while (Date.now() <= timeout_at) {
      const total_pending_call_count = Array.from(this.worker_state_by_id.values()).reduce(
        (pending_count, worker_state): number => {
          return pending_count + worker_state.pending_call_request_ids.size;
        },
        0
      );

      if (total_pending_call_count === 0) {
        return true;
      }

      await this.waitForMs({
        delay_ms: 5
      });
    }

    return false;
  }

  private captureLocalMutationRollbackSnapshot(params: {
    request: cluster_admin_mutation_request_message_i;
  }): cluster_local_mutation_snapshot_t {
    const { request } = params;
    const payload = IsRecordObject(request.payload) ? request.payload : {};

    if (
      request.mutation_type === 'define_function' ||
      request.mutation_type === 'redefine_function' ||
      request.mutation_type === 'undefine_function'
    ) {
      const name = typeof payload.name === 'string' ? payload.name : '';
      const existing_definition = this.function_definition_by_name.get(name);
      return {
        kind: 'function',
        name,
        existed: Boolean(existing_definition),
        worker_func: existing_definition?.worker_func
      };
    }

    if (
      request.mutation_type === 'define_dependency' ||
      request.mutation_type === 'undefine_dependency'
    ) {
      const alias = typeof payload.alias === 'string' ? payload.alias : '';
      const existing_definition = this.dependency_definition_by_alias.get(alias);
      return {
        kind: 'dependency',
        alias,
        existed: Boolean(existing_definition),
        definition: existing_definition
          ? {
              module_specifier: existing_definition.module_specifier,
              export_name: existing_definition.export_name ?? undefined,
              is_default_export: existing_definition.is_default_export
            }
          : undefined
      };
    }

    if (
      request.mutation_type === 'define_constant' ||
      request.mutation_type === 'undefine_constant'
    ) {
      const name = typeof payload.name === 'string' ? payload.name : '';
      const existing_definition = this.constant_definition_by_name.get(name);
      return {
        kind: 'constant',
        name,
        existed: Boolean(existing_definition),
        value: existing_definition?.value
      };
    }

    if (
      request.mutation_type === 'define_database_connection' ||
      request.mutation_type === 'undefine_database_connection'
    ) {
      const name = typeof payload.name === 'string' ? payload.name : '';
      const existing_definition = this.database_connection_definition_by_name.get(name);
      return {
        kind: 'database_connection',
        name,
        existed: Boolean(existing_definition),
        connector: existing_definition
          ? {
              type: existing_definition.connector_type,
              semantics: structuredClone(existing_definition.semantics)
            } as database_connector_definition_t
          : undefined
      };
    }

    return {
      kind: 'unsupported',
      mutation_type: request.mutation_type
    };
  }

  private async rollbackLocalMutationSnapshot(params: {
    request: cluster_admin_mutation_request_message_i;
    snapshot: unknown;
  }): Promise<void> {
    const { snapshot } = params;
    if (!snapshot || typeof snapshot !== 'object') {
      return;
    }

    const typed_snapshot = snapshot as cluster_local_mutation_snapshot_t;

    if (typed_snapshot.kind === 'function') {
      if (typed_snapshot.existed && typed_snapshot.worker_func) {
        await this.defineWorkerFunction({
          name: typed_snapshot.name,
          worker_func: typed_snapshot.worker_func
        });
      } else if (!typed_snapshot.existed && typed_snapshot.name.length > 0) {
        await this.undefineWokerFunction({
          name: typed_snapshot.name
        });
      }
      return;
    }

    if (typed_snapshot.kind === 'dependency') {
      if (typed_snapshot.existed && typed_snapshot.definition) {
        await this.defineWorkerDependency({
          alias: typed_snapshot.alias,
          module_specifier: typed_snapshot.definition.module_specifier,
          export_name: typed_snapshot.definition.export_name,
          is_default_export: typed_snapshot.definition.is_default_export
        });
      } else if (!typed_snapshot.existed && typed_snapshot.alias.length > 0) {
        await this.undefineWorkerDependency({
          alias: typed_snapshot.alias
        });
      }
      return;
    }

    if (typed_snapshot.kind === 'constant') {
      if (typed_snapshot.existed) {
        await this.defineWorkerConstant({
          name: typed_snapshot.name,
          value: typed_snapshot.value
        });
      } else if (typed_snapshot.name.length > 0) {
        await this.undefineWorkerConstant({
          name: typed_snapshot.name
        });
      }
      return;
    }

    if (typed_snapshot.kind === 'database_connection') {
      if (typed_snapshot.existed && typed_snapshot.connector) {
        await this.defineDatabaseConnection({
          name: typed_snapshot.name,
          connector: typed_snapshot.connector
        });
      } else if (typed_snapshot.name.length > 0) {
        await this.undefineDatabaseConnection({
          name: typed_snapshot.name
        });
      }
    }
  }

  private verifyLocalMutationRequestApplied(params: {
    request: cluster_admin_mutation_request_message_i;
  }): boolean {
    const { request } = params;
    const payload = IsRecordObject(request.payload) ? request.payload : {};

    if (
      request.mutation_type === 'define_function' ||
      request.mutation_type === 'redefine_function'
    ) {
      const name = typeof payload.name === 'string' ? payload.name : '';
      return this.function_definition_by_name.has(name);
    }

    if (request.mutation_type === 'undefine_function') {
      const name = typeof payload.name === 'string' ? payload.name : '';
      return !this.function_definition_by_name.has(name);
    }

    if (request.mutation_type === 'define_dependency') {
      const alias = typeof payload.alias === 'string' ? payload.alias : '';
      return this.dependency_definition_by_alias.has(alias);
    }

    if (request.mutation_type === 'undefine_dependency') {
      const alias = typeof payload.alias === 'string' ? payload.alias : '';
      return !this.dependency_definition_by_alias.has(alias);
    }

    if (request.mutation_type === 'define_constant') {
      const name = typeof payload.name === 'string' ? payload.name : '';
      return this.constant_definition_by_name.has(name);
    }

    if (request.mutation_type === 'undefine_constant') {
      const name = typeof payload.name === 'string' ? payload.name : '';
      return !this.constant_definition_by_name.has(name);
    }

    if (request.mutation_type === 'define_database_connection') {
      const name = typeof payload.name === 'string' ? payload.name : '';
      return this.database_connection_definition_by_name.has(name);
    }

    if (request.mutation_type === 'undefine_database_connection') {
      const name = typeof payload.name === 'string' ? payload.name : '';
      return !this.database_connection_definition_by_name.has(name);
    }

    return false;
  }

  private resolveLocalAppliedVersionForMutation(params: {
    request: cluster_admin_mutation_request_message_i;
  }): string {
    const { request } = params;
    const payload = IsRecordObject(request.payload) ? request.payload : {};

    if (
      request.mutation_type === 'define_function' ||
      request.mutation_type === 'redefine_function'
    ) {
      const name = typeof payload.name === 'string' ? payload.name : '';
      return (
        this.function_definition_by_name.get(name)?.function_hash_sha1 ??
        request.mutation_id
      );
    }

    const entity_name = this.getMutationEntityName({
      request
    });
    const serialized_payload = JSON.stringify({
      mutation_type: request.mutation_type,
      payload
    });

    return createHash('sha1')
      .update(entity_name + '|' + serialized_payload, 'utf8')
      .digest('hex');
  }

  private getRequiredStringFromMutationPayload(params: {
    payload: Record<string, unknown>;
    field_name: string;
  }): string {
    const { payload, field_name } = params;
    const value = payload[field_name];
    if (typeof value !== 'string' || value.length === 0) {
      throw this.buildAdminMutationErrorWithCode({
        code: 'ADMIN_VALIDATION_FAILED',
        message: `Mutation payload field "${field_name}" must be a non-empty string.`,
        details: {
          field_name
        }
      });
    }

    return value;
  }

  private getOptionalStringFromMutationPayload(params: {
    payload: Record<string, unknown>;
    field_name: string;
  }): string | undefined {
    const { payload, field_name } = params;
    const value = payload[field_name];
    if (typeof value === 'undefined') {
      return undefined;
    }

    if (typeof value !== 'string' || value.length === 0) {
      throw this.buildAdminMutationErrorWithCode({
        code: 'ADMIN_VALIDATION_FAILED',
        message: `Mutation payload field "${field_name}" must be a non-empty string when provided.`,
        details: {
          field_name
        }
      });
    }

    return value;
  }

  private getOptionalBooleanFromMutationPayload(params: {
    payload: Record<string, unknown>;
    field_name: string;
  }): boolean | undefined {
    const { payload, field_name } = params;
    const value = payload[field_name];
    if (typeof value === 'undefined') {
      return undefined;
    }

    if (typeof value !== 'boolean') {
      throw this.buildAdminMutationErrorWithCode({
        code: 'ADMIN_VALIDATION_FAILED',
        message: `Mutation payload field "${field_name}" must be a boolean when provided.`,
        details: {
          field_name
        }
      });
    }

    return value;
  }

  private buildWorkerFunctionFromMutationPayload(params: {
    payload: Record<string, unknown>;
  }): worker_function_handler_t<any, any> {
    const { payload } = params;

    if (typeof payload.worker_func === 'function') {
      return payload.worker_func as worker_function_handler_t<any, any>;
    }

    const worker_func_source = this.getOptionalStringFromMutationPayload({
      payload,
      field_name: 'worker_func_source'
    });

    if (!worker_func_source) {
      throw this.buildAdminMutationErrorWithCode({
        code: 'ADMIN_VALIDATION_FAILED',
        message:
          'Mutation payload must provide either worker_func (function) or worker_func_source (string).',
        details: {
          field_name: 'worker_func_source'
        }
      });
    }

    let evaluated_worker_function: unknown;
    try {
      evaluated_worker_function = new Function(
        `"use strict"; return (${worker_func_source});`
      )();
    } catch (error) {
      throw this.buildAdminMutationErrorWithCode({
        code: 'ADMIN_VALIDATION_FAILED',
        message: `worker_func_source could not be evaluated: ${GetErrorMessage({ error })}`,
        details: {
          field_name: 'worker_func_source'
        }
      });
    }

    if (typeof evaluated_worker_function !== 'function') {
      throw this.buildAdminMutationErrorWithCode({
        code: 'ADMIN_VALIDATION_FAILED',
        message: 'worker_func_source must evaluate to a function.',
        details: {
          field_name: 'worker_func_source'
        }
      });
    }

    return evaluated_worker_function as worker_function_handler_t<any, any>;
  }

  private async applySingleNodeMutationRequest(params: {
    request: cluster_admin_mutation_request_message_i;
  }): Promise<void> {
    const { request } = params;
    const payload = request.payload;

    if (!IsRecordObject(payload)) {
      throw this.buildAdminMutationErrorWithCode({
        code: 'ADMIN_VALIDATION_FAILED',
        message: 'Mutation payload must be an object.',
        details: {}
      });
    }

    if (
      request.mutation_type === 'define_function' ||
      request.mutation_type === 'redefine_function'
    ) {
      const name = this.getRequiredStringFromMutationPayload({
        payload,
        field_name: 'name'
      });
      const worker_func = this.buildWorkerFunctionFromMutationPayload({ payload });
      await this.defineWorkerFunction({
        name,
        worker_func
      });
      return;
    }

    if (request.mutation_type === 'undefine_function') {
      const name = this.getRequiredStringFromMutationPayload({
        payload,
        field_name: 'name'
      });
      await this.undefineWokerFunction({
        name
      });
      return;
    }

    if (request.mutation_type === 'define_dependency') {
      const alias = this.getRequiredStringFromMutationPayload({
        payload,
        field_name: 'alias'
      });
      const module_specifier = this.getRequiredStringFromMutationPayload({
        payload,
        field_name: 'module_specifier'
      });
      const export_name = this.getOptionalStringFromMutationPayload({
        payload,
        field_name: 'export_name'
      });
      const is_default_export = this.getOptionalBooleanFromMutationPayload({
        payload,
        field_name: 'is_default_export'
      });
      await this.defineWorkerDependency({
        alias,
        module_specifier,
        export_name,
        is_default_export
      });
      return;
    }

    if (request.mutation_type === 'undefine_dependency') {
      const alias = this.getRequiredStringFromMutationPayload({
        payload,
        field_name: 'alias'
      });
      await this.undefineWorkerDependency({
        alias
      });
      return;
    }

    if (request.mutation_type === 'define_constant') {
      const name = this.getRequiredStringFromMutationPayload({
        payload,
        field_name: 'name'
      });
      await this.defineWorkerConstant({
        name,
        value: payload.value
      });
      return;
    }

    if (request.mutation_type === 'undefine_constant') {
      const name = this.getRequiredStringFromMutationPayload({
        payload,
        field_name: 'name'
      });
      await this.undefineWorkerConstant({
        name
      });
      return;
    }

    if (request.mutation_type === 'define_database_connection') {
      const name = this.getRequiredStringFromMutationPayload({
        payload,
        field_name: 'name'
      });
      const connector_value = payload.connector;
      if (!IsRecordObject(connector_value)) {
        throw this.buildAdminMutationErrorWithCode({
          code: 'ADMIN_VALIDATION_FAILED',
          message: 'Mutation payload field "connector" must be an object.',
          details: {
            field_name: 'connector'
          }
        });
      }
      await this.defineDatabaseConnection({
        name,
        connector: connector_value as database_connector_definition_t
      });
      return;
    }

    if (request.mutation_type === 'undefine_database_connection') {
      const name = this.getRequiredStringFromMutationPayload({
        payload,
        field_name: 'name'
      });
      await this.undefineDatabaseConnection({
        name
      });
      return;
    }

    throw this.buildAdminMutationErrorWithCode({
      code: 'ADMIN_VALIDATION_FAILED',
      message: `Mutation type "${request.mutation_type}" is not supported by local node mutation apply handler.`,
      details: {
        mutation_type: request.mutation_type
      }
    });
  }

  private buildAdminMutationErrorWithCode(params: {
    code: cluster_protocol_error_code_t;
    message: string;
    details: Record<string, unknown>;
  }): admin_mutation_error_with_code_i {
    const { code, message, details } = params;
    const error = new Error(message) as admin_mutation_error_with_code_i;
    error.code = code;
    error.details = details;
    return error;
  }

  private toAdminMutationError(params: {
    error: unknown;
  }): {
    code: cluster_protocol_error_code_t;
    message: string;
    details?: Record<string, unknown>;
  } {
    const { error } = params;
    if (
      error instanceof Error &&
      'code' in error &&
      typeof (error as admin_mutation_error_with_code_i).code === 'string'
    ) {
      return {
        code: (error as admin_mutation_error_with_code_i).code,
        message: error.message,
        details: (error as admin_mutation_error_with_code_i).details
      };
    }

    return {
      code: 'ADMIN_INTERNAL',
      message: GetErrorMessage({ error })
    };
  }

  private getParentSharedOwnerContext(): shared_lock_owner_context_t {
    return {
      owner_kind: 'parent',
      owner_id: 'parent'
    };
  }

  private getAutoReleaseEventSeverity(params: {
    release_reason: shared_lock_auto_release_reason_t;
  }): worker_event_severity_t {
    const { release_reason } = params;
    if (
      release_reason === 'call_timeout_auto_release' ||
      release_reason === 'worker_exit_auto_release'
    ) {
      return 'warn';
    }

    return 'info';
  }

  private autoReleaseSharedLocksByOwnerId(params: {
    worker_id: number;
    call_request_id: string;
    release_reason: shared_lock_auto_release_reason_t;
  }): void {
    const { worker_id, call_request_id, release_reason } = params;
    const owner_id = BuildWorkerSharedOwnerId({
      worker_id,
      call_request_id
    });

    try {
      const release_result = this.shared_memory_store.releaseLocksByOwnerId({
        owner_id,
        release_reason,
        worker_id,
        call_request_id
      });

      if (release_result.released_lock_count > 0) {
        this.emitParentWorkerEvent({
          worker_id,
          event_name: release_reason,
          severity: this.getAutoReleaseEventSeverity({ release_reason }),
          correlation_id: call_request_id,
          details: {
            owner_id,
            call_request_id,
            lock_count_released: release_result.released_lock_count,
            released_chunk_ids: release_result.released_chunk_ids
          }
        });
      }
    } catch (error) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'shared_lock_auto_release_failed',
        severity: 'warn',
        correlation_id: call_request_id,
        error: ToRemoteError({ error }),
        details: {
          owner_id,
          call_request_id,
          release_reason
        }
      });
    }
  }

  private autoReleaseSharedLocksByWorkerId(params: {
    worker_id: number;
    release_reason: shared_lock_auto_release_reason_t;
  }): void {
    const { worker_id, release_reason } = params;
    try {
      const release_result = this.shared_memory_store.releaseLocksByWorkerId({
        worker_id,
        release_reason
      });

      if (release_result.released_lock_count > 0) {
        this.emitParentWorkerEvent({
          worker_id,
          event_name: release_reason,
          severity: this.getAutoReleaseEventSeverity({ release_reason }),
          details: {
            lock_count_released: release_result.released_lock_count,
            released_chunk_ids: release_result.released_chunk_ids
          }
        });
      }
    } catch (error) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'shared_lock_auto_release_failed',
        severity: 'warn',
        error: ToRemoteError({ error }),
        details: {
          worker_id,
          release_reason
        }
      });
    }
  }

  private applyRuntimeOptions(params: { options: runtime_options_t }): void {
    const { options } = params;

    if (typeof options.call_timeout_ms === 'number') {
      ValidatePositiveInteger({
        value: options.call_timeout_ms,
        label: 'call_timeout_ms'
      });
      this.default_call_timeout_ms = options.call_timeout_ms;
    }

    if (typeof options.control_timeout_ms === 'number') {
      ValidatePositiveInteger({
        value: options.control_timeout_ms,
        label: 'control_timeout_ms'
      });
      this.default_control_timeout_ms = options.control_timeout_ms;
    }

    if (typeof options.start_timeout_ms === 'number') {
      ValidatePositiveInteger({
        value: options.start_timeout_ms,
        label: 'start_timeout_ms'
      });
      this.default_start_timeout_ms = options.start_timeout_ms;
    }

    if (typeof options.stop_timeout_ms === 'number') {
      ValidatePositiveInteger({
        value: options.stop_timeout_ms,
        label: 'stop_timeout_ms'
      });
      this.default_stop_timeout_ms = options.stop_timeout_ms;
    }

    if (typeof options.restart_on_failure === 'boolean') {
      this.restart_on_failure = options.restart_on_failure;
    }

    if (typeof options.max_restarts_per_worker === 'number') {
      ValidateNonNegativeInteger({
        value: options.max_restarts_per_worker,
        label: 'max_restarts_per_worker'
      });
      this.max_restarts_per_worker = options.max_restarts_per_worker;
    }

    if (typeof options.max_pending_calls_per_worker === 'number') {
      ValidatePositiveInteger({
        value: options.max_pending_calls_per_worker,
        label: 'max_pending_calls_per_worker'
      });
      this.max_pending_calls_per_worker = options.max_pending_calls_per_worker;
    }

    if (typeof options.restart_base_delay_ms === 'number') {
      ValidatePositiveInteger({
        value: options.restart_base_delay_ms,
        label: 'restart_base_delay_ms'
      });
      this.restart_base_delay_ms = options.restart_base_delay_ms;
    }

    if (typeof options.restart_max_delay_ms === 'number') {
      ValidatePositiveInteger({
        value: options.restart_max_delay_ms,
        label: 'restart_max_delay_ms'
      });
      this.restart_max_delay_ms = options.restart_max_delay_ms;
    }

    if (typeof options.restart_jitter_ms === 'number') {
      ValidateNonNegativeInteger({
        value: options.restart_jitter_ms,
        label: 'restart_jitter_ms'
      });
      this.restart_jitter_ms = options.restart_jitter_ms;
    }

    if (typeof options.mutation_dedupe_retention_ms === 'number') {
      ValidatePositiveInteger({
        value: options.mutation_dedupe_retention_ms,
        label: 'mutation_dedupe_retention_ms'
      });
      this.mutation_dedupe_retention_ms = options.mutation_dedupe_retention_ms;
    }

    if (typeof options.mutation_retry_max_attempts === 'number') {
      ValidatePositiveInteger({
        value: options.mutation_retry_max_attempts,
        label: 'mutation_retry_max_attempts'
      });
      this.mutation_retry_max_attempts = options.mutation_retry_max_attempts;
    }

    if (typeof options.mutation_retry_base_delay_ms === 'number') {
      ValidatePositiveInteger({
        value: options.mutation_retry_base_delay_ms,
        label: 'mutation_retry_base_delay_ms'
      });
      this.mutation_retry_base_delay_ms = options.mutation_retry_base_delay_ms;
    }

    if (typeof options.mutation_retry_max_delay_ms === 'number') {
      ValidatePositiveInteger({
        value: options.mutation_retry_max_delay_ms,
        label: 'mutation_retry_max_delay_ms'
      });
      this.mutation_retry_max_delay_ms = options.mutation_retry_max_delay_ms;
    }

    if (typeof options.operations_event_history_max_count === 'number') {
      ValidatePositiveInteger({
        value: options.operations_event_history_max_count,
        label: 'operations_event_history_max_count'
      });
      this.operations_event_history_max_count = options.operations_event_history_max_count;
    }

    const routing_policy_update: Partial<cluster_call_routing_policy_t> = {};

    if (typeof options.call_routing_heartbeat_ttl_ms === 'number') {
      ValidatePositiveInteger({
        value: options.call_routing_heartbeat_ttl_ms,
        label: 'call_routing_heartbeat_ttl_ms'
      });
      routing_policy_update.heartbeat_ttl_ms = options.call_routing_heartbeat_ttl_ms;
    }

    if (typeof options.call_routing_sticky_ttl_ms === 'number') {
      ValidatePositiveInteger({
        value: options.call_routing_sticky_ttl_ms,
        label: 'call_routing_sticky_ttl_ms'
      });
      routing_policy_update.sticky_ttl_ms = options.call_routing_sticky_ttl_ms;
    }

    if (typeof options.call_routing_allow_degraded_node_routing === 'boolean') {
      routing_policy_update.allow_degraded_node_routing =
        options.call_routing_allow_degraded_node_routing;
    }

    if (typeof options.call_routing_inflight_weight === 'number') {
      if (
        !Number.isFinite(options.call_routing_inflight_weight) ||
        options.call_routing_inflight_weight < 0
      ) {
        throw new Error('call_routing_inflight_weight must be a finite number >= 0.');
      }
      routing_policy_update.inflight_weight = options.call_routing_inflight_weight;
    }

    if (typeof options.call_routing_latency_weight === 'number') {
      if (
        !Number.isFinite(options.call_routing_latency_weight) ||
        options.call_routing_latency_weight < 0
      ) {
        throw new Error('call_routing_latency_weight must be a finite number >= 0.');
      }
      routing_policy_update.latency_weight = options.call_routing_latency_weight;
    }

    if (typeof options.call_routing_error_weight === 'number') {
      if (
        !Number.isFinite(options.call_routing_error_weight) ||
        options.call_routing_error_weight < 0
      ) {
        throw new Error('call_routing_error_weight must be a finite number >= 0.');
      }
      routing_policy_update.error_weight = options.call_routing_error_weight;
    }

    if (typeof options.call_routing_degraded_penalty === 'number') {
      if (
        !Number.isFinite(options.call_routing_degraded_penalty) ||
        options.call_routing_degraded_penalty < 0
      ) {
        throw new Error('call_routing_degraded_penalty must be a finite number >= 0.');
      }
      routing_policy_update.degraded_penalty = options.call_routing_degraded_penalty;
    }

    if (Object.keys(routing_policy_update).length > 0) {
      this.cluster_call_routing_strategy.setRoutingConfig({
        routing_config: routing_policy_update
      });
    }
  }

  private generateCallRequestId(): string {
    const request_id = `call_${this.next_call_request_id}`;
    this.next_call_request_id += 1;
    return request_id;
  }

  private generateControlRequestId(): string {
    const request_id = `control_${this.next_control_request_id}`;
    this.next_control_request_id += 1;
    return request_id;
  }

  private generateWorkerEventId(): string {
    const event_id = `event_${this.next_worker_event_id}`;
    this.next_worker_event_id += 1;
    return event_id;
  }

  private emitWorkerEvent(params: { worker_event: worker_event_t }): void {
    const { worker_event } = params;

    for (const listener of this.worker_event_listener_by_id.values()) {
      try {
        listener(worker_event);
      } catch {
        // Listener exceptions must never impact worker supervision.
      }
    }
  }

  private appendClusterOperationLifecycleEvent(params: {
    event: cluster_operation_lifecycle_event_t;
  }): void {
    const { event } = params;
    this.cluster_operation_lifecycle_event_history.push({
      ...event,
      details: event.details ? { ...event.details } : undefined
    });

    if (
      this.cluster_operation_lifecycle_event_history.length >
      this.operations_event_history_max_count
    ) {
      this.cluster_operation_lifecycle_event_history.splice(
        0,
        this.cluster_operation_lifecycle_event_history.length -
          this.operations_event_history_max_count
      );
    }
  }

  private emitParentWorkerEvent(params: {
    worker_id: number;
    event_name: string;
    severity: worker_event_severity_t;
    correlation_id?: string;
    error?: remote_error_t;
    details?: Record<string, unknown>;
  }): void {
    const { worker_id, event_name, severity, correlation_id, error, details } =
      params;

    this.emitWorkerEvent({
      worker_event: {
        event_id: this.generateWorkerEventId(),
        worker_id,
        source: 'parent',
        event_name,
        severity,
        timestamp: new Date().toISOString(),
        correlation_id,
        error,
        details
      }
    });
  }

  private setWorkerHealthState(params: {
    worker_id: number;
    health_state: worker_health_state_t;
    reason?: string;
  }): void {
    const { worker_id, health_state, reason } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      return;
    }

    if (worker_state.health_state === health_state) {
      return;
    }

    worker_state.health_state = health_state;

    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'worker_health_state_changed',
      severity: health_state === 'degraded' ? 'warn' : 'info',
      details: {
        health_state,
        reason: reason ?? null
      }
    });
  }

  private async waitForMs(params: { delay_ms: number }): Promise<void> {
    const { delay_ms } = params;

    if (delay_ms <= 0) {
      return;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, delay_ms);
    });
  }

  private getRestartDelayMs(params: { restart_attempt: number }): number {
    const { restart_attempt } = params;

    const exponential_multiplier = Math.max(1, 2 ** Math.max(0, restart_attempt - 1));
    const base_delay_ms = this.restart_base_delay_ms * exponential_multiplier;
    const capped_delay_ms = Math.min(base_delay_ms, this.restart_max_delay_ms);

    if (this.restart_jitter_ms <= 0) {
      return capped_delay_ms;
    }

    const jitter_delta = Math.floor(Math.random() * (this.restart_jitter_ms + 1));
    return Math.min(capped_delay_ms + jitter_delta, this.restart_max_delay_ms);
  }

  private async createWorker(params: { restart_attempt: number }): Promise<number> {
    const worker_id = this.next_worker_id;
    this.next_worker_id += 1;

    const worker_instance = new Worker(BuildWorkerRuntimeScript(), { eval: true });

    const worker_state: worker_state_t = {
      worker_id,
      worker_instance,
      health_state: 'starting',
      ready: false,
      shutting_down: false,
      restart_attempt: params.restart_attempt,
      pending_call_request_ids: new Set<string>(),
      pending_control_request_ids: new Set<string>()
    };

    this.worker_state_by_id.set(worker_id, worker_state);

    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'worker_spawned',
      severity: 'info',
      details: {
        restart_attempt: params.restart_attempt
      }
    });

    worker_instance.on('message', (message: unknown): void => {
      this.handleWorkerMessage({ worker_id, message });
    });

    worker_instance.on('error', (error: Error): void => {
      this.handleWorkerError({ worker_id, error });
    });

    worker_instance.on('exit', (exit_code: number): void => {
      this.handleWorkerExit({ worker_id, exit_code });
    });

    await this.waitForWorkerReady({
      worker_id,
      timeout_ms: this.default_start_timeout_ms
    });

    await this.installAllDependenciesOnWorker({ worker_id });
    await this.installAllConstantsOnWorker({ worker_id });
    await this.installAllDatabaseConnectionsOnWorker({ worker_id });
    await this.installAllFunctionsOnWorker({ worker_id });

    return worker_id;
  }

  private async installAllFunctionsOnWorker(params: {
    worker_id: number;
  }): Promise<void> {
    const { worker_id } = params;

    const function_definitions = Array.from(
      this.function_definition_by_name.values()
    ).sort((left_definition, right_definition): number => {
      return left_definition.name.localeCompare(right_definition.name);
    });

    for (const function_definition of function_definitions) {
      await this.installFunctionOnWorker({ worker_id, function_definition });
    }
  }

  private async installAllDependenciesOnWorker(params: {
    worker_id: number;
  }): Promise<void> {
    const { worker_id } = params;

    const dependency_definitions = Array.from(
      this.dependency_definition_by_alias.values()
    ).sort((left_definition, right_definition): number => {
      return left_definition.alias.localeCompare(right_definition.alias);
    });

    for (const dependency_definition of dependency_definitions) {
      await this.installDependencyOnWorker({ worker_id, dependency_definition });
    }
  }

  private async installAllConstantsOnWorker(params: {
    worker_id: number;
  }): Promise<void> {
    const { worker_id } = params;

    const constant_definitions = Array.from(
      this.constant_definition_by_name.values()
    ).sort((left_definition, right_definition): number => {
      return left_definition.name.localeCompare(right_definition.name);
    });

    for (const constant_definition of constant_definitions) {
      await this.installConstantOnWorker({ worker_id, constant_definition });
    }
  }

  private async installAllDatabaseConnectionsOnWorker(params: {
    worker_id: number;
  }): Promise<void> {
    const { worker_id } = params;

    const database_connection_definitions = Array.from(
      this.database_connection_definition_by_name.values()
    ).sort((left_definition, right_definition): number => {
      return left_definition.name.localeCompare(right_definition.name);
    });

    for (const database_connection_definition of database_connection_definitions) {
      await this.installDatabaseConnectionOnWorker({
        worker_id,
        database_connection_definition
      });
    }
  }

  private async installDependencyAcrossRunningWorkers(params: {
    alias: string;
    dependency_definition: worker_dependency_definition_t;
  }): Promise<void> {
    const { alias, dependency_definition } = params;

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const install_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.installDependencyOnWorker({
            worker_id: worker_state.worker_id,
            dependency_definition
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = install_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to install dependency "${alias}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  private async installConstantAcrossRunningWorkers(params: {
    name: string;
    constant_definition: worker_constant_definition_t;
  }): Promise<void> {
    const { name, constant_definition } = params;

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const install_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.installConstantOnWorker({
            worker_id: worker_state.worker_id,
            constant_definition
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = install_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to install constant "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  private async installDatabaseConnectionAcrossRunningWorkers(params: {
    name: string;
    database_connection_definition: worker_database_connection_definition_t;
  }): Promise<void> {
    const { name, database_connection_definition } = params;

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const install_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.installDatabaseConnectionOnWorker({
            worker_id: worker_state.worker_id,
            database_connection_definition
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = install_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to install database connection "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  private async installFunctionOnWorker(params: {
    worker_id: number;
    function_definition: worker_function_definition_t;
  }): Promise<void> {
    const { worker_id, function_definition } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'define_function',
      payload: {
        name: function_definition.name,
        function_source: function_definition.function_source
      }
    });

    function_definition.installed_worker_ids.add(worker_id);
  }

  private async removeFunctionOnWorker(params: {
    worker_id: number;
    function_name: string;
  }): Promise<void> {
    const { worker_id, function_name } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'undefine_function',
      payload: {
        name: function_name
      }
    });
  }

  private async installDependencyOnWorker(params: {
    worker_id: number;
    dependency_definition: worker_dependency_definition_t;
  }): Promise<void> {
    const { worker_id, dependency_definition } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'define_dependency',
      payload: {
        alias: dependency_definition.alias,
        module_specifier: dependency_definition.module_specifier,
        export_name: dependency_definition.export_name ?? undefined,
        is_default_export: dependency_definition.is_default_export
      }
    });

    dependency_definition.installed_worker_ids.add(worker_id);
  }

  private async removeDependencyOnWorker(params: {
    worker_id: number;
    alias: string;
  }): Promise<void> {
    const { worker_id, alias } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'undefine_dependency',
      payload: {
        alias
      }
    });
  }

  private async installConstantOnWorker(params: {
    worker_id: number;
    constant_definition: worker_constant_definition_t;
  }): Promise<void> {
    const { worker_id, constant_definition } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'define_constant',
      payload: {
        name: constant_definition.name,
        value: constant_definition.value
      }
    });

    constant_definition.installed_worker_ids.add(worker_id);
  }

  private async removeConstantOnWorker(params: {
    worker_id: number;
    name: string;
  }): Promise<void> {
    const { worker_id, name } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'undefine_constant',
      payload: {
        name
      }
    });
  }

  private async installDatabaseConnectionOnWorker(params: {
    worker_id: number;
    database_connection_definition: worker_database_connection_definition_t;
  }): Promise<void> {
    const { worker_id, database_connection_definition } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'define_database_connection',
      payload: {
        name: database_connection_definition.name,
        connector: {
          type: database_connection_definition.connector_type,
          semantics: database_connection_definition.semantics
        }
      }
    });

    database_connection_definition.installed_worker_ids.add(worker_id);
  }

  private async removeDatabaseConnectionOnWorker(params: {
    worker_id: number;
    name: string;
  }): Promise<void> {
    const { worker_id, name } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'undefine_database_connection',
      payload: {
        name
      }
    });
  }

  private async sendControlCommand(params: {
    worker_id: number;
    command: control_command_t;
    payload?: Record<string, unknown>;
    timeout_ms?: number;
  }): Promise<void> {
    const { worker_id, command, payload, timeout_ms } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      throw new Error(`Worker ${worker_id} is not available.`);
    }

    const control_request_id = this.generateControlRequestId();
    const control_timeout_ms = timeout_ms ?? this.default_control_timeout_ms;

    await new Promise<void>((resolve, reject): void => {
      const timeout_handle = setTimeout((): void => {
        const pending_control = this.pending_control_by_request_id.get(control_request_id);
        if (!pending_control) {
          return;
        }

        this.pending_control_by_request_id.delete(control_request_id);
        worker_state.pending_control_request_ids.delete(control_request_id);
        this.setWorkerHealthState({
          worker_id,
          health_state: 'degraded',
          reason: `control timeout for ${command}`
        });

        reject(
          new Error(
            `Control command "${command}" timed out after ${control_timeout_ms}ms on worker ${worker_id}.`
          )
        );
      }, control_timeout_ms);

      this.pending_control_by_request_id.set(control_request_id, {
        resolve,
        reject,
        timeout_handle,
        worker_id,
        command
      });

      worker_state.pending_control_request_ids.add(control_request_id);

      const message: control_request_message_t = {
        message_type: 'control_request',
        control_request_id,
        command,
        payload
      };

      try {
        worker_state.worker_instance.postMessage(message);
      } catch (error) {
        clearTimeout(timeout_handle);
        worker_state.pending_control_request_ids.delete(control_request_id);
        this.pending_control_by_request_id.delete(control_request_id);
        this.setWorkerHealthState({
          worker_id,
          health_state: 'degraded',
          reason: `control dispatch failed for ${command}`
        });

        reject(
          new Error(
            `Failed to send control command "${command}" to worker ${worker_id}: ${GetErrorMessage({ error })}`
          )
        );
      }
    });
  }

  private async waitForWorkerReady(params: {
    worker_id: number;
    timeout_ms: number;
  }): Promise<void> {
    const { worker_id, timeout_ms } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      throw new Error(`Worker ${worker_id} is not available.`);
    }

    if (worker_state.ready) {
      return;
    }

    await new Promise<void>((resolve, reject): void => {
      const timeout_handle = setTimeout((): void => {
        this.ready_waiter_by_worker_id.delete(worker_id);
        this.setWorkerHealthState({
          worker_id,
          health_state: 'degraded',
          reason: 'ready timeout'
        });
        reject(
          new Error(`Worker ${worker_id} did not report ready within ${timeout_ms}ms.`)
        );
      }, timeout_ms);

      const waiter: worker_ready_waiter_t = {
        resolve: (): void => {
          clearTimeout(timeout_handle);
          resolve();
        },
        reject: (error: Error): void => {
          clearTimeout(timeout_handle);
          reject(error);
        },
        timeout_handle
      };

      this.ready_waiter_by_worker_id.set(worker_id, waiter);

      if (!this.worker_state_by_id.has(worker_id)) {
        this.ready_waiter_by_worker_id.delete(worker_id);
        clearTimeout(timeout_handle);
        reject(new Error(`Worker ${worker_id} exited before becoming ready.`));
      }
    });
  }

  private async waitForWorkerExitOrTerminate(params: {
    worker_id: number;
    timeout_ms: number;
  }): Promise<void> {
    const { worker_id, timeout_ms } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      return;
    }

    await new Promise<void>((resolve): void => {
      const timeout_handle = setTimeout((): void => {
        void worker_state.worker_instance.terminate().finally((): void => {
          this.exit_waiter_by_worker_id.delete(worker_id);
          resolve();
        });
      }, timeout_ms);

      const waiter: worker_exit_waiter_t = {
        resolve: (): void => {
          clearTimeout(timeout_handle);
          resolve();
        },
        timeout_handle
      };

      this.exit_waiter_by_worker_id.set(worker_id, waiter);

      if (!this.worker_state_by_id.has(worker_id)) {
        this.exit_waiter_by_worker_id.delete(worker_id);
        clearTimeout(timeout_handle);
        resolve();
      }
    });
  }

  private selectWorkerForCall(params: {
    function_definition: worker_function_definition_t;
    function_name: string;
  }): worker_state_t {
    const { function_definition, function_name } = params;

    for (const required_alias of function_definition.required_dependency_aliases) {
      if (!this.dependency_definition_by_alias.has(required_alias)) {
        throw new Error(
          `Function "${function_name}" requires dependency alias "${required_alias}" which is not defined.`
        );
      }
    }

    for (const required_name of function_definition.required_constant_names) {
      if (!this.constant_definition_by_name.has(required_name)) {
        throw new Error(
          `Function "${function_name}" requires constant "${required_name}" which is not defined.`
        );
      }
    }

    for (const required_name of function_definition.required_database_connection_names) {
      if (!this.database_connection_definition_by_name.has(required_name)) {
        throw new Error(
          `Function "${function_name}" requires database connection "${required_name}" which is not defined.`
        );
      }
    }

    const eligible_worker_states = this.getReadyWorkerStates().filter(
      (worker_state): boolean => {
        if (!function_definition.installed_worker_ids.has(worker_state.worker_id)) {
          return false;
        }

        for (const required_alias of function_definition.required_dependency_aliases) {
          const dependency_definition =
            this.dependency_definition_by_alias.get(required_alias);
          if (
            !dependency_definition ||
            !dependency_definition.installed_worker_ids.has(worker_state.worker_id)
          ) {
            return false;
          }
        }

        for (const required_name of function_definition.required_constant_names) {
          const constant_definition =
            this.constant_definition_by_name.get(required_name);
          if (
            !constant_definition ||
            !constant_definition.installed_worker_ids.has(worker_state.worker_id)
          ) {
            return false;
          }
        }

        for (const required_name of function_definition.required_database_connection_names) {
          const database_connection_definition =
            this.database_connection_definition_by_name.get(required_name);
          if (
            !database_connection_definition ||
            !database_connection_definition.installed_worker_ids.has(
              worker_state.worker_id
            )
          ) {
            return false;
          }
        }

        return true;
      }
    );

    if (eligible_worker_states.length === 0) {
      throw new Error(
        `No ready workers with function "${function_name}" installed are available.`
      );
    }

    const search_start_index = this.round_robin_index % eligible_worker_states.length;

    let minimum_pending_call_count = Number.POSITIVE_INFINITY;
    for (const worker_state of eligible_worker_states) {
      const pending_call_count = worker_state.pending_call_request_ids.size;
      if (pending_call_count < minimum_pending_call_count) {
        minimum_pending_call_count = pending_call_count;
      }
    }

    if (minimum_pending_call_count >= this.max_pending_calls_per_worker) {
      throw new Error(
        `All workers are saturated for "${function_name}" (${this.max_pending_calls_per_worker} pending call limit per worker reached).`
      );
    }

    let selected_worker_index = -1;
    for (
      let offset_index = 0;
      offset_index < eligible_worker_states.length;
      offset_index += 1
    ) {
      const candidate_index =
        (search_start_index + offset_index) % eligible_worker_states.length;
      const candidate_worker_state = eligible_worker_states[candidate_index];

      if (
        candidate_worker_state.pending_call_request_ids.size ===
        minimum_pending_call_count
      ) {
        selected_worker_index = candidate_index;
        break;
      }
    }

    if (selected_worker_index < 0) {
      throw new Error('Failed to select a worker for call dispatch.');
    }

    this.round_robin_index =
      (selected_worker_index + 1) % eligible_worker_states.length;

    return eligible_worker_states[selected_worker_index];
  }

  private getReadyWorkerStates(): worker_state_t[] {
    return Array.from(this.worker_state_by_id.values())
      .filter((worker_state): boolean => {
        return (
          worker_state.ready &&
          !worker_state.shutting_down &&
          worker_state.health_state === 'ready'
        );
      })
      .sort((left_worker_state, right_worker_state): number => {
        return left_worker_state.worker_id - right_worker_state.worker_id;
      });
  }

  private handleWorkerMessage(params: {
    worker_id: number;
    message: unknown;
  }): void {
    const { worker_id, message } = params;

    if (typeof message !== 'object' || message === null) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'malformed_worker_message',
        severity: 'warn',
        details: {
          reason: 'message is not an object'
        }
      });
      return;
    }

    const message_data = message as Record<string, unknown>;
    const message_type = message_data.message_type;

    if (message_type === 'ready') {
      this.handleReadyMessage({ worker_id });
      return;
    }

    if (message_type === 'call_response') {
      this.handleCallResponseMessage({
        worker_id,
        message: message as worker_to_parent_message_t
      });
      return;
    }

    if (message_type === 'control_response') {
      this.handleControlResponseMessage({
        worker_id,
        message: message as worker_to_parent_message_t
      });
      return;
    }

    if (message_type === 'worker_event') {
      this.handleWorkerEventMessage({
        worker_id,
        message: message as worker_to_parent_message_t
      });
      return;
    }

    if (message_type === 'shared_request') {
      void this.handleSharedRequestMessage({
        worker_id,
        message: message as worker_to_parent_message_t
      });
      return;
    }

    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'unknown_worker_message',
      severity: 'warn',
      details: {
        message_type: typeof message_type === 'string' ? message_type : null
      }
    });
  }

  private handleReadyMessage(params: { worker_id: number }): void {
    const { worker_id } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      return;
    }

    worker_state.ready = true;
    this.setWorkerHealthState({
      worker_id,
      health_state: 'ready',
      reason: 'worker sent ready message'
    });

    const ready_waiter = this.ready_waiter_by_worker_id.get(worker_id);
    if (!ready_waiter) {
      return;
    }

    this.ready_waiter_by_worker_id.delete(worker_id);
    clearTimeout(ready_waiter.timeout_handle);
    ready_waiter.resolve();
  }

  private handleWorkerEventMessage(params: {
    worker_id: number;
    message: worker_to_parent_message_t;
  }): void {
    const { worker_id, message } = params;

    if (message.message_type !== 'worker_event') {
      return;
    }

    const normalized_worker_event: worker_event_t = {
      event_id: message.event_id,
      worker_id,
      source: 'worker',
      event_name: message.event_name,
      severity: message.severity,
      timestamp: message.timestamp,
      correlation_id: message.correlation_id,
      error: message.error,
      details: message.details
    };

    const worker_state = this.worker_state_by_id.get(worker_id);
    const degraded_event_names = new Set<string>([
      'uncaught_exception',
      'message_handler_failure',
      'post_message_failed'
    ]);

    if (
      worker_state &&
      message.severity === 'error' &&
      degraded_event_names.has(message.event_name)
    ) {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'degraded',
        reason: message.event_name
      });
    }

    this.emitWorkerEvent({
      worker_event: normalized_worker_event
    });
  }

  private async handleSharedRequestMessage(params: {
    worker_id: number;
    message: worker_to_parent_message_t;
  }): Promise<void> {
    const { worker_id, message } = params;

    if (message.message_type !== 'shared_request') {
      return;
    }

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      return;
    }

    const shared_request_id = message.shared_request_id;
    const call_request_id = message.call_request_id;

    if (typeof shared_request_id !== 'string' || shared_request_id.length === 0) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'malformed_shared_request',
        severity: 'warn',
        details: {
          reason: 'shared_request_id missing or invalid'
        }
      });
      return;
    }

    if (typeof call_request_id !== 'string' || call_request_id.length === 0) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'malformed_shared_request',
        severity: 'warn',
        details: {
          reason: 'call_request_id missing or invalid',
          shared_request_id
        }
      });
      return;
    }

    if (!worker_state.pending_call_request_ids.has(call_request_id)) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'stale_shared_request',
        severity: 'warn',
        correlation_id: call_request_id,
        details: {
          shared_request_id,
          command: message.command,
          reason: 'call_request_id is no longer active'
        }
      });

      try {
        worker_state.worker_instance.postMessage({
          message_type: 'shared_response',
          shared_request_id,
          ok: false,
          error: {
            name: 'Error',
            message: `Shared request "${message.command}" rejected because call "${call_request_id}" is no longer active.`
          }
        } satisfies parent_to_worker_message_t);
      } catch (post_error) {
        this.emitParentWorkerEvent({
          worker_id,
          event_name: 'shared_response_send_failed',
          severity: 'warn',
          correlation_id: shared_request_id,
          error: ToRemoteError({ error: post_error })
        });
      }
      return;
    }

    const owner_context: shared_lock_owner_context_t = {
      owner_kind: 'worker',
      owner_id: BuildWorkerSharedOwnerId({
        worker_id,
        call_request_id
      }),
      worker_id,
      call_request_id
    };

    let return_value: unknown = undefined;

    try {
      if (message.command === 'shared_create') {
        const payload =
          typeof message.payload === 'object' && message.payload !== null
            ? message.payload
            : {};
        await this.shared_memory_store.createChunk({
          chunk: payload as shared_create_params_t
        });
      } else if (message.command === 'shared_access') {
        const payload =
          typeof message.payload === 'object' && message.payload !== null
            ? message.payload
            : {};
        return_value = await this.shared_memory_store.accessChunk({
          access: payload as shared_access_params_t,
          owner_context
        });
      } else if (message.command === 'shared_write') {
        const payload =
          typeof message.payload === 'object' && message.payload !== null
            ? message.payload
            : {};
        await this.shared_memory_store.writeChunk({
          write: payload as shared_write_params_t,
          owner_context
        });
      } else if (message.command === 'shared_release') {
        const payload =
          typeof message.payload === 'object' && message.payload !== null
            ? message.payload
            : {};
        await this.shared_memory_store.releaseChunk({
          release: payload as shared_release_params_t,
          owner_context
        });
      } else if (message.command === 'shared_free') {
        const payload =
          typeof message.payload === 'object' && message.payload !== null
            ? message.payload
            : {};
        await this.shared_memory_store.freeChunk({
          free: payload as shared_free_params_t,
          owner_context
        });
      } else {
        throw new Error(`Unknown shared command "${String(message.command)}".`);
      }

      worker_state.worker_instance.postMessage({
        message_type: 'shared_response',
        shared_request_id,
        ok: true,
        return_value
      } satisfies parent_to_worker_message_t);
    } catch (error) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'shared_request_failed',
        severity: 'warn',
        correlation_id: shared_request_id,
        error: ToRemoteError({ error }),
        details: {
          command: message.command
        }
      });

      try {
        worker_state.worker_instance.postMessage({
          message_type: 'shared_response',
          shared_request_id,
          ok: false,
          error: ToRemoteError({ error })
        } satisfies parent_to_worker_message_t);
      } catch (post_error) {
        this.emitParentWorkerEvent({
          worker_id,
          event_name: 'shared_response_send_failed',
          severity: 'warn',
          correlation_id: shared_request_id,
          error: ToRemoteError({ error: post_error })
        });
      }
    }
  }

  private handleCallResponseMessage(params: {
    worker_id: number;
    message: worker_to_parent_message_t;
  }): void {
    const { worker_id, message } = params;

    if (message.message_type !== 'call_response') {
      return;
    }

    const pending_call = this.pending_call_by_request_id.get(message.request_id);
    if (!pending_call) {
      return;
    }

    clearTimeout(pending_call.timeout_handle);
    this.autoReleaseSharedLocksByOwnerId({
      worker_id,
      call_request_id: message.request_id,
      release_reason: 'call_complete_auto_release'
    });
    this.pending_call_by_request_id.delete(message.request_id);

    const worker_state = this.worker_state_by_id.get(worker_id);
    worker_state?.pending_call_request_ids.delete(message.request_id);

    if (message.ok) {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'ready',
        reason: 'call response ok'
      });
      pending_call.resolve(message.return_value);
      return;
    }

    pending_call.reject(
      ToError({
        remote_error: message.error,
        fallback_message: `Call to "${pending_call.function_name}" failed.`
      })
    );
  }

  private handleControlResponseMessage(params: {
    worker_id: number;
    message: worker_to_parent_message_t;
  }): void {
    const { worker_id, message } = params;

    if (message.message_type !== 'control_response') {
      return;
    }

    const pending_control = this.pending_control_by_request_id.get(
      message.control_request_id
    );

    if (!pending_control) {
      return;
    }

    this.pending_control_by_request_id.delete(message.control_request_id);

    const worker_state = this.worker_state_by_id.get(worker_id);
    worker_state?.pending_control_request_ids.delete(message.control_request_id);

    clearTimeout(pending_control.timeout_handle);

    if (message.ok) {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'ready',
        reason: 'control response ok'
      });
      pending_control.resolve();
      return;
    }

    pending_control.reject(
      ToError({
        remote_error: message.error,
        fallback_message: `Control command "${pending_control.command}" failed.`
      })
    );
  }

  private handleWorkerError(params: { worker_id: number; error: Error }): void {
    const { worker_id, error } = params;
    this.last_error_by_worker_id.set(worker_id, error);
    this.setWorkerHealthState({
      worker_id,
      health_state: 'degraded',
      reason: error.message
    });
    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'worker_error',
      severity: 'error',
      error: ToRemoteError({ error }),
      details: {
        message: error.message
      }
    });
  }

  private handleWorkerExit(params: { worker_id: number; exit_code: number }): void {
    const { worker_id, exit_code } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      const exit_waiter = this.exit_waiter_by_worker_id.get(worker_id);
      if (exit_waiter) {
        this.exit_waiter_by_worker_id.delete(worker_id);
        clearTimeout(exit_waiter.timeout_handle);
        exit_waiter.resolve();
      }

      return;
    }

    if (!worker_state.shutting_down && this.restart_on_failure) {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'restarting',
        reason: 'worker exited unexpectedly'
      });
    } else {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'stopped',
        reason: 'worker exited'
      });
    }

    this.worker_state_by_id.delete(worker_id);
    this.autoReleaseSharedLocksByWorkerId({
      worker_id,
      release_reason: 'worker_exit_auto_release'
    });

    for (const function_definition of this.function_definition_by_name.values()) {
      function_definition.installed_worker_ids.delete(worker_id);
    }

    for (const dependency_definition of this.dependency_definition_by_alias.values()) {
      dependency_definition.installed_worker_ids.delete(worker_id);
    }

    for (const constant_definition of this.constant_definition_by_name.values()) {
      constant_definition.installed_worker_ids.delete(worker_id);
    }

    for (const database_connection_definition of this.database_connection_definition_by_name.values()) {
      database_connection_definition.installed_worker_ids.delete(worker_id);
    }

    const last_worker_error = this.last_error_by_worker_id.get(worker_id);
    this.last_error_by_worker_id.delete(worker_id);

    const ready_waiter = this.ready_waiter_by_worker_id.get(worker_id);
    if (ready_waiter) {
      this.ready_waiter_by_worker_id.delete(worker_id);
      clearTimeout(ready_waiter.timeout_handle);
      ready_waiter.reject(
        new Error(`Worker ${worker_id} exited before it became ready.`)
      );
    }

    const exit_waiter = this.exit_waiter_by_worker_id.get(worker_id);
    if (exit_waiter) {
      this.exit_waiter_by_worker_id.delete(worker_id);
      clearTimeout(exit_waiter.timeout_handle);
      exit_waiter.resolve();
    }

    const failure_parts = [`Worker ${worker_id} exited with code ${exit_code}.`];
    if (last_worker_error) {
      failure_parts.push(`Last worker error: ${last_worker_error.message}`);
    }

    const failure_reason = failure_parts.join(' ');

    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'worker_exited',
      severity: exit_code === 0 ? 'info' : 'error',
      details: {
        exit_code,
        failure_reason
      }
    });

    this.rejectPendingCallsForWorker({ worker_state, reason: failure_reason });
    this.rejectPendingControlsForWorker({ worker_state, reason: failure_reason });

    if (
      this.lifecycle_state !== 'running' ||
      worker_state.shutting_down ||
      !this.restart_on_failure
    ) {
      return;
    }

    if (this.worker_state_by_id.size >= this.target_worker_count) {
      return;
    }

    if (worker_state.restart_attempt >= this.max_restarts_per_worker) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'worker_restart_exhausted',
        severity: 'error',
        details: {
          restart_attempt: worker_state.restart_attempt,
          max_restarts_per_worker: this.max_restarts_per_worker
        }
      });
      return;
    }

    void this.restartWorker({
      restart_attempt: worker_state.restart_attempt + 1,
      failed_worker_id: worker_id
    });
  }

  private async restartWorker(params: {
    restart_attempt: number;
    failed_worker_id: number;
  }): Promise<void> {
    const { restart_attempt, failed_worker_id } = params;

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const restart_delay_ms = this.getRestartDelayMs({ restart_attempt });
    this.emitParentWorkerEvent({
      worker_id: failed_worker_id,
      event_name: 'worker_restart_scheduled',
      severity: 'warn',
      details: {
        restart_attempt,
        restart_delay_ms
      }
    });

    await this.waitForMs({ delay_ms: restart_delay_ms });

    if (this.lifecycle_state !== 'running') {
      return;
    }

    try {
      await this.createWorker({ restart_attempt });
      const replacement_worker = Array.from(this.worker_state_by_id.values()).find(
        (worker_state): boolean => {
          return worker_state.restart_attempt === restart_attempt;
        }
      );

      this.emitParentWorkerEvent({
        worker_id: replacement_worker?.worker_id ?? failed_worker_id,
        event_name: 'worker_restart_completed',
        severity: 'info',
        details: {
          restart_attempt
        }
      });
    } catch {
      if (restart_attempt >= this.max_restarts_per_worker) {
        this.emitParentWorkerEvent({
          worker_id: failed_worker_id,
          event_name: 'worker_restart_failed',
          severity: 'error',
          details: {
            restart_attempt,
            exhausted: true
          }
        });
        return;
      }

      if (this.lifecycle_state !== 'running') {
        return;
      }

      this.emitParentWorkerEvent({
        worker_id: failed_worker_id,
        event_name: 'worker_restart_failed',
        severity: 'warn',
        details: {
          restart_attempt,
          exhausted: false
        }
      });

      void this.restartWorker({
        restart_attempt: restart_attempt + 1,
        failed_worker_id
      });
    }
  }

  private rejectPendingCallsForWorker(params: {
    worker_state: worker_state_t;
    reason: string;
  }): void {
    const { worker_state, reason } = params;

    for (const request_id of worker_state.pending_call_request_ids) {
      const pending_call = this.pending_call_by_request_id.get(request_id);
      if (!pending_call) {
        continue;
      }

      this.pending_call_by_request_id.delete(request_id);
      clearTimeout(pending_call.timeout_handle);
      this.autoReleaseSharedLocksByOwnerId({
        worker_id: worker_state.worker_id,
        call_request_id: request_id,
        release_reason: 'call_rejection_auto_release'
      });
      pending_call.reject(new Error(reason));
    }

    worker_state.pending_call_request_ids.clear();
  }

  private rejectPendingControlsForWorker(params: {
    worker_state: worker_state_t;
    reason: string;
  }): void {
    const { worker_state, reason } = params;

    for (const control_request_id of worker_state.pending_control_request_ids) {
      const pending_control = this.pending_control_by_request_id.get(control_request_id);
      if (!pending_control) {
        continue;
      }

      this.pending_control_by_request_id.delete(control_request_id);
      clearTimeout(pending_control.timeout_handle);
      pending_control.reject(new Error(reason));
    }

    worker_state.pending_control_request_ids.clear();
  }

  private rejectAllPendingCalls(params: { reason: Error }): void {
    const { reason } = params;

    for (const [request_id, pending_call] of this.pending_call_by_request_id.entries()) {
      clearTimeout(pending_call.timeout_handle);
      this.autoReleaseSharedLocksByOwnerId({
        worker_id: pending_call.worker_id,
        call_request_id: request_id,
        release_reason: 'call_rejection_auto_release'
      });
      pending_call.reject(reason);
      this.pending_call_by_request_id.delete(request_id);
    }
  }

  private rejectAllPendingControls(params: { reason: Error }): void {
    const { reason } = params;

    for (const [request_id, pending_control] of this.pending_control_by_request_id.entries()) {
      clearTimeout(pending_control.timeout_handle);
      pending_control.reject(reason);
      this.pending_control_by_request_id.delete(request_id);
    }
  }
}
