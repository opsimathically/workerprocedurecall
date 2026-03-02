import type {
  cluster_admin_mutation_ack_message_i,
  cluster_admin_mutation_error_message_i,
  cluster_admin_mutation_request_message_i,
  cluster_admin_mutation_result_message_i,
  cluster_admin_mutation_type_t,
  cluster_admin_rollout_mode_t,
  cluster_admin_target_scope_t,
  cluster_call_ack_message_i,
  cluster_call_cancel_ack_message_i,
  cluster_call_cancel_message_i,
  cluster_call_request_message_i,
  cluster_call_response_error_message_i,
  cluster_call_response_success_message_i,
  cluster_protocol_envelope_i,
  cluster_protocol_error_code_t,
  cluster_protocol_error_t,
  cluster_protocol_message_t,
  cluster_protocol_message_type_t,
  cluster_protocol_validation_result_t,
  node_capability_announce_message_i,
  node_heartbeat_message_i
} from './ClusterProtocolTypes';

const supported_protocol_version = 1;

const cluster_protocol_message_type_list: cluster_protocol_message_type_t[] = [
  'cluster_call_request',
  'cluster_call_ack',
  'cluster_call_response_success',
  'cluster_call_response_error',
  'cluster_call_cancel',
  'cluster_call_cancel_ack',
  'node_heartbeat',
  'node_capability_announce',
  'cluster_admin_mutation_request',
  'cluster_admin_mutation_ack',
  'cluster_admin_mutation_result',
  'cluster_admin_mutation_error'
];

const cluster_admin_target_scope_list: cluster_admin_target_scope_t[] = [
  'single_node',
  'node_selector',
  'cluster_wide'
];

const cluster_admin_mutation_type_list: cluster_admin_mutation_type_t[] = [
  'define_function',
  'redefine_function',
  'undefine_function',
  'define_dependency',
  'undefine_dependency',
  'define_constant',
  'undefine_constant',
  'define_database_connection',
  'undefine_database_connection',
  'shared_create_chunk',
  'shared_free_chunk',
  'shared_reconfigure_limits',
  'shared_clear_all_chunks'
];

const cluster_admin_rollout_mode_list: cluster_admin_rollout_mode_t[] = [
  'all_at_once',
  'rolling_percent',
  'canary_then_expand',
  'single_node'
];

type record_t = Record<string, unknown>;

function BuildValidationError(params: {
  message: string;
  details: Record<string, unknown>;
  code?: cluster_protocol_error_code_t;
}): cluster_protocol_error_t {
  const { message, details, code = 'PROTOCOL_VALIDATION_FAILED' } = params;
  return {
    code,
    message,
    retryable: false,
    unknown_outcome: false,
    details
  };
}

function ErrorResult<value_t>(params: {
  message: string;
  details: Record<string, unknown>;
  code?: cluster_protocol_error_code_t;
}): cluster_protocol_validation_result_t<value_t> {
  return {
    ok: false,
    error: BuildValidationError(params)
  };
}

function SuccessResult<value_t>(value: value_t): cluster_protocol_validation_result_t<value_t> {
  return {
    ok: true,
    value
  };
}

function AsRecord(params: {
  value: unknown;
  path: string;
}): cluster_protocol_validation_result_t<record_t> {
  const { value, path } = params;
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return ErrorResult({
      message: `Expected ${path} to be an object.`,
      details: {
        reason: 'invalid_object',
        path
      }
    });
  }

  return SuccessResult(value as record_t);
}

function GetRequiredString(params: {
  record: record_t;
  field_name: string;
  code?: cluster_protocol_error_code_t;
}): cluster_protocol_validation_result_t<string> {
  const { record, field_name, code } = params;
  const value = record[field_name];
  if (typeof value !== 'string' || value.length === 0) {
    return ErrorResult({
      message: `Field "${field_name}" must be a non-empty string.`,
      code,
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name
      }
    });
  }

  return SuccessResult(value);
}

function GetOptionalString(params: {
  record: record_t;
  field_name: string;
}): cluster_protocol_validation_result_t<string | undefined> {
  const { record, field_name } = params;
  const value = record[field_name];
  if (typeof value === 'undefined') {
    return SuccessResult(undefined);
  }

  if (typeof value !== 'string' || value.length === 0) {
    return ErrorResult({
      message: `Field "${field_name}" must be a non-empty string when provided.`,
      details: {
        reason: 'invalid_optional_field',
        field_name
      }
    });
  }

  return SuccessResult(value);
}

function GetRequiredInteger(params: {
  record: record_t;
  field_name: string;
  min_value?: number;
  max_value?: number;
  code?: cluster_protocol_error_code_t;
}): cluster_protocol_validation_result_t<number> {
  const { record, field_name, min_value, max_value, code } = params;
  const value = record[field_name];
  if (!Number.isInteger(value)) {
    return ErrorResult({
      message: `Field "${field_name}" must be an integer.`,
      code,
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name
      }
    });
  }

  const integer_value = value as number;

  if (typeof min_value === 'number' && integer_value < min_value) {
    return ErrorResult({
      message: `Field "${field_name}" must be >= ${min_value}.`,
      code,
      details: {
        reason: 'invalid_range',
        field_name,
        min_value
      }
    });
  }

  if (typeof max_value === 'number' && integer_value > max_value) {
    return ErrorResult({
      message: `Field "${field_name}" must be <= ${max_value}.`,
      code,
      details: {
        reason: 'invalid_range',
        field_name,
        max_value
      }
    });
  }

  return SuccessResult(integer_value);
}

function GetRequiredBoolean(params: {
  record: record_t;
  field_name: string;
}): cluster_protocol_validation_result_t<boolean> {
  const { record, field_name } = params;
  const value = record[field_name];
  if (typeof value !== 'boolean') {
    return ErrorResult({
      message: `Field "${field_name}" must be a boolean.`,
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name
      }
    });
  }

  return SuccessResult(value);
}

function GetOptionalObject(params: {
  record: record_t;
  field_name: string;
}): cluster_protocol_validation_result_t<record_t | undefined> {
  const { record, field_name } = params;
  const value = record[field_name];
  if (typeof value === 'undefined') {
    return SuccessResult(undefined);
  }

  const object_result = AsRecord({
    value,
    path: field_name
  });
  if (!object_result.ok) {
    return object_result;
  }

  return SuccessResult(object_result.value);
}

function GetRequiredObject(params: {
  record: record_t;
  field_name: string;
  code?: cluster_protocol_error_code_t;
}): cluster_protocol_validation_result_t<record_t> {
  const { record, field_name, code } = params;
  const value = record[field_name];
  const object_result = AsRecord({
    value,
    path: field_name
  });
  if (!object_result.ok) {
    return ErrorResult({
      message: object_result.error.message,
      code,
      details: {
        ...object_result.error.details,
        field_name
      }
    });
  }

  return SuccessResult(object_result.value);
}

function GetRequiredStringArray(params: {
  record: record_t;
  field_name: string;
}): cluster_protocol_validation_result_t<string[]> {
  const { record, field_name } = params;
  const value = record[field_name];
  if (!Array.isArray(value)) {
    return ErrorResult({
      message: `Field "${field_name}" must be an array of strings.`,
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name
      }
    });
  }

  for (const item of value) {
    if (typeof item !== 'string' || item.length === 0) {
      return ErrorResult({
        message: `Field "${field_name}" must contain only non-empty strings.`,
        details: {
          reason: 'invalid_array_item',
          field_name
        }
      });
    }
  }

  return SuccessResult(value);
}

function GetOptionalStringArray(params: {
  record: record_t;
  field_name: string;
}): cluster_protocol_validation_result_t<string[] | undefined> {
  const { record, field_name } = params;
  const value = record[field_name];
  if (typeof value === 'undefined') {
    return SuccessResult(undefined);
  }

  if (!Array.isArray(value)) {
    return ErrorResult({
      message: `Field "${field_name}" must be an array of strings when provided.`,
      details: {
        reason: 'invalid_optional_field',
        field_name
      }
    });
  }

  for (const item of value) {
    if (typeof item !== 'string' || item.length === 0) {
      return ErrorResult({
        message: `Field "${field_name}" must contain only non-empty strings.`,
        details: {
          reason: 'invalid_array_item',
          field_name
        }
      });
    }
  }

  return SuccessResult(value);
}

function ValidateProtocolVersion(params: {
  record: record_t;
}): cluster_protocol_validation_result_t<1> {
  const { record } = params;
  const protocol_version = record.protocol_version;
  if (!Number.isInteger(protocol_version)) {
    return ErrorResult({
      message: 'Field "protocol_version" must be an integer.',
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name: 'protocol_version'
      }
    });
  }

  if (protocol_version !== supported_protocol_version) {
    return ErrorResult({
      message: `Unsupported protocol_version "${protocol_version}".`,
      details: {
        reason: 'unsupported_protocol_version',
        supported_protocol_version,
        received_protocol_version: protocol_version
      }
    });
  }

  return SuccessResult(supported_protocol_version);
}

function ValidateEnvelope(params: {
  message: unknown;
  expected_message_type?: cluster_protocol_message_type_t;
}): cluster_protocol_validation_result_t<{
  envelope: cluster_protocol_envelope_i;
  record: record_t;
}> {
  const object_result = AsRecord({
    value: params.message,
    path: 'message'
  });
  if (!object_result.ok) {
    return object_result;
  }

  const record = object_result.value;

  const version_result = ValidateProtocolVersion({
    record
  });
  if (!version_result.ok) {
    return version_result;
  }

  const message_type_result = GetRequiredString({
    record,
    field_name: 'message_type'
  });
  if (!message_type_result.ok) {
    return message_type_result;
  }

  const message_type = message_type_result.value;
  if (
    !cluster_protocol_message_type_list.includes(
      message_type as cluster_protocol_message_type_t
    )
  ) {
    return ErrorResult({
      message: `Unsupported message_type "${message_type}".`,
      details: {
        reason: 'invalid_enum_value',
        field_name: 'message_type',
        received_value: message_type
      }
    });
  }

  if (
    params.expected_message_type &&
    message_type !== params.expected_message_type
  ) {
    return ErrorResult({
      message: `Expected message_type "${params.expected_message_type}" but received "${message_type}".`,
      details: {
        reason: 'unexpected_message_type',
        expected_message_type: params.expected_message_type,
        received_message_type: message_type
      }
    });
  }

  const timestamp_result = GetRequiredInteger({
    record,
    field_name: 'timestamp_unix_ms',
    min_value: 1
  });
  if (!timestamp_result.ok) {
    return timestamp_result;
  }

  return SuccessResult({
    envelope: {
      protocol_version: version_result.value,
      message_type: message_type as cluster_protocol_message_type_t,
      timestamp_unix_ms: timestamp_result.value
    },
    record
  });
}

function ValidateDeadline(params: {
  deadline_unix_ms: number;
  now_unix_ms: number;
  field_name: string;
  code?: cluster_protocol_error_code_t;
}): cluster_protocol_validation_result_t<number> {
  if (!Number.isInteger(params.deadline_unix_ms)) {
    return ErrorResult({
      message: `Field "${params.field_name}" must be an integer.`,
      code: params.code,
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name: params.field_name
      }
    });
  }

  if (params.deadline_unix_ms <= params.now_unix_ms) {
    return ErrorResult({
      message: `Field "${params.field_name}" must be in the future.`,
      code: params.code,
      details: {
        reason: 'expired_deadline',
        field_name: params.field_name,
        deadline_unix_ms: params.deadline_unix_ms,
        now_unix_ms: params.now_unix_ms
      }
    });
  }

  return SuccessResult(params.deadline_unix_ms);
}

function ValidateTargetSelectorForScope(params: {
  target_scope: cluster_admin_target_scope_t;
  target_selector: record_t | undefined;
}): cluster_protocol_validation_result_t<record_t | undefined> {
  const { target_scope, target_selector } = params;

  const node_ids = Array.isArray(target_selector?.node_ids)
    ? target_selector?.node_ids
    : undefined;
  const labels =
    target_selector?.labels && typeof target_selector.labels === 'object'
      ? (target_selector.labels as Record<string, unknown>)
      : undefined;
  const zones = Array.isArray(target_selector?.zones)
    ? target_selector?.zones
    : undefined;
  const version_constraints =
    target_selector?.version_constraints &&
    typeof target_selector.version_constraints === 'object'
      ? (target_selector.version_constraints as Record<string, unknown>)
      : undefined;

  if (target_scope === 'single_node') {
    if (!Array.isArray(node_ids) || node_ids.length !== 1) {
      return ErrorResult({
        message:
          'target_scope "single_node" requires target_selector.node_ids with exactly one node id.',
        code: 'ADMIN_VALIDATION_FAILED',
        details: {
          reason: 'invalid_target_selector_for_scope',
          target_scope
        }
      });
    }

    for (const node_id of node_ids) {
      if (typeof node_id !== 'string' || node_id.length === 0) {
        return ErrorResult({
          message:
            'target_scope "single_node" requires target_selector.node_ids to contain a non-empty string.',
          code: 'ADMIN_VALIDATION_FAILED',
          details: {
            reason: 'invalid_target_selector_for_scope',
            target_scope
          }
        });
      }
    }

    return SuccessResult(target_selector);
  }

  if (target_scope === 'cluster_wide') {
    if (Array.isArray(node_ids) && node_ids.length > 0) {
      return ErrorResult({
        message:
          'target_scope "cluster_wide" must not include target_selector.node_ids.',
        code: 'ADMIN_VALIDATION_FAILED',
        details: {
          reason: 'invalid_target_selector_for_scope',
          target_scope
        }
      });
    }

    return SuccessResult(target_selector);
  }

  const has_node_ids = Array.isArray(node_ids) && node_ids.length > 0;
  const has_labels = !!labels && Object.keys(labels).length > 0;
  const has_zones = Array.isArray(zones) && zones.length > 0;
  const has_version_constraints =
    !!version_constraints && Object.keys(version_constraints).length > 0;

  if (!has_node_ids && !has_labels && !has_zones && !has_version_constraints) {
    return ErrorResult({
      message:
        'target_scope "node_selector" requires at least one selector predicate (node_ids, labels, zones, version_constraints).',
      code: 'ADMIN_VALIDATION_FAILED',
      details: {
        reason: 'invalid_target_selector_for_scope',
        target_scope
      }
    });
  }

  return SuccessResult(target_selector);
}

function ParseAdminRolloutStrategy(params: {
  record: record_t;
}): cluster_protocol_validation_result_t<
  cluster_admin_mutation_request_message_i['rollout_strategy']
> {
  const rollout_strategy_result = GetRequiredObject({
    record: params.record,
    field_name: 'rollout_strategy',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!rollout_strategy_result.ok) {
    return rollout_strategy_result;
  }

  const rollout_record = rollout_strategy_result.value;
  const mode_result = GetRequiredString({
    record: rollout_record,
    field_name: 'mode',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!mode_result.ok) {
    return mode_result;
  }

  const mode = mode_result.value;
  if (!cluster_admin_rollout_mode_list.includes(mode as cluster_admin_rollout_mode_t)) {
    return ErrorResult({
      message: `Invalid rollout_strategy.mode "${mode}".`,
      code: 'ADMIN_VALIDATION_FAILED',
      details: {
        reason: 'invalid_enum_value',
        field_name: 'rollout_strategy.mode',
        received_value: mode
      }
    });
  }

  const min_success_percent_result = GetOptionalInteger({
    record: rollout_record,
    field_name: 'min_success_percent',
    min_value: 1,
    max_value: 100
  });
  if (!min_success_percent_result.ok) {
    return min_success_percent_result;
  }

  const batch_percent_result = GetOptionalInteger({
    record: rollout_record,
    field_name: 'batch_percent',
    min_value: 1,
    max_value: 100
  });
  if (!batch_percent_result.ok) {
    return batch_percent_result;
  }

  const canary_node_count_result = GetOptionalInteger({
    record: rollout_record,
    field_name: 'canary_node_count',
    min_value: 1
  });
  if (!canary_node_count_result.ok) {
    return canary_node_count_result;
  }

  const inter_batch_delay_result = GetOptionalInteger({
    record: rollout_record,
    field_name: 'inter_batch_delay_ms',
    min_value: 0
  });
  if (!inter_batch_delay_result.ok) {
    return inter_batch_delay_result;
  }

  const apply_timeout_result = GetOptionalInteger({
    record: rollout_record,
    field_name: 'apply_timeout_ms',
    min_value: 1
  });
  if (!apply_timeout_result.ok) {
    return apply_timeout_result;
  }

  const verify_timeout_result = GetOptionalInteger({
    record: rollout_record,
    field_name: 'verify_timeout_ms',
    min_value: 1
  });
  if (!verify_timeout_result.ok) {
    return verify_timeout_result;
  }

  const rollback_policy_result = GetOptionalObject({
    record: rollout_record,
    field_name: 'rollback_policy'
  });
  if (!rollback_policy_result.ok) {
    return rollback_policy_result;
  }

  let rollback_policy:
    | cluster_admin_mutation_request_message_i['rollout_strategy']['rollback_policy']
    | undefined = undefined;
  if (rollback_policy_result.value) {
    const rollback_policy_record = rollback_policy_result.value;
    const auto_rollback_result = GetOptionalBoolean({
      record: rollback_policy_record,
      field_name: 'auto_rollback'
    });
    if (!auto_rollback_result.ok) {
      return auto_rollback_result;
    }

    const rollback_on_partial_failure_result = GetOptionalBoolean({
      record: rollback_policy_record,
      field_name: 'rollback_on_partial_failure'
    });
    if (!rollback_on_partial_failure_result.ok) {
      return rollback_on_partial_failure_result;
    }

    const rollback_on_verification_failure_result = GetOptionalBoolean({
      record: rollback_policy_record,
      field_name: 'rollback_on_verification_failure'
    });
    if (!rollback_on_verification_failure_result.ok) {
      return rollback_on_verification_failure_result;
    }

    rollback_policy = {
      auto_rollback: auto_rollback_result.value,
      rollback_on_partial_failure: rollback_on_partial_failure_result.value,
      rollback_on_verification_failure:
        rollback_on_verification_failure_result.value
    };
  }

  return SuccessResult({
    mode: mode as cluster_admin_rollout_mode_t,
    min_success_percent: min_success_percent_result.value,
    batch_percent: batch_percent_result.value,
    canary_node_count: canary_node_count_result.value,
    inter_batch_delay_ms: inter_batch_delay_result.value,
    apply_timeout_ms: apply_timeout_result.value,
    verify_timeout_ms: verify_timeout_result.value,
    rollback_policy
  });
}

function GetOptionalInteger(params: {
  record: record_t;
  field_name: string;
  min_value?: number;
  max_value?: number;
}): cluster_protocol_validation_result_t<number | undefined> {
  const { record, field_name } = params;
  const value = record[field_name];
  if (typeof value === 'undefined') {
    return SuccessResult(undefined);
  }

  const integer_result = GetRequiredInteger({
    record,
    field_name,
    min_value: params.min_value,
    max_value: params.max_value
  });
  if (!integer_result.ok) {
    return integer_result;
  }

  return SuccessResult(integer_result.value);
}

function GetOptionalBoolean(params: {
  record: record_t;
  field_name: string;
}): cluster_protocol_validation_result_t<boolean | undefined> {
  const value = params.record[params.field_name];
  if (typeof value === 'undefined') {
    return SuccessResult(undefined);
  }

  if (typeof value !== 'boolean') {
    return ErrorResult({
      message: `Field "${params.field_name}" must be a boolean when provided.`,
      details: {
        reason: 'invalid_optional_field',
        field_name: params.field_name
      }
    });
  }

  return SuccessResult(value);
}

// Spec cross-reference: design_documentation/Specification.md section 14.3.
export function ParseClusterAdminMutationRequestMessage(params: {
  message: unknown;
  now_unix_ms?: number;
}): cluster_protocol_validation_result_t<cluster_admin_mutation_request_message_i> {
  const { message, now_unix_ms = Date.now() } = params;
  const envelope_result = ValidateEnvelope({
    message,
    expected_message_type: 'cluster_admin_mutation_request'
  });
  if (!envelope_result.ok) {
    return ErrorResult({
      message: envelope_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: envelope_result.error.details
    });
  }

  const record = envelope_result.value.record;

  const mutation_id_result = GetRequiredString({
    record,
    field_name: 'mutation_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!mutation_id_result.ok) {
    return mutation_id_result;
  }

  const request_id_result = GetRequiredString({
    record,
    field_name: 'request_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!request_id_result.ok) {
    return request_id_result;
  }

  const trace_id_result = GetRequiredString({
    record,
    field_name: 'trace_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!trace_id_result.ok) {
    return trace_id_result;
  }

  const deadline_result = GetRequiredInteger({
    record,
    field_name: 'deadline_unix_ms',
    min_value: 1,
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!deadline_result.ok) {
    return deadline_result;
  }

  const deadline_sanity_result = ValidateDeadline({
    deadline_unix_ms: deadline_result.value,
    now_unix_ms,
    field_name: 'deadline_unix_ms',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!deadline_sanity_result.ok) {
    return deadline_sanity_result;
  }

  const target_scope_result = GetRequiredString({
    record,
    field_name: 'target_scope',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!target_scope_result.ok) {
    return target_scope_result;
  }

  if (
    !cluster_admin_target_scope_list.includes(
      target_scope_result.value as cluster_admin_target_scope_t
    )
  ) {
    return ErrorResult({
      message: `Invalid target_scope "${target_scope_result.value}".`,
      code: 'ADMIN_VALIDATION_FAILED',
      details: {
        reason: 'invalid_enum_value',
        field_name: 'target_scope',
        received_value: target_scope_result.value
      }
    });
  }
  const target_scope = target_scope_result.value as cluster_admin_target_scope_t;

  const target_selector_result = GetOptionalObject({
    record,
    field_name: 'target_selector'
  });
  if (!target_selector_result.ok) {
    return ErrorResult({
      message: target_selector_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: target_selector_result.error.details
    });
  }

  const target_selector_scope_result = ValidateTargetSelectorForScope({
    target_scope,
    target_selector: target_selector_result.value
  });
  if (!target_selector_scope_result.ok) {
    return target_selector_scope_result;
  }

  const mutation_type_result = GetRequiredString({
    record,
    field_name: 'mutation_type',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!mutation_type_result.ok) {
    return mutation_type_result;
  }

  if (
    !cluster_admin_mutation_type_list.includes(
      mutation_type_result.value as cluster_admin_mutation_type_t
    )
  ) {
    return ErrorResult({
      message: `Invalid mutation_type "${mutation_type_result.value}".`,
      code: 'ADMIN_VALIDATION_FAILED',
      details: {
        reason: 'invalid_enum_value',
        field_name: 'mutation_type',
        received_value: mutation_type_result.value
      }
    });
  }

  const payload_result = GetRequiredObject({
    record,
    field_name: 'payload',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!payload_result.ok) {
    return payload_result;
  }

  const dry_run_result = GetRequiredBoolean({
    record,
    field_name: 'dry_run'
  });
  if (!dry_run_result.ok) {
    return ErrorResult({
      message: dry_run_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: {
        ...dry_run_result.error.details,
        reason: 'invalid_dry_run_constraint'
      }
    });
  }

  const rollout_strategy_result = ParseAdminRolloutStrategy({
    record
  });
  if (!rollout_strategy_result.ok) {
    return ErrorResult({
      message: rollout_strategy_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: rollout_strategy_result.error.details
    });
  }

  const auth_context_result = GetRequiredObject({
    record,
    field_name: 'auth_context',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!auth_context_result.ok) {
    return auth_context_result;
  }

  const auth_context_record = auth_context_result.value;
  const auth_subject_result = GetRequiredString({
    record: auth_context_record,
    field_name: 'subject',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!auth_subject_result.ok) {
    return auth_subject_result;
  }

  const auth_tenant_result = GetRequiredString({
    record: auth_context_record,
    field_name: 'tenant_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!auth_tenant_result.ok) {
    return auth_tenant_result;
  }

  const capability_claims_result = GetRequiredStringArray({
    record: auth_context_record,
    field_name: 'capability_claims'
  });
  if (!capability_claims_result.ok) {
    return ErrorResult({
      message: capability_claims_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: capability_claims_result.error.details
    });
  }

  const signed_claims_result = GetRequiredString({
    record: auth_context_record,
    field_name: 'signed_claims',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!signed_claims_result.ok) {
    return signed_claims_result;
  }

  const in_flight_policy_result = GetOptionalString({
    record,
    field_name: 'in_flight_policy'
  });
  if (!in_flight_policy_result.ok) {
    return ErrorResult({
      message: in_flight_policy_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: in_flight_policy_result.error.details
    });
  }

  const in_flight_policy = in_flight_policy_result.value;
  if (
    typeof in_flight_policy !== 'undefined' &&
    in_flight_policy !== 'no_interruption' &&
    in_flight_policy !== 'drain_and_swap'
  ) {
    return ErrorResult({
      message: `Invalid in_flight_policy "${in_flight_policy}".`,
      code: 'ADMIN_VALIDATION_FAILED',
      details: {
        reason: 'invalid_enum_value',
        field_name: 'in_flight_policy',
        received_value: in_flight_policy
      }
    });
  }

  const idempotency_key_result = GetOptionalString({
    record,
    field_name: 'idempotency_key'
  });
  if (!idempotency_key_result.ok) {
    return ErrorResult({
      message: idempotency_key_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: idempotency_key_result.error.details
    });
  }

  const auth_environment_result = GetOptionalString({
    record: auth_context_record,
    field_name: 'environment'
  });
  if (!auth_environment_result.ok) {
    return ErrorResult({
      message: auth_environment_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: auth_environment_result.error.details
    });
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_admin_mutation_request',
    mutation_id: mutation_id_result.value,
    request_id: request_id_result.value,
    trace_id: trace_id_result.value,
    deadline_unix_ms: deadline_sanity_result.value,
    target_scope,
    target_selector: target_selector_scope_result.value as
      | cluster_admin_mutation_request_message_i['target_selector']
      | undefined,
    mutation_type: mutation_type_result.value as cluster_admin_mutation_type_t,
    payload: payload_result.value,
    expected_version: (record.expected_version ?? undefined) as
      | cluster_admin_mutation_request_message_i['expected_version']
      | undefined,
    dry_run: dry_run_result.value,
    rollout_strategy: rollout_strategy_result.value,
    in_flight_policy:
      in_flight_policy as cluster_admin_mutation_request_message_i['in_flight_policy'],
    change_context: (record.change_context ?? undefined) as
      | cluster_admin_mutation_request_message_i['change_context']
      | undefined,
    artifact: (record.artifact ?? undefined) as
      | cluster_admin_mutation_request_message_i['artifact']
      | undefined,
    auth_context: {
      subject: auth_subject_result.value,
      tenant_id: auth_tenant_result.value,
      environment: auth_environment_result.value,
      capability_claims: capability_claims_result.value,
      signed_claims: signed_claims_result.value
    },
    idempotency_key: idempotency_key_result.value
  });
}

export function ParseClusterAdminMutationAckMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<cluster_admin_mutation_ack_message_i> {
  const envelope_result = ValidateEnvelope({
    message: params.message,
    expected_message_type: 'cluster_admin_mutation_ack'
  });
  if (!envelope_result.ok) {
    return ErrorResult({
      message: envelope_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: envelope_result.error.details
    });
  }

  const record = envelope_result.value.record;
  const mutation_id_result = GetRequiredString({
    record,
    field_name: 'mutation_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!mutation_id_result.ok) {
    return mutation_id_result;
  }
  const request_id_result = GetRequiredString({
    record,
    field_name: 'request_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!request_id_result.ok) {
    return request_id_result;
  }
  const accepted_result = GetRequiredBoolean({
    record,
    field_name: 'accepted'
  });
  if (!accepted_result.ok) {
    return ErrorResult({
      message: accepted_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: accepted_result.error.details
    });
  }
  const planner_id_result = GetRequiredString({
    record,
    field_name: 'planner_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!planner_id_result.ok) {
    return planner_id_result;
  }
  const target_node_count_result = GetRequiredInteger({
    record,
    field_name: 'target_node_count',
    min_value: 0,
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!target_node_count_result.ok) {
    return target_node_count_result;
  }
  const dry_run_result = GetRequiredBoolean({
    record,
    field_name: 'dry_run'
  });
  if (!dry_run_result.ok) {
    return ErrorResult({
      message: dry_run_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: dry_run_result.error.details
    });
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_admin_mutation_ack',
    mutation_id: mutation_id_result.value,
    request_id: request_id_result.value,
    accepted: accepted_result.value,
    planner_id: planner_id_result.value,
    target_node_count: target_node_count_result.value,
    dry_run: dry_run_result.value
  });
}

export function ParseClusterAdminMutationResultMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<cluster_admin_mutation_result_message_i> {
  const envelope_result = ValidateEnvelope({
    message: params.message,
    expected_message_type: 'cluster_admin_mutation_result'
  });
  if (!envelope_result.ok) {
    return ErrorResult({
      message: envelope_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: envelope_result.error.details
    });
  }

  const record = envelope_result.value.record;
  const mutation_id_result = GetRequiredString({
    record,
    field_name: 'mutation_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!mutation_id_result.ok) {
    return mutation_id_result;
  }
  const request_id_result = GetRequiredString({
    record,
    field_name: 'request_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!request_id_result.ok) {
    return request_id_result;
  }
  const status_result = GetRequiredString({
    record,
    field_name: 'status',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!status_result.ok) {
    return status_result;
  }
  const rollout_strategy_result = GetRequiredString({
    record,
    field_name: 'rollout_strategy',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!rollout_strategy_result.ok) {
    return rollout_strategy_result;
  }
  const summary_result = GetRequiredObject({
    record,
    field_name: 'summary',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!summary_result.ok) {
    return summary_result;
  }
  const per_node_results_value = record.per_node_results;
  if (!Array.isArray(per_node_results_value)) {
    return ErrorResult({
      message: 'Field "per_node_results" must be an array.',
      code: 'ADMIN_VALIDATION_FAILED',
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name: 'per_node_results'
      }
    });
  }
  const audit_record_id_result = GetRequiredString({
    record,
    field_name: 'audit_record_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!audit_record_id_result.ok) {
    return audit_record_id_result;
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_admin_mutation_result',
    mutation_id: mutation_id_result.value,
    request_id: request_id_result.value,
    status:
      status_result.value as cluster_admin_mutation_result_message_i['status'],
    rollout_strategy:
      rollout_strategy_result.value as cluster_admin_mutation_result_message_i['rollout_strategy'],
    summary:
      summary_result.value as cluster_admin_mutation_result_message_i['summary'],
    per_node_results:
      per_node_results_value as cluster_admin_mutation_result_message_i['per_node_results'],
    timing: (record.timing ?? undefined) as
      | cluster_admin_mutation_result_message_i['timing']
      | undefined,
    audit_record_id: audit_record_id_result.value
  });
}

export function ParseClusterAdminMutationErrorMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<cluster_admin_mutation_error_message_i> {
  const envelope_result = ValidateEnvelope({
    message: params.message,
    expected_message_type: 'cluster_admin_mutation_error'
  });
  if (!envelope_result.ok) {
    return ErrorResult({
      message: envelope_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: envelope_result.error.details
    });
  }

  const record = envelope_result.value.record;
  const mutation_id_result = GetRequiredString({
    record,
    field_name: 'mutation_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!mutation_id_result.ok) {
    return mutation_id_result;
  }
  const request_id_result = GetRequiredString({
    record,
    field_name: 'request_id',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!request_id_result.ok) {
    return request_id_result;
  }
  const error_result = GetRequiredObject({
    record,
    field_name: 'error',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!error_result.ok) {
    return error_result;
  }

  const error_record = error_result.value;
  const code_result = GetRequiredString({
    record: error_record,
    field_name: 'code',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!code_result.ok) {
    return code_result;
  }
  const message_result = GetRequiredString({
    record: error_record,
    field_name: 'message',
    code: 'ADMIN_VALIDATION_FAILED'
  });
  if (!message_result.ok) {
    return message_result;
  }
  const retryable_result = GetRequiredBoolean({
    record: error_record,
    field_name: 'retryable'
  });
  if (!retryable_result.ok) {
    return ErrorResult({
      message: retryable_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: retryable_result.error.details
    });
  }
  const unknown_outcome_result = GetRequiredBoolean({
    record: error_record,
    field_name: 'unknown_outcome'
  });
  if (!unknown_outcome_result.ok) {
    return ErrorResult({
      message: unknown_outcome_result.error.message,
      code: 'ADMIN_VALIDATION_FAILED',
      details: unknown_outcome_result.error.details
    });
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_admin_mutation_error',
    mutation_id: mutation_id_result.value,
    request_id: request_id_result.value,
    error: {
      code: code_result.value as cluster_protocol_error_code_t,
      message: message_result.value,
      retryable: retryable_result.value,
      unknown_outcome: unknown_outcome_result.value,
      details:
        (error_record.details as Record<string, unknown> | undefined) ?? {}
    }
  });
}

export function ParseClusterCallRequestMessage(params: {
  message: unknown;
  now_unix_ms?: number;
}): cluster_protocol_validation_result_t<cluster_call_request_message_i> {
  const { now_unix_ms = Date.now() } = params;
  const envelope_result = ValidateEnvelope({
    message: params.message,
    expected_message_type: 'cluster_call_request'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }

  const record = envelope_result.value.record;
  const request_id_result = GetRequiredString({
    record,
    field_name: 'request_id'
  });
  if (!request_id_result.ok) {
    return request_id_result;
  }
  const trace_id_result = GetRequiredString({
    record,
    field_name: 'trace_id'
  });
  if (!trace_id_result.ok) {
    return trace_id_result;
  }
  const span_id_result = GetRequiredString({
    record,
    field_name: 'span_id'
  });
  if (!span_id_result.ok) {
    return span_id_result;
  }
  const attempt_index_result = GetRequiredInteger({
    record,
    field_name: 'attempt_index',
    min_value: 1
  });
  if (!attempt_index_result.ok) {
    return attempt_index_result;
  }
  const max_attempts_result = GetRequiredInteger({
    record,
    field_name: 'max_attempts',
    min_value: 1
  });
  if (!max_attempts_result.ok) {
    return max_attempts_result;
  }
  const deadline_result = GetRequiredInteger({
    record,
    field_name: 'deadline_unix_ms',
    min_value: 1
  });
  if (!deadline_result.ok) {
    return deadline_result;
  }
  const deadline_sanity_result = ValidateDeadline({
    deadline_unix_ms: deadline_result.value,
    now_unix_ms,
    field_name: 'deadline_unix_ms'
  });
  if (!deadline_sanity_result.ok) {
    return deadline_sanity_result;
  }
  const function_name_result = GetRequiredString({
    record,
    field_name: 'function_name'
  });
  if (!function_name_result.ok) {
    return function_name_result;
  }
  const function_hash_result = GetOptionalString({
    record,
    field_name: 'function_hash_sha1'
  });
  if (!function_hash_result.ok) {
    return function_hash_result;
  }
  const args_value = record.args;
  if (!Array.isArray(args_value)) {
    return ErrorResult({
      message: 'Field "args" must be an array.',
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name: 'args'
      }
    });
  }
  const routing_hint_result = GetRequiredObject({
    record,
    field_name: 'routing_hint'
  });
  if (!routing_hint_result.ok) {
    return routing_hint_result;
  }
  const caller_identity_result = GetRequiredObject({
    record,
    field_name: 'caller_identity'
  });
  if (!caller_identity_result.ok) {
    return caller_identity_result;
  }

  const routing_mode_result = GetRequiredString({
    record: routing_hint_result.value,
    field_name: 'mode'
  });
  if (!routing_mode_result.ok) {
    return routing_mode_result;
  }
  const routing_mode = routing_mode_result.value;
  if (
    routing_mode !== 'auto' &&
    routing_mode !== 'target_node' &&
    routing_mode !== 'affinity'
  ) {
    return ErrorResult({
      message: `Invalid routing_hint.mode "${routing_mode}".`,
      details: {
        reason: 'invalid_enum_value',
        field_name: 'routing_hint.mode',
        received_value: routing_mode
      }
    });
  }

  const caller_subject_result = GetRequiredString({
    record: caller_identity_result.value,
    field_name: 'subject'
  });
  if (!caller_subject_result.ok) {
    return caller_subject_result;
  }
  const caller_tenant_result = GetRequiredString({
    record: caller_identity_result.value,
    field_name: 'tenant_id'
  });
  if (!caller_tenant_result.ok) {
    return caller_tenant_result;
  }
  const caller_scopes_result = GetRequiredStringArray({
    record: caller_identity_result.value,
    field_name: 'scopes'
  });
  if (!caller_scopes_result.ok) {
    return caller_scopes_result;
  }
  const signed_claims_result = GetRequiredString({
    record: caller_identity_result.value,
    field_name: 'signed_claims'
  });
  if (!signed_claims_result.ok) {
    return signed_claims_result;
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_call_request',
    request_id: request_id_result.value,
    trace_id: trace_id_result.value,
    span_id: span_id_result.value,
    attempt_index: attempt_index_result.value,
    max_attempts: max_attempts_result.value,
    deadline_unix_ms: deadline_sanity_result.value,
    function_name: function_name_result.value,
    function_hash_sha1: function_hash_result.value,
    args: args_value as unknown[],
    routing_hint: routing_hint_result.value as cluster_call_request_message_i['routing_hint'],
    idempotency_key:
      (record.idempotency_key as string | undefined) ?? undefined,
    caller_identity: {
      subject: caller_subject_result.value,
      tenant_id: caller_tenant_result.value,
      scopes: caller_scopes_result.value,
      signed_claims: signed_claims_result.value
    },
    metadata: (record.metadata as Record<string, unknown> | undefined) ?? undefined
  });
}

function ParseEnvelopeOnlyMessage<message_t extends cluster_protocol_envelope_i>(params: {
  message: unknown;
  expected_message_type: cluster_protocol_message_type_t;
}): cluster_protocol_validation_result_t<{
  envelope: cluster_protocol_envelope_i;
  record: record_t;
}> {
  return ValidateEnvelope({
    message: params.message,
    expected_message_type: params.expected_message_type
  });
}

export function ParseClusterCallAckMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<cluster_call_ack_message_i> {
  const envelope_result = ParseEnvelopeOnlyMessage({
    message: params.message,
    expected_message_type: 'cluster_call_ack'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }
  const record = envelope_result.value.record;

  const request_id_result = GetRequiredString({ record, field_name: 'request_id' });
  if (!request_id_result.ok) {
    return request_id_result;
  }
  const attempt_index_result = GetRequiredInteger({
    record,
    field_name: 'attempt_index',
    min_value: 1
  });
  if (!attempt_index_result.ok) {
    return attempt_index_result;
  }
  const node_id_result = GetRequiredString({ record, field_name: 'node_id' });
  if (!node_id_result.ok) {
    return node_id_result;
  }
  const accepted_result = GetRequiredBoolean({ record, field_name: 'accepted' });
  if (!accepted_result.ok) {
    return accepted_result;
  }
  const queue_position_result = GetRequiredInteger({
    record,
    field_name: 'queue_position',
    min_value: 0
  });
  if (!queue_position_result.ok) {
    return queue_position_result;
  }
  const estimated_start_delay_result = GetRequiredInteger({
    record,
    field_name: 'estimated_start_delay_ms',
    min_value: 0
  });
  if (!estimated_start_delay_result.ok) {
    return estimated_start_delay_result;
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_call_ack',
    request_id: request_id_result.value,
    attempt_index: attempt_index_result.value,
    node_id: node_id_result.value,
    accepted: accepted_result.value,
    queue_position: queue_position_result.value,
    estimated_start_delay_ms: estimated_start_delay_result.value
  });
}

export function ParseClusterCallResponseSuccessMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<cluster_call_response_success_message_i> {
  const envelope_result = ParseEnvelopeOnlyMessage({
    message: params.message,
    expected_message_type: 'cluster_call_response_success'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }
  const record = envelope_result.value.record;
  const request_id_result = GetRequiredString({ record, field_name: 'request_id' });
  if (!request_id_result.ok) {
    return request_id_result;
  }
  const attempt_index_result = GetRequiredInteger({
    record,
    field_name: 'attempt_index',
    min_value: 1
  });
  if (!attempt_index_result.ok) {
    return attempt_index_result;
  }
  const node_id_result = GetRequiredString({ record, field_name: 'node_id' });
  if (!node_id_result.ok) {
    return node_id_result;
  }
  const function_name_result = GetRequiredString({
    record,
    field_name: 'function_name'
  });
  if (!function_name_result.ok) {
    return function_name_result;
  }
  const function_hash_result = GetRequiredString({
    record,
    field_name: 'function_hash_sha1'
  });
  if (!function_hash_result.ok) {
    return function_hash_result;
  }
  const timing_result = GetRequiredObject({ record, field_name: 'timing' });
  if (!timing_result.ok) {
    return timing_result;
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_call_response_success',
    request_id: request_id_result.value,
    attempt_index: attempt_index_result.value,
    node_id: node_id_result.value,
    function_name: function_name_result.value,
    function_hash_sha1: function_hash_result.value,
    return_value: record.return_value,
    timing: timing_result.value as cluster_call_response_success_message_i['timing']
  });
}

export function ParseClusterCallResponseErrorMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<cluster_call_response_error_message_i> {
  const envelope_result = ParseEnvelopeOnlyMessage({
    message: params.message,
    expected_message_type: 'cluster_call_response_error'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }
  const record = envelope_result.value.record;
  const request_id_result = GetRequiredString({ record, field_name: 'request_id' });
  if (!request_id_result.ok) {
    return request_id_result;
  }
  const attempt_index_result = GetRequiredInteger({
    record,
    field_name: 'attempt_index',
    min_value: 1
  });
  if (!attempt_index_result.ok) {
    return attempt_index_result;
  }
  const error_result = GetRequiredObject({ record, field_name: 'error' });
  if (!error_result.ok) {
    return error_result;
  }
  const timing_result = GetRequiredObject({ record, field_name: 'timing' });
  if (!timing_result.ok) {
    return timing_result;
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_call_response_error',
    request_id: request_id_result.value,
    attempt_index: attempt_index_result.value,
    node_id: (record.node_id as string | undefined) ?? undefined,
    error: error_result.value as cluster_call_response_error_message_i['error'],
    timing: timing_result.value as cluster_call_response_error_message_i['timing']
  });
}

export function ParseClusterCallCancelMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<cluster_call_cancel_message_i> {
  const envelope_result = ParseEnvelopeOnlyMessage({
    message: params.message,
    expected_message_type: 'cluster_call_cancel'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }
  const record = envelope_result.value.record;
  const request_id_result = GetRequiredString({ record, field_name: 'request_id' });
  if (!request_id_result.ok) {
    return request_id_result;
  }
  const reason_result = GetRequiredString({ record, field_name: 'reason' });
  if (!reason_result.ok) {
    return reason_result;
  }

  const reason = reason_result.value;
  if (
    reason !== 'client_cancelled' &&
    reason !== 'deadline_exceeded' &&
    reason !== 'gateway_shutdown'
  ) {
    return ErrorResult({
      message: `Invalid reason "${reason}".`,
      details: {
        reason: 'invalid_enum_value',
        field_name: 'reason',
        received_value: reason
      }
    });
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_call_cancel',
    request_id: request_id_result.value,
    reason
  });
}

export function ParseClusterCallCancelAckMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<cluster_call_cancel_ack_message_i> {
  const envelope_result = ParseEnvelopeOnlyMessage({
    message: params.message,
    expected_message_type: 'cluster_call_cancel_ack'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }
  const record = envelope_result.value.record;
  const request_id_result = GetRequiredString({ record, field_name: 'request_id' });
  if (!request_id_result.ok) {
    return request_id_result;
  }
  const cancelled_result = GetRequiredBoolean({ record, field_name: 'cancelled' });
  if (!cancelled_result.ok) {
    return cancelled_result;
  }
  const best_effort_result = GetRequiredBoolean({
    record,
    field_name: 'best_effort_only'
  });
  if (!best_effort_result.ok) {
    return best_effort_result;
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'cluster_call_cancel_ack',
    request_id: request_id_result.value,
    cancelled: cancelled_result.value,
    best_effort_only: best_effort_result.value
  });
}

export function ParseNodeHeartbeatMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<node_heartbeat_message_i> {
  const envelope_result = ParseEnvelopeOnlyMessage({
    message: params.message,
    expected_message_type: 'node_heartbeat'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }
  const record = envelope_result.value.record;
  const node_id_result = GetRequiredString({ record, field_name: 'node_id' });
  if (!node_id_result.ok) {
    return node_id_result;
  }
  const health_state_result = GetRequiredString({
    record,
    field_name: 'health_state'
  });
  if (!health_state_result.ok) {
    return health_state_result;
  }
  const metrics_result = GetRequiredObject({ record, field_name: 'metrics' });
  if (!metrics_result.ok) {
    return metrics_result;
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'node_heartbeat',
    node_id: node_id_result.value,
    health_state: health_state_result.value as node_heartbeat_message_i['health_state'],
    metrics: metrics_result.value as node_heartbeat_message_i['metrics']
  });
}

export function ParseNodeCapabilityAnnounceMessage(params: {
  message: unknown;
}): cluster_protocol_validation_result_t<node_capability_announce_message_i> {
  const envelope_result = ParseEnvelopeOnlyMessage({
    message: params.message,
    expected_message_type: 'node_capability_announce'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }
  const record = envelope_result.value.record;
  const node_id_result = GetRequiredString({ record, field_name: 'node_id' });
  if (!node_id_result.ok) {
    return node_id_result;
  }
  const capabilities_value = record.capabilities;
  if (!Array.isArray(capabilities_value)) {
    return ErrorResult({
      message: 'Field "capabilities" must be an array.',
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name: 'capabilities'
      }
    });
  }

  return SuccessResult({
    ...envelope_result.value.envelope,
    message_type: 'node_capability_announce',
    node_id: node_id_result.value,
    capabilities:
      capabilities_value as node_capability_announce_message_i['capabilities']
  });
}

export function ParseClusterProtocolMessage(params: {
  message: unknown;
  now_unix_ms?: number;
}): cluster_protocol_validation_result_t<cluster_protocol_message_t> {
  const envelope_result = ValidateEnvelope({
    message: params.message
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }

  const { message_type } = envelope_result.value.envelope;

  if (message_type === 'cluster_call_request') {
    return ParseClusterCallRequestMessage({
      message: params.message,
      now_unix_ms: params.now_unix_ms
    });
  }
  if (message_type === 'cluster_call_ack') {
    return ParseClusterCallAckMessage({
      message: params.message
    });
  }
  if (message_type === 'cluster_call_response_success') {
    return ParseClusterCallResponseSuccessMessage({
      message: params.message
    });
  }
  if (message_type === 'cluster_call_response_error') {
    return ParseClusterCallResponseErrorMessage({
      message: params.message
    });
  }
  if (message_type === 'cluster_call_cancel') {
    return ParseClusterCallCancelMessage({
      message: params.message
    });
  }
  if (message_type === 'cluster_call_cancel_ack') {
    return ParseClusterCallCancelAckMessage({
      message: params.message
    });
  }
  if (message_type === 'node_heartbeat') {
    return ParseNodeHeartbeatMessage({
      message: params.message
    });
  }
  if (message_type === 'node_capability_announce') {
    return ParseNodeCapabilityAnnounceMessage({
      message: params.message
    });
  }
  if (message_type === 'cluster_admin_mutation_request') {
    return ParseClusterAdminMutationRequestMessage({
      message: params.message,
      now_unix_ms: params.now_unix_ms
    });
  }
  if (message_type === 'cluster_admin_mutation_ack') {
    return ParseClusterAdminMutationAckMessage({
      message: params.message
    });
  }
  if (message_type === 'cluster_admin_mutation_result') {
    return ParseClusterAdminMutationResultMessage({
      message: params.message
    });
  }
  return ParseClusterAdminMutationErrorMessage({
    message: params.message
  });
}
