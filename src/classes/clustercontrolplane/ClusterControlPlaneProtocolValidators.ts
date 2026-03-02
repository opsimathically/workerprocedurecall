import type {
  cluster_control_plane_event_name_t,
  cluster_control_plane_gateway_status_t,
  cluster_control_plane_mutation_status_t,
  cluster_control_plane_protocol_error_code_t,
  cluster_control_plane_protocol_error_t,
  cluster_control_plane_protocol_request_message_type_t,
  cluster_control_plane_request_message_t,
  cluster_control_plane_response_error_message_i,
  cluster_control_plane_response_success_message_i,
  cluster_control_plane_validation_result_t
} from './ClusterControlPlaneProtocol';

const supported_protocol_version = 1;

const request_message_type_list: cluster_control_plane_protocol_request_message_type_t[] = [
  'cluster_control_plane_gateway_register',
  'cluster_control_plane_gateway_heartbeat',
  'cluster_control_plane_gateway_deregister',
  'cluster_control_plane_get_topology_snapshot',
  'cluster_control_plane_get_policy_snapshot',
  'cluster_control_plane_update_policy_snapshot',
  'cluster_control_plane_subscribe_updates',
  'cluster_control_plane_config_version_announce',
  'cluster_control_plane_config_version_ack',
  'cluster_control_plane_get_service_status',
  'cluster_control_plane_mutation_intent_update',
  'cluster_control_plane_get_mutation_status'
];

const gateway_status_list: cluster_control_plane_gateway_status_t[] = [
  'active',
  'degraded',
  'draining',
  'offline'
];

const mutation_status_list: cluster_control_plane_mutation_status_t[] = [
  'pending',
  'running',
  'completed',
  'failed',
  'cancelled'
];

const control_plane_event_name_list: cluster_control_plane_event_name_t[] = [
  'control_plane_gateway_registered',
  'control_plane_gateway_expired',
  'control_plane_policy_published',
  'control_plane_policy_applied',
  'control_plane_policy_apply_failed',
  'control_plane_sync_degraded',
  'control_plane_sync_restored'
];

type record_t = Record<string, unknown>;

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function ErrorResult<value_t>(params: {
  message: string;
  details: Record<string, unknown>;
  code?: cluster_control_plane_protocol_error_code_t;
  retryable?: boolean;
}): cluster_control_plane_validation_result_t<value_t> {
  return {
    ok: false,
    error: {
      code: params.code ?? 'CONTROL_PLANE_VALIDATION_FAILED',
      message: params.message,
      retryable: params.retryable ?? false,
      details: params.details
    }
  };
}

function SuccessResult<value_t>(value: value_t): cluster_control_plane_validation_result_t<value_t> {
  return {
    ok: true,
    value
  };
}

function AsRecord(params: {
  value: unknown;
  path: string;
}): cluster_control_plane_validation_result_t<record_t> {
  if (!IsRecordObject(params.value)) {
    return ErrorResult({
      message: `Expected ${params.path} to be an object.`,
      details: {
        reason: 'invalid_object',
        path: params.path
      }
    });
  }

  return SuccessResult(params.value);
}

function GetRequiredString(params: {
  record: record_t;
  field_name: string;
}): cluster_control_plane_validation_result_t<string> {
  const value = params.record[params.field_name];
  if (typeof value !== 'string' || value.length === 0) {
    return ErrorResult({
      message: `Field "${params.field_name}" must be a non-empty string.`,
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name: params.field_name
      }
    });
  }

  return SuccessResult(value);
}

function GetOptionalString(params: {
  record: record_t;
  field_name: string;
}): cluster_control_plane_validation_result_t<string | undefined> {
  const value = params.record[params.field_name];
  if (typeof value === 'undefined') {
    return SuccessResult(undefined);
  }

  if (typeof value !== 'string') {
    return ErrorResult({
      message: `Field "${params.field_name}" must be a string when provided.`,
      details: {
        reason: 'invalid_optional_field',
        field_name: params.field_name
      }
    });
  }

  return SuccessResult(value);
}

function GetRequiredInteger(params: {
  record: record_t;
  field_name: string;
  min_value?: number;
}): cluster_control_plane_validation_result_t<number> {
  const value = params.record[params.field_name];
  if (!Number.isInteger(value)) {
    return ErrorResult({
      message: `Field "${params.field_name}" must be an integer.`,
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name: params.field_name
      }
    });
  }

  const integer_value = value as number;
  if (typeof params.min_value === 'number' && integer_value < params.min_value) {
    return ErrorResult({
      message: `Field "${params.field_name}" must be >= ${params.min_value}.`,
      details: {
        reason: 'invalid_range',
        field_name: params.field_name,
        min_value: params.min_value
      }
    });
  }

  return SuccessResult(integer_value);
}

function GetOptionalInteger(params: {
  record: record_t;
  field_name: string;
  min_value?: number;
}): cluster_control_plane_validation_result_t<number | undefined> {
  const value = params.record[params.field_name];
  if (typeof value === 'undefined') {
    return SuccessResult(undefined);
  }

  if (!Number.isInteger(value)) {
    return ErrorResult({
      message: `Field "${params.field_name}" must be an integer when provided.`,
      details: {
        reason: 'invalid_optional_field',
        field_name: params.field_name
      }
    });
  }

  const integer_value = value as number;
  if (typeof params.min_value === 'number' && integer_value < params.min_value) {
    return ErrorResult({
      message: `Field "${params.field_name}" must be >= ${params.min_value}.`,
      details: {
        reason: 'invalid_range',
        field_name: params.field_name,
        min_value: params.min_value
      }
    });
  }

  return SuccessResult(integer_value);
}

function GetOptionalBoolean(params: {
  record: record_t;
  field_name: string;
}): cluster_control_plane_validation_result_t<boolean | undefined> {
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

function ValidateEnvelope(params: {
  message: unknown;
}): cluster_control_plane_validation_result_t<{
  record: record_t;
  request_id: string;
  trace_id?: string;
  message_type: cluster_control_plane_protocol_request_message_type_t;
}> {
  const record_result = AsRecord({
    value: params.message,
    path: 'message'
  });
  if (!record_result.ok) {
    return record_result;
  }

  const record = record_result.value;

  const protocol_version_result = GetRequiredInteger({
    record,
    field_name: 'protocol_version',
    min_value: 1
  });
  if (!protocol_version_result.ok) {
    return protocol_version_result;
  }

  if (protocol_version_result.value !== supported_protocol_version) {
    return ErrorResult({
      message: `Unsupported protocol_version "${protocol_version_result.value}".`,
      details: {
        reason: 'unsupported_protocol_version',
        supported_protocol_version,
        received_protocol_version: protocol_version_result.value
      }
    });
  }

  const message_type_result = GetRequiredString({
    record,
    field_name: 'message_type'
  });
  if (!message_type_result.ok) {
    return message_type_result;
  }

  if (!request_message_type_list.includes(message_type_result.value as never)) {
    return ErrorResult({
      message: `Unsupported message_type "${message_type_result.value}".`,
      details: {
        reason: 'unsupported_message_type',
        message_type: message_type_result.value
      }
    });
  }

  const timestamp_result = GetRequiredInteger({
    record,
    field_name: 'timestamp_unix_ms',
    min_value: 0
  });
  if (!timestamp_result.ok) {
    return timestamp_result;
  }

  const request_id_result = GetRequiredString({
    record,
    field_name: 'request_id'
  });
  if (!request_id_result.ok) {
    return request_id_result;
  }

  const trace_id_result = GetOptionalString({
    record,
    field_name: 'trace_id'
  });
  if (!trace_id_result.ok) {
    return trace_id_result;
  }

  return SuccessResult({
    record,
    request_id: request_id_result.value,
    trace_id: trace_id_result.value,
    message_type: message_type_result.value as cluster_control_plane_protocol_request_message_type_t
  });
}

export function ParseClusterControlPlaneRequestMessage(params: {
  message: unknown;
}): cluster_control_plane_validation_result_t<cluster_control_plane_request_message_t> {
  const envelope_result = ValidateEnvelope({
    message: params.message
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }

  const { record, request_id, trace_id, message_type } = envelope_result.value;

  if (message_type === 'cluster_control_plane_gateway_register') {
    const gateway_id_result = GetRequiredString({
      record,
      field_name: 'gateway_id'
    });
    if (!gateway_id_result.ok) {
      return gateway_id_result;
    }

    const node_reference_result = AsRecord({
      value: record.node_reference,
      path: 'node_reference'
    });
    if (!node_reference_result.ok) {
      return node_reference_result;
    }

    const address_result = AsRecord({
      value: record.address,
      path: 'address'
    });
    if (!address_result.ok) {
      return address_result;
    }

    const status_result = GetRequiredString({
      record,
      field_name: 'status'
    });
    if (!status_result.ok) {
      return status_result;
    }

    if (
      !gateway_status_list.includes(status_result.value as never) ||
      status_result.value === 'offline'
    ) {
      return ErrorResult({
        message: 'Field "status" must be active|degraded|draining for gateway register.',
        details: {
          reason: 'invalid_enum',
          field_name: 'status',
          value: status_result.value
        }
      });
    }

    const gateway_version_result = GetRequiredString({
      record,
      field_name: 'gateway_version'
    });
    if (!gateway_version_result.ok) {
      return gateway_version_result;
    }

    const lease_ttl_result = GetOptionalInteger({
      record,
      field_name: 'lease_ttl_ms',
      min_value: 1
    });
    if (!lease_ttl_result.ok) {
      return lease_ttl_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      gateway_id: gateway_id_result.value,
      node_reference: node_reference_result.value as any,
      address: address_result.value as any,
      status: status_result.value as 'active' | 'degraded' | 'draining',
      gateway_version: gateway_version_result.value,
      lease_ttl_ms: lease_ttl_result.value,
      metadata: IsRecordObject(record.metadata)
        ? (record.metadata as Record<string, unknown>)
        : undefined
    });
  }

  if (message_type === 'cluster_control_plane_gateway_heartbeat') {
    const gateway_id_result = GetRequiredString({
      record,
      field_name: 'gateway_id'
    });
    if (!gateway_id_result.ok) {
      return gateway_id_result;
    }

    const status_result = GetOptionalString({
      record,
      field_name: 'status'
    });
    if (!status_result.ok) {
      return status_result;
    }

    if (
      typeof status_result.value === 'string' &&
      !gateway_status_list.includes(status_result.value as never)
    ) {
      return ErrorResult({
        message: 'Field "status" has invalid gateway status value.',
        details: {
          reason: 'invalid_enum',
          field_name: 'status',
          value: status_result.value
        }
      });
    }

    const gateway_version_result = GetOptionalString({
      record,
      field_name: 'gateway_version'
    });
    if (!gateway_version_result.ok) {
      return gateway_version_result;
    }

    const lease_ttl_result = GetOptionalInteger({
      record,
      field_name: 'lease_ttl_ms',
      min_value: 1
    });
    if (!lease_ttl_result.ok) {
      return lease_ttl_result;
    }

    if (typeof record.node_reference !== 'undefined') {
      const node_reference_result = AsRecord({
        value: record.node_reference,
        path: 'node_reference'
      });
      if (!node_reference_result.ok) {
        return node_reference_result;
      }
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      gateway_id: gateway_id_result.value,
      node_reference: IsRecordObject(record.node_reference)
        ? (record.node_reference as any)
        : undefined,
      status: status_result.value as cluster_control_plane_gateway_status_t | undefined,
      gateway_version: gateway_version_result.value,
      lease_ttl_ms: lease_ttl_result.value,
      metadata: IsRecordObject(record.metadata)
        ? (record.metadata as Record<string, unknown>)
        : undefined
    });
  }

  if (message_type === 'cluster_control_plane_gateway_deregister') {
    const gateway_id_result = GetRequiredString({
      record,
      field_name: 'gateway_id'
    });
    if (!gateway_id_result.ok) {
      return gateway_id_result;
    }

    const reason_result = GetOptionalString({
      record,
      field_name: 'reason'
    });
    if (!reason_result.ok) {
      return reason_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      gateway_id: gateway_id_result.value,
      reason: reason_result.value
    });
  }

  if (message_type === 'cluster_control_plane_get_topology_snapshot') {
    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id
    });
  }

  if (message_type === 'cluster_control_plane_get_policy_snapshot') {
    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id
    });
  }

  if (message_type === 'cluster_control_plane_update_policy_snapshot') {
    const actor_subject_result = GetRequiredString({
      record,
      field_name: 'actor_subject'
    });
    if (!actor_subject_result.ok) {
      return actor_subject_result;
    }

    const expected_version_result = GetOptionalString({
      record,
      field_name: 'expected_active_policy_version_id'
    });
    if (!expected_version_result.ok) {
      return expected_version_result;
    }

    const activate_immediately_result = GetOptionalBoolean({
      record,
      field_name: 'activate_immediately'
    });
    if (!activate_immediately_result.ok) {
      return activate_immediately_result;
    }

    const requested_version_result = GetOptionalString({
      record,
      field_name: 'requested_version_id'
    });
    if (!requested_version_result.ok) {
      return requested_version_result;
    }

    const policy_snapshot_result = AsRecord({
      value: record.policy_snapshot,
      path: 'policy_snapshot'
    });
    if (!policy_snapshot_result.ok) {
      return policy_snapshot_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      actor_subject: actor_subject_result.value,
      expected_active_policy_version_id: expected_version_result.value,
      activate_immediately: activate_immediately_result.value,
      requested_version_id: requested_version_result.value,
      policy_snapshot: policy_snapshot_result.value as any
    });
  }

  if (message_type === 'cluster_control_plane_subscribe_updates') {
    const gateway_id_result = GetOptionalString({
      record,
      field_name: 'gateway_id'
    });
    if (!gateway_id_result.ok) {
      return gateway_id_result;
    }

    const known_policy_result = GetOptionalString({
      record,
      field_name: 'known_policy_version_id'
    });
    if (!known_policy_result.ok) {
      return known_policy_result;
    }

    const known_generation_result = GetOptionalInteger({
      record,
      field_name: 'known_service_generation',
      min_value: 0
    });
    if (!known_generation_result.ok) {
      return known_generation_result;
    }

    const include_topology_result = GetOptionalBoolean({
      record,
      field_name: 'include_topology'
    });
    if (!include_topology_result.ok) {
      return include_topology_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      gateway_id: gateway_id_result.value,
      known_policy_version_id: known_policy_result.value,
      known_service_generation: known_generation_result.value,
      include_topology: include_topology_result.value
    });
  }

  if (message_type === 'cluster_control_plane_config_version_announce') {
    const gateway_id_result = GetRequiredString({
      record,
      field_name: 'gateway_id'
    });
    if (!gateway_id_result.ok) {
      return gateway_id_result;
    }

    const version_id_result = GetRequiredString({
      record,
      field_name: 'policy_version_id'
    });
    if (!version_id_result.ok) {
      return version_id_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      gateway_id: gateway_id_result.value,
      policy_version_id: version_id_result.value
    });
  }

  if (message_type === 'cluster_control_plane_config_version_ack') {
    const gateway_id_result = GetRequiredString({
      record,
      field_name: 'gateway_id'
    });
    if (!gateway_id_result.ok) {
      return gateway_id_result;
    }

    const version_id_result = GetRequiredString({
      record,
      field_name: 'policy_version_id'
    });
    if (!version_id_result.ok) {
      return version_id_result;
    }

    const applied_result = GetOptionalBoolean({
      record,
      field_name: 'applied'
    });
    if (!applied_result.ok) {
      return applied_result;
    }

    if (typeof applied_result.value !== 'boolean') {
      return ErrorResult({
        message: 'Field "applied" is required for config_version_ack.',
        details: {
          reason: 'missing_or_invalid_required_field',
          field_name: 'applied'
        }
      });
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      gateway_id: gateway_id_result.value,
      policy_version_id: version_id_result.value,
      applied: applied_result.value,
      details: IsRecordObject(record.details)
        ? (record.details as Record<string, unknown>)
        : undefined
    });
  }

  if (message_type === 'cluster_control_plane_get_service_status') {
    const include_events_limit_result = GetOptionalInteger({
      record,
      field_name: 'include_events_limit',
      min_value: 0
    });
    if (!include_events_limit_result.ok) {
      return include_events_limit_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      include_events_limit: include_events_limit_result.value
    });
  }

  if (message_type === 'cluster_control_plane_mutation_intent_update') {
    const mutation_id_result = GetRequiredString({
      record,
      field_name: 'mutation_id'
    });
    if (!mutation_id_result.ok) {
      return mutation_id_result;
    }

    const status_result = GetRequiredString({
      record,
      field_name: 'status'
    });
    if (!status_result.ok) {
      return status_result;
    }

    if (!mutation_status_list.includes(status_result.value as never)) {
      return ErrorResult({
        message: 'Field "status" has invalid mutation status.',
        details: {
          reason: 'invalid_enum',
          field_name: 'status',
          value: status_result.value
        }
      });
    }

    const gateway_id_result = GetOptionalString({
      record,
      field_name: 'gateway_id'
    });
    if (!gateway_id_result.ok) {
      return gateway_id_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      mutation_id: mutation_id_result.value,
      status: status_result.value as cluster_control_plane_mutation_status_t,
      gateway_id: gateway_id_result.value,
      dispatch_intent: IsRecordObject(record.dispatch_intent)
        ? (record.dispatch_intent as Record<string, unknown>)
        : undefined,
      details: IsRecordObject(record.details)
        ? (record.details as Record<string, unknown>)
        : undefined
    });
  }

  const mutation_id_result = GetRequiredString({
    record,
    field_name: 'mutation_id'
  });
  if (!mutation_id_result.ok) {
    return mutation_id_result;
  }

  return SuccessResult({
    protocol_version: 1,
    message_type: 'cluster_control_plane_get_mutation_status',
    timestamp_unix_ms: record.timestamp_unix_ms as number,
    request_id,
    trace_id,
    mutation_id: mutation_id_result.value
  });
}

export function ParseClusterControlPlaneResponseSuccessMessage(params: {
  message: unknown;
}): cluster_control_plane_validation_result_t<cluster_control_plane_response_success_message_i> {
  const record_result = AsRecord({
    value: params.message,
    path: 'message'
  });
  if (!record_result.ok) {
    return record_result;
  }

  const record = record_result.value;

  const protocol_version_result = GetRequiredInteger({
    record,
    field_name: 'protocol_version',
    min_value: 1
  });
  if (!protocol_version_result.ok) {
    return protocol_version_result;
  }

  if (protocol_version_result.value !== supported_protocol_version) {
    return ErrorResult({
      message: `Unsupported protocol_version "${protocol_version_result.value}".`,
      details: {
        reason: 'unsupported_protocol_version',
        supported_protocol_version,
        received_protocol_version: protocol_version_result.value
      }
    });
  }

  const message_type_result = GetRequiredString({
    record,
    field_name: 'message_type'
  });
  if (!message_type_result.ok) {
    return message_type_result;
  }

  if (message_type_result.value !== 'cluster_control_plane_response_success') {
    return ErrorResult({
      message: `Expected control-plane success response but got "${message_type_result.value}".`,
      details: {
        reason: 'invalid_response_message_type',
        message_type: message_type_result.value
      }
    });
  }

  const request_id_result = GetRequiredString({
    record,
    field_name: 'request_id'
  });
  if (!request_id_result.ok) {
    return request_id_result;
  }

  const operation_result = GetRequiredString({
    record,
    field_name: 'operation_message_type'
  });
  if (!operation_result.ok) {
    return operation_result;
  }

  if (!request_message_type_list.includes(operation_result.value as never)) {
    return ErrorResult({
      message: `Unsupported operation_message_type "${operation_result.value}".`,
      details: {
        reason: 'unsupported_operation_message_type',
        operation_message_type: operation_result.value
      }
    });
  }

  const data_result = AsRecord({
    value: record.data,
    path: 'data'
  });
  if (!data_result.ok) {
    return data_result;
  }

  return SuccessResult({
    protocol_version: 1,
    message_type: 'cluster_control_plane_response_success',
    timestamp_unix_ms: (record.timestamp_unix_ms as number) ?? Date.now(),
    request_id: request_id_result.value,
    trace_id: (record.trace_id as string | undefined) ?? undefined,
    operation_message_type: operation_result.value as cluster_control_plane_protocol_request_message_type_t,
    data: data_result.value as any
  });
}

export function ParseClusterControlPlaneResponseErrorMessage(params: {
  message: unknown;
}): cluster_control_plane_validation_result_t<cluster_control_plane_response_error_message_i> {
  const record_result = AsRecord({
    value: params.message,
    path: 'message'
  });
  if (!record_result.ok) {
    return record_result;
  }

  const record = record_result.value;

  const protocol_version_result = GetRequiredInteger({
    record,
    field_name: 'protocol_version',
    min_value: 1
  });
  if (!protocol_version_result.ok) {
    return protocol_version_result;
  }

  if (protocol_version_result.value !== supported_protocol_version) {
    return ErrorResult({
      message: `Unsupported protocol_version "${protocol_version_result.value}".`,
      details: {
        reason: 'unsupported_protocol_version',
        supported_protocol_version,
        received_protocol_version: protocol_version_result.value
      }
    });
  }

  const message_type_result = GetRequiredString({
    record,
    field_name: 'message_type'
  });
  if (!message_type_result.ok) {
    return message_type_result;
  }

  if (message_type_result.value !== 'cluster_control_plane_response_error') {
    return ErrorResult({
      message: `Expected control-plane error response but got "${message_type_result.value}".`,
      details: {
        reason: 'invalid_response_message_type',
        message_type: message_type_result.value
      }
    });
  }

  const request_id_result = GetRequiredString({
    record,
    field_name: 'request_id'
  });
  if (!request_id_result.ok) {
    return request_id_result;
  }

  const error_record_result = AsRecord({
    value: record.error,
    path: 'error'
  });
  if (!error_record_result.ok) {
    return error_record_result;
  }

  const error_code_result = GetRequiredString({
    record: error_record_result.value,
    field_name: 'code'
  });
  if (!error_code_result.ok) {
    return error_code_result;
  }

  const error_message_result = GetRequiredString({
    record: error_record_result.value,
    field_name: 'message'
  });
  if (!error_message_result.ok) {
    return error_message_result;
  }

  const retryable_value = error_record_result.value.retryable;
  if (typeof retryable_value !== 'boolean') {
    return ErrorResult({
      message: 'Field "error.retryable" must be a boolean.',
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name: 'error.retryable'
      }
    });
  }

  const operation_result = GetOptionalString({
    record,
    field_name: 'operation_message_type'
  });
  if (!operation_result.ok) {
    return operation_result;
  }

  if (
    typeof operation_result.value === 'string' &&
    !request_message_type_list.includes(operation_result.value as never)
  ) {
    return ErrorResult({
      message: 'Field "operation_message_type" has unsupported value.',
      details: {
        reason: 'unsupported_operation_message_type',
        operation_message_type: operation_result.value
      }
    });
  }

  return SuccessResult({
    protocol_version: 1,
    message_type: 'cluster_control_plane_response_error',
    timestamp_unix_ms: (record.timestamp_unix_ms as number) ?? Date.now(),
    request_id: request_id_result.value,
    trace_id: (record.trace_id as string | undefined) ?? undefined,
    operation_message_type:
      operation_result.value as cluster_control_plane_protocol_request_message_type_t | undefined,
    error: {
      code: error_code_result.value as any,
      message: error_message_result.value,
      retryable: retryable_value,
      details: IsRecordObject(error_record_result.value.details)
        ? (error_record_result.value.details as Record<string, unknown>)
        : undefined
    }
  });
}

export function BuildClusterControlPlaneProtocolError(params: {
  code: cluster_control_plane_protocol_error_code_t;
  message: string;
  retryable: boolean;
  details?: Record<string, unknown>;
}): cluster_control_plane_protocol_error_t {
  return {
    code: params.code,
    message: params.message,
    retryable: params.retryable,
    details: params.details ? { ...params.details } : undefined
  };
}

export function IsSupportedControlPlaneEventName(params: {
  event_name: string;
}): boolean {
  return control_plane_event_name_list.includes(params.event_name as never);
}
