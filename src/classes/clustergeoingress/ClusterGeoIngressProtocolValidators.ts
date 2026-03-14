import type {
  cluster_geo_ingress_instance_health_status_t,
  cluster_geo_ingress_policy_mode_t,
  cluster_geo_ingress_protocol_error_code_t,
  cluster_geo_ingress_protocol_error_t,
  cluster_geo_ingress_protocol_request_message_type_t,
  cluster_geo_ingress_region_status_t,
  cluster_geo_ingress_request_message_t,
  cluster_geo_ingress_response_error_message_i,
  cluster_geo_ingress_response_success_message_i,
  cluster_geo_ingress_validation_result_t
} from './ClusterGeoIngressProtocol';

const supported_protocol_version = 1;

const request_message_type_list: cluster_geo_ingress_protocol_request_message_type_t[] = [
  'cluster_geo_ingress_ingress_register',
  'cluster_geo_ingress_ingress_heartbeat',
  'cluster_geo_ingress_ingress_deregister',
  'cluster_geo_ingress_publish_global_routing_policy',
  'cluster_geo_ingress_get_global_routing_policy',
  'cluster_geo_ingress_get_global_topology_snapshot',
  'cluster_geo_ingress_report_metrics_summary',
  'cluster_geo_ingress_subscribe_updates',
  'cluster_geo_ingress_get_service_status'
];

const region_status_list: cluster_geo_ingress_region_status_t[] = [
  'active',
  'degraded',
  'draining',
  'offline'
];

const ingress_health_status_list: cluster_geo_ingress_instance_health_status_t[] = [
  'ready',
  'overloaded',
  'degraded',
  'offline'
];

const policy_mode_list: cluster_geo_ingress_policy_mode_t[] = [
  'latency_aware',
  'capacity_aware',
  'affinity'
];

type record_t = Record<string, unknown>;

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function ErrorResult<value_t>(params: {
  message: string;
  details: Record<string, unknown>;
  code?: cluster_geo_ingress_protocol_error_code_t;
  retryable?: boolean;
}): cluster_geo_ingress_validation_result_t<value_t> {
  return {
    ok: false,
    error: {
      code: params.code ?? 'GEO_INGRESS_VALIDATION_FAILED',
      message: params.message,
      retryable: params.retryable ?? false,
      details: {
        ...params.details
      }
    }
  };
}

function SuccessResult<value_t>(value: value_t): cluster_geo_ingress_validation_result_t<value_t> {
  return {
    ok: true,
    value
  };
}

function AsRecord(params: {
  value: unknown;
  path: string;
}): cluster_geo_ingress_validation_result_t<record_t> {
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
}): cluster_geo_ingress_validation_result_t<string> {
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
}): cluster_geo_ingress_validation_result_t<string | undefined> {
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
}): cluster_geo_ingress_validation_result_t<number> {
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
}): cluster_geo_ingress_validation_result_t<number | undefined> {
  const value = params.record[params.field_name];
  if (typeof value === 'undefined') {
    return SuccessResult(undefined);
  }

  if (!Number.isFinite(value as number)) {
    return ErrorResult({
      message: `Field "${params.field_name}" must be a number when provided.`,
      details: {
        reason: 'invalid_optional_field',
        field_name: params.field_name
      }
    });
  }

  const number_value = value as number;
  if (typeof params.min_value === 'number' && number_value < params.min_value) {
    return ErrorResult({
      message: `Field "${params.field_name}" must be >= ${params.min_value}.`,
      details: {
        reason: 'invalid_range',
        field_name: params.field_name,
        min_value: params.min_value
      }
    });
  }

  return SuccessResult(number_value);
}

function GetOptionalBoolean(params: {
  record: record_t;
  field_name: string;
}): cluster_geo_ingress_validation_result_t<boolean | undefined> {
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
}): cluster_geo_ingress_validation_result_t<{
  record: record_t;
  request_id: string;
  trace_id?: string;
  message_type: cluster_geo_ingress_protocol_request_message_type_t;
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
    message_type: message_type_result.value as cluster_geo_ingress_protocol_request_message_type_t
  });
}

function ParseEndpoint(params: {
  value: unknown;
  path: string;
}): cluster_geo_ingress_validation_result_t<{
  host: string;
  port: number;
  request_path: string;
  tls_mode: 'required';
}> {
  const endpoint_result = AsRecord({
    value: params.value,
    path: params.path
  });
  if (!endpoint_result.ok) {
    return endpoint_result;
  }

  const endpoint = endpoint_result.value;
  const host_result = GetRequiredString({
    record: endpoint,
    field_name: 'host'
  });
  if (!host_result.ok) {
    return host_result;
  }

  const port_result = GetRequiredInteger({
    record: endpoint,
    field_name: 'port',
    min_value: 1
  });
  if (!port_result.ok) {
    return port_result;
  }

  const request_path_result = GetRequiredString({
    record: endpoint,
    field_name: 'request_path'
  });
  if (!request_path_result.ok) {
    return request_path_result;
  }

  const tls_mode_result = GetRequiredString({
    record: endpoint,
    field_name: 'tls_mode'
  });
  if (!tls_mode_result.ok) {
    return tls_mode_result;
  }

  if (tls_mode_result.value !== 'required') {
    return ErrorResult({
      message: 'endpoint.tls_mode must be required.',
      details: {
        reason: 'invalid_enum',
        field_name: 'tls_mode',
        value: tls_mode_result.value
      }
    });
  }

  return SuccessResult({
    host: host_result.value,
    port: port_result.value,
    request_path: request_path_result.value,
    tls_mode: tls_mode_result.value
  });
}

export function BuildClusterGeoIngressProtocolError(params: {
  code: cluster_geo_ingress_protocol_error_code_t;
  message: string;
  retryable: boolean;
  details?: Record<string, unknown>;
}): cluster_geo_ingress_protocol_error_t {
  return {
    code: params.code,
    message: params.message,
    retryable: params.retryable,
    details: params.details ? { ...params.details } : undefined
  };
}

export function ParseClusterGeoIngressRequestMessage(params: {
  message: unknown;
}): cluster_geo_ingress_validation_result_t<cluster_geo_ingress_request_message_t> {
  const envelope_result = ValidateEnvelope({
    message: params.message
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }

  const { record, request_id, trace_id, message_type } = envelope_result.value;

  if (message_type === 'cluster_geo_ingress_ingress_register') {
    const ingress_id_result = GetRequiredString({ record, field_name: 'ingress_id' });
    if (!ingress_id_result.ok) {
      return ingress_id_result;
    }

    const region_id_result = GetRequiredString({ record, field_name: 'region_id' });
    if (!region_id_result.ok) {
      return region_id_result;
    }

    const endpoint_result = ParseEndpoint({
      value: record.endpoint,
      path: 'endpoint'
    });
    if (!endpoint_result.ok) {
      return endpoint_result;
    }

    const health_status_result = GetRequiredString({
      record,
      field_name: 'health_status'
    });
    if (!health_status_result.ok) {
      return health_status_result;
    }

    if (
      !ingress_health_status_list.includes(health_status_result.value as never) ||
      health_status_result.value === 'offline'
    ) {
      return ErrorResult({
        message: 'health_status must be ready|overloaded|degraded for register.',
        details: {
          reason: 'invalid_enum',
          field_name: 'health_status',
          value: health_status_result.value
        }
      });
    }

    const ingress_version_result = GetRequiredString({
      record,
      field_name: 'ingress_version'
    });
    if (!ingress_version_result.ok) {
      return ingress_version_result;
    }

    const lease_ttl_ms_result = GetOptionalInteger({
      record,
      field_name: 'lease_ttl_ms',
      min_value: 1
    });
    if (!lease_ttl_ms_result.ok) {
      return lease_ttl_ms_result;
    }

    const policy_version_id_result = GetOptionalString({
      record,
      field_name: 'policy_version_id'
    });
    if (!policy_version_id_result.ok) {
      return policy_version_id_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      ingress_id: ingress_id_result.value,
      region_id: region_id_result.value,
      endpoint: endpoint_result.value,
      health_status: health_status_result.value as 'ready' | 'overloaded' | 'degraded',
      ingress_version: ingress_version_result.value,
      lease_ttl_ms: lease_ttl_ms_result.value,
      policy_version_id: policy_version_id_result.value,
      region_metadata: IsRecordObject(record.region_metadata)
        ? (record.region_metadata as Record<string, unknown> as {
            priority?: number;
            latency_slo_ms?: number;
            capacity_score?: number;
            latency_ewma_ms?: number;
            status?: cluster_geo_ingress_region_status_t;
          })
        : undefined,
      metrics_summary: IsRecordObject(record.metrics_summary)
        ? (record.metrics_summary as Record<string, unknown> as {
            inflight_calls?: number;
            pending_calls?: number;
            success_rate_1m?: number;
            timeout_rate_1m?: number;
            ewma_latency_ms?: number;
          })
        : undefined,
      metadata: IsRecordObject(record.metadata)
        ? (record.metadata as Record<string, unknown>)
        : undefined
    });
  }

  if (message_type === 'cluster_geo_ingress_ingress_heartbeat') {
    const ingress_id_result = GetRequiredString({ record, field_name: 'ingress_id' });
    if (!ingress_id_result.ok) {
      return ingress_id_result;
    }

    const region_id_result = GetOptionalString({ record, field_name: 'region_id' });
    if (!region_id_result.ok) {
      return region_id_result;
    }

    const health_status_result = GetOptionalString({
      record,
      field_name: 'health_status'
    });
    if (!health_status_result.ok) {
      return health_status_result;
    }

    if (
      typeof health_status_result.value === 'string' &&
      !ingress_health_status_list.includes(health_status_result.value as never)
    ) {
      return ErrorResult({
        message: 'health_status must be ready|overloaded|degraded|offline.',
        details: {
          reason: 'invalid_enum',
          field_name: 'health_status',
          value: health_status_result.value
        }
      });
    }

    const ingress_version_result = GetOptionalString({
      record,
      field_name: 'ingress_version'
    });
    if (!ingress_version_result.ok) {
      return ingress_version_result;
    }

    const lease_ttl_ms_result = GetOptionalInteger({
      record,
      field_name: 'lease_ttl_ms',
      min_value: 1
    });
    if (!lease_ttl_ms_result.ok) {
      return lease_ttl_ms_result;
    }

    const policy_version_id_result = GetOptionalString({
      record,
      field_name: 'policy_version_id'
    });
    if (!policy_version_id_result.ok) {
      return policy_version_id_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      ingress_id: ingress_id_result.value,
      region_id: region_id_result.value,
      health_status: health_status_result.value as
        | 'ready'
        | 'overloaded'
        | 'degraded'
        | 'offline'
        | undefined,
      ingress_version: ingress_version_result.value,
      lease_ttl_ms: lease_ttl_ms_result.value,
      policy_version_id: policy_version_id_result.value,
      region_metadata: IsRecordObject(record.region_metadata)
        ? (record.region_metadata as Record<string, unknown> as {
            priority?: number;
            latency_slo_ms?: number;
            capacity_score?: number;
            latency_ewma_ms?: number;
            status?: cluster_geo_ingress_region_status_t;
          })
        : undefined,
      metrics_summary: IsRecordObject(record.metrics_summary)
        ? (record.metrics_summary as Record<string, unknown> as {
            inflight_calls?: number;
            pending_calls?: number;
            success_rate_1m?: number;
            timeout_rate_1m?: number;
            ewma_latency_ms?: number;
          })
        : undefined,
      metadata: IsRecordObject(record.metadata)
        ? (record.metadata as Record<string, unknown>)
        : undefined
    });
  }

  if (message_type === 'cluster_geo_ingress_ingress_deregister') {
    const ingress_id_result = GetRequiredString({ record, field_name: 'ingress_id' });
    if (!ingress_id_result.ok) {
      return ingress_id_result;
    }

    const reason_result = GetOptionalString({ record, field_name: 'reason' });
    if (!reason_result.ok) {
      return reason_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      ingress_id: ingress_id_result.value,
      reason: reason_result.value
    });
  }

  if (message_type === 'cluster_geo_ingress_publish_global_routing_policy') {
    const actor_subject_result = GetRequiredString({
      record,
      field_name: 'actor_subject'
    });
    if (!actor_subject_result.ok) {
      return actor_subject_result;
    }

    const policy_snapshot_result = AsRecord({
      value: record.policy_snapshot,
      path: 'policy_snapshot'
    });
    if (!policy_snapshot_result.ok) {
      return policy_snapshot_result;
    }

    const default_mode_result = GetRequiredString({
      record: policy_snapshot_result.value,
      field_name: 'default_mode'
    });
    if (!default_mode_result.ok) {
      return default_mode_result;
    }

    if (!policy_mode_list.includes(default_mode_result.value as never)) {
      return ErrorResult({
        message: 'policy_snapshot.default_mode must be latency_aware|capacity_aware|affinity.',
        details: {
          reason: 'invalid_enum',
          field_name: 'policy_snapshot.default_mode',
          value: default_mode_result.value
        }
      });
    }

    const retry_within_region_first_result = GetOptionalBoolean({
      record: policy_snapshot_result.value,
      field_name: 'retry_within_region_first'
    });
    if (!retry_within_region_first_result.ok) {
      return retry_within_region_first_result;
    }

    const max_cross_region_attempts_result = GetOptionalInteger({
      record: policy_snapshot_result.value,
      field_name: 'max_cross_region_attempts',
      min_value: 0
    });
    if (!max_cross_region_attempts_result.ok) {
      return max_cross_region_attempts_result;
    }

    const stickiness_scope_result = GetOptionalString({
      record: policy_snapshot_result.value,
      field_name: 'stickiness_scope'
    });
    if (!stickiness_scope_result.ok) {
      return stickiness_scope_result;
    }

    if (
      typeof stickiness_scope_result.value === 'string' &&
      stickiness_scope_result.value !== 'none' &&
      stickiness_scope_result.value !== 'session' &&
      stickiness_scope_result.value !== 'tenant'
    ) {
      return ErrorResult({
        message: 'policy_snapshot.stickiness_scope must be none|session|tenant.',
        details: {
          reason: 'invalid_enum',
          field_name: 'policy_snapshot.stickiness_scope',
          value: stickiness_scope_result.value
        }
      });
    }

    const requested_version_id_result = GetOptionalString({
      record,
      field_name: 'requested_version_id'
    });
    if (!requested_version_id_result.ok) {
      return requested_version_id_result;
    }

    const expected_active_policy_version_id_result = GetOptionalString({
      record,
      field_name: 'expected_active_policy_version_id'
    });
    if (!expected_active_policy_version_id_result.ok) {
      return expected_active_policy_version_id_result;
    }

    const activate_immediately_result = GetOptionalBoolean({
      record,
      field_name: 'activate_immediately'
    });
    if (!activate_immediately_result.ok) {
      return activate_immediately_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      actor_subject: actor_subject_result.value,
      policy_snapshot: policy_snapshot_result.value as any,
      requested_version_id: requested_version_id_result.value,
      expected_active_policy_version_id: expected_active_policy_version_id_result.value,
      activate_immediately: activate_immediately_result.value
    });
  }

  if (message_type === 'cluster_geo_ingress_get_global_routing_policy') {
    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id
    });
  }

  if (message_type === 'cluster_geo_ingress_get_global_topology_snapshot') {
    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id
    });
  }

  if (message_type === 'cluster_geo_ingress_report_metrics_summary') {
    const ingress_id_result = GetRequiredString({
      record,
      field_name: 'ingress_id'
    });
    if (!ingress_id_result.ok) {
      return ingress_id_result;
    }

    const metrics_summary_result = AsRecord({
      value: record.metrics_summary,
      path: 'metrics_summary'
    });
    if (!metrics_summary_result.ok) {
      return metrics_summary_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      ingress_id: ingress_id_result.value,
      metrics_summary: metrics_summary_result.value as any
    });
  }

  if (message_type === 'cluster_geo_ingress_subscribe_updates') {
    const ingress_id_result = GetOptionalString({
      record,
      field_name: 'ingress_id'
    });
    if (!ingress_id_result.ok) {
      return ingress_id_result;
    }

    const known_service_generation_result = GetOptionalInteger({
      record,
      field_name: 'known_service_generation',
      min_value: 0
    });
    if (!known_service_generation_result.ok) {
      return known_service_generation_result;
    }

    const known_policy_version_id_result = GetOptionalString({
      record,
      field_name: 'known_policy_version_id'
    });
    if (!known_policy_version_id_result.ok) {
      return known_policy_version_id_result;
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
      ingress_id: ingress_id_result.value,
      known_service_generation: known_service_generation_result.value,
      known_policy_version_id: known_policy_version_id_result.value,
      include_topology: include_topology_result.value
    });
  }

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

function ValidateResponseEnvelope(params: {
  message: unknown;
  expected_message_type:
    | 'cluster_geo_ingress_response_success'
    | 'cluster_geo_ingress_response_error';
}): cluster_geo_ingress_validation_result_t<record_t> {
  const record_result = AsRecord({
    value: params.message,
    path: 'response_message'
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
      message: `Unsupported protocol_version "${protocol_version_result.value}" in response.`,
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

  if (message_type_result.value !== params.expected_message_type) {
    return ErrorResult({
      message: `Expected message_type "${params.expected_message_type}" but received "${message_type_result.value}".`,
      details: {
        reason: 'unexpected_message_type',
        expected_message_type: params.expected_message_type,
        received_message_type: message_type_result.value
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

  const timestamp_result = GetRequiredInteger({
    record,
    field_name: 'timestamp_unix_ms',
    min_value: 0
  });
  if (!timestamp_result.ok) {
    return timestamp_result;
  }

  return SuccessResult(record);
}

export function ParseClusterGeoIngressResponseSuccessMessage(params: {
  message: unknown;
}): cluster_geo_ingress_validation_result_t<cluster_geo_ingress_response_success_message_i> {
  const envelope_result = ValidateResponseEnvelope({
    message: params.message,
    expected_message_type: 'cluster_geo_ingress_response_success'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }

  const record = envelope_result.value;
  const operation_message_type_result = GetRequiredString({
    record,
    field_name: 'operation_message_type'
  });
  if (!operation_message_type_result.ok) {
    return operation_message_type_result;
  }

  if (!request_message_type_list.includes(operation_message_type_result.value as never)) {
    return ErrorResult({
      message: 'operation_message_type must be a supported geo ingress request type.',
      details: {
        reason: 'invalid_enum',
        field_name: 'operation_message_type',
        value: operation_message_type_result.value
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
    message_type: 'cluster_geo_ingress_response_success',
    timestamp_unix_ms: record.timestamp_unix_ms as number,
    request_id: record.request_id as string,
    trace_id: typeof record.trace_id === 'string' ? record.trace_id : undefined,
    operation_message_type:
      operation_message_type_result.value as cluster_geo_ingress_protocol_request_message_type_t,
    data: data_result.value as any
  });
}

export function ParseClusterGeoIngressResponseErrorMessage(params: {
  message: unknown;
}): cluster_geo_ingress_validation_result_t<cluster_geo_ingress_response_error_message_i> {
  const envelope_result = ValidateResponseEnvelope({
    message: params.message,
    expected_message_type: 'cluster_geo_ingress_response_error'
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }

  const record = envelope_result.value;

  const error_result = AsRecord({
    value: record.error,
    path: 'error'
  });
  if (!error_result.ok) {
    return error_result;
  }

  const error_record = error_result.value;

  const code_result = GetRequiredString({
    record: error_record,
    field_name: 'code'
  });
  if (!code_result.ok) {
    return code_result;
  }

  const message_result = GetRequiredString({
    record: error_record,
    field_name: 'message'
  });
  if (!message_result.ok) {
    return message_result;
  }

  const retryable = error_record.retryable;
  if (typeof retryable !== 'boolean') {
    return ErrorResult({
      message: 'error.retryable must be boolean.',
      details: {
        reason: 'invalid_optional_field',
        field_name: 'error.retryable'
      }
    });
  }

  const operation_message_type_result = GetOptionalString({
    record,
    field_name: 'operation_message_type'
  });
  if (!operation_message_type_result.ok) {
    return operation_message_type_result;
  }

  return SuccessResult({
    protocol_version: 1,
    message_type: 'cluster_geo_ingress_response_error',
    timestamp_unix_ms: record.timestamp_unix_ms as number,
    request_id: record.request_id as string,
    trace_id: typeof record.trace_id === 'string' ? record.trace_id : undefined,
    operation_message_type:
      operation_message_type_result.value as
        | cluster_geo_ingress_protocol_request_message_type_t
        | undefined,
    error: {
      code: code_result.value as cluster_geo_ingress_protocol_error_code_t,
      message: message_result.value,
      retryable,
      details: IsRecordObject(error_record.details)
        ? (error_record.details as Record<string, unknown>)
        : undefined
    }
  });
}
