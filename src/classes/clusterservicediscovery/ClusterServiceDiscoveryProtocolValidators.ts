import type {
  cluster_service_discovery_event_name_t,
  cluster_service_discovery_node_status_t
} from './ClusterServiceDiscoveryStore.class';
import type {
  cluster_service_discovery_consensus_append_entries_request_message_i,
  cluster_service_discovery_consensus_log_entry_t,
  cluster_service_discovery_consensus_request_vote_request_message_i,
  cluster_service_discovery_consistency_mode_t,
  cluster_service_discovery_protocol_error_code_t,
  cluster_service_discovery_protocol_error_t,
  cluster_service_discovery_protocol_request_message_type_t,
  cluster_service_discovery_request_message_t,
  cluster_service_discovery_response_error_message_i,
  cluster_service_discovery_response_success_message_i,
  cluster_service_discovery_validation_result_t,
  cluster_service_discovery_write_request_message_t
} from './ClusterServiceDiscoveryProtocol';

const supported_protocol_version = 1;

const request_message_type_list: cluster_service_discovery_protocol_request_message_type_t[] = [
  'cluster_service_discovery_register_node',
  'cluster_service_discovery_heartbeat_node',
  'cluster_service_discovery_update_capability',
  'cluster_service_discovery_remove_node',
  'cluster_service_discovery_list_nodes',
  'cluster_service_discovery_query_nodes',
  'cluster_service_discovery_get_metrics',
  'cluster_service_discovery_get_events',
  'cluster_service_discovery_expire_stale_nodes',
  'cluster_service_discovery_consensus_request_vote',
  'cluster_service_discovery_consensus_append_entries',
  'cluster_service_discovery_get_ha_status'
];

const write_message_type_set = new Set<cluster_service_discovery_protocol_request_message_type_t>([
  'cluster_service_discovery_register_node',
  'cluster_service_discovery_heartbeat_node',
  'cluster_service_discovery_update_capability',
  'cluster_service_discovery_remove_node',
  'cluster_service_discovery_expire_stale_nodes'
]);

const node_status_list: cluster_service_discovery_node_status_t[] = [
  'ready',
  'degraded',
  'restarting',
  'stopped',
  'expired'
];

const discovery_event_name_list: cluster_service_discovery_event_name_t[] = [
  'node_registered',
  'node_heartbeat_updated',
  'node_capability_updated',
  'node_expired',
  'node_removed'
];

const consistency_mode_list: cluster_service_discovery_consistency_mode_t[] = [
  'leader_linearizable',
  'stale_ok'
];

type record_t = Record<string, unknown>;

function ErrorResult<value_t>(params: {
  message: string;
  details: Record<string, unknown>;
  code?: cluster_service_discovery_protocol_error_code_t;
  retryable?: boolean;
}): cluster_service_discovery_validation_result_t<value_t> {
  return {
    ok: false,
    error: {
      code: params.code ?? 'DISCOVERY_VALIDATION_FAILED',
      message: params.message,
      retryable: params.retryable ?? false,
      details: params.details
    }
  };
}

function SuccessResult<value_t>(value: value_t): cluster_service_discovery_validation_result_t<value_t> {
  return {
    ok: true,
    value
  };
}

function AsRecord(params: { value: unknown; path: string }): cluster_service_discovery_validation_result_t<record_t> {
  if (!params.value || typeof params.value !== 'object' || Array.isArray(params.value)) {
    return ErrorResult({
      message: `Expected ${params.path} to be an object.`,
      details: {
        reason: 'invalid_object',
        path: params.path
      }
    });
  }

  return SuccessResult(params.value as record_t);
}

function GetRequiredString(params: {
  record: record_t;
  field_name: string;
}): cluster_service_discovery_validation_result_t<string> {
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
}): cluster_service_discovery_validation_result_t<string | undefined> {
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
}): cluster_service_discovery_validation_result_t<number> {
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
}): cluster_service_discovery_validation_result_t<number | undefined> {
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
}): cluster_service_discovery_validation_result_t<boolean | undefined> {
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

function GetOptionalConsistencyMode(params: {
  record: record_t;
}): cluster_service_discovery_validation_result_t<
  cluster_service_discovery_consistency_mode_t | undefined
> {
  const consistency_mode_result = GetOptionalString({
    record: params.record,
    field_name: 'consistency_mode'
  });
  if (!consistency_mode_result.ok) {
    return consistency_mode_result;
  }

  if (typeof consistency_mode_result.value === 'undefined') {
    return SuccessResult(undefined);
  }

  if (!consistency_mode_list.includes(consistency_mode_result.value as never)) {
    return ErrorResult({
      message: 'Field "consistency_mode" must be leader_linearizable|stale_ok when provided.',
      details: {
        reason: 'invalid_enum',
        field_name: 'consistency_mode',
        value: consistency_mode_result.value
      }
    });
  }

  return SuccessResult(consistency_mode_result.value as cluster_service_discovery_consistency_mode_t);
}

function ValidateEnvelope(params: {
  message: unknown;
}): cluster_service_discovery_validation_result_t<{
  record: record_t;
  request_id: string;
  trace_id?: string;
  message_type: cluster_service_discovery_protocol_request_message_type_t;
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
    message_type: message_type_result.value as cluster_service_discovery_protocol_request_message_type_t
  });
}

function ValidateNodeStatus(params: {
  value: unknown;
  field_name: string;
  allow_expired: boolean;
}): cluster_service_discovery_validation_result_t<cluster_service_discovery_node_status_t> {
  if (typeof params.value !== 'string' || !node_status_list.includes(params.value as never)) {
    return ErrorResult({
      message: `Field "${params.field_name}" has invalid node status.`,
      details: {
        reason: 'invalid_enum',
        field_name: params.field_name,
        value: params.value
      }
    });
  }

  if (!params.allow_expired && params.value === 'expired') {
    return ErrorResult({
      message: `Field "${params.field_name}" cannot be "expired" for this operation.`,
      details: {
        reason: 'invalid_enum',
        field_name: params.field_name,
        value: params.value
      }
    });
  }

  return SuccessResult(params.value as cluster_service_discovery_node_status_t);
}

function ValidateOptionalStatusList(params: {
  record: record_t;
}): cluster_service_discovery_validation_result_t<cluster_service_discovery_node_status_t[] | undefined> {
  const value = params.record.status_list;
  if (typeof value === 'undefined') {
    return SuccessResult(undefined);
  }

  if (!Array.isArray(value)) {
    return ErrorResult({
      message: 'Field "status_list" must be an array when provided.',
      details: {
        reason: 'invalid_optional_field',
        field_name: 'status_list'
      }
    });
  }

  const status_list: cluster_service_discovery_node_status_t[] = [];
  for (const status_value of value) {
    if (typeof status_value !== 'string' || !node_status_list.includes(status_value as never)) {
      return ErrorResult({
        message: 'Field "status_list" contains an invalid status value.',
        details: {
          reason: 'invalid_enum',
          field_name: 'status_list',
          value: status_value
        }
      });
    }

    status_list.push(status_value as cluster_service_discovery_node_status_t);
  }

  return SuccessResult(status_list);
}

function ParseConsensusLogEntry(params: {
  entry: unknown;
}): cluster_service_discovery_validation_result_t<cluster_service_discovery_consensus_log_entry_t> {
  const entry_record_result = AsRecord({
    value: params.entry,
    path: 'entry'
  });
  if (!entry_record_result.ok) {
    return entry_record_result;
  }

  const entry_record = entry_record_result.value;

  const index_result = GetRequiredInteger({
    record: entry_record,
    field_name: 'index',
    min_value: 1
  });
  if (!index_result.ok) {
    return index_result;
  }

  const term_result = GetRequiredInteger({
    record: entry_record,
    field_name: 'term',
    min_value: 0
  });
  if (!term_result.ok) {
    return term_result;
  }

  const request_id_result = GetRequiredString({
    record: entry_record,
    field_name: 'request_id'
  });
  if (!request_id_result.ok) {
    return request_id_result;
  }

  const request_message_result = ParseClusterServiceDiscoveryRequestMessage({
    message: entry_record.request_message
  });
  if (!request_message_result.ok) {
    return request_message_result as cluster_service_discovery_validation_result_t<cluster_service_discovery_consensus_log_entry_t>;
  }

  if (!write_message_type_set.has(request_message_result.value.message_type)) {
    return ErrorResult({
      message: 'Consensus log entries only support discovery write operations.',
      details: {
        reason: 'invalid_consensus_entry_type',
        message_type: request_message_result.value.message_type
      }
    });
  }

  return SuccessResult({
    index: index_result.value,
    term: term_result.value,
    request_id: request_id_result.value,
    request_message: request_message_result.value as cluster_service_discovery_write_request_message_t
  });
}

export function ParseClusterServiceDiscoveryRequestMessage(params: {
  message: unknown;
}): cluster_service_discovery_validation_result_t<cluster_service_discovery_request_message_t> {
  const envelope_result = ValidateEnvelope({
    message: params.message
  });
  if (!envelope_result.ok) {
    return envelope_result;
  }

  const { record, message_type, request_id, trace_id } = envelope_result.value;

  if (message_type === 'cluster_service_discovery_register_node') {
    const node_identity_result = AsRecord({
      value: record.node_identity,
      path: 'node_identity'
    });
    if (!node_identity_result.ok) {
      return node_identity_result;
    }

    const status_result = ValidateNodeStatus({
      value: record.status,
      field_name: 'status',
      allow_expired: false
    });
    if (!status_result.ok) {
      return status_result;
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
      node_identity: node_identity_result.value as any,
      status: status_result.value as Exclude<cluster_service_discovery_node_status_t, 'expired'>,
      metrics: (record.metrics as any) ?? undefined,
      capability_list: (record.capability_list as any) ?? undefined,
      lease_ttl_ms: lease_ttl_result.value
    });
  }

  if (message_type === 'cluster_service_discovery_heartbeat_node') {
    const node_id_result = GetRequiredString({
      record,
      field_name: 'node_id'
    });
    if (!node_id_result.ok) {
      return node_id_result;
    }

    const status_result = ValidateNodeStatus({
      value: record.status,
      field_name: 'status',
      allow_expired: false
    });
    if (!status_result.ok) {
      return status_result;
    }

    const lease_ttl_result = GetOptionalInteger({
      record,
      field_name: 'lease_ttl_ms',
      min_value: 1
    });
    if (!lease_ttl_result.ok) {
      return lease_ttl_result;
    }

    const lease_revision_result = GetOptionalInteger({
      record,
      field_name: 'lease_revision',
      min_value: 0
    });
    if (!lease_revision_result.ok) {
      return lease_revision_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      node_id: node_id_result.value,
      status: status_result.value as Exclude<cluster_service_discovery_node_status_t, 'expired'>,
      metrics: (record.metrics as any) ?? {},
      lease_ttl_ms: lease_ttl_result.value,
      lease_revision: lease_revision_result.value
    });
  }

  if (message_type === 'cluster_service_discovery_update_capability') {
    const node_id_result = GetRequiredString({
      record,
      field_name: 'node_id'
    });
    if (!node_id_result.ok) {
      return node_id_result;
    }

    if (!Array.isArray(record.capability_list)) {
      return ErrorResult({
        message: 'Field "capability_list" must be an array.',
        details: {
          reason: 'missing_or_invalid_required_field',
          field_name: 'capability_list'
        }
      });
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      node_id: node_id_result.value,
      capability_list: record.capability_list as any
    });
  }

  if (message_type === 'cluster_service_discovery_remove_node') {
    const node_id_result = GetRequiredString({
      record,
      field_name: 'node_id'
    });
    if (!node_id_result.ok) {
      return node_id_result;
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
      node_id: node_id_result.value,
      reason: reason_result.value
    });
  }

  if (message_type === 'cluster_service_discovery_list_nodes') {
    const include_expired_result = GetOptionalBoolean({
      record,
      field_name: 'include_expired'
    });
    if (!include_expired_result.ok) {
      return include_expired_result;
    }

    const consistency_mode_result = GetOptionalConsistencyMode({
      record
    });
    if (!consistency_mode_result.ok) {
      return consistency_mode_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      include_expired: include_expired_result.value,
      consistency_mode: consistency_mode_result.value
    });
  }

  if (message_type === 'cluster_service_discovery_query_nodes') {
    const include_expired_result = GetOptionalBoolean({
      record,
      field_name: 'include_expired'
    });
    if (!include_expired_result.ok) {
      return include_expired_result;
    }

    const status_list_result = ValidateOptionalStatusList({
      record
    });
    if (!status_list_result.ok) {
      return status_list_result;
    }

    const zone_result = GetOptionalString({
      record,
      field_name: 'zone'
    });
    if (!zone_result.ok) {
      return zone_result;
    }

    const capability_name_result = GetOptionalString({
      record,
      field_name: 'capability_function_name'
    });
    if (!capability_name_result.ok) {
      return capability_name_result;
    }

    const capability_hash_result = GetOptionalString({
      record,
      field_name: 'capability_function_hash_sha1'
    });
    if (!capability_hash_result.ok) {
      return capability_hash_result;
    }

    const consistency_mode_result = GetOptionalConsistencyMode({
      record
    });
    if (!consistency_mode_result.ok) {
      return consistency_mode_result;
    }

    if (typeof record.label_match !== 'undefined') {
      const label_match_result = AsRecord({
        value: record.label_match,
        path: 'label_match'
      });
      if (!label_match_result.ok) {
        return label_match_result;
      }

      for (const [label_key, label_value] of Object.entries(label_match_result.value)) {
        if (typeof label_key !== 'string' || typeof label_value !== 'string') {
          return ErrorResult({
            message: 'Field "label_match" must only contain string key/value pairs.',
            details: {
              reason: 'invalid_optional_field',
              field_name: 'label_match'
            }
          });
        }
      }
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      include_expired: include_expired_result.value,
      status_list: status_list_result.value,
      label_match: (record.label_match as Record<string, string> | undefined) ?? undefined,
      zone: zone_result.value,
      capability_function_name: capability_name_result.value,
      capability_function_hash_sha1: capability_hash_result.value,
      consistency_mode: consistency_mode_result.value
    });
  }

  if (message_type === 'cluster_service_discovery_get_metrics') {
    const consistency_mode_result = GetOptionalConsistencyMode({
      record
    });
    if (!consistency_mode_result.ok) {
      return consistency_mode_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      consistency_mode: consistency_mode_result.value
    });
  }

  if (message_type === 'cluster_service_discovery_get_events') {
    const limit_result = GetOptionalInteger({
      record,
      field_name: 'limit',
      min_value: 0
    });
    if (!limit_result.ok) {
      return limit_result;
    }

    const node_id_result = GetOptionalString({
      record,
      field_name: 'node_id'
    });
    if (!node_id_result.ok) {
      return node_id_result;
    }

    const event_name_result = GetOptionalString({
      record,
      field_name: 'event_name'
    });
    if (!event_name_result.ok) {
      return event_name_result;
    }

    if (
      typeof event_name_result.value === 'string' &&
      !discovery_event_name_list.includes(event_name_result.value as never)
    ) {
      return ErrorResult({
        message: 'Field "event_name" must be a supported discovery event when provided.',
        details: {
          reason: 'invalid_enum',
          field_name: 'event_name',
          value: event_name_result.value
        }
      });
    }

    const consistency_mode_result = GetOptionalConsistencyMode({
      record
    });
    if (!consistency_mode_result.ok) {
      return consistency_mode_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      limit: limit_result.value,
      node_id: node_id_result.value,
      event_name: event_name_result.value as cluster_service_discovery_event_name_t | undefined,
      consistency_mode: consistency_mode_result.value
    });
  }

  if (message_type === 'cluster_service_discovery_expire_stale_nodes') {
    const now_unix_ms_result = GetOptionalInteger({
      record,
      field_name: 'now_unix_ms',
      min_value: 0
    });
    if (!now_unix_ms_result.ok) {
      return now_unix_ms_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      now_unix_ms: now_unix_ms_result.value
    });
  }

  if (message_type === 'cluster_service_discovery_consensus_request_vote') {
    const candidate_id_result = GetRequiredString({
      record,
      field_name: 'candidate_id'
    });
    if (!candidate_id_result.ok) {
      return candidate_id_result;
    }

    const term_result = GetRequiredInteger({
      record,
      field_name: 'term',
      min_value: 0
    });
    if (!term_result.ok) {
      return term_result;
    }

    const last_log_index_result = GetRequiredInteger({
      record,
      field_name: 'last_log_index',
      min_value: 0
    });
    if (!last_log_index_result.ok) {
      return last_log_index_result;
    }

    const last_log_term_result = GetRequiredInteger({
      record,
      field_name: 'last_log_term',
      min_value: 0
    });
    if (!last_log_term_result.ok) {
      return last_log_term_result;
    }

    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id,
      candidate_id: candidate_id_result.value,
      term: term_result.value,
      last_log_index: last_log_index_result.value,
      last_log_term: last_log_term_result.value
    } as cluster_service_discovery_consensus_request_vote_request_message_i);
  }

  if (message_type === 'cluster_service_discovery_get_ha_status') {
    return SuccessResult({
      protocol_version: 1,
      message_type,
      timestamp_unix_ms: record.timestamp_unix_ms as number,
      request_id,
      trace_id
    });
  }

  const leader_id_result = GetRequiredString({
    record,
    field_name: 'leader_id'
  });
  if (!leader_id_result.ok) {
    return leader_id_result;
  }

  const term_result = GetRequiredInteger({
    record,
    field_name: 'term',
    min_value: 0
  });
  if (!term_result.ok) {
    return term_result;
  }

  const prev_log_index_result = GetRequiredInteger({
    record,
    field_name: 'prev_log_index',
    min_value: 0
  });
  if (!prev_log_index_result.ok) {
    return prev_log_index_result;
  }

  const prev_log_term_result = GetRequiredInteger({
    record,
    field_name: 'prev_log_term',
    min_value: 0
  });
  if (!prev_log_term_result.ok) {
    return prev_log_term_result;
  }

  const leader_commit_index_result = GetRequiredInteger({
    record,
    field_name: 'leader_commit_index',
    min_value: 0
  });
  if (!leader_commit_index_result.ok) {
    return leader_commit_index_result;
  }

  if (!Array.isArray(record.entry_list)) {
    return ErrorResult({
      message: 'Field "entry_list" must be an array.',
      details: {
        reason: 'missing_or_invalid_required_field',
        field_name: 'entry_list'
      }
    });
  }

  const entry_list: cluster_service_discovery_consensus_log_entry_t[] = [];
  for (const entry of record.entry_list) {
    const entry_result = ParseConsensusLogEntry({
      entry
    });

    if (!entry_result.ok) {
      return entry_result as cluster_service_discovery_validation_result_t<cluster_service_discovery_request_message_t>;
    }

    entry_list.push(entry_result.value);
  }

  return SuccessResult({
    protocol_version: 1,
    message_type,
    timestamp_unix_ms: record.timestamp_unix_ms as number,
    request_id,
    trace_id,
    leader_id: leader_id_result.value,
    term: term_result.value,
    prev_log_index: prev_log_index_result.value,
    prev_log_term: prev_log_term_result.value,
    entry_list,
    leader_commit_index: leader_commit_index_result.value
  } as cluster_service_discovery_consensus_append_entries_request_message_i);
}

export function ParseClusterServiceDiscoveryResponseSuccessMessage(params: {
  message: unknown;
}): cluster_service_discovery_validation_result_t<cluster_service_discovery_response_success_message_i> {
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

  if (message_type_result.value !== 'cluster_service_discovery_response_success') {
    return ErrorResult({
      message: `Expected discovery success response but received "${message_type_result.value}".`,
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
    message_type: 'cluster_service_discovery_response_success',
    timestamp_unix_ms: (record.timestamp_unix_ms as number) ?? Date.now(),
    request_id: request_id_result.value,
    trace_id: (record.trace_id as string | undefined) ?? undefined,
    operation_message_type:
      operation_result.value as cluster_service_discovery_protocol_request_message_type_t,
    data: data_result.value as any,
    ha_metadata: (record.ha_metadata as any) ?? undefined
  });
}

export function ParseClusterServiceDiscoveryResponseErrorMessage(params: {
  message: unknown;
}): cluster_service_discovery_validation_result_t<cluster_service_discovery_response_error_message_i> {
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

  if (message_type_result.value !== 'cluster_service_discovery_response_error') {
    return ErrorResult({
      message: `Expected discovery error response but received "${message_type_result.value}".`,
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

  const operation_value = record.operation_message_type;
  if (typeof operation_value !== 'undefined') {
    if (typeof operation_value !== 'string' || !request_message_type_list.includes(operation_value as never)) {
      return ErrorResult({
        message: 'Field "operation_message_type" is invalid when provided.',
        details: {
          reason: 'invalid_optional_field',
          field_name: 'operation_message_type'
        }
      });
    }
  }

  return SuccessResult({
    protocol_version: 1,
    message_type: 'cluster_service_discovery_response_error',
    timestamp_unix_ms: (record.timestamp_unix_ms as number) ?? Date.now(),
    request_id: request_id_result.value,
    trace_id: (record.trace_id as string | undefined) ?? undefined,
    operation_message_type:
      operation_value as cluster_service_discovery_protocol_request_message_type_t | undefined,
    error: {
      code: error_code_result.value as any,
      message: error_message_result.value,
      retryable: retryable_value,
      details: (error_record_result.value.details as Record<string, unknown> | undefined) ?? undefined
    },
    ha_metadata: (record.ha_metadata as any) ?? undefined
  });
}

export function BuildClusterServiceDiscoveryProtocolError(params: {
  code: cluster_service_discovery_protocol_error_code_t;
  message: string;
  retryable: boolean;
  details?: Record<string, unknown>;
}): cluster_service_discovery_protocol_error_t {
  return {
    code: params.code,
    message: params.message,
    retryable: params.retryable,
    details: params.details ? { ...params.details } : undefined
  };
}
