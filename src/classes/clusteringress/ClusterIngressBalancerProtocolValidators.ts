import {
  ParseClusterCallAckMessage,
  ParseClusterCallRequestMessage,
  ParseClusterCallResponseErrorMessage,
  ParseClusterCallResponseSuccessMessage
} from '../clusterprotocol/ClusterProtocolValidators';
import type {
  cluster_call_ack_message_i,
  cluster_call_request_message_i,
  cluster_call_response_error_message_i,
  cluster_call_response_success_message_i
} from '../clusterprotocol/ClusterProtocolTypes';
import type {
  ingress_balancer_error_code_t,
  ingress_balancer_error_t,
  ingress_balancer_validation_result_t
} from './ClusterIngressBalancerProtocol';

function BuildErrorResult<value_t>(params: {
  code: ingress_balancer_error_code_t;
  message: string;
  retryable: boolean;
  unknown_outcome: boolean;
  details?: Record<string, unknown>;
}): ingress_balancer_validation_result_t<value_t> {
  return {
    ok: false,
    error: {
      code: params.code,
      message: params.message,
      retryable: params.retryable,
      unknown_outcome: params.unknown_outcome,
      details: params.details ? { ...params.details } : {}
    }
  };
}

function BuildSuccessResult<value_t>(
  value: value_t
): ingress_balancer_validation_result_t<value_t> {
  return {
    ok: true,
    value
  };
}

function IsRecordObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function BuildIngressBalancerError(params: {
  code: ingress_balancer_error_code_t;
  message: string;
  retryable: boolean;
  unknown_outcome: boolean;
  details?: Record<string, unknown>;
}): ingress_balancer_error_t {
  return {
    code: params.code,
    message: params.message,
    retryable: params.retryable,
    unknown_outcome: params.unknown_outcome,
    details: params.details ? { ...params.details } : {}
  };
}

export function ParseIngressCallRequestMessage(params: {
  message: unknown;
  now_unix_ms?: number;
}): ingress_balancer_validation_result_t<cluster_call_request_message_i> {
  const parse_result = ParseClusterCallRequestMessage({
    message: params.message
  });
  if (!parse_result.ok) {
    return BuildErrorResult({
      code: 'INGRESS_VALIDATION_FAILED',
      message: parse_result.error.message,
      retryable: false,
      unknown_outcome: false,
      details: {
        ...parse_result.error.details,
        protocol_error_code: parse_result.error.code
      }
    });
  }

  const call_request = parse_result.value;
  const now_unix_ms = params.now_unix_ms ?? Date.now();

  if (call_request.deadline_unix_ms <= now_unix_ms) {
    return BuildErrorResult({
      code: 'INGRESS_VALIDATION_FAILED',
      message: 'deadline_unix_ms must be in the future.',
      retryable: false,
      unknown_outcome: false,
      details: {
        request_id: call_request.request_id,
        deadline_unix_ms: call_request.deadline_unix_ms,
        now_unix_ms
      }
    });
  }

  return BuildSuccessResult(call_request);
}

export function ParseIngressForwardWireResponse(params: {
  response: unknown;
}): ingress_balancer_validation_result_t<{
  ack: cluster_call_ack_message_i;
  terminal_message:
    | cluster_call_response_success_message_i
    | cluster_call_response_error_message_i;
}> {
  if (!IsRecordObject(params.response)) {
    return BuildErrorResult({
      code: 'INGRESS_TARGET_UNAVAILABLE',
      message: 'Forwarded response payload is not an object.',
      retryable: true,
      unknown_outcome: true,
      details: {
        reason: 'invalid_response_shape'
      }
    });
  }

  const ack_result = ParseClusterCallAckMessage({
    message: params.response.ack
  });
  if (!ack_result.ok) {
    return BuildErrorResult({
      code: 'INGRESS_TARGET_UNAVAILABLE',
      message: `Invalid forwarded ACK payload: ${ack_result.error.message}`,
      retryable: true,
      unknown_outcome: true,
      details: {
        protocol_error_code: ack_result.error.code,
        protocol_error_details: ack_result.error.details
      }
    });
  }

  const success_result = ParseClusterCallResponseSuccessMessage({
    message: params.response.terminal_message
  });
  if (success_result.ok) {
    return BuildSuccessResult({
      ack: ack_result.value,
      terminal_message: success_result.value
    });
  }

  const error_result = ParseClusterCallResponseErrorMessage({
    message: params.response.terminal_message
  });
  if (!error_result.ok) {
    return BuildErrorResult({
      code: 'INGRESS_TARGET_UNAVAILABLE',
      message: `Invalid forwarded terminal payload: ${error_result.error.message}`,
      retryable: true,
      unknown_outcome: true,
      details: {
        protocol_error_code: error_result.error.code,
        protocol_error_details: error_result.error.details
      }
    });
  }

  return BuildSuccessResult({
    ack: ack_result.value,
    terminal_message: error_result.value
  });
}

