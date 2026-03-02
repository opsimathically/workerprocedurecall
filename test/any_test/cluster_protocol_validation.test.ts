import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ParseClusterAdminMutationRequestMessage,
  ParseClusterCallRequestMessage,
  ParseClusterProtocolMessage
} from '../../src/index';

function BuildValidAdminMutationRequest(params?: {
  deadline_unix_ms?: number;
}): Record<string, unknown> {
  return {
    protocol_version: 1,
    message_type: 'cluster_admin_mutation_request',
    timestamp_unix_ms: 1000,
    mutation_id: 'mut_001',
    request_id: 'req_001',
    trace_id: 'trace_001',
    deadline_unix_ms: params?.deadline_unix_ms ?? 50_000,
    target_scope: 'single_node',
    target_selector: {
      node_ids: ['node_1']
    },
    mutation_type: 'redefine_function',
    payload: {
      name: 'SomeFunction',
      worker_func_source: 'async function SomeFunction() { return "ok"; }'
    },
    dry_run: false,
    rollout_strategy: {
      mode: 'single_node',
      min_success_percent: 100
    },
    auth_context: {
      subject: 'admin@example.com',
      tenant_id: 'tenant_1',
      capability_claims: ['rpc.admin.mutate:function:redefine'],
      signed_claims: 'signed-token'
    }
  };
}

function BuildValidCallRequest(params?: {
  deadline_unix_ms?: number;
}): Record<string, unknown> {
  return {
    protocol_version: 1,
    message_type: 'cluster_call_request',
    timestamp_unix_ms: 1_000,
    request_id: 'req_call_1',
    trace_id: 'trace_call_1',
    span_id: 'span_call_1',
    attempt_index: 1,
    max_attempts: 2,
    deadline_unix_ms: params?.deadline_unix_ms ?? 50_000,
    function_name: 'SomeFunction',
    args: [],
    routing_hint: {
      mode: 'auto'
    },
    caller_identity: {
      subject: 'caller@example.com',
      tenant_id: 'tenant_1',
      scopes: ['rpc.call:function:SomeFunction'],
      signed_claims: 'signed-token'
    }
  };
}

test('cluster admin mutation request parses valid payload and ignores unknown optional fields', function () {
  const payload = BuildValidAdminMutationRequest({
    deadline_unix_ms: 120_000
  });
  payload.unrecognized_root_field = 'ignored';
  (payload.rollout_strategy as Record<string, unknown>).unknown_rollout_flag = true;

  const parse_result = ParseClusterAdminMutationRequestMessage({
    message: payload,
    now_unix_ms: 100_000
  });

  assert.equal(parse_result.ok, true);
  if (!parse_result.ok) {
    return;
  }

  assert.equal(parse_result.value.message_type, 'cluster_admin_mutation_request');
  assert.equal(parse_result.value.target_scope, 'single_node');
  assert.equal(parse_result.value.target_selector?.node_ids?.[0], 'node_1');
  assert.equal('unrecognized_root_field' in parse_result.value, false);
});

test('cluster admin mutation request rejects missing required fields deterministically', function () {
  const payload = BuildValidAdminMutationRequest({
    deadline_unix_ms: 120_000
  });
  delete payload.mutation_id;

  const parse_result = ParseClusterAdminMutationRequestMessage({
    message: payload,
    now_unix_ms: 100_000
  });

  assert.equal(parse_result.ok, false);
  if (parse_result.ok) {
    return;
  }

  assert.equal(parse_result.error.code, 'ADMIN_VALIDATION_FAILED');
  assert.equal(parse_result.error.details.field_name, 'mutation_id');
  assert.equal(
    parse_result.error.details.reason,
    'missing_or_invalid_required_field'
  );
});

test('cluster admin mutation request rejects bad enum values', function () {
  const payload = BuildValidAdminMutationRequest({
    deadline_unix_ms: 120_000
  });
  payload.target_scope = 'not_a_valid_scope';

  const parse_result = ParseClusterAdminMutationRequestMessage({
    message: payload,
    now_unix_ms: 100_000
  });

  assert.equal(parse_result.ok, false);
  if (parse_result.ok) {
    return;
  }

  assert.equal(parse_result.error.code, 'ADMIN_VALIDATION_FAILED');
  assert.equal(parse_result.error.details.field_name, 'target_scope');
  assert.equal(parse_result.error.details.reason, 'invalid_enum_value');
});

test('cluster admin mutation request rejects bad target_scope selector constraints', function () {
  const payload = BuildValidAdminMutationRequest({
    deadline_unix_ms: 120_000
  });
  payload.target_scope = 'single_node';
  payload.target_selector = {
    node_ids: ['node_1', 'node_2']
  };

  const parse_result = ParseClusterAdminMutationRequestMessage({
    message: payload,
    now_unix_ms: 100_000
  });

  assert.equal(parse_result.ok, false);
  if (parse_result.ok) {
    return;
  }

  assert.equal(parse_result.error.code, 'ADMIN_VALIDATION_FAILED');
  assert.equal(
    parse_result.error.details.reason,
    'invalid_target_selector_for_scope'
  );
});

test('cluster admin mutation request rejects expired deadline', function () {
  const payload = BuildValidAdminMutationRequest({
    deadline_unix_ms: 90_000
  });

  const parse_result = ParseClusterAdminMutationRequestMessage({
    message: payload,
    now_unix_ms: 100_000
  });

  assert.equal(parse_result.ok, false);
  if (parse_result.ok) {
    return;
  }

  assert.equal(parse_result.error.code, 'ADMIN_VALIDATION_FAILED');
  assert.equal(parse_result.error.details.reason, 'expired_deadline');
});

test('cluster admin mutation request rejects unsupported protocol major version', function () {
  const payload = BuildValidAdminMutationRequest({
    deadline_unix_ms: 120_000
  });
  payload.protocol_version = 2;

  const parse_result = ParseClusterAdminMutationRequestMessage({
    message: payload,
    now_unix_ms: 100_000
  });

  assert.equal(parse_result.ok, false);
  if (parse_result.ok) {
    return;
  }

  assert.equal(parse_result.error.code, 'ADMIN_VALIDATION_FAILED');
  assert.equal(parse_result.error.details.reason, 'unsupported_protocol_version');
  assert.equal(parse_result.error.details.supported_protocol_version, 1);
});

test('cluster call request rejects unsupported protocol version', function () {
  const payload = BuildValidCallRequest({
    deadline_unix_ms: 120_000
  });
  payload.protocol_version = 9;

  const parse_result = ParseClusterCallRequestMessage({
    message: payload,
    now_unix_ms: 100_000
  });

  assert.equal(parse_result.ok, false);
  if (parse_result.ok) {
    return;
  }

  assert.equal(parse_result.error.code, 'PROTOCOL_VALIDATION_FAILED');
  assert.equal(parse_result.error.details.reason, 'unsupported_protocol_version');
});

test('cluster call request fails on missing required fields', function () {
  const payload = BuildValidCallRequest({
    deadline_unix_ms: 120_000
  });
  delete payload.request_id;

  const parse_result = ParseClusterCallRequestMessage({
    message: payload,
    now_unix_ms: 100_000
  });

  assert.equal(parse_result.ok, false);
  if (parse_result.ok) {
    return;
  }

  assert.equal(parse_result.error.code, 'PROTOCOL_VALIDATION_FAILED');
  assert.equal(parse_result.error.details.field_name, 'request_id');
});

test('generic cluster protocol parser dispatches by message_type', function () {
  const payload = BuildValidAdminMutationRequest({
    deadline_unix_ms: 120_000
  });

  const parse_result = ParseClusterProtocolMessage({
    message: payload,
    now_unix_ms: 100_000
  });

  assert.equal(parse_result.ok, true);
  if (!parse_result.ok) {
    return;
  }

  assert.equal(parse_result.value.message_type, 'cluster_admin_mutation_request');
});
