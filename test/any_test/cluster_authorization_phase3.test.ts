import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ClusterAuthorizationPolicyEngine,
  WorkerProcedureCall,
  call_only_cluster_authorization_policy_fixture,
  full_admin_cluster_authorization_policy_fixture,
  function_limited_cluster_authorization_policy_fixture,
  type cluster_authorization_policy_t
} from '../../src/index';

function BuildClusterCallRequest(params: {
  function_name: string;
  scopes: string[];
}): Record<string, unknown> {
  const { function_name, scopes } = params;
  return {
    protocol_version: 1,
    message_type: 'cluster_call_request',
    timestamp_unix_ms: Date.now(),
    request_id: 'call_req_1',
    trace_id: 'trace_1',
    span_id: 'span_1',
    attempt_index: 1,
    max_attempts: 1,
    deadline_unix_ms: Date.now() + 30_000,
    function_name,
    args: [],
    routing_hint: {
      mode: 'auto'
    },
    caller_identity: {
      subject: 'caller@example.com',
      tenant_id: 'tenant_1',
      scopes,
      signed_claims: 'signed-token'
    }
  };
}

function BuildClusterMutationRequest(params: {
  mutation_type: string;
  capability_claims: string[];
}): Record<string, unknown> {
  const { mutation_type, capability_claims } = params;
  return {
    protocol_version: 1,
    message_type: 'cluster_admin_mutation_request',
    timestamp_unix_ms: Date.now(),
    mutation_id: 'mutation_1',
    request_id: 'request_1',
    trace_id: 'trace_1',
    deadline_unix_ms: Date.now() + 30_000,
    target_scope: 'single_node',
    target_selector: {
      node_ids: ['node_local_1']
    },
    mutation_type,
    payload: {
      name: 'Phase3MutationFunction',
      worker_func_source: 'async function () { return "ok"; }'
    },
    dry_run: true,
    rollout_strategy: {
      mode: 'single_node',
      min_success_percent: 100
    },
    auth_context: {
      subject: 'admin@example.com',
      tenant_id: 'tenant_1',
      capability_claims,
      signed_claims: 'signed-token'
    }
  };
}

test('authorization engine deny overrides allow for call capability', function () {
  const policy_list: cluster_authorization_policy_t[] = [
    {
      policy_id: 'allow_all_calls',
      effect: 'allow',
      capabilities: ['rpc.call:*'],
      constraints: {
        tenant: '*'
      }
    },
    {
      policy_id: 'deny_delete_user',
      effect: 'deny',
      capabilities: ['rpc.call:function:DeleteUser'],
      constraints: {
        tenant: 'tenant_1'
      }
    }
  ];
  const policy_engine = new ClusterAuthorizationPolicyEngine({
    policy_list
  });

  const evaluation_result = policy_engine.authorizeCall({
    function_name: 'DeleteUser',
    subject: 'caller@example.com',
    tenant: 'tenant_1'
  });

  assert.equal(evaluation_result.authorized, false);
  assert.equal(evaluation_result.reason, 'deny_matched_policy');
  assert.deepEqual(evaluation_result.matched_deny_policy_id_list, ['deny_delete_user']);
  assert.deepEqual(evaluation_result.matched_allow_policy_id_list, ['allow_all_calls']);
});

test('authorization engine enforces tenant/environment/function/node_selector/mutation_type scopes', function () {
  const policy_engine = new ClusterAuthorizationPolicyEngine({
    policy_list: [
      {
        policy_id: 'billing_mutation_allow',
        effect: 'allow',
        capabilities: ['rpc.admin.mutate:function:redefine'],
        constraints: {
          tenant: 'billing',
          environment: 'staging',
          function_name_pattern: '^Billing.*$',
          mutation_types: ['redefine_function'],
          node_selector: {
            labels: {
              service: 'billing'
            }
          }
        }
      }
    ]
  });

  const allowed_result = policy_engine.authorizeMutation({
    mutation_type: 'redefine_function',
    function_name: 'BillingCharge',
    tenant: 'billing',
    environment: 'staging',
    target_selector: {
      labels: {
        service: 'billing'
      }
    }
  });
  assert.equal(allowed_result.authorized, true);

  const denied_result = policy_engine.authorizeMutation({
    mutation_type: 'redefine_function',
    function_name: 'BillingCharge',
    tenant: 'billing',
    environment: 'prod',
    target_selector: {
      labels: {
        service: 'billing'
      }
    }
  });
  assert.equal(denied_result.authorized, false);
  assert.equal(denied_result.reason, 'deny_no_matching_policy');
});

test('call permission does not imply mutation permission', function () {
  const policy_engine = new ClusterAuthorizationPolicyEngine({
    policy_list: [
      {
        policy_id: 'call_only',
        effect: 'allow',
        capabilities: ['rpc.call:*'],
        constraints: {
          tenant: '*'
        }
      }
    ]
  });

  const call_result = policy_engine.authorizeCall({
    function_name: 'GetUserProfile',
    tenant: 'tenant_1'
  });
  const mutation_result = policy_engine.authorizeMutation({
    mutation_type: 'define_function',
    function_name: 'GetUserProfile',
    tenant: 'tenant_1'
  });

  assert.equal(call_result.authorized, true);
  assert.equal(mutation_result.authorized, false);
});

test('policy fixtures match expected authorization outcomes', function () {
  const full_admin_engine = new ClusterAuthorizationPolicyEngine({
    policy_list: [full_admin_cluster_authorization_policy_fixture]
  });
  const full_admin_result = full_admin_engine.authorizeMutation({
    mutation_type: 'define_function',
    tenant: 'tenant_1',
    environment: 'prod',
    subject: 'admin@example.com',
    group_list: ['cluster-admins']
  });
  assert.equal(full_admin_result.authorized, true);

  const function_limited_engine = new ClusterAuthorizationPolicyEngine({
    policy_list: [function_limited_cluster_authorization_policy_fixture]
  });
  const function_limited_result = function_limited_engine.authorizeMutation({
    mutation_type: 'redefine_function',
    function_name: 'BillingCharge',
    tenant: 'billing',
    environment: 'staging',
    service_account: 'svc_billing_release',
    target_selector: {
      labels: {
        service: 'billing'
      }
    }
  });
  assert.equal(function_limited_result.authorized, true);

  const call_only_engine = new ClusterAuthorizationPolicyEngine({
    policy_list: [call_only_cluster_authorization_policy_fixture]
  });
  const call_allowed = call_only_engine.authorizeCall({
    function_name: 'GetUserProfile',
    tenant: 'customer_portal',
    environment: 'prod',
    client_id: 'client_portal_backend'
  });
  const mutation_denied = call_only_engine.authorizeMutation({
    mutation_type: 'define_function',
    function_name: 'GetUserProfile',
    tenant: 'customer_portal',
    environment: 'prod',
    client_id: 'client_portal_backend'
  });

  assert.equal(call_allowed.authorized, true);
  assert.equal(mutation_denied.authorized, false);
});

test('cluster call request returns AUTH_FAILED when caller claims are missing required capability', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: [
        {
          policy_id: 'allow_calls_for_tenant',
          effect: 'allow',
          capabilities: ['rpc.call:*'],
          constraints: {
            tenant: 'tenant_1'
          }
        }
      ]
    });

    await workerprocedurecall.startWorkers({ count: 1 });
    await workerprocedurecall.defineWorkerFunction({
      name: 'AuthorizedCallFunction',
      worker_func: async function (): Promise<string> {
        return 'ok';
      }
    });

    const response = await workerprocedurecall.handleClusterCallRequest({
      node_id: 'node_local_1',
      message: BuildClusterCallRequest({
        function_name: 'AuthorizedCallFunction',
        scopes: []
      })
    });

    assert.equal(response.ack.accepted, false);
    assert.equal(response.terminal_message.message_type, 'cluster_call_response_error');
    if (response.terminal_message.message_type !== 'cluster_call_response_error') {
      return;
    }

    assert.equal(response.terminal_message.error.code, 'AUTH_FAILED');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('cluster call request returns FORBIDDEN_FUNCTION when policy denies function access', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: []
    });

    await workerprocedurecall.startWorkers({ count: 1 });
    await workerprocedurecall.defineWorkerFunction({
      name: 'ForbiddenCallFunction',
      worker_func: async function (): Promise<string> {
        return 'ok';
      }
    });

    const response = await workerprocedurecall.handleClusterCallRequest({
      node_id: 'node_local_1',
      message: BuildClusterCallRequest({
        function_name: 'ForbiddenCallFunction',
        scopes: ['rpc.call:function:ForbiddenCallFunction']
      })
    });

    assert.equal(response.ack.accepted, false);
    assert.equal(response.terminal_message.message_type, 'cluster_call_response_error');
    if (response.terminal_message.message_type !== 'cluster_call_response_error') {
      return;
    }

    assert.equal(response.terminal_message.error.code, 'FORBIDDEN_FUNCTION');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('cluster call request succeeds with permissive policy regression baseline', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: [
        {
          policy_id: 'allow_all_calls',
          effect: 'allow',
          capabilities: ['rpc.call:*']
        }
      ]
    });

    await workerprocedurecall.startWorkers({ count: 1 });
    await workerprocedurecall.defineWorkerFunction({
      name: 'PermissiveCallFunction',
      worker_func: async function (): Promise<string> {
        return 'ok';
      }
    });

    const response = await workerprocedurecall.handleClusterCallRequest({
      node_id: 'node_local_1',
      message: BuildClusterCallRequest({
        function_name: 'PermissiveCallFunction',
        scopes: ['rpc.call:*']
      })
    });

    assert.equal(response.ack.accepted, true);
    assert.equal(
      response.terminal_message.message_type,
      'cluster_call_response_success'
    );
    if (response.terminal_message.message_type !== 'cluster_call_response_success') {
      return;
    }

    assert.equal(response.terminal_message.return_value, 'ok');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('cluster mutation request returns ADMIN_AUTH_FAILED when capability claims are missing', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: [
        {
          policy_id: 'allow_mutations',
          effect: 'allow',
          capabilities: ['rpc.admin.mutate:*']
        }
      ]
    });

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id: 'node_local_1',
      message: BuildClusterMutationRequest({
        mutation_type: 'define_function',
        capability_claims: []
      })
    });

    assert.equal(response.ack.accepted, false);
    assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_error');
    if (response.terminal_message.message_type !== 'cluster_admin_mutation_error') {
      return;
    }

    assert.equal(response.terminal_message.error.code, 'ADMIN_AUTH_FAILED');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});

test('cluster mutation request returns ADMIN_FORBIDDEN when policy denies mutation', async function () {
  const workerprocedurecall = new WorkerProcedureCall();

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: []
    });

    const response = await workerprocedurecall.handleClusterAdminMutationRequest({
      node_id: 'node_local_1',
      message: BuildClusterMutationRequest({
        mutation_type: 'define_function',
        capability_claims: ['rpc.admin.mutate:function:define']
      })
    });

    assert.equal(response.ack.accepted, false);
    assert.equal(response.terminal_message.message_type, 'cluster_admin_mutation_error');
    if (response.terminal_message.message_type !== 'cluster_admin_mutation_error') {
      return;
    }

    assert.equal(response.terminal_message.error.code, 'ADMIN_FORBIDDEN');
  } finally {
    await workerprocedurecall.stopWorkers();
  }
});
