import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import test from 'node:test';

import {
  ClusterClient,
  ClusterClientError,
  ClusterNodeAgent,
  WorkerProcedureCall
} from '../../src/index';
import {
  phase10_ca_cert_pem,
  phase10_client_cert_pem,
  phase10_client_key_pem,
  phase10_server_cert_pem,
  phase10_server_key_pem
} from '../fixtures/phase10_tls_material';

function Base64UrlEncode(params: { value: Buffer | string }): string {
  const buffer_value =
    typeof params.value === 'string'
      ? Buffer.from(params.value, 'utf8')
      : params.value;

  return buffer_value
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function BuildHs256JwtToken(params: {
  secret: string;
  kid: string;
  payload: Record<string, unknown>;
}): string {
  const header = {
    alg: 'HS256',
    typ: 'JWT',
    kid: params.kid
  };

  const encoded_header = Base64UrlEncode({
    value: JSON.stringify(header)
  });
  const encoded_payload = Base64UrlEncode({
    value: JSON.stringify(params.payload)
  });

  const signing_input = `${encoded_header}.${encoded_payload}`;
  const signature = createHmac('sha256', params.secret).update(signing_input).digest();

  const encoded_signature = Base64UrlEncode({ value: signature });

  return `${signing_input}.${encoded_signature}`;
}

function BuildAllowAllCallPolicyList(): {
  policy_id: string;
  effect: 'allow';
  capabilities: string[];
}[] {
  return [
    {
      policy_id: 'phase10_allow_all_calls',
      effect: 'allow',
      capabilities: ['rpc.call:*']
    }
  ];
}

function BuildAllowAllMutationPolicyList(): {
  policy_id: string;
  effect: 'allow';
  capabilities: string[];
}[] {
  return [
    {
      policy_id: 'phase10_allow_all_mutations',
      effect: 'allow',
      capabilities: ['rpc.admin.mutate:*']
    }
  ];
}

function BuildTokenPayload(params: {
  expires_in_seconds: number;
  issuer?: string;
  audience?: string | string[];
  token_id?: string;
  scopes?: string[];
}): Record<string, unknown> {
  const now_unix_seconds = Math.floor(Date.now() / 1000);

  return {
    sub: 'phase10_subject',
    tenant_id: 'tenant_phase10',
    scopes: params.scopes ?? ['rpc.call:*', 'rpc.admin.mutate:*'],
    iss: params.issuer ?? 'phase10_issuer',
    aud: params.audience ?? 'phase10_audience',
    nbf: now_unix_seconds - 5,
    exp: now_unix_seconds + params.expires_in_seconds,
    jti: params.token_id ?? `phase10_jti_${now_unix_seconds}`
  };
}

function BuildSecureTransportConfig(params: {
  jwt_secret: string;
  key_id?: string;
}): {
  security: {
    tls: {
      mode: 'required';
      key_pem: string;
      cert_pem: string;
      ca_pem_list: string[];
      request_client_cert: true;
      reject_unauthorized_client_cert: true;
    };
    token_validation: {
      enabled: true;
      expected_issuer: string;
      expected_audience: string;
      key_by_kid: Record<
        string,
        {
          kid: string;
          algorithm: 'HS256';
          secret: string;
        }
      >;
    };
    replay_protection: {
      enabled: true;
      window_ms: number;
      require_token_id_for_privileged_requests: true;
    };
    session: {
      max_identity_ttl_ms: number;
    };
  };
} {
  const key_id = params.key_id ?? 'phase10_hs_key_1';

  return {
    security: {
      tls: {
        mode: 'required',
        key_pem: phase10_server_key_pem,
        cert_pem: phase10_server_cert_pem,
        ca_pem_list: [phase10_ca_cert_pem],
        request_client_cert: true,
        reject_unauthorized_client_cert: true
      },
      token_validation: {
        enabled: true,
        expected_issuer: 'phase10_issuer',
        expected_audience: 'phase10_audience',
        key_by_kid: {
          [key_id]: {
            kid: key_id,
            algorithm: 'HS256',
            secret: params.jwt_secret
          }
        }
      },
      replay_protection: {
        enabled: true,
        window_ms: 120_000,
        require_token_id_for_privileged_requests: true
      },
      session: {
        max_identity_ttl_ms: 5_000
      }
    }
  };
}

test('phase10 valid mTLS and signed token succeeds for call execution', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const jwt_secret = 'phase10_secret_valid';

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase10_node_valid',
    worker_start_count: 1,
    transport: {
      ...BuildSecureTransportConfig({ jwt_secret })
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: BuildAllowAllCallPolicyList()
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase10SecureEcho',
      worker_func: async function (params: { value: string }): Promise<string> {
        return `secure:${params.value}`;
      }
    });

    const address = await cluster_node_agent.start();

    const token = BuildHs256JwtToken({
      secret: jwt_secret,
      kid: 'phase10_hs_key_1',
      payload: BuildTokenPayload({
        expires_in_seconds: 60,
        scopes: ['rpc.call:*']
      })
    });

    const cluster_client = new ClusterClient({
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      auth_context: {
        subject: 'ignored_client_subject',
        tenant_id: 'ignored_client_tenant',
        scopes: ['rpc.call:*'],
        signed_claims: 'ignored_client_claims'
      },
      auth_headers: {
        authorization: `Bearer ${token}`
      },
      transport_security: {
        use_tls: true,
        ca_pem_list: [phase10_ca_cert_pem],
        client_cert_pem: phase10_client_cert_pem,
        client_key_pem: phase10_client_key_pem,
        reject_unauthorized: false
      }
    });

    try {
      await cluster_client.connect();

      const result = await cluster_client.call<string>({
        function_name: 'Phase10SecureEcho',
        args: [{ value: 'ok' }]
      });

      assert.equal(result, 'secure:ok');
    } finally {
      await cluster_client.close();
    }
  } finally {
    await cluster_node_agent.stop();
  }
});

test('phase10 rejects invalid, expired, wrong issuer, and wrong audience tokens deterministically', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const jwt_secret = 'phase10_secret_validation';

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase10_node_validation',
    worker_start_count: 1,
    transport: {
      ...BuildSecureTransportConfig({ jwt_secret })
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: BuildAllowAllCallPolicyList()
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase10ValidationFunction',
      worker_func: async function (): Promise<string> {
        return 'ok';
      }
    });

    const address = await cluster_node_agent.start();

    const invalid_signature_token = BuildHs256JwtToken({
      secret: 'wrong_secret',
      kid: 'phase10_hs_key_1',
      payload: BuildTokenPayload({
        expires_in_seconds: 60,
        scopes: ['rpc.call:*']
      })
    });

    const expired_token = BuildHs256JwtToken({
      secret: jwt_secret,
      kid: 'phase10_hs_key_1',
      payload: BuildTokenPayload({
        expires_in_seconds: -10,
        scopes: ['rpc.call:*']
      })
    });

    const wrong_issuer_token = BuildHs256JwtToken({
      secret: jwt_secret,
      kid: 'phase10_hs_key_1',
      payload: BuildTokenPayload({
        expires_in_seconds: 60,
        issuer: 'wrong_issuer',
        scopes: ['rpc.call:*']
      })
    });

    const wrong_audience_token = BuildHs256JwtToken({
      secret: jwt_secret,
      kid: 'phase10_hs_key_1',
      payload: BuildTokenPayload({
        expires_in_seconds: 60,
        audience: 'wrong_audience',
        scopes: ['rpc.call:*']
      })
    });

    const token_list = [
      invalid_signature_token,
      expired_token,
      wrong_issuer_token,
      wrong_audience_token
    ];

    for (const token of token_list) {
      const cluster_client = new ClusterClient({
        host: address.host,
        port: address.port,
        request_path: address.request_path,
        auth_context: {
          subject: 'ignored_client_subject',
          tenant_id: 'ignored_client_tenant',
          scopes: ['rpc.call:*'],
          signed_claims: 'ignored_client_claims'
        },
        auth_headers: {
          authorization: `Bearer ${token}`
        },
        retry_policy: {
          max_attempts: 1
        },
        transport_security: {
          use_tls: true,
          ca_pem_list: [phase10_ca_cert_pem],
          client_cert_pem: phase10_client_cert_pem,
          client_key_pem: phase10_client_key_pem,
          reject_unauthorized: false
        }
      });

      try {
        await cluster_client.connect();

        await assert.rejects(
          async function (): Promise<void> {
            await cluster_client.call({
              function_name: 'Phase10ValidationFunction'
            });
          },
          function (error: unknown): boolean {
            assert(error instanceof ClusterClientError);
            assert.equal(error.code, 'AUTH_FAILED');
            return true;
          }
        );
      } finally {
        await cluster_client.close();
      }
    }
  } finally {
    await cluster_node_agent.stop();
  }
});

test('phase10 rejects replayed privileged admin mutation requests', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const jwt_secret = 'phase10_secret_replay';

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase10_node_replay',
    worker_start_count: 1,
    transport: {
      ...BuildSecureTransportConfig({ jwt_secret })
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: BuildAllowAllMutationPolicyList()
    });

    const address = await cluster_node_agent.start();

    const replay_token = BuildHs256JwtToken({
      secret: jwt_secret,
      kid: 'phase10_hs_key_1',
      payload: BuildTokenPayload({
        expires_in_seconds: 60,
        scopes: ['rpc.admin.mutate:*'],
        token_id: 'phase10_replay_jti_1'
      })
    });

    const cluster_client = new ClusterClient({
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      auth_context: {
        subject: 'ignored_client_subject',
        tenant_id: 'ignored_client_tenant',
        scopes: ['rpc.admin.mutate:*'],
        signed_claims: 'ignored_client_claims'
      },
      auth_headers: {
        authorization: `Bearer ${replay_token}`
      },
      transport_security: {
        use_tls: true,
        ca_pem_list: [phase10_ca_cert_pem],
        client_cert_pem: phase10_client_cert_pem,
        client_key_pem: phase10_client_key_pem,
        reject_unauthorized: false
      }
    });

    try {
      await cluster_client.connect();

      const first_result = await cluster_client.adminMutate({
        mutation_id: 'phase10_replay_mutation_1',
        mutation_type: 'define_constant',
        payload: {
          name: 'PHASE10_REPLAY_CONSTANT',
          value: 1
        },
        target_scope: 'single_node',
        target_selector: {
          node_ids: ['phase10_node_replay']
        },
        rollout_strategy: {
          mode: 'single_node'
        }
      });

      assert.equal(first_result.message_type, 'cluster_admin_mutation_result');

      await assert.rejects(
        async function (): Promise<void> {
          await cluster_client.adminMutate({
            mutation_id: 'phase10_replay_mutation_1',
            mutation_type: 'define_constant',
            payload: {
              name: 'PHASE10_REPLAY_CONSTANT',
              value: 2
            },
            target_scope: 'single_node',
            target_selector: {
              node_ids: ['phase10_node_replay']
            },
            rollout_strategy: {
              mode: 'single_node'
            }
          });
        },
        function (error: unknown): boolean {
          assert(error instanceof ClusterClientError);
          assert.equal(error.code, 'ADMIN_AUTH_FAILED');
          return true;
        }
      );
    } finally {
      await cluster_client.close();
    }
  } finally {
    await cluster_node_agent.stop();
  }
});

test('phase10 supports token key rotation hooks with deterministic auth behavior', async function () {
  const workerprocedurecall = new WorkerProcedureCall();
  const initial_secret = 'phase10_initial_secret';
  const rotated_secret = 'phase10_rotated_secret';

  const cluster_node_agent = new ClusterNodeAgent({
    workerprocedurecall,
    node_id: 'phase10_node_rotation',
    worker_start_count: 1,
    transport: {
      ...BuildSecureTransportConfig({
        jwt_secret: initial_secret,
        key_id: 'phase10_key_v1'
      })
    }
  });

  try {
    workerprocedurecall.setClusterAuthorizationPolicyList({
      policy_list: BuildAllowAllCallPolicyList()
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'Phase10RotationFunction',
      worker_func: async function (): Promise<string> {
        return 'rotation_ok';
      }
    });

    const address = await cluster_node_agent.start();

    let current_token = BuildHs256JwtToken({
      secret: initial_secret,
      kid: 'phase10_key_v1',
      payload: BuildTokenPayload({
        expires_in_seconds: 60,
        scopes: ['rpc.call:*']
      })
    });

    const cluster_client = new ClusterClient({
      host: address.host,
      port: address.port,
      request_path: address.request_path,
      auth_context: {
        subject: 'ignored_client_subject',
        tenant_id: 'ignored_client_tenant',
        scopes: ['rpc.call:*'],
        signed_claims: 'ignored_client_claims'
      },
      auth_headers_provider: function (): Record<string, string> {
        return {
          authorization: `Bearer ${current_token}`
        };
      },
      retry_policy: {
        max_attempts: 1
      },
      transport_security: {
        use_tls: true,
        ca_pem_list: [phase10_ca_cert_pem],
        client_cert_pem: phase10_client_cert_pem,
        client_key_pem: phase10_client_key_pem,
        reject_unauthorized: false
      }
    });

    try {
      await cluster_client.connect();

      const first_result = await cluster_client.call<string>({
        function_name: 'Phase10RotationFunction'
      });
      assert.equal(first_result, 'rotation_ok');

      cluster_node_agent.setTokenVerificationKeySet({
        key_by_kid: {
          phase10_key_v2: {
            kid: 'phase10_key_v2',
            algorithm: 'HS256',
            secret: rotated_secret
          }
        }
      });

      await assert.rejects(
        async function (): Promise<void> {
          await cluster_client.call({
            function_name: 'Phase10RotationFunction'
          });
        },
        function (error: unknown): boolean {
          assert(error instanceof ClusterClientError);
          assert.equal(error.code, 'AUTH_FAILED');
          return true;
        }
      );

      current_token = BuildHs256JwtToken({
        secret: rotated_secret,
        kid: 'phase10_key_v2',
        payload: BuildTokenPayload({
          expires_in_seconds: 60,
          scopes: ['rpc.call:*']
        })
      });

      const second_result = await cluster_client.call<string>({
        function_name: 'Phase10RotationFunction'
      });

      assert.equal(second_result, 'rotation_ok');
    } finally {
      await cluster_client.close();
    }
  } finally {
    await cluster_node_agent.stop();
  }
});
