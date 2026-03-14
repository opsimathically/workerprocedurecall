import test from 'node:test';

import {
  ClusterClient,
  ClusterClientError,
  type cluster_client_error_code_t
} from '../../src/index';
import { BuildSecureClientTlsConfig } from '../fixtures/secure_transport_config';

async function AssertClusterClientCallGenericTypingWorks(params: {
  cluster_client: ClusterClient;
}): Promise<void> {
  const result = await params.cluster_client.call<{ value: number }>({
    function_name: 'TypedFunction',
    args: []
  });

  result.value.toFixed(0);
}

async function AssertClusterClientErrorTypingWorks(params: {
  cluster_client: ClusterClient;
}): Promise<void> {
  try {
    await params.cluster_client.call({
      function_name: 'TypedFunction',
      args: []
    });
  } catch (error) {
    if (error instanceof ClusterClientError) {
      const typed_error_code: cluster_client_error_code_t = error.code;
      void typed_error_code;
      return;
    }

    throw error;
  }
}

async function AssertClusterClientReturnTypeRejectsInvalidProperty(params: {
  cluster_client: ClusterClient;
}): Promise<void> {
  const result = await params.cluster_client.call<{ value: number }>({
    function_name: 'TypedFunction',
    args: []
  });

  // @ts-expect-error property does not exist on typed return value.
  result.not_a_valid_property();
}

test('cluster client typing assertions compile', function () {
  const cluster_client = new ClusterClient({
    host: '127.0.0.1',
    port: 1,
    auth_context: {
      subject: 'typing_subject',
      tenant_id: 'typing_tenant',
      scopes: ['rpc.call:*'],
      signed_claims: 'typing_signed_claims'
    },
    transport_security: BuildSecureClientTlsConfig()
  });

  void cluster_client;
  void AssertClusterClientCallGenericTypingWorks;
  void AssertClusterClientErrorTypingWorks;
  void AssertClusterClientReturnTypeRejectsInvalidProperty;
});
