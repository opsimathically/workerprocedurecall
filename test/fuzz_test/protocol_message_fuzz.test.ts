import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ParseClusterControlPlaneRequestMessage,
  ParseClusterGeoIngressRequestMessage,
  ParseClusterProtocolMessage,
  ParseClusterServiceDiscoveryRequestMessage,
  ParseIngressCallRequestMessage,
  ParseIngressForwardWireResponse
} from '../../src/index';
import {
  BuildRandomJsonValue,
  InstallProcessCrashGuard,
  RandomAlphaNumericString,
  RandomBoolean,
  RandomIntegerInRange,
  RunFuzzSuite
} from './fuzz_harness';

function BuildBaseClusterMessage(params: {
  random_func: () => number;
  case_index: number;
}): Record<string, unknown> {
  return {
    protocol_version: RandomIntegerInRange({
      random_func: params.random_func,
      minimum_value: -2,
      maximum_value: 3
    }),
    message_type: RandomAlphaNumericString({
      random_func: params.random_func,
      minimum_length: 3,
      maximum_length: 48
    }),
    timestamp_unix_ms: Date.now() + params.case_index,
    request_id: `fuzz_req_${params.case_index}`,
    trace_id: `fuzz_trace_${params.case_index}`,
    deadline_unix_ms: Date.now() + RandomIntegerInRange({
      random_func: params.random_func,
      minimum_value: -5_000,
      maximum_value: 20_000
    })
  };
}

function BuildMutatedMessage(params: {
  random_func: () => number;
  case_index: number;
}): unknown {
  const mutation_mode = RandomIntegerInRange({
    random_func: params.random_func,
    minimum_value: 0,
    maximum_value: 8
  });

  if (mutation_mode === 0) {
    return BuildRandomJsonValue({
      random_func: params.random_func,
      depth: 0,
      max_depth: 4,
      max_array_length: 6,
      max_object_keys: 6,
      max_string_length: 128
    });
  }

  const message = BuildBaseClusterMessage(params);

  if (mutation_mode === 1) {
    message.message_type = 'cluster_call_request';
    message.function_name = RandomAlphaNumericString({
      random_func: params.random_func,
      minimum_length: 1,
      maximum_length: 32
    });
    message.args = [
      BuildRandomJsonValue({
        random_func: params.random_func,
        depth: 0,
        max_depth: 3,
        max_array_length: 4,
        max_object_keys: 4,
        max_string_length: 80
      })
    ];
  } else if (mutation_mode === 2) {
    message.message_type = 'cluster_admin_mutation_request';
    message.mutation_id = `fuzz_mutation_${params.case_index}`;
    message.target_scope = 'single_node';
    message.mutation_type = 'define_function';
    message.payload = {
      function_name: 'FuzzFunction',
      source: 'return true;'
    };
  } else if (mutation_mode === 3) {
    message.message_type = 'cluster_service_discovery_register_node';
    message.node_identity = {
      node_id: `fuzz_node_${params.case_index}`,
      address: {
        host: 'localhost',
        port: RandomIntegerInRange({
          random_func: params.random_func,
          minimum_value: 1,
          maximum_value: 65_535
        }),
        request_path: '/wpc/cluster/protocol',
        tls_mode: 'required'
      }
    };
    message.status = 'ready';
  } else if (mutation_mode === 4) {
    message.message_type = 'cluster_control_plane_gateway_register';
    message.gateway_id = `gateway_${params.case_index}`;
    message.node_reference = {
      node_id: `fuzz_node_${params.case_index}`
    };
    message.address = {
      host: 'localhost',
      port: 7101,
      request_path: '/wpc/cluster/protocol',
      tls_mode: 'required'
    };
    message.status = RandomBoolean({
      random_func: params.random_func
    })
      ? 'active'
      : 'degraded';
  } else if (mutation_mode === 5) {
    message.message_type = 'cluster_geo_ingress_ingress_register';
    message.ingress_id = `ingress_${params.case_index}`;
    message.region_id = 'us-fuzz-1';
    message.endpoint = {
      host: 'localhost',
      port: 7301,
      request_path: '/wpc/cluster/ingress',
      tls_mode: 'required'
    };
    message.health_status = 'ready';
    message.ingress_version = 'fuzz';
  } else if (mutation_mode === 6) {
    delete message.timestamp_unix_ms;
    message.protocol_version = 'bad_protocol_version';
  } else if (mutation_mode === 7) {
    message.message_type = 5_000;
    message.request_id = [];
  } else {
    message.message_type = 'cluster_call_request';
    message.args = new Array(40).fill(0).map((_, arg_index): unknown => {
      return {
        arg_index,
        value: RandomAlphaNumericString({
          random_func: params.random_func,
          minimum_length: 0,
          maximum_length: 64
        })
      };
    });
  }

  return message;
}

function AssertParsersAreStable(params: {
  candidate_message: unknown;
}): void {
  const parser_func_list: (() => unknown)[] = [
    (): unknown => {
      return ParseClusterProtocolMessage({
        message: params.candidate_message
      });
    },
    (): unknown => {
      return ParseIngressCallRequestMessage({
        message: params.candidate_message
      });
    },
    (): unknown => {
      return ParseClusterServiceDiscoveryRequestMessage({
        message: params.candidate_message
      });
    },
    (): unknown => {
      return ParseClusterControlPlaneRequestMessage({
        message: params.candidate_message
      });
    },
    (): unknown => {
      return ParseClusterGeoIngressRequestMessage({
        message: params.candidate_message
      });
    },
    (): unknown => {
      return ParseIngressForwardWireResponse({
        response: params.candidate_message
      });
    }
  ];

  for (const parser_func of parser_func_list) {
    const parser_result = parser_func();
    if (
      typeof parser_result === 'object' &&
      parser_result !== null &&
      'ok' in parser_result
    ) {
      assert.equal(typeof (parser_result as { ok: unknown }).ok, 'boolean');
    }
  }
}

test('protocol message fuzz parser stability', async function (): Promise<void> {
  const process_guard = InstallProcessCrashGuard({
    suite_name: 'protocol_message_fuzz'
  });

  try {
    await RunFuzzSuite({
      suite_name: 'protocol_message_fuzz',
      default_iterations: 120,
      default_case_timeout_ms: 250,
      run_case_func: async (params): Promise<void> => {
        const candidate_message = BuildMutatedMessage({
          random_func: params.random_func,
          case_index: params.case_index
        });
        AssertParsersAreStable({
          candidate_message
        });
      }
    });
  } finally {
    const process_event_list = process_guard.dispose();
    assert.equal(process_event_list.length, 0);
  }
});
