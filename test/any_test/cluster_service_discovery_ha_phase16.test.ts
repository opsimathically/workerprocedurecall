import assert from 'node:assert/strict';
import { connect, type ClientHttp2Session } from 'node:http2';
import test from 'node:test';

import {
  ClusterRemoteServiceDiscoveryStoreAdapter,
  ClusterServiceDiscoveryDaemon,
  type cluster_remote_service_discovery_endpoint_t,
  type cluster_service_discovery_response_error_message_i,
  type cluster_service_discovery_response_success_message_i,
  type cluster_service_discovery_daemon_peer_endpoint_t
} from '../../src/index';
import {
  BuildSecureClientTlsConfig,
  BuildSecureServerTlsConfig
} from '../fixtures/secure_transport_config';

type discovery_response_t =
  | cluster_service_discovery_response_success_message_i
  | cluster_service_discovery_response_error_message_i;

function Sleep(params: { delay_ms: number }): Promise<void> {
  return new Promise<void>((resolve): void => {
    setTimeout(resolve, params.delay_ms);
  });
}

async function WaitForCondition(params: {
  timeout_ms: number;
  condition_func: () => boolean;
}): Promise<void> {
  const started_unix_ms = Date.now();

  while (Date.now() - started_unix_ms <= params.timeout_ms) {
    if (params.condition_func()) {
      return;
    }

    await Sleep({
      delay_ms: 20
    });
  }

  throw new Error(`Condition was not satisfied within ${params.timeout_ms}ms.`);
}

async function SendDiscoveryMessage(params: {
  host: string;
  port: number;
  request_path: string;
  message: unknown;
}): Promise<discovery_response_t> {
  const client_session: ClientHttp2Session = connect(
    `https://${params.host}:${params.port}`,
    {
      ca: BuildSecureClientTlsConfig().ca_pem_list,
      cert: BuildSecureClientTlsConfig().client_cert_pem,
      key: BuildSecureClientTlsConfig().client_key_pem,
      rejectUnauthorized: BuildSecureClientTlsConfig().reject_unauthorized ?? true,
      servername: BuildSecureClientTlsConfig().servername
    }
  );

  try {
    const response_text = await new Promise<string>((resolve, reject): void => {
      const request_stream = client_session.request({
        ':method': 'POST',
        ':path': params.request_path,
        'content-type': 'application/json'
      });

      const chunk_list: Buffer[] = [];

      request_stream.on('data', (chunk): void => {
        if (typeof chunk === 'string') {
          chunk_list.push(Buffer.from(chunk));
          return;
        }

        chunk_list.push(chunk as Buffer);
      });

      request_stream.on('end', (): void => {
        resolve(Buffer.concat(chunk_list).toString('utf8'));
      });

      request_stream.on('error', (error): void => {
        reject(error);
      });

      request_stream.end(JSON.stringify(params.message));
    });

    return JSON.parse(response_text) as discovery_response_t;
  } finally {
    client_session.close();
  }
}

function BuildListNodesRequest(params: {
  request_id: string;
}): Record<string, unknown> {
  return {
    protocol_version: 1,
    message_type: 'cluster_service_discovery_list_nodes',
    timestamp_unix_ms: Date.now(),
    request_id: params.request_id,
    include_expired: false,
    consistency_mode: 'stale_ok'
  };
}

function GetHaStatus(params: {
  daemon: ClusterServiceDiscoveryDaemon;
}): {
  daemon_id: string;
  role: string;
  leader_id: string | null;
  term: number;
} {
  const snapshot = params.daemon.getSnapshot();
  const data = snapshot.ha_status as {
    ha_status?: {
      daemon_id: string;
      role: string;
      leader_id: string | null;
      term: number;
    };
  };

  return (
    data.ha_status ?? {
      daemon_id: 'unknown',
      role: 'follower',
      leader_id: null,
      term: 0
    }
  );
}

async function StartHaDaemonCluster(params: {
  daemon_id_list: string[];
}): Promise<
  {
    daemon: ClusterServiceDiscoveryDaemon;
    daemon_id: string;
    address: {
      host: string;
      port: number;
      request_path: string;
    };
  }[]
> {
  const daemon_state_list: {
    daemon: ClusterServiceDiscoveryDaemon;
    daemon_id: string;
    address: {
      host: string;
      port: number;
      request_path: string;
    };
  }[] = [];

  for (const daemon_id of params.daemon_id_list) {
    const daemon = new ClusterServiceDiscoveryDaemon({
      host: '127.0.0.1',
      port: 0,
      security: {
        tls: BuildSecureServerTlsConfig()
      },
      transport_security: BuildSecureClientTlsConfig(),
      ha: {
        enabled: true,
        daemon_id,
        election_timeout_min_ms: 180,
        election_timeout_max_ms: 260,
        leader_heartbeat_interval_ms: 60,
        replication_timeout_ms: 1_500
      }
    });

    const address = await daemon.start();
    daemon_state_list.push({
      daemon,
      daemon_id,
      address
    });
  }

  for (const daemon_state of daemon_state_list) {
    const peer_endpoint_list: cluster_service_discovery_daemon_peer_endpoint_t[] =
      daemon_state_list
        .filter((peer_daemon_state): boolean => {
          return peer_daemon_state.daemon_id !== daemon_state.daemon_id;
        })
        .map((peer_daemon_state): cluster_service_discovery_daemon_peer_endpoint_t => {
          return {
            daemon_id: peer_daemon_state.daemon_id,
            host: peer_daemon_state.address.host,
            port: peer_daemon_state.address.port,
            request_path: peer_daemon_state.address.request_path,
            tls_mode: 'required'
          };
        });

    daemon_state.daemon.setHaPeerEndpointList({
      peer_endpoint_list
    });
  }

  return daemon_state_list;
}

async function StopDaemonCluster(params: {
  daemon_state_list: {
    daemon: ClusterServiceDiscoveryDaemon;
  }[];
}): Promise<void> {
  for (const daemon_state of params.daemon_state_list) {
    await daemon_state.daemon.stop();
  }
}

async function WaitForSingleLeader(params: {
  daemon_state_list: {
    daemon: ClusterServiceDiscoveryDaemon;
    daemon_id: string;
    address: {
      host: string;
      port: number;
      request_path: string;
    };
  }[];
  timeout_ms: number;
}): Promise<{
  daemon: ClusterServiceDiscoveryDaemon;
  daemon_id: string;
  address: {
    host: string;
    port: number;
    request_path: string;
  };
}> {
  let leader_state:
    | {
        daemon: ClusterServiceDiscoveryDaemon;
        daemon_id: string;
        address: {
          host: string;
          port: number;
          request_path: string;
        };
      }
    | null = null;

  await WaitForCondition({
    timeout_ms: params.timeout_ms,
    condition_func: (): boolean => {
      const leader_state_list = params.daemon_state_list.filter((daemon_state): boolean => {
        const ha_status = GetHaStatus({
          daemon: daemon_state.daemon
        });
        return ha_status.role === 'leader';
      });

      if (leader_state_list.length !== 1) {
        return false;
      }

      leader_state = leader_state_list[0];
      return true;
    }
  });

  if (!leader_state) {
    throw new Error('Leader election did not converge to a single leader.');
  }

  return leader_state;
}

test('phase16 cluster elects single leader, redirects writes on followers, and replicates committed writes', async function () {
  const daemon_state_list = await StartHaDaemonCluster({
    daemon_id_list: ['phase16_a', 'phase16_b', 'phase16_c']
  });

  try {
    const leader_state = await WaitForSingleLeader({
      daemon_state_list,
      timeout_ms: 5_000
    });

    const follower_state = daemon_state_list.find((daemon_state): boolean => {
      return daemon_state.daemon_id !== leader_state.daemon_id;
    });

    assert.notEqual(follower_state, undefined);

    const follower_register_response = await SendDiscoveryMessage({
      host: follower_state?.address.host as string,
      port: follower_state?.address.port as number,
      request_path: follower_state?.address.request_path as string,
      message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_register_node',
        timestamp_unix_ms: Date.now(),
        request_id: 'phase16_register_follower_should_redirect',
        node_identity: {
          node_id: 'phase16_node_1',
          address: {
            host: '127.0.0.1',
            port: 9999,
            request_path: '/wpc/cluster/protocol',
            tls_mode: 'required'
          }
        },
        status: 'ready'
      }
    });

    assert.equal(
      follower_register_response.message_type,
      'cluster_service_discovery_response_error'
    );

    if (follower_register_response.message_type === 'cluster_service_discovery_response_error') {
      assert.equal(follower_register_response.error.code, 'DISCOVERY_NOT_LEADER');
    }

    const leader_register_response = await SendDiscoveryMessage({
      host: leader_state.address.host,
      port: leader_state.address.port,
      request_path: leader_state.address.request_path,
      message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_register_node',
        timestamp_unix_ms: Date.now(),
        request_id: 'phase16_register_leader_success',
        node_identity: {
          node_id: 'phase16_node_1',
          address: {
            host: '127.0.0.1',
            port: 9999,
            request_path: '/wpc/cluster/protocol',
            tls_mode: 'required'
          }
        },
        status: 'ready'
      }
    });

    assert.equal(
      leader_register_response.message_type,
      'cluster_service_discovery_response_success'
    );

    await WaitForCondition({
      timeout_ms: 5_000,
      condition_func: (): boolean => {
        return daemon_state_list.every((daemon_state): boolean => {
          const node_list = daemon_state.daemon
            .getSnapshot()
            .recent_discovery_events;

          return node_list.some((event): boolean => {
            return event.node_id === 'phase16_node_1';
          });
        });
      }
    });

    for (const daemon_state of daemon_state_list) {
      const list_response = await SendDiscoveryMessage({
        host: daemon_state.address.host,
        port: daemon_state.address.port,
        request_path: daemon_state.address.request_path,
        message: BuildListNodesRequest({
          request_id: `phase16_list_${daemon_state.daemon_id}`
        })
      });

      assert.equal(
        list_response.message_type,
        'cluster_service_discovery_response_success'
      );

      if (list_response.message_type === 'cluster_service_discovery_response_success') {
        const node_list = (list_response.data as { node_list?: unknown }).node_list as
          | { node_identity: { node_id: string } }[]
          | undefined;

        assert.equal(
          node_list?.some((node): boolean => node.node_identity.node_id === 'phase16_node_1'),
          true
        );
      }
    }
  } finally {
    await StopDaemonCluster({
      daemon_state_list
    });
  }
});

test('phase16 leader failover continues writes through multi-endpoint remote adapter', async function () {
  const daemon_state_list = await StartHaDaemonCluster({
    daemon_id_list: ['phase16_adapter_a', 'phase16_adapter_b', 'phase16_adapter_c']
  });

  const adapter = new ClusterRemoteServiceDiscoveryStoreAdapter({
    endpoint_list: daemon_state_list.map((daemon_state): cluster_remote_service_discovery_endpoint_t => {
      return {
        daemon_id: daemon_state.daemon_id,
        host: daemon_state.address.host,
        port: daemon_state.address.port,
        request_path: daemon_state.address.request_path,
        tls_mode: 'required'
      };
    }),
    request_timeout_ms: 1_000,
    synchronization_interval_ms: 100,
    retry_base_delay_ms: 25,
    retry_max_delay_ms: 100,
    endpoint_cooldown_ms: 60,
    max_request_attempts: 8,
    transport_security: BuildSecureClientTlsConfig()
  });

  adapter.startExpirationLoop();

  try {
    let leader_state = await WaitForSingleLeader({
      daemon_state_list,
      timeout_ms: 5_000
    });

    adapter.upsertNodeRegistration({
      node_identity: {
        node_id: 'phase16_adapter_node',
        address: {
          host: '127.0.0.1',
          port: 9998,
          request_path: '/wpc/cluster/protocol',
          tls_mode: 'required'
        }
      },
      status: 'ready',
      lease_ttl_ms: 20_000
    });

    await WaitForCondition({
      timeout_ms: 5_000,
      condition_func: (): boolean => {
        return daemon_state_list.some((daemon_state): boolean => {
          const node_record = daemon_state.daemon
            .getSnapshot()
            .discovery_metrics.node_count;
          return node_record > 0;
        });
      }
    });

    await leader_state.daemon.stop();

    const surviving_daemon_state_list = daemon_state_list.filter((daemon_state): boolean => {
      return daemon_state.daemon_id !== leader_state.daemon_id;
    });

    leader_state = await WaitForSingleLeader({
      daemon_state_list: surviving_daemon_state_list,
      timeout_ms: 6_000
    });

    adapter.upsertNodeRegistration({
      node_identity: {
        node_id: 'phase16_adapter_node',
        address: {
          host: '127.0.0.1',
          port: 9998,
          request_path: '/wpc/cluster/protocol',
          tls_mode: 'required'
        }
      },
      status: 'degraded',
      metrics: {
        inflight_calls: 1
      },
      lease_ttl_ms: 20_000
    });

    await WaitForCondition({
      timeout_ms: 6_000,
      condition_func: (): boolean => {
        const leader_node_list = leader_state.daemon
          .getSnapshot()
          .discovery_metrics.node_count;

        if (leader_node_list === 0) {
          return false;
        }

        const node_record = leader_state.daemon
          .getSnapshot()
          .recent_discovery_events
          .filter((event): boolean => {
            return event.node_id === 'phase16_adapter_node';
          });

        return node_record.length > 0;
      }
    });
  } finally {
    adapter.stopExpirationLoop();
    await StopDaemonCluster({
      daemon_state_list
    });
  }
});

test('phase16 returns deterministic quorum unavailable error when majority is lost', async function () {
  const daemon_state_list = await StartHaDaemonCluster({
    daemon_id_list: ['phase16_quorum_a', 'phase16_quorum_b', 'phase16_quorum_c']
  });

  try {
    const leader_state = await WaitForSingleLeader({
      daemon_state_list,
      timeout_ms: 5_000
    });

    for (const follower_state of daemon_state_list) {
      if (follower_state.daemon_id === leader_state.daemon_id) {
        continue;
      }

      await follower_state.daemon.stop();
    }

    const quorum_failure_response = await SendDiscoveryMessage({
      host: leader_state.address.host,
      port: leader_state.address.port,
      request_path: leader_state.address.request_path,
      message: {
        protocol_version: 1,
        message_type: 'cluster_service_discovery_register_node',
        timestamp_unix_ms: Date.now(),
        request_id: 'phase16_quorum_failure_register',
        node_identity: {
          node_id: 'phase16_quorum_node',
          address: {
            host: '127.0.0.1',
            port: 10001,
            request_path: '/wpc/cluster/protocol',
            tls_mode: 'required'
          }
        },
        status: 'ready'
      }
    });

    assert.equal(
      quorum_failure_response.message_type,
      'cluster_service_discovery_response_error'
    );

    if (quorum_failure_response.message_type === 'cluster_service_discovery_response_error') {
      assert.equal(
        quorum_failure_response.error.code,
        'DISCOVERY_QUORUM_UNAVAILABLE'
      );
    }
  } finally {
    await StopDaemonCluster({
      daemon_state_list
    });
  }
});
