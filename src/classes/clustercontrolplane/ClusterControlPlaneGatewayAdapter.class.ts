import { connect, type ClientHttp2Session } from 'node:http2';

import type {
  cluster_control_plane_gateway_address_t,
  cluster_control_plane_gateway_record_t,
  cluster_control_plane_mutation_status_t,
  cluster_control_plane_mutation_tracking_record_t,
  cluster_control_plane_node_reference_t,
  cluster_control_plane_policy_snapshot_t,
  cluster_control_plane_policy_version_record_t,
  cluster_control_plane_protocol_error_t,
  cluster_control_plane_protocol_request_message_type_t,
  cluster_control_plane_request_message_t,
  cluster_control_plane_service_status_snapshot_t,
  cluster_control_plane_topology_snapshot_t
} from './ClusterControlPlaneProtocol';
import {
  ParseClusterControlPlaneResponseErrorMessage,
  ParseClusterControlPlaneResponseSuccessMessage
} from './ClusterControlPlaneProtocolValidators';

export type cluster_control_plane_gateway_adapter_auth_headers_provider_t = () =>
  | Record<string, string>
  | Promise<Record<string, string>>;

export type cluster_control_plane_gateway_adapter_endpoint_t = {
  host: string;
  port: number;
  request_path?: string;
  tls_mode?: 'disabled' | 'required';
};

export type cluster_control_plane_gateway_adapter_constructor_params_t = {
  endpoint?: cluster_control_plane_gateway_adapter_endpoint_t;
  endpoint_list?: cluster_control_plane_gateway_adapter_endpoint_t[];
  request_timeout_ms?: number;
  retry_base_delay_ms?: number;
  retry_max_delay_ms?: number;
  endpoint_cooldown_ms?: number;
  max_request_attempts?: number;
  auth_headers?: Record<string, string>;
  auth_headers_provider?: cluster_control_plane_gateway_adapter_auth_headers_provider_t;
};

type cluster_control_plane_gateway_adapter_endpoint_state_t = {
  endpoint_id: string;
  host: string;
  port: number;
  request_path: string;
  tls_mode: 'disabled' | 'required';
  failure_count: number;
  last_failure_unix_ms: number | null;
  cooldown_until_unix_ms: number;
  last_success_unix_ms: number | null;
};

type cluster_control_plane_gateway_adapter_operation_error_t = Error & {
  protocol_error?: cluster_control_plane_protocol_error_t;
};

export type cluster_control_plane_gateway_adapter_metrics_t = {
  request_total: number;
  request_success_total: number;
  request_failure_total: number;
  retry_total: number;
  endpoint_failure_total: number;
  last_success_unix_ms: number | null;
};

export type cluster_control_plane_gateway_adapter_snapshot_t = {
  endpoint_list: {
    endpoint_id: string;
    host: string;
    port: number;
    request_path: string;
    tls_mode: 'disabled' | 'required';
    failure_count: number;
    cooldown_until_unix_ms: number;
    last_success_unix_ms: number | null;
  }[];
  metrics: cluster_control_plane_gateway_adapter_metrics_t;
};

function BuildRequestPath(params: { request_path: string | undefined }): string {
  if (typeof params.request_path !== 'string' || params.request_path.length === 0) {
    return '/wpc/cluster/control-plane';
  }

  if (params.request_path.startsWith('/')) {
    return params.request_path;
  }

  return `/${params.request_path}`;
}

function BuildTransportAuthority(params: {
  host: string;
  port: number;
  tls_mode: 'disabled' | 'required';
}): string {
  const scheme = params.tls_mode === 'required' ? 'https' : 'http';
  return `${scheme}://${params.host}:${params.port}`;
}

function NormalizeHeaders(params: {
  headers: Record<string, string> | undefined;
}): Record<string, string> {
  const normalized_headers: Record<string, string> = {};
  if (!params.headers) {
    return normalized_headers;
  }

  for (const [header_name, header_value] of Object.entries(params.headers)) {
    if (typeof header_name !== 'string' || header_name.length === 0) {
      continue;
    }

    if (typeof header_value !== 'string') {
      continue;
    }

    normalized_headers[header_name.toLowerCase()] = header_value;
  }

  return normalized_headers;
}

function BuildRetryDelayMs(params: {
  retry_attempt: number;
  retry_base_delay_ms: number;
  retry_max_delay_ms: number;
}): number {
  const exponent = Math.max(0, params.retry_attempt - 1);
  const delay_ms = params.retry_base_delay_ms * Math.pow(2, exponent);
  return Math.min(params.retry_max_delay_ms, delay_ms);
}

function Sleep(params: { delay_ms: number }): Promise<void> {
  return new Promise<void>((resolve): void => {
    setTimeout(resolve, params.delay_ms);
  });
}

export class ClusterControlPlaneGatewayAdapter {
  private endpoint_state_list: cluster_control_plane_gateway_adapter_endpoint_state_t[] = [];

  private request_timeout_ms: number;
  private retry_base_delay_ms: number;
  private retry_max_delay_ms: number;
  private endpoint_cooldown_ms: number;
  private max_request_attempts: number;

  private readonly static_auth_headers: Record<string, string>;
  private readonly auth_headers_provider:
    | cluster_control_plane_gateway_adapter_auth_headers_provider_t
    | undefined;

  private next_request_index = 1;

  private metrics: cluster_control_plane_gateway_adapter_metrics_t = {
    request_total: 0,
    request_success_total: 0,
    request_failure_total: 0,
    retry_total: 0,
    endpoint_failure_total: 0,
    last_success_unix_ms: null
  };

  constructor(params: cluster_control_plane_gateway_adapter_constructor_params_t) {
    this.request_timeout_ms = params.request_timeout_ms ?? 2_000;
    this.retry_base_delay_ms = params.retry_base_delay_ms ?? 100;
    this.retry_max_delay_ms = params.retry_max_delay_ms ?? 2_000;
    this.endpoint_cooldown_ms = params.endpoint_cooldown_ms ?? 2_000;
    this.max_request_attempts = params.max_request_attempts ?? 4;

    this.static_auth_headers = NormalizeHeaders({
      headers: params.auth_headers
    });
    this.auth_headers_provider = params.auth_headers_provider;

    const endpoint_list = Array.isArray(params.endpoint_list)
      ? params.endpoint_list
      : params.endpoint
        ? [params.endpoint]
        : [];

    if (endpoint_list.length === 0) {
      throw new Error(
        'ClusterControlPlaneGatewayAdapter requires endpoint or endpoint_list.'
      );
    }

    this.setEndpointList({
      endpoint_list
    });
  }

  setEndpointList(params: {
    endpoint_list: cluster_control_plane_gateway_adapter_endpoint_t[];
  }): void {
    const next_endpoint_state_list: cluster_control_plane_gateway_adapter_endpoint_state_t[] = [];

    for (const endpoint of params.endpoint_list) {
      const request_path = BuildRequestPath({
        request_path: endpoint.request_path
      });
      const endpoint_id = `${endpoint.host}:${endpoint.port}${request_path}`;

      const existing_endpoint_state = this.endpoint_state_list.find((state): boolean => {
        return state.endpoint_id === endpoint_id;
      });

      next_endpoint_state_list.push({
        endpoint_id,
        host: endpoint.host,
        port: endpoint.port,
        request_path,
        tls_mode: endpoint.tls_mode ?? 'disabled',
        failure_count: existing_endpoint_state?.failure_count ?? 0,
        last_failure_unix_ms: existing_endpoint_state?.last_failure_unix_ms ?? null,
        cooldown_until_unix_ms: existing_endpoint_state?.cooldown_until_unix_ms ?? 0,
        last_success_unix_ms: existing_endpoint_state?.last_success_unix_ms ?? null
      });
    }

    this.endpoint_state_list = next_endpoint_state_list;
  }

  getMetrics(): cluster_control_plane_gateway_adapter_metrics_t {
    return {
      ...this.metrics
    };
  }

  getSnapshot(): cluster_control_plane_gateway_adapter_snapshot_t {
    return {
      endpoint_list: this.endpoint_state_list.map((endpoint_state) => {
        return {
          endpoint_id: endpoint_state.endpoint_id,
          host: endpoint_state.host,
          port: endpoint_state.port,
          request_path: endpoint_state.request_path,
          tls_mode: endpoint_state.tls_mode,
          failure_count: endpoint_state.failure_count,
          cooldown_until_unix_ms: endpoint_state.cooldown_until_unix_ms,
          last_success_unix_ms: endpoint_state.last_success_unix_ms
        };
      }),
      metrics: this.getMetrics()
    };
  }

  async registerGateway(params: {
    gateway_id: string;
    node_reference: cluster_control_plane_node_reference_t;
    address: cluster_control_plane_gateway_address_t;
    status: 'active' | 'degraded' | 'draining';
    gateway_version: string;
    lease_ttl_ms?: number;
    metadata?: Record<string, unknown>;
    trace_id?: string;
  }): Promise<{
    gateway_record: cluster_control_plane_gateway_record_t;
    topology_snapshot: cluster_control_plane_topology_snapshot_t;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_gateway_register',
      trace_id: params.trace_id,
      request_data: {
        gateway_id: params.gateway_id,
        node_reference: params.node_reference,
        address: params.address,
        status: params.status,
        gateway_version: params.gateway_version,
        lease_ttl_ms: params.lease_ttl_ms,
        metadata: params.metadata
      }
    });

    return response_data as {
      gateway_record: cluster_control_plane_gateway_record_t;
      topology_snapshot: cluster_control_plane_topology_snapshot_t;
    };
  }

  async heartbeatGateway(params: {
    gateway_id: string;
    node_reference?: cluster_control_plane_node_reference_t;
    status?: 'active' | 'degraded' | 'draining' | 'offline';
    gateway_version?: string;
    lease_ttl_ms?: number;
    metadata?: Record<string, unknown>;
    trace_id?: string;
  }): Promise<{
    gateway_record: cluster_control_plane_gateway_record_t;
    topology_snapshot: cluster_control_plane_topology_snapshot_t;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_gateway_heartbeat',
      trace_id: params.trace_id,
      request_data: {
        gateway_id: params.gateway_id,
        node_reference: params.node_reference,
        status: params.status,
        gateway_version: params.gateway_version,
        lease_ttl_ms: params.lease_ttl_ms,
        metadata: params.metadata
      }
    });

    return response_data as {
      gateway_record: cluster_control_plane_gateway_record_t;
      topology_snapshot: cluster_control_plane_topology_snapshot_t;
    };
  }

  async deregisterGateway(params: {
    gateway_id: string;
    reason?: string;
    trace_id?: string;
  }): Promise<{
    deregistered: boolean;
    topology_snapshot: cluster_control_plane_topology_snapshot_t;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_gateway_deregister',
      trace_id: params.trace_id,
      request_data: {
        gateway_id: params.gateway_id,
        reason: params.reason
      }
    });

    return response_data as {
      deregistered: boolean;
      topology_snapshot: cluster_control_plane_topology_snapshot_t;
    };
  }

  async getTopologySnapshot(params: {
    trace_id?: string;
  } = {}): Promise<{
    topology_snapshot: cluster_control_plane_topology_snapshot_t;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_get_topology_snapshot',
      trace_id: params.trace_id,
      request_data: {}
    });

    return response_data as {
      topology_snapshot: cluster_control_plane_topology_snapshot_t;
    };
  }

  async getPolicySnapshot(params: {
    trace_id?: string;
  } = {}): Promise<{
    active_policy_version_id: string | null;
    active_policy_version: cluster_control_plane_policy_version_record_t | null;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_get_policy_snapshot',
      trace_id: params.trace_id,
      request_data: {}
    });

    return response_data as {
      active_policy_version_id: string | null;
      active_policy_version: cluster_control_plane_policy_version_record_t | null;
    };
  }

  async subscribeUpdates(params: {
    gateway_id?: string;
    known_policy_version_id?: string;
    known_service_generation?: number;
    include_topology?: boolean;
    trace_id?: string;
  }): Promise<{
    changed: boolean;
    topology_snapshot?: cluster_control_plane_topology_snapshot_t;
    active_policy_version_id: string | null;
    active_policy_version: cluster_control_plane_policy_version_record_t | null;
    service_generation: number;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_subscribe_updates',
      trace_id: params.trace_id,
      request_data: {
        gateway_id: params.gateway_id,
        known_policy_version_id: params.known_policy_version_id,
        known_service_generation: params.known_service_generation,
        include_topology: params.include_topology
      }
    });

    return response_data as {
      changed: boolean;
      topology_snapshot?: cluster_control_plane_topology_snapshot_t;
      active_policy_version_id: string | null;
      active_policy_version: cluster_control_plane_policy_version_record_t | null;
      service_generation: number;
    };
  }

  async announceConfigVersion(params: {
    gateway_id: string;
    policy_version_id: string;
    trace_id?: string;
  }): Promise<{
    acknowledged: boolean;
    gateway_record: cluster_control_plane_gateway_record_t | null;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_config_version_announce',
      trace_id: params.trace_id,
      request_data: {
        gateway_id: params.gateway_id,
        policy_version_id: params.policy_version_id
      }
    });

    return response_data as {
      acknowledged: boolean;
      gateway_record: cluster_control_plane_gateway_record_t | null;
    };
  }

  async acknowledgeConfigVersion(params: {
    gateway_id: string;
    policy_version_id: string;
    applied: boolean;
    details?: Record<string, unknown>;
    trace_id?: string;
  }): Promise<{
    acknowledged: boolean;
    gateway_record: cluster_control_plane_gateway_record_t | null;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_config_version_ack',
      trace_id: params.trace_id,
      request_data: {
        gateway_id: params.gateway_id,
        policy_version_id: params.policy_version_id,
        applied: params.applied,
        details: params.details
      }
    });

    return response_data as {
      acknowledged: boolean;
      gateway_record: cluster_control_plane_gateway_record_t | null;
    };
  }

  async updateMutationIntent(params: {
    mutation_id: string;
    status: cluster_control_plane_mutation_status_t;
    gateway_id?: string;
    dispatch_intent?: Record<string, unknown>;
    details?: Record<string, unknown>;
    trace_id?: string;
  }): Promise<{
    mutation_tracking_record: cluster_control_plane_mutation_tracking_record_t;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_mutation_intent_update',
      trace_id: params.trace_id,
      request_data: {
        mutation_id: params.mutation_id,
        status: params.status,
        gateway_id: params.gateway_id,
        dispatch_intent: params.dispatch_intent,
        details: params.details
      }
    });

    return response_data as {
      mutation_tracking_record: cluster_control_plane_mutation_tracking_record_t;
    };
  }

  async getMutationStatus(params: {
    mutation_id: string;
    trace_id?: string;
  }): Promise<{
    mutation_tracking_record: cluster_control_plane_mutation_tracking_record_t | null;
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_get_mutation_status',
      trace_id: params.trace_id,
      request_data: {
        mutation_id: params.mutation_id
      }
    });

    return response_data as {
      mutation_tracking_record: cluster_control_plane_mutation_tracking_record_t | null;
    };
  }

  async getServiceStatus(params: {
    include_events_limit?: number;
    trace_id?: string;
  } = {}): Promise<{
    service_status: cluster_control_plane_service_status_snapshot_t;
    metrics: Record<string, unknown>;
    event_list?: Record<string, unknown>[];
  }> {
    const response_data = await this.sendControlPlaneRequest({
      message_type: 'cluster_control_plane_get_service_status',
      trace_id: params.trace_id,
      request_data: {
        include_events_limit: params.include_events_limit
      }
    });

    return response_data as {
      service_status: cluster_control_plane_service_status_snapshot_t;
      metrics: Record<string, unknown>;
      event_list?: Record<string, unknown>[];
    };
  }

  private async sendControlPlaneRequest(params: {
    message_type: cluster_control_plane_protocol_request_message_type_t;
    request_data: Record<string, unknown>;
    trace_id?: string;
  }): Promise<Record<string, unknown>> {
    let last_error: Error | null = null;

    for (
      let attempt_index = 1;
      attempt_index <= this.max_request_attempts;
      attempt_index += 1
    ) {
      const endpoint_state = this.pickNextEndpointState();
      if (!endpoint_state) {
        throw new Error('No control-plane endpoints are configured.');
      }

      const request_message: cluster_control_plane_request_message_t = {
        protocol_version: 1,
        message_type: params.message_type,
        timestamp_unix_ms: Date.now(),
        request_id: `control_plane_gateway_adapter_${params.message_type}_${this.next_request_index++}`,
        trace_id: params.trace_id,
        ...(params.request_data as Record<string, unknown>)
      } as cluster_control_plane_request_message_t;

      this.metrics.request_total += 1;

      try {
        const response_text = await this.sendRequestToEndpoint({
          endpoint_state,
          request_message
        });
        const parsed_response = JSON.parse(response_text) as unknown;

        const success_result = ParseClusterControlPlaneResponseSuccessMessage({
          message: parsed_response
        });
        if (success_result.ok) {
          this.markEndpointSuccess({
            endpoint_state
          });

          this.metrics.request_success_total += 1;
          this.metrics.last_success_unix_ms = Date.now();
          return success_result.value.data as Record<string, unknown>;
        }

        const error_result = ParseClusterControlPlaneResponseErrorMessage({
          message: parsed_response
        });

        if (!error_result.ok) {
          this.markEndpointFailure({
            endpoint_state
          });
          throw new Error(error_result.error.message);
        }

        const operation_error = new Error(
          `${error_result.value.error.code}: ${error_result.value.error.message}`
        ) as cluster_control_plane_gateway_adapter_operation_error_t;
        operation_error.protocol_error = error_result.value.error;

        if (!error_result.value.error.retryable) {
          this.markEndpointFailure({
            endpoint_state
          });

          this.metrics.request_failure_total += 1;
          throw operation_error;
        }

        this.markEndpointFailure({
          endpoint_state
        });
        last_error = operation_error;
      } catch (error) {
        this.markEndpointFailure({
          endpoint_state
        });
        last_error = error instanceof Error ? error : new Error(String(error));
      }

      if (attempt_index >= this.max_request_attempts) {
        break;
      }

      this.metrics.retry_total += 1;
      const retry_delay_ms = BuildRetryDelayMs({
        retry_attempt: attempt_index,
        retry_base_delay_ms: this.retry_base_delay_ms,
        retry_max_delay_ms: this.retry_max_delay_ms
      });

      await Sleep({
        delay_ms: retry_delay_ms
      });
    }

    this.metrics.request_failure_total += 1;
    throw last_error ?? new Error('Control-plane request failed.');
  }

  private pickNextEndpointState():
    | cluster_control_plane_gateway_adapter_endpoint_state_t
    | null {
    if (this.endpoint_state_list.length === 0) {
      return null;
    }

    const now_unix_ms = Date.now();
    const available_endpoint_list = this.endpoint_state_list.filter((endpoint_state): boolean => {
      return endpoint_state.cooldown_until_unix_ms <= now_unix_ms;
    });

    const candidate_endpoint_list =
      available_endpoint_list.length > 0 ? available_endpoint_list : this.endpoint_state_list;

    candidate_endpoint_list.sort((left_endpoint, right_endpoint): number => {
      const left_score =
        left_endpoint.failure_count * 10 +
        (left_endpoint.last_success_unix_ms ? 0 : 1);
      const right_score =
        right_endpoint.failure_count * 10 +
        (right_endpoint.last_success_unix_ms ? 0 : 1);

      if (left_score !== right_score) {
        return left_score - right_score;
      }

      return left_endpoint.endpoint_id.localeCompare(right_endpoint.endpoint_id);
    });

    return candidate_endpoint_list[0];
  }

  private async sendRequestToEndpoint(params: {
    endpoint_state: cluster_control_plane_gateway_adapter_endpoint_state_t;
    request_message: cluster_control_plane_request_message_t;
  }): Promise<string> {
    const authority = BuildTransportAuthority({
      host: params.endpoint_state.host,
      port: params.endpoint_state.port,
      tls_mode: params.endpoint_state.tls_mode
    });

    const client_session: ClientHttp2Session = connect(authority);
    client_session.on('error', (): void => {
      // request_stream error handling below captures operation failure paths;
      // this prevents unhandled session errors during endpoint outage windows.
    });

    return await new Promise<string>(async (resolve, reject): Promise<void> => {
      const timeout_handle = setTimeout((): void => {
        try {
          client_session.close();
        } catch {
          // Ignore close errors while timing out.
        }

        reject(
          new Error(
            `Control-plane request timed out after ${this.request_timeout_ms}ms.`
          )
        );
      }, this.request_timeout_ms);

      try {
        const runtime_auth_headers = this.auth_headers_provider
          ? NormalizeHeaders({
              headers: await this.auth_headers_provider()
            })
          : {};

        const request_stream = client_session.request({
          ':method': 'POST',
          ':path': params.endpoint_state.request_path,
          'content-type': 'application/json',
          ...this.static_auth_headers,
          ...runtime_auth_headers
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
          clearTimeout(timeout_handle);
          try {
            client_session.close();
          } catch {
            // Ignore close errors.
          }

          resolve(Buffer.concat(chunk_list).toString('utf8'));
        });

        request_stream.on('error', (error): void => {
          clearTimeout(timeout_handle);
          try {
            client_session.close();
          } catch {
            // Ignore close errors.
          }

          reject(error);
        });

        request_stream.end(JSON.stringify(params.request_message));
      } catch (error) {
        clearTimeout(timeout_handle);
        try {
          client_session.close();
        } catch {
          // Ignore close errors.
        }

        reject(error);
      }
    });
  }

  private markEndpointSuccess(params: {
    endpoint_state: cluster_control_plane_gateway_adapter_endpoint_state_t;
  }): void {
    params.endpoint_state.failure_count = 0;
    params.endpoint_state.last_success_unix_ms = Date.now();
    params.endpoint_state.cooldown_until_unix_ms = 0;
  }

  private markEndpointFailure(params: {
    endpoint_state: cluster_control_plane_gateway_adapter_endpoint_state_t;
  }): void {
    params.endpoint_state.failure_count += 1;
    params.endpoint_state.last_failure_unix_ms = Date.now();
    params.endpoint_state.cooldown_until_unix_ms =
      Date.now() + this.endpoint_cooldown_ms;
    this.metrics.endpoint_failure_total += 1;
  }
}
