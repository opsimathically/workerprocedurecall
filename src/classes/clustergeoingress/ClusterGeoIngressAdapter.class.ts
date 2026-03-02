import { connect, type ClientHttp2Session } from 'node:http2';

import type {
  cluster_geo_ingress_global_routing_policy_snapshot_t,
  cluster_geo_ingress_instance_health_status_t,
  cluster_geo_ingress_instance_record_t,
  cluster_geo_ingress_policy_version_record_t,
  cluster_geo_ingress_protocol_error_t,
  cluster_geo_ingress_protocol_request_message_type_t,
  cluster_geo_ingress_region_status_t,
  cluster_geo_ingress_request_message_t,
  cluster_geo_ingress_topology_snapshot_t
} from './ClusterGeoIngressProtocol';
import {
  ParseClusterGeoIngressResponseErrorMessage,
  ParseClusterGeoIngressResponseSuccessMessage
} from './ClusterGeoIngressProtocolValidators';

export type cluster_geo_ingress_adapter_auth_headers_provider_t = () =>
  | Record<string, string>
  | Promise<Record<string, string>>;

export type cluster_geo_ingress_adapter_endpoint_t = {
  host: string;
  port: number;
  request_path?: string;
  tls_mode?: 'disabled' | 'required';
};

export type cluster_geo_ingress_adapter_constructor_params_t = {
  endpoint?: cluster_geo_ingress_adapter_endpoint_t;
  endpoint_list?: cluster_geo_ingress_adapter_endpoint_t[];
  request_timeout_ms?: number;
  retry_base_delay_ms?: number;
  retry_max_delay_ms?: number;
  endpoint_cooldown_ms?: number;
  max_request_attempts?: number;
  auth_headers?: Record<string, string>;
  auth_headers_provider?: cluster_geo_ingress_adapter_auth_headers_provider_t;
};

type cluster_geo_ingress_adapter_endpoint_state_t = {
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

type cluster_geo_ingress_adapter_operation_error_t = Error & {
  protocol_error?: cluster_geo_ingress_protocol_error_t;
};

export type cluster_geo_ingress_adapter_metrics_t = {
  request_total: number;
  request_success_total: number;
  request_failure_total: number;
  retry_total: number;
  endpoint_failure_total: number;
  last_success_unix_ms: number | null;
};

export type cluster_geo_ingress_adapter_snapshot_t = {
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
  metrics: cluster_geo_ingress_adapter_metrics_t;
};

function BuildRequestPath(params: { request_path: string | undefined }): string {
  if (typeof params.request_path !== 'string' || params.request_path.length === 0) {
    return '/wpc/cluster/geo-ingress';
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

export class ClusterGeoIngressAdapter {
  private endpoint_state_list: cluster_geo_ingress_adapter_endpoint_state_t[] = [];

  private request_timeout_ms: number;
  private retry_base_delay_ms: number;
  private retry_max_delay_ms: number;
  private endpoint_cooldown_ms: number;
  private max_request_attempts: number;

  private readonly static_auth_headers: Record<string, string>;
  private readonly auth_headers_provider:
    | cluster_geo_ingress_adapter_auth_headers_provider_t
    | undefined;

  private next_request_index = 1;

  private metrics: cluster_geo_ingress_adapter_metrics_t = {
    request_total: 0,
    request_success_total: 0,
    request_failure_total: 0,
    retry_total: 0,
    endpoint_failure_total: 0,
    last_success_unix_ms: null
  };

  constructor(params: cluster_geo_ingress_adapter_constructor_params_t) {
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
      throw new Error('ClusterGeoIngressAdapter requires endpoint or endpoint_list.');
    }

    this.setEndpointList({
      endpoint_list
    });
  }

  setEndpointList(params: {
    endpoint_list: cluster_geo_ingress_adapter_endpoint_t[];
  }): void {
    const next_endpoint_state_list: cluster_geo_ingress_adapter_endpoint_state_t[] = [];

    for (const endpoint of params.endpoint_list) {
      const request_path = BuildRequestPath({
        request_path: endpoint.request_path
      });
      const endpoint_id = `${endpoint.host}:${endpoint.port}${request_path}`;

      const existing_endpoint_state = this.endpoint_state_list.find((endpoint_state): boolean => {
        return endpoint_state.endpoint_id === endpoint_id;
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

  getMetrics(): cluster_geo_ingress_adapter_metrics_t {
    return {
      ...this.metrics
    };
  }

  getSnapshot(): cluster_geo_ingress_adapter_snapshot_t {
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

  async registerIngress(params: {
    ingress_id: string;
    region_id: string;
    endpoint: {
      host: string;
      port: number;
      request_path: string;
      tls_mode: 'disabled' | 'required' | 'terminated_upstream';
    };
    health_status: 'ready' | 'overloaded' | 'degraded';
    ingress_version: string;
    lease_ttl_ms?: number;
    policy_version_id?: string;
    region_metadata?: {
      priority?: number;
      latency_slo_ms?: number;
      capacity_score?: number;
      latency_ewma_ms?: number;
      status?: cluster_geo_ingress_region_status_t;
    };
    metrics_summary?: {
      inflight_calls?: number;
      pending_calls?: number;
      success_rate_1m?: number;
      timeout_rate_1m?: number;
      ewma_latency_ms?: number;
    };
    metadata?: Record<string, unknown>;
    trace_id?: string;
  }): Promise<{
    ingress_instance_record: cluster_geo_ingress_instance_record_t;
    topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
  }> {
    const response_data = await this.sendGeoIngressRequest({
      message_type: 'cluster_geo_ingress_ingress_register',
      trace_id: params.trace_id,
      request_data: {
        ingress_id: params.ingress_id,
        region_id: params.region_id,
        endpoint: params.endpoint,
        health_status: params.health_status,
        ingress_version: params.ingress_version,
        lease_ttl_ms: params.lease_ttl_ms,
        policy_version_id: params.policy_version_id,
        region_metadata: params.region_metadata,
        metrics_summary: params.metrics_summary,
        metadata: params.metadata
      }
    });

    return response_data as {
      ingress_instance_record: cluster_geo_ingress_instance_record_t;
      topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
    };
  }

  async heartbeatIngress(params: {
    ingress_id: string;
    region_id?: string;
    health_status?: cluster_geo_ingress_instance_health_status_t;
    ingress_version?: string;
    lease_ttl_ms?: number;
    policy_version_id?: string;
    region_metadata?: {
      priority?: number;
      latency_slo_ms?: number;
      capacity_score?: number;
      latency_ewma_ms?: number;
      status?: cluster_geo_ingress_region_status_t;
    };
    metrics_summary?: {
      inflight_calls?: number;
      pending_calls?: number;
      success_rate_1m?: number;
      timeout_rate_1m?: number;
      ewma_latency_ms?: number;
    };
    metadata?: Record<string, unknown>;
    trace_id?: string;
  }): Promise<{
    ingress_instance_record: cluster_geo_ingress_instance_record_t;
    topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
  }> {
    const response_data = await this.sendGeoIngressRequest({
      message_type: 'cluster_geo_ingress_ingress_heartbeat',
      trace_id: params.trace_id,
      request_data: {
        ingress_id: params.ingress_id,
        region_id: params.region_id,
        health_status: params.health_status,
        ingress_version: params.ingress_version,
        lease_ttl_ms: params.lease_ttl_ms,
        policy_version_id: params.policy_version_id,
        region_metadata: params.region_metadata,
        metrics_summary: params.metrics_summary,
        metadata: params.metadata
      }
    });

    return response_data as {
      ingress_instance_record: cluster_geo_ingress_instance_record_t;
      topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
    };
  }

  async deregisterIngress(params: {
    ingress_id: string;
    reason?: string;
    trace_id?: string;
  }): Promise<{
    deregistered: boolean;
    topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
  }> {
    const response_data = await this.sendGeoIngressRequest({
      message_type: 'cluster_geo_ingress_ingress_deregister',
      trace_id: params.trace_id,
      request_data: {
        ingress_id: params.ingress_id,
        reason: params.reason
      }
    });

    return response_data as {
      deregistered: boolean;
      topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
    };
  }

  async publishGlobalRoutingPolicy(params: {
    actor_subject: string;
    policy_snapshot: cluster_geo_ingress_global_routing_policy_snapshot_t;
    requested_version_id?: string;
    expected_active_policy_version_id?: string;
    activate_immediately?: boolean;
    trace_id?: string;
  }): Promise<{
    policy_version_record: cluster_geo_ingress_policy_version_record_t;
    active_policy_version_id: string | null;
  }> {
    const response_data = await this.sendGeoIngressRequest({
      message_type: 'cluster_geo_ingress_publish_global_routing_policy',
      trace_id: params.trace_id,
      request_data: {
        actor_subject: params.actor_subject,
        policy_snapshot: params.policy_snapshot,
        requested_version_id: params.requested_version_id,
        expected_active_policy_version_id: params.expected_active_policy_version_id,
        activate_immediately: params.activate_immediately
      }
    });

    return response_data as {
      policy_version_record: cluster_geo_ingress_policy_version_record_t;
      active_policy_version_id: string | null;
    };
  }

  async getGlobalRoutingPolicy(params: {
    trace_id?: string;
  } = {}): Promise<{
    active_policy_version_id: string | null;
    active_policy_version_record: cluster_geo_ingress_policy_version_record_t | null;
  }> {
    const response_data = await this.sendGeoIngressRequest({
      message_type: 'cluster_geo_ingress_get_global_routing_policy',
      trace_id: params.trace_id,
      request_data: {}
    });

    return response_data as {
      active_policy_version_id: string | null;
      active_policy_version_record: cluster_geo_ingress_policy_version_record_t | null;
    };
  }

  async getGlobalTopologySnapshot(params: {
    trace_id?: string;
  } = {}): Promise<{
    topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
  }> {
    const response_data = await this.sendGeoIngressRequest({
      message_type: 'cluster_geo_ingress_get_global_topology_snapshot',
      trace_id: params.trace_id,
      request_data: {}
    });

    return response_data as {
      topology_snapshot: cluster_geo_ingress_topology_snapshot_t;
    };
  }

  async reportMetricsSummary(params: {
    ingress_id: string;
    metrics_summary: {
      inflight_calls?: number;
      pending_calls?: number;
      success_rate_1m?: number;
      timeout_rate_1m?: number;
      ewma_latency_ms?: number;
    };
    trace_id?: string;
  }): Promise<{
    acknowledged: boolean;
    ingress_instance_record: cluster_geo_ingress_instance_record_t | null;
  }> {
    const response_data = await this.sendGeoIngressRequest({
      message_type: 'cluster_geo_ingress_report_metrics_summary',
      trace_id: params.trace_id,
      request_data: {
        ingress_id: params.ingress_id,
        metrics_summary: params.metrics_summary
      }
    });

    return response_data as {
      acknowledged: boolean;
      ingress_instance_record: cluster_geo_ingress_instance_record_t | null;
    };
  }

  async subscribeUpdates(params: {
    ingress_id?: string;
    known_service_generation?: number;
    known_policy_version_id?: string;
    include_topology?: boolean;
    trace_id?: string;
  } = {}): Promise<{
    changed: boolean;
    topology_snapshot?: cluster_geo_ingress_topology_snapshot_t;
    active_policy_version_id: string | null;
    active_policy_version_record: cluster_geo_ingress_policy_version_record_t | null;
    service_generation: number;
  }> {
    const response_data = await this.sendGeoIngressRequest({
      message_type: 'cluster_geo_ingress_subscribe_updates',
      trace_id: params.trace_id,
      request_data: {
        ingress_id: params.ingress_id,
        known_service_generation: params.known_service_generation,
        known_policy_version_id: params.known_policy_version_id,
        include_topology: params.include_topology
      }
    });

    return response_data as {
      changed: boolean;
      topology_snapshot?: cluster_geo_ingress_topology_snapshot_t;
      active_policy_version_id: string | null;
      active_policy_version_record: cluster_geo_ingress_policy_version_record_t | null;
      service_generation: number;
    };
  }

  async getServiceStatus(params: {
    include_events_limit?: number;
    trace_id?: string;
  } = {}): Promise<{
    service_generation: number;
    active_policy_version_id: string | null;
    region_count: number;
    ingress_instance_count: number;
    metrics: Record<string, unknown>;
    event_list?: Record<string, unknown>[];
  }> {
    const response_data = await this.sendGeoIngressRequest({
      message_type: 'cluster_geo_ingress_get_service_status',
      trace_id: params.trace_id,
      request_data: {
        include_events_limit: params.include_events_limit
      }
    });

    return response_data as {
      service_generation: number;
      active_policy_version_id: string | null;
      region_count: number;
      ingress_instance_count: number;
      metrics: Record<string, unknown>;
      event_list?: Record<string, unknown>[];
    };
  }

  private async sendGeoIngressRequest(params: {
    message_type: cluster_geo_ingress_protocol_request_message_type_t;
    trace_id?: string;
    request_data: Record<string, unknown>;
  }): Promise<Record<string, unknown>> {
    this.metrics.request_total += 1;

    let last_error: cluster_geo_ingress_adapter_operation_error_t | null = null;

    for (
      let request_attempt_index = 1;
      request_attempt_index <= this.max_request_attempts;
      request_attempt_index += 1
    ) {
      const now_unix_ms = Date.now();
      const endpoint_state = this.selectEndpointState({
        now_unix_ms,
        request_attempt_index
      });

      if (!endpoint_state) {
        const no_endpoint_error = new Error(
          'No available geo ingress control-plane endpoint.'
        ) as cluster_geo_ingress_adapter_operation_error_t;
        no_endpoint_error.protocol_error = {
          code: 'GEO_INGRESS_UNAVAILABLE',
          message: 'No available geo ingress control-plane endpoint.',
          retryable: true,
          details: {
            request_attempt_index
          }
        };

        last_error = no_endpoint_error;
      } else {
        const request_id = `geo_ingress_client_request_${this.next_request_index++}`;
        const request_message: cluster_geo_ingress_request_message_t = {
          protocol_version: 1,
          message_type: params.message_type,
          timestamp_unix_ms: Date.now(),
          request_id,
          trace_id: params.trace_id,
          ...(params.request_data as object)
        } as cluster_geo_ingress_request_message_t;

        try {
          const parsed_response = await this.dispatchToEndpoint({
            endpoint_state,
            request_message
          });

          this.metrics.request_success_total += 1;
          this.metrics.last_success_unix_ms = Date.now();

          endpoint_state.failure_count = 0;
          endpoint_state.cooldown_until_unix_ms = 0;
          endpoint_state.last_success_unix_ms = Date.now();

          return parsed_response;
        } catch (error) {
          const operation_error =
            error instanceof Error
              ? (error as cluster_geo_ingress_adapter_operation_error_t)
              : (new Error(String(error)) as cluster_geo_ingress_adapter_operation_error_t);

          last_error = operation_error;

          endpoint_state.failure_count += 1;
          endpoint_state.last_failure_unix_ms = Date.now();
          endpoint_state.cooldown_until_unix_ms =
            Date.now() +
            Math.min(
              this.endpoint_cooldown_ms,
              this.retry_base_delay_ms * Math.max(1, endpoint_state.failure_count)
            );

          this.metrics.endpoint_failure_total += 1;
          this.metrics.retry_total += 1;

          if (
            operation_error.protocol_error &&
            operation_error.protocol_error.retryable === false
          ) {
            break;
          }
        }
      }

      if (request_attempt_index < this.max_request_attempts) {
        await Sleep({
          delay_ms: BuildRetryDelayMs({
            retry_attempt: request_attempt_index,
            retry_base_delay_ms: this.retry_base_delay_ms,
            retry_max_delay_ms: this.retry_max_delay_ms
          })
        });
      }
    }

    this.metrics.request_failure_total += 1;

    if (last_error?.protocol_error) {
      const error_message = new Error(last_error.protocol_error.message) as cluster_geo_ingress_adapter_operation_error_t;
      error_message.protocol_error = last_error.protocol_error;
      throw error_message;
    }

    throw (
      last_error ??
      new Error('Geo ingress request failed after max retries.')
    );
  }

  private selectEndpointState(params: {
    now_unix_ms: number;
    request_attempt_index: number;
  }): cluster_geo_ingress_adapter_endpoint_state_t | null {
    const available_endpoint_state_list = this.endpoint_state_list.filter((endpoint_state): boolean => {
      return endpoint_state.cooldown_until_unix_ms <= params.now_unix_ms;
    });

    const endpoint_state_list_to_use =
      available_endpoint_state_list.length > 0
        ? available_endpoint_state_list
        : this.endpoint_state_list;

    if (endpoint_state_list_to_use.length === 0) {
      return null;
    }

    endpoint_state_list_to_use.sort((left_endpoint_state, right_endpoint_state): number => {
      if (left_endpoint_state.failure_count !== right_endpoint_state.failure_count) {
        return left_endpoint_state.failure_count - right_endpoint_state.failure_count;
      }

      return left_endpoint_state.endpoint_id.localeCompare(right_endpoint_state.endpoint_id);
    });

    const endpoint_state_index =
      (params.request_attempt_index - 1) % endpoint_state_list_to_use.length;

    return endpoint_state_list_to_use[endpoint_state_index] ?? null;
  }

  private async dispatchToEndpoint(params: {
    endpoint_state: cluster_geo_ingress_adapter_endpoint_state_t;
    request_message: cluster_geo_ingress_request_message_t;
  }): Promise<Record<string, unknown>> {
    const authority = BuildTransportAuthority({
      host: params.endpoint_state.host,
      port: params.endpoint_state.port,
      tls_mode: params.endpoint_state.tls_mode
    });

    const response_text = await this.sendHttp2Request({
      authority,
      request_path: params.endpoint_state.request_path,
      request_body: JSON.stringify(params.request_message)
    });

    let parsed_response: unknown;
    try {
      parsed_response = JSON.parse(response_text) as unknown;
    } catch {
      const parse_error = new Error('Geo ingress response was not valid JSON.') as cluster_geo_ingress_adapter_operation_error_t;
      parse_error.protocol_error = {
        code: 'GEO_INGRESS_UNAVAILABLE',
        message: 'Geo ingress response was not valid JSON.',
        retryable: true,
        details: {
          endpoint_id: params.endpoint_state.endpoint_id
        }
      };
      throw parse_error;
    }

    const success_result = ParseClusterGeoIngressResponseSuccessMessage({
      message: parsed_response
    });
    if (success_result.ok) {
      return success_result.value.data as Record<string, unknown>;
    }

    const error_result = ParseClusterGeoIngressResponseErrorMessage({
      message: parsed_response
    });
    if (!error_result.ok) {
      const validation_error = new Error(
        success_result.error.message
      ) as cluster_geo_ingress_adapter_operation_error_t;
      validation_error.protocol_error = {
        code: 'GEO_INGRESS_UNAVAILABLE',
        message: success_result.error.message,
        retryable: true,
        details: success_result.error.details
      };
      throw validation_error;
    }

    const protocol_error = error_result.value.error;
    const operation_error = new Error(protocol_error.message) as cluster_geo_ingress_adapter_operation_error_t;
    operation_error.protocol_error = {
      code: protocol_error.code,
      message: protocol_error.message,
      retryable: protocol_error.retryable,
      details: protocol_error.details
    };
    throw operation_error;
  }

  private async sendHttp2Request(params: {
    authority: string;
    request_path: string;
    request_body: string;
  }): Promise<string> {
    const client_session: ClientHttp2Session = connect(params.authority);

    return await new Promise<string>(async (resolve, reject): Promise<void> => {
      const chunk_list: Buffer[] = [];
      let timeout_handle: NodeJS.Timeout | null = null;
      let settled = false;

      const resolveOnce = (response_text: string): void => {
        if (settled) {
          return;
        }
        settled = true;

        if (timeout_handle) {
          clearTimeout(timeout_handle);
        }

        try {
          client_session.close();
        } catch {
          // Ignore close errors.
        }

        resolve(response_text);
      };

      const rejectOnce = (error: unknown): void => {
        if (settled) {
          return;
        }
        settled = true;

        if (timeout_handle) {
          clearTimeout(timeout_handle);
        }

        try {
          client_session.close();
        } catch {
          // Ignore close errors.
        }

        reject(error);
      };

      client_session.on('error', (error): void => {
        const operation_error =
          error instanceof Error
            ? (error as cluster_geo_ingress_adapter_operation_error_t)
            : (new Error(String(error)) as cluster_geo_ingress_adapter_operation_error_t);
        operation_error.protocol_error = {
          code: 'GEO_INGRESS_UNAVAILABLE',
          message: operation_error.message,
          retryable: true,
          details: {
            reason: 'http2_session_error'
          }
        };

        rejectOnce(operation_error);
      });

      try {
        timeout_handle = setTimeout((): void => {
          const timeout_error = new Error(
            `Geo ingress request timed out after ${this.request_timeout_ms}ms.`
          ) as cluster_geo_ingress_adapter_operation_error_t;
          timeout_error.protocol_error = {
            code: 'GEO_INGRESS_TIMEOUT',
            message: timeout_error.message,
            retryable: true,
            details: {
              request_timeout_ms: this.request_timeout_ms
            }
          };

          rejectOnce(timeout_error);
        }, this.request_timeout_ms);

        const dynamic_auth_headers = this.auth_headers_provider
          ? NormalizeHeaders({
              headers: await this.auth_headers_provider()
            })
          : {};

        const request_headers: Record<string, string> = {
          ':method': 'POST',
          ':path': params.request_path,
          'content-type': 'application/json',
          ...this.static_auth_headers,
          ...dynamic_auth_headers
        };

        const request_stream = client_session.request(request_headers);

        request_stream.on('response', (): void => {
          // Headers unused currently.
        });

        request_stream.on('data', (chunk): void => {
          chunk_list.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
        });

        request_stream.on('error', (error): void => {
          const operation_error =
            error instanceof Error
              ? (error as cluster_geo_ingress_adapter_operation_error_t)
              : (new Error(String(error)) as cluster_geo_ingress_adapter_operation_error_t);
          operation_error.protocol_error = {
            code: 'GEO_INGRESS_UNAVAILABLE',
            message: operation_error.message,
            retryable: true,
            details: {
              reason: 'http2_stream_error'
            }
          };

          rejectOnce(operation_error);
        });

        request_stream.on('end', (): void => {
          resolveOnce(Buffer.concat(chunk_list).toString('utf8'));
        });

        request_stream.end(params.request_body);
      } catch (error) {
        rejectOnce(error);
      }
    });
  }
}
