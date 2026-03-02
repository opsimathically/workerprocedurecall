import { createHash } from 'node:crypto';
import type { cluster_protocol_routing_mode_t } from '../clusterprotocol/ClusterProtocolTypes';

export type cluster_call_node_health_state_t =
  | 'ready'
  | 'degraded'
  | 'restarting'
  | 'stopped';

export type cluster_call_node_metrics_t = {
  inflight_calls: number;
  pending_calls: number;
  success_rate_1m: number;
  timeout_rate_1m: number;
  ewma_latency_ms: number;
};

export type cluster_call_node_capability_t = {
  function_name: string;
  function_hash_sha1: string;
  installed: boolean;
};

export type cluster_call_routing_candidate_node_t = {
  node_id: string;
  labels?: Record<string, string>;
  zones?: string[];
  health_state: cluster_call_node_health_state_t;
  last_health_update_unix_ms: number;
  metrics: cluster_call_node_metrics_t;
  capability_list: cluster_call_node_capability_t[];
};

export type cluster_call_routing_hint_t = {
  mode: cluster_protocol_routing_mode_t;
  target_node_id?: string;
  affinity_key?: string;
  zone?: string;
};

export type cluster_call_routing_request_t = {
  request_id: string;
  function_name: string;
  function_hash_sha1?: string;
  routing_hint: cluster_call_routing_hint_t;
  now_unix_ms: number;
  candidate_node_list: cluster_call_routing_candidate_node_t[];
};

export type cluster_call_routing_error_reason_t =
  | 'target_node_missing'
  | 'target_node_unhealthy'
  | 'target_node_missing_capability'
  | 'no_healthy_nodes'
  | 'no_capable_nodes';

export type cluster_call_routing_result_t =
  | {
      ok: true;
      selected_node: cluster_call_routing_candidate_node_t;
      fallback_node_list: cluster_call_routing_candidate_node_t[];
      filtered_unhealthy_node_count: number;
      filtered_capability_node_count: number;
      filtered_zone_node_count: number;
      routing_mode: cluster_protocol_routing_mode_t;
    }
  | {
      ok: false;
      reason: cluster_call_routing_error_reason_t;
      details: Record<string, unknown>;
      filtered_unhealthy_node_count: number;
      filtered_capability_node_count: number;
      filtered_zone_node_count: number;
      routing_mode: cluster_protocol_routing_mode_t;
    };

export type cluster_call_routing_config_t = {
  heartbeat_ttl_ms: number;
  sticky_ttl_ms: number;
  allow_degraded_node_routing: boolean;
  inflight_weight: number;
  latency_weight: number;
  error_weight: number;
  degraded_penalty: number;
};

type cluster_call_sticky_route_record_t = {
  node_id: string;
  expires_unix_ms: number;
};

function Clamp(params: {
  value: number;
  min_value: number;
  max_value: number;
}): number {
  return Math.max(params.min_value, Math.min(params.max_value, params.value));
}

function NormalizeSafePositiveNumber(params: { value: number }): number {
  if (!Number.isFinite(params.value) || params.value < 0) {
    return 0;
  }

  return params.value;
}

function BuildDeterministicFloat(params: { value: string }): number {
  const digest = createHash('sha1').update(params.value, 'utf8').digest();
  const numerator = digest.readUInt32BE(0);
  return numerator / 0xffffffff;
}

function BuildNodeSupportsFunction(params: {
  node: cluster_call_routing_candidate_node_t;
  function_name: string;
  function_hash_sha1?: string;
}): boolean {
  const { node, function_name, function_hash_sha1 } = params;
  if (!Array.isArray(node.capability_list) || node.capability_list.length === 0) {
    return true;
  }

  for (const capability of node.capability_list) {
    if (!capability.installed || capability.function_name !== function_name) {
      continue;
    }

    if (typeof function_hash_sha1 === 'string') {
      if (capability.function_hash_sha1 === function_hash_sha1) {
        return true;
      }
      continue;
    }

    return true;
  }

  return false;
}

function BuildEffectiveHealthState(params: {
  node: cluster_call_routing_candidate_node_t;
  now_unix_ms: number;
  heartbeat_ttl_ms: number;
}): cluster_call_node_health_state_t | 'stale' {
  const { node, now_unix_ms, heartbeat_ttl_ms } = params;
  if (now_unix_ms - node.last_health_update_unix_ms > heartbeat_ttl_ms) {
    return 'stale';
  }

  return node.health_state;
}

export class ClusterCallRoutingStrategy {
  private routing_config: cluster_call_routing_config_t = {
    heartbeat_ttl_ms: 30_000,
    sticky_ttl_ms: 120_000,
    allow_degraded_node_routing: true,
    inflight_weight: 0.5,
    latency_weight: 0.4,
    error_weight: 0.1,
    degraded_penalty: 0.2
  };

  private sticky_route_by_affinity_key = new Map<
    string,
    cluster_call_sticky_route_record_t
  >();

  setRoutingConfig(params: {
    routing_config: Partial<cluster_call_routing_config_t>;
  }): void {
    const next_config = params.routing_config;

    this.routing_config = {
      heartbeat_ttl_ms: next_config.heartbeat_ttl_ms ?? this.routing_config.heartbeat_ttl_ms,
      sticky_ttl_ms: next_config.sticky_ttl_ms ?? this.routing_config.sticky_ttl_ms,
      allow_degraded_node_routing:
        next_config.allow_degraded_node_routing ??
        this.routing_config.allow_degraded_node_routing,
      inflight_weight: next_config.inflight_weight ?? this.routing_config.inflight_weight,
      latency_weight: next_config.latency_weight ?? this.routing_config.latency_weight,
      error_weight: next_config.error_weight ?? this.routing_config.error_weight,
      degraded_penalty: next_config.degraded_penalty ?? this.routing_config.degraded_penalty
    };
  }

  getRoutingConfig(): cluster_call_routing_config_t {
    return {
      ...this.routing_config
    };
  }

  selectNode(params: cluster_call_routing_request_t): cluster_call_routing_result_t {
    const { routing_hint, candidate_node_list, function_name, function_hash_sha1, now_unix_ms } =
      params;

    this.removeExpiredStickyRoutes({
      now_unix_ms
    });

    const filtered_by_zone_node_list = candidate_node_list.filter((node): boolean => {
      if (typeof routing_hint.zone !== 'string' || routing_hint.zone.length === 0) {
        return true;
      }

      if (!Array.isArray(node.zones)) {
        return false;
      }

      return node.zones.includes(routing_hint.zone);
    });

    const filtered_zone_node_count = candidate_node_list.length - filtered_by_zone_node_list.length;

    const healthy_node_list: cluster_call_routing_candidate_node_t[] = [];
    let filtered_unhealthy_node_count = 0;

    for (const node of filtered_by_zone_node_list) {
      const effective_health_state = BuildEffectiveHealthState({
        node,
        now_unix_ms,
        heartbeat_ttl_ms: this.routing_config.heartbeat_ttl_ms
      });

      if (effective_health_state === 'ready') {
        healthy_node_list.push(node);
        continue;
      }

      if (
        effective_health_state === 'degraded' &&
        this.routing_config.allow_degraded_node_routing
      ) {
        healthy_node_list.push(node);
        continue;
      }

      filtered_unhealthy_node_count += 1;
    }

    if (healthy_node_list.length === 0) {
      return {
        ok: false,
        reason: 'no_healthy_nodes',
        details: {
          candidate_node_count: candidate_node_list.length
        },
        filtered_unhealthy_node_count,
        filtered_capability_node_count: 0,
        filtered_zone_node_count,
        routing_mode: routing_hint.mode
      };
    }

    const capability_eligible_node_list = healthy_node_list.filter((node): boolean => {
      return BuildNodeSupportsFunction({
        node,
        function_name,
        function_hash_sha1
      });
    });

    const filtered_capability_node_count =
      healthy_node_list.length - capability_eligible_node_list.length;

    if (capability_eligible_node_list.length === 0) {
      return {
        ok: false,
        reason: 'no_capable_nodes',
        details: {
          function_name,
          function_hash_sha1
        },
        filtered_unhealthy_node_count,
        filtered_capability_node_count,
        filtered_zone_node_count,
        routing_mode: routing_hint.mode
      };
    }

    if (routing_hint.mode === 'target_node') {
      const target_node_id = routing_hint.target_node_id;
      if (typeof target_node_id !== 'string' || target_node_id.length === 0) {
        return {
          ok: false,
          reason: 'target_node_missing',
          details: {
            field_name: 'routing_hint.target_node_id'
          },
          filtered_unhealthy_node_count,
          filtered_capability_node_count,
          filtered_zone_node_count,
          routing_mode: routing_hint.mode
        };
      }

      const target_node = candidate_node_list.find((node): boolean => {
        return node.node_id === target_node_id;
      });
      if (!target_node) {
        return {
          ok: false,
          reason: 'target_node_missing',
          details: {
            target_node_id
          },
          filtered_unhealthy_node_count,
          filtered_capability_node_count,
          filtered_zone_node_count,
          routing_mode: routing_hint.mode
        };
      }

      const target_is_eligible = capability_eligible_node_list.some((node): boolean => {
        return node.node_id === target_node_id;
      });
      if (!target_is_eligible) {
        const target_in_healthy_set = healthy_node_list.some((node): boolean => {
          return node.node_id === target_node_id;
        });

        return {
          ok: false,
          reason: target_in_healthy_set
            ? 'target_node_missing_capability'
            : 'target_node_unhealthy',
          details: {
            target_node_id,
            function_name,
            function_hash_sha1
          },
          filtered_unhealthy_node_count,
          filtered_capability_node_count,
          filtered_zone_node_count,
          routing_mode: routing_hint.mode
        };
      }

      return {
        ok: true,
        selected_node: target_node,
        fallback_node_list: [],
        filtered_unhealthy_node_count,
        filtered_capability_node_count,
        filtered_zone_node_count,
        routing_mode: routing_hint.mode
      };
    }

    if (routing_hint.mode === 'affinity') {
      return this.selectAffinityNode({
        request: params,
        candidate_node_list: capability_eligible_node_list,
        filtered_unhealthy_node_count,
        filtered_capability_node_count,
        filtered_zone_node_count
      });
    }

    return this.selectAutoNode({
      request: params,
      candidate_node_list: capability_eligible_node_list,
      filtered_unhealthy_node_count,
      filtered_capability_node_count,
      filtered_zone_node_count
    });
  }

  private selectAffinityNode(params: {
    request: cluster_call_routing_request_t;
    candidate_node_list: cluster_call_routing_candidate_node_t[];
    filtered_unhealthy_node_count: number;
    filtered_capability_node_count: number;
    filtered_zone_node_count: number;
  }): cluster_call_routing_result_t {
    const {
      request,
      candidate_node_list,
      filtered_unhealthy_node_count,
      filtered_capability_node_count,
      filtered_zone_node_count
    } = params;

    const affinity_key = request.routing_hint.affinity_key ?? request.request_id;
    const sticky_record = this.sticky_route_by_affinity_key.get(affinity_key);
    if (sticky_record) {
      const sticky_node = candidate_node_list.find((node): boolean => {
        return node.node_id === sticky_record.node_id;
      });
      if (sticky_node) {
        return {
          ok: true,
          selected_node: sticky_node,
          fallback_node_list: candidate_node_list.filter((node): boolean => {
            return node.node_id !== sticky_node.node_id;
          }),
          filtered_unhealthy_node_count,
          filtered_capability_node_count,
          filtered_zone_node_count,
          routing_mode: 'affinity'
        };
      }
    }

    const scored_node_list = candidate_node_list
      .map((node): { node: cluster_call_routing_candidate_node_t; score: number } => {
        const hash_score = BuildDeterministicFloat({
          value: `${affinity_key}|${node.node_id}`
        });
        const load_penalty =
          1 +
          NormalizeSafePositiveNumber({ value: node.metrics.inflight_calls }) +
          NormalizeSafePositiveNumber({ value: node.metrics.pending_calls });

        let score = hash_score / load_penalty;
        if (node.health_state === 'degraded') {
          score *= 0.8;
        }

        return {
          node,
          score
        };
      })
      .sort((left_item, right_item): number => {
        if (right_item.score !== left_item.score) {
          return right_item.score - left_item.score;
        }
        return left_item.node.node_id.localeCompare(right_item.node.node_id);
      });

    const selected_node = scored_node_list[0].node;
    this.sticky_route_by_affinity_key.set(affinity_key, {
      node_id: selected_node.node_id,
      expires_unix_ms: request.now_unix_ms + this.routing_config.sticky_ttl_ms
    });

    return {
      ok: true,
      selected_node,
      fallback_node_list: scored_node_list.slice(1).map((entry) => entry.node),
      filtered_unhealthy_node_count,
      filtered_capability_node_count,
      filtered_zone_node_count,
      routing_mode: 'affinity'
    };
  }

  private selectAutoNode(params: {
    request: cluster_call_routing_request_t;
    candidate_node_list: cluster_call_routing_candidate_node_t[];
    filtered_unhealthy_node_count: number;
    filtered_capability_node_count: number;
    filtered_zone_node_count: number;
  }): cluster_call_routing_result_t {
    const {
      request,
      candidate_node_list,
      filtered_unhealthy_node_count,
      filtered_capability_node_count,
      filtered_zone_node_count
    } = params;

    const scored_node_list = this.scoreAutoNodeList({
      candidate_node_list
    });

    if (scored_node_list.length === 1) {
      return {
        ok: true,
        selected_node: scored_node_list[0].node,
        fallback_node_list: [],
        filtered_unhealthy_node_count,
        filtered_capability_node_count,
        filtered_zone_node_count,
        routing_mode: 'auto'
      };
    }

    const first_random = BuildDeterministicFloat({
      value: `${request.request_id}|p2c|0`
    });
    const second_random = BuildDeterministicFloat({
      value: `${request.request_id}|p2c|1`
    });

    const first_index = Math.floor(first_random * scored_node_list.length);
    let second_index = Math.floor(second_random * scored_node_list.length);
    if (second_index === first_index) {
      second_index = (second_index + 1) % scored_node_list.length;
    }

    const first_choice = scored_node_list[first_index];
    const second_choice = scored_node_list[second_index];
    const selected_entry =
      first_choice.score <= second_choice.score ? first_choice : second_choice;

    const fallback_node_list = scored_node_list
      .filter((entry): boolean => {
        return entry.node.node_id !== selected_entry.node.node_id;
      })
      .sort((left_item, right_item): number => {
        if (left_item.score !== right_item.score) {
          return left_item.score - right_item.score;
        }
        return left_item.node.node_id.localeCompare(right_item.node.node_id);
      })
      .map((entry) => entry.node);

    return {
      ok: true,
      selected_node: selected_entry.node,
      fallback_node_list,
      filtered_unhealthy_node_count,
      filtered_capability_node_count,
      filtered_zone_node_count,
      routing_mode: 'auto'
    };
  }

  private scoreAutoNodeList(params: {
    candidate_node_list: cluster_call_routing_candidate_node_t[];
  }): { node: cluster_call_routing_candidate_node_t; score: number }[] {
    const node_load_list = params.candidate_node_list.map((node): number => {
      return (
        NormalizeSafePositiveNumber({ value: node.metrics.inflight_calls }) +
        NormalizeSafePositiveNumber({ value: node.metrics.pending_calls })
      );
    });
    const node_latency_list = params.candidate_node_list.map((node): number => {
      return NormalizeSafePositiveNumber({ value: node.metrics.ewma_latency_ms });
    });
    const max_load = Math.max(1, ...node_load_list);
    const max_latency = Math.max(1, ...node_latency_list);

    return params.candidate_node_list.map((node, index) => {
      const normalized_inflight = node_load_list[index] / max_load;
      const normalized_latency = node_latency_list[index] / max_latency;

      const timeout_rate = Clamp({
        value: NormalizeSafePositiveNumber({ value: node.metrics.timeout_rate_1m }),
        min_value: 0,
        max_value: 1
      });
      const success_rate = Clamp({
        value: NormalizeSafePositiveNumber({ value: node.metrics.success_rate_1m }),
        min_value: 0,
        max_value: 1
      });

      const error_penalty = Clamp({
        value: timeout_rate + (1 - success_rate),
        min_value: 0,
        max_value: 1
      });

      let score =
        this.routing_config.inflight_weight * normalized_inflight +
        this.routing_config.latency_weight * normalized_latency +
        this.routing_config.error_weight * error_penalty;

      if (node.health_state === 'degraded') {
        score += this.routing_config.degraded_penalty;
      }

      return {
        node,
        score
      };
    });
  }

  private removeExpiredStickyRoutes(params: { now_unix_ms: number }): void {
    const { now_unix_ms } = params;
    for (const [affinity_key, sticky_record] of this.sticky_route_by_affinity_key) {
      if (sticky_record.expires_unix_ms <= now_unix_ms) {
        this.sticky_route_by_affinity_key.delete(affinity_key);
      }
    }
  }
}
