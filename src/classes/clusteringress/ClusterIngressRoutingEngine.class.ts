import type { cluster_call_request_message_i } from '../clusterprotocol/ClusterProtocolTypes';
import type {
  ingress_balancer_routing_decision_t,
  ingress_balancer_target_record_t
} from './ClusterIngressBalancerProtocol';

export type cluster_ingress_routing_mode_t =
  | 'least_loaded'
  | 'weighted_rr'
  | 'affinity';

export type cluster_ingress_routing_engine_constructor_params_t = {
  default_mode?: cluster_ingress_routing_mode_t;
  sticky_ttl_ms?: number;
};

type ingress_affinity_mapping_t = {
  target_id: string;
  expires_unix_ms: number;
};

function ComputeTargetLoadScore(params: {
  target: ingress_balancer_target_record_t;
}): number {
  const { target } = params;
  const degraded_penalty = target.health_state === 'degraded' ? 100_000 : 0;
  const restarting_penalty = target.health_state === 'restarting' ? 1_000_000 : 0;
  const timeout_penalty = target.timeout_rate_1m * 10_000;
  const latency_penalty = target.ewma_latency_ms * 10;
  const pending_penalty = target.pending_calls * 10;
  const inflight_penalty = target.inflight_calls * 100;

  return (
    degraded_penalty +
    restarting_penalty +
    timeout_penalty +
    latency_penalty +
    pending_penalty +
    inflight_penalty
  );
}

function ComputeTargetWeight(params: {
  target: ingress_balancer_target_record_t;
}): number {
  const { target } = params;
  if (target.health_state === 'stopped' || target.health_state === 'restarting') {
    return 0;
  }

  const health_multiplier = target.health_state === 'degraded' ? 0.35 : 1;
  const success_multiplier = Math.max(0.05, Math.min(1, target.success_rate_1m));
  const timeout_multiplier = Math.max(0.05, 1 - target.timeout_rate_1m);
  const load_penalty = 1 / (1 + target.inflight_calls + target.pending_calls * 0.25);

  const normalized_weight =
    health_multiplier * success_multiplier * timeout_multiplier * load_penalty;

  return Math.max(1, Math.round(normalized_weight * 100));
}

export class ClusterIngressRoutingEngine {
  private default_mode: cluster_ingress_routing_mode_t;
  private sticky_ttl_ms: number;

  private weighted_round_robin_cursor = 0;
  private affinity_target_by_key = new Map<string, ingress_affinity_mapping_t>();

  constructor(params: cluster_ingress_routing_engine_constructor_params_t = {}) {
    this.default_mode = params.default_mode ?? 'least_loaded';
    this.sticky_ttl_ms = params.sticky_ttl_ms ?? 30_000;
  }

  buildDispatchPlan(params: {
    request: cluster_call_request_message_i;
    candidate_list: ingress_balancer_target_record_t[];
    now_unix_ms?: number;
  }): {
    ordered_candidate_list: ingress_balancer_target_record_t[];
    routing_decision: ingress_balancer_routing_decision_t;
  } {
    const now_unix_ms = params.now_unix_ms ?? Date.now();
    const candidate_list = [...params.candidate_list];

    const target_mode =
      params.request.routing_hint.mode === 'target_node'
        ? 'target_node'
        : params.request.routing_hint.mode === 'affinity'
          ? 'affinity'
          : this.default_mode;

    if (target_mode === 'target_node') {
      const target_node_id = params.request.routing_hint.target_node_id;
      const selected_target = candidate_list.find((candidate): boolean => {
        return candidate.node_id === target_node_id || candidate.target_id === target_node_id;
      });

      const ordered_candidate_list = selected_target
        ? [
            selected_target,
            ...candidate_list
              .filter((candidate): boolean => {
                return candidate.target_id !== selected_target.target_id;
              })
              .sort((left_target, right_target): number => {
                return left_target.target_id.localeCompare(right_target.target_id);
              })
          ]
        : [];

      return {
        ordered_candidate_list,
        routing_decision: {
          request_id: params.request.request_id,
          trace_id: params.request.trace_id,
          mode: 'target_node',
          selected_target_id: selected_target?.target_id ?? null,
          candidate_target_id_list: ordered_candidate_list.map((target): string => {
            return target.target_id;
          }),
          reason: selected_target
            ? 'explicit_target_node_selected'
            : 'explicit_target_node_not_found',
          timestamp_unix_ms: now_unix_ms
        }
      };
    }

    if (target_mode === 'affinity') {
      this.evictExpiredAffinityMappings({
        now_unix_ms
      });

      const affinity_key =
        params.request.routing_hint.affinity_key ??
        params.request.metadata?.ingress_affinity_key ??
        params.request.caller_identity.subject;

      const existing_mapping = this.affinity_target_by_key.get(String(affinity_key));
      let selected_target: ingress_balancer_target_record_t | null = null;

      if (existing_mapping) {
        selected_target =
          candidate_list.find((candidate): boolean => {
            return candidate.target_id === existing_mapping.target_id;
          }) ?? null;
      }

      if (!selected_target) {
        selected_target = this.selectLeastLoadedTarget({
          candidate_list
        });
      }

      if (selected_target) {
        this.affinity_target_by_key.set(String(affinity_key), {
          target_id: selected_target.target_id,
          expires_unix_ms: now_unix_ms + this.sticky_ttl_ms
        });
      }

      const ordered_candidate_list = selected_target
        ? [
            selected_target,
            ...candidate_list
              .filter((candidate): boolean => {
                return candidate.target_id !== selected_target?.target_id;
              })
              .sort((left_target, right_target): number => {
                const left_score = ComputeTargetLoadScore({
                  target: left_target
                });
                const right_score = ComputeTargetLoadScore({
                  target: right_target
                });

                if (left_score !== right_score) {
                  return left_score - right_score;
                }

                return left_target.target_id.localeCompare(right_target.target_id);
              })
          ]
        : [];

      return {
        ordered_candidate_list,
        routing_decision: {
          request_id: params.request.request_id,
          trace_id: params.request.trace_id,
          mode: 'affinity',
          selected_target_id: selected_target?.target_id ?? null,
          candidate_target_id_list: ordered_candidate_list.map((target): string => {
            return target.target_id;
          }),
          reason: selected_target
            ? existing_mapping
              ? 'affinity_existing_mapping'
              : 'affinity_new_mapping'
            : 'affinity_no_candidate',
          timestamp_unix_ms: now_unix_ms
        }
      };
    }

    if (target_mode === 'weighted_rr') {
      const weighted_target_list: ingress_balancer_target_record_t[] = [];

      const weighted_candidate_list = [...candidate_list].sort(
        (left_target, right_target): number => {
          return left_target.target_id.localeCompare(right_target.target_id);
        }
      );

      for (const candidate of weighted_candidate_list) {
        const weight = ComputeTargetWeight({
          target: candidate
        });
        for (let index = 0; index < weight; index += 1) {
          weighted_target_list.push(candidate);
        }
      }

      const selected_target =
        weighted_target_list.length > 0
          ? weighted_target_list[
              this.weighted_round_robin_cursor % weighted_target_list.length
            ]
          : null;

      this.weighted_round_robin_cursor += 1;

      const ordered_candidate_list = selected_target
        ? [
            selected_target,
            ...candidate_list
              .filter((candidate): boolean => {
                return candidate.target_id !== selected_target.target_id;
              })
              .sort((left_target, right_target): number => {
                const left_weight = ComputeTargetWeight({
                  target: left_target
                });
                const right_weight = ComputeTargetWeight({
                  target: right_target
                });

                if (left_weight !== right_weight) {
                  return right_weight - left_weight;
                }

                return left_target.target_id.localeCompare(right_target.target_id);
              })
          ]
        : [];

      return {
        ordered_candidate_list,
        routing_decision: {
          request_id: params.request.request_id,
          trace_id: params.request.trace_id,
          mode: 'weighted_rr',
          selected_target_id: selected_target?.target_id ?? null,
          candidate_target_id_list: ordered_candidate_list.map((target): string => {
            return target.target_id;
          }),
          reason: selected_target
            ? 'weighted_round_robin_selected'
            : 'weighted_round_robin_no_candidate',
          timestamp_unix_ms: now_unix_ms
        }
      };
    }

    const selected_target = this.selectLeastLoadedTarget({
      candidate_list
    });

    const ordered_candidate_list = selected_target
      ? [
          selected_target,
          ...candidate_list
            .filter((candidate): boolean => {
              return candidate.target_id !== selected_target.target_id;
            })
            .sort((left_target, right_target): number => {
              const left_score = ComputeTargetLoadScore({
                target: left_target
              });
              const right_score = ComputeTargetLoadScore({
                target: right_target
              });

              if (left_score !== right_score) {
                return left_score - right_score;
              }

              return left_target.target_id.localeCompare(right_target.target_id);
            })
        ]
      : [];

    return {
      ordered_candidate_list,
      routing_decision: {
        request_id: params.request.request_id,
        trace_id: params.request.trace_id,
        mode: 'least_loaded',
        selected_target_id: selected_target?.target_id ?? null,
        candidate_target_id_list: ordered_candidate_list.map((target): string => {
          return target.target_id;
        }),
        reason: selected_target ? 'least_loaded_selected' : 'least_loaded_no_candidate',
        timestamp_unix_ms: now_unix_ms
      }
    };
  }

  private selectLeastLoadedTarget(params: {
    candidate_list: ingress_balancer_target_record_t[];
  }): ingress_balancer_target_record_t | null {
    if (params.candidate_list.length === 0) {
      return null;
    }

    const sorted_candidate_list = [...params.candidate_list].sort(
      (left_target, right_target): number => {
        const left_score = ComputeTargetLoadScore({
          target: left_target
        });
        const right_score = ComputeTargetLoadScore({
          target: right_target
        });

        if (left_score !== right_score) {
          return left_score - right_score;
        }

        return left_target.target_id.localeCompare(right_target.target_id);
      }
    );

    return sorted_candidate_list[0];
  }

  private evictExpiredAffinityMappings(params: { now_unix_ms: number }): void {
    for (const [affinity_key, mapping] of this.affinity_target_by_key.entries()) {
      if (mapping.expires_unix_ms <= params.now_unix_ms) {
        this.affinity_target_by_key.delete(affinity_key);
      }
    }
  }
}

