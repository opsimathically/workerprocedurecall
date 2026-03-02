export type cluster_authorization_effect_t = 'allow' | 'deny';

export type cluster_authorization_path_t =
  | 'call_execution'
  | 'mutation_operation'
  | 'read_cluster_metadata'
  | 'read_debug_metadata';

export type cluster_authorization_target_selector_t = {
  node_ids?: string[];
  labels?: Record<string, string>;
  zones?: string[];
};

export type cluster_authorization_subject_match_t = {
  subject?: string;
  group?: string;
  service_account?: string;
  client_id?: string;
};

export type cluster_authorization_scope_constraint_t = {
  tenant?: string | string[];
  environment?: string | string[];
  cluster?: string | string[];
  node_selector?: cluster_authorization_target_selector_t;
  function_name_pattern?: string;
  mutation_types?: string[];
};

export type cluster_authorization_policy_t = {
  policy_id: string;
  effect: cluster_authorization_effect_t;
  subject_match?: cluster_authorization_subject_match_t;
  capabilities: string[];
  constraints?: cluster_authorization_scope_constraint_t;
};

export type cluster_authorization_request_context_t = {
  path: cluster_authorization_path_t;
  capability_list: string[];
  subject?: string;
  group_list?: string[];
  service_account?: string;
  client_id?: string;
  tenant?: string;
  environment?: string;
  cluster?: string;
  target_selector?: cluster_authorization_target_selector_t;
  function_name?: string;
  mutation_type?: string;
};

export type cluster_authorization_evaluation_result_t = {
  authorized: boolean;
  decision: 'allow' | 'deny';
  reason:
    | 'allow_matched_policy'
    | 'deny_matched_policy'
    | 'deny_no_matching_policy';
  path: cluster_authorization_path_t;
  capability_list: string[];
  matched_allow_policy_id_list: string[];
  matched_deny_policy_id_list: string[];
  evaluated_policy_count: number;
};

function ToNormalizedStringList(params: {
  value: string | string[] | undefined;
}): string[] | null {
  const { value } = params;
  if (typeof value === 'undefined') {
    return null;
  }

  if (Array.isArray(value)) {
    return value.filter((item): item is string => {
      return typeof item === 'string' && item.length > 0;
    });
  }

  if (typeof value !== 'string' || value.length === 0) {
    return [];
  }

  return [value];
}

function CapabilityMatches(params: {
  policy_capability: string;
  requested_capability: string;
}): boolean {
  const { policy_capability, requested_capability } = params;
  if (policy_capability === requested_capability) {
    return true;
  }

  if (!policy_capability.endsWith('*')) {
    return false;
  }

  const wildcard_prefix = policy_capability.slice(0, -1);
  return requested_capability.startsWith(wildcard_prefix);
}

function SubjectMatches(params: {
  subject_match: cluster_authorization_subject_match_t | undefined;
  context: cluster_authorization_request_context_t;
}): boolean {
  const { subject_match, context } = params;
  if (!subject_match) {
    return true;
  }

  if (
    typeof subject_match.subject === 'string' &&
    subject_match.subject !== context.subject
  ) {
    return false;
  }

  if (
    typeof subject_match.service_account === 'string' &&
    subject_match.service_account !== context.service_account
  ) {
    return false;
  }

  if (
    typeof subject_match.client_id === 'string' &&
    subject_match.client_id !== context.client_id
  ) {
    return false;
  }

  if (typeof subject_match.group === 'string') {
    if (!Array.isArray(context.group_list)) {
      return false;
    }

    if (!context.group_list.includes(subject_match.group)) {
      return false;
    }
  }

  return true;
}

function ScopedValueMatches(params: {
  constraint_value: string | string[] | undefined;
  context_value: string | undefined;
}): boolean {
  const normalized_constraint_list = ToNormalizedStringList({
    value: params.constraint_value
  });

  if (normalized_constraint_list === null) {
    return true;
  }

  if (normalized_constraint_list.includes('*')) {
    return true;
  }

  if (typeof params.context_value !== 'string' || params.context_value.length === 0) {
    return false;
  }

  return normalized_constraint_list.includes(params.context_value);
}

function TargetSelectorMatches(params: {
  constraint_selector: cluster_authorization_target_selector_t | undefined;
  request_selector: cluster_authorization_target_selector_t | undefined;
}): boolean {
  const { constraint_selector, request_selector } = params;
  if (!constraint_selector) {
    return true;
  }

  if (!request_selector) {
    return false;
  }

  if (Array.isArray(constraint_selector.node_ids)) {
    if (!Array.isArray(request_selector.node_ids)) {
      return false;
    }
    for (const request_node_id of request_selector.node_ids) {
      if (!constraint_selector.node_ids.includes(request_node_id)) {
        return false;
      }
    }
  }

  if (Array.isArray(constraint_selector.zones)) {
    if (!Array.isArray(request_selector.zones)) {
      return false;
    }
    for (const request_zone of request_selector.zones) {
      if (!constraint_selector.zones.includes(request_zone)) {
        return false;
      }
    }
  }

  if (constraint_selector.labels) {
    if (!request_selector.labels) {
      return false;
    }
    for (const [label_key, label_value] of Object.entries(constraint_selector.labels)) {
      if (request_selector.labels[label_key] !== label_value) {
        return false;
      }
    }
  }

  return true;
}

function ConstraintMatches(params: {
  constraint: cluster_authorization_scope_constraint_t | undefined;
  context: cluster_authorization_request_context_t;
}): boolean {
  const { constraint, context } = params;
  if (!constraint) {
    return true;
  }

  if (
    !ScopedValueMatches({
      constraint_value: constraint.tenant,
      context_value: context.tenant
    })
  ) {
    return false;
  }

  if (
    !ScopedValueMatches({
      constraint_value: constraint.environment,
      context_value: context.environment
    })
  ) {
    return false;
  }

  if (
    !ScopedValueMatches({
      constraint_value: constraint.cluster,
      context_value: context.cluster
    })
  ) {
    return false;
  }

  if (
    !TargetSelectorMatches({
      constraint_selector: constraint.node_selector,
      request_selector: context.target_selector
    })
  ) {
    return false;
  }

  if (typeof constraint.function_name_pattern === 'string') {
    if (
      typeof context.function_name !== 'string' ||
      context.function_name.length === 0
    ) {
      return false;
    }

    let regex: RegExp;
    try {
      regex = new RegExp(constraint.function_name_pattern);
    } catch {
      return false;
    }

    if (!regex.test(context.function_name)) {
      return false;
    }
  }

  if (Array.isArray(constraint.mutation_types)) {
    if (
      typeof context.mutation_type !== 'string' ||
      !constraint.mutation_types.includes(context.mutation_type)
    ) {
      return false;
    }
  }

  return true;
}

function PolicyCapabilitiesMatch(params: {
  policy_capability_list: string[];
  requested_capability_list: string[];
}): boolean {
  const { policy_capability_list, requested_capability_list } = params;
  for (const policy_capability of policy_capability_list) {
    for (const requested_capability of requested_capability_list) {
      if (
        CapabilityMatches({
          policy_capability,
          requested_capability
        })
      ) {
        return true;
      }
    }
  }

  return false;
}

function BuildMutationCapabilityList(params: { mutation_type: string }): string[] {
  const { mutation_type } = params;
  const shared_prefix = 'rpc.admin.mutate';

  if (mutation_type === 'define_function') {
    return [`${shared_prefix}:function:define`, `${shared_prefix}:*`];
  }
  if (mutation_type === 'redefine_function') {
    return [`${shared_prefix}:function:redefine`, `${shared_prefix}:*`];
  }
  if (mutation_type === 'undefine_function') {
    return [`${shared_prefix}:function:undefine`, `${shared_prefix}:*`];
  }
  if (mutation_type === 'define_dependency') {
    return [`${shared_prefix}:dependency:define`, `${shared_prefix}:*`];
  }
  if (mutation_type === 'undefine_dependency') {
    return [`${shared_prefix}:dependency:undefine`, `${shared_prefix}:*`];
  }
  if (mutation_type === 'define_constant') {
    return [`${shared_prefix}:constant:define`, `${shared_prefix}:*`];
  }
  if (mutation_type === 'undefine_constant') {
    return [`${shared_prefix}:constant:undefine`, `${shared_prefix}:*`];
  }
  if (mutation_type === 'define_database_connection') {
    return [`${shared_prefix}:database_connection:define`, `${shared_prefix}:*`];
  }
  if (mutation_type === 'undefine_database_connection') {
    return [`${shared_prefix}:database_connection:undefine`, `${shared_prefix}:*`];
  }

  if (mutation_type.startsWith('shared_')) {
    return [`${shared_prefix}:shared:*`, `${shared_prefix}:*`];
  }

  return [`${shared_prefix}:*`];
}

export class ClusterAuthorizationPolicyEngine {
  private policy_list: cluster_authorization_policy_t[] = [];

  constructor(params: { policy_list?: cluster_authorization_policy_t[] } = {}) {
    if (Array.isArray(params.policy_list)) {
      this.setPolicyList({
        policy_list: params.policy_list
      });
    }
  }

  setPolicyList(params: { policy_list: cluster_authorization_policy_t[] }): void {
    this.policy_list = params.policy_list.map((policy): cluster_authorization_policy_t => {
      return {
        policy_id: policy.policy_id,
        effect: policy.effect,
        subject_match: policy.subject_match
          ? { ...policy.subject_match }
          : undefined,
        capabilities: [...policy.capabilities],
        constraints: policy.constraints
          ? structuredClone(policy.constraints)
          : undefined
      };
    });
  }

  getPolicyList(): cluster_authorization_policy_t[] {
    return this.policy_list.map((policy): cluster_authorization_policy_t => {
      return {
        policy_id: policy.policy_id,
        effect: policy.effect,
        subject_match: policy.subject_match
          ? { ...policy.subject_match }
          : undefined,
        capabilities: [...policy.capabilities],
        constraints: policy.constraints
          ? structuredClone(policy.constraints)
          : undefined
      };
    });
  }

  evaluate(params: {
    context: cluster_authorization_request_context_t;
  }): cluster_authorization_evaluation_result_t {
    const { context } = params;

    const matched_allow_policy_id_list: string[] = [];
    const matched_deny_policy_id_list: string[] = [];

    for (const policy of this.policy_list) {
      if (
        !PolicyCapabilitiesMatch({
          policy_capability_list: policy.capabilities,
          requested_capability_list: context.capability_list
        })
      ) {
        continue;
      }

      if (
        !SubjectMatches({
          subject_match: policy.subject_match,
          context
        })
      ) {
        continue;
      }

      if (
        !ConstraintMatches({
          constraint: policy.constraints,
          context
        })
      ) {
        continue;
      }

      if (policy.effect === 'deny') {
        matched_deny_policy_id_list.push(policy.policy_id);
      } else {
        matched_allow_policy_id_list.push(policy.policy_id);
      }
    }

    matched_allow_policy_id_list.sort();
    matched_deny_policy_id_list.sort();

    if (matched_deny_policy_id_list.length > 0) {
      return {
        authorized: false,
        decision: 'deny',
        reason: 'deny_matched_policy',
        path: context.path,
        capability_list: [...context.capability_list],
        matched_allow_policy_id_list,
        matched_deny_policy_id_list,
        evaluated_policy_count: this.policy_list.length
      };
    }

    if (matched_allow_policy_id_list.length > 0) {
      return {
        authorized: true,
        decision: 'allow',
        reason: 'allow_matched_policy',
        path: context.path,
        capability_list: [...context.capability_list],
        matched_allow_policy_id_list,
        matched_deny_policy_id_list,
        evaluated_policy_count: this.policy_list.length
      };
    }

    return {
      authorized: false,
      decision: 'deny',
      reason: 'deny_no_matching_policy',
      path: context.path,
      capability_list: [...context.capability_list],
      matched_allow_policy_id_list,
      matched_deny_policy_id_list,
      evaluated_policy_count: this.policy_list.length
    };
  }

  authorizeCall(params: {
    function_name: string;
    subject?: string;
    group_list?: string[];
    service_account?: string;
    client_id?: string;
    tenant?: string;
    environment?: string;
    cluster?: string;
    target_selector?: cluster_authorization_target_selector_t;
  }): cluster_authorization_evaluation_result_t {
    const {
      function_name,
      subject,
      group_list,
      service_account,
      client_id,
      tenant,
      environment,
      cluster,
      target_selector
    } = params;

    return this.evaluate({
      context: {
        path: 'call_execution',
        capability_list: [`rpc.call:function:${function_name}`, 'rpc.call:*'],
        function_name,
        subject,
        group_list,
        service_account,
        client_id,
        tenant,
        environment,
        cluster,
        target_selector
      }
    });
  }

  authorizeMutation(params: {
    mutation_type: string;
    function_name?: string;
    target_selector?: cluster_authorization_target_selector_t;
    subject?: string;
    group_list?: string[];
    service_account?: string;
    client_id?: string;
    tenant?: string;
    environment?: string;
    cluster?: string;
  }): cluster_authorization_evaluation_result_t {
    const {
      mutation_type,
      function_name,
      target_selector,
      subject,
      group_list,
      service_account,
      client_id,
      tenant,
      environment,
      cluster
    } = params;

    return this.evaluate({
      context: {
        path: 'mutation_operation',
        capability_list: BuildMutationCapabilityList({ mutation_type }),
        mutation_type,
        function_name,
        target_selector,
        subject,
        group_list,
        service_account,
        client_id,
        tenant,
        environment,
        cluster
      }
    });
  }

  authorizeReadClusterMetadata(params: {
    subject?: string;
    group_list?: string[];
    service_account?: string;
    client_id?: string;
    tenant?: string;
    environment?: string;
    cluster?: string;
  }): cluster_authorization_evaluation_result_t {
    return this.evaluate({
      context: {
        path: 'read_cluster_metadata',
        capability_list: ['rpc.read.cluster:*'],
        ...params
      }
    });
  }

  authorizeReadDebugMetadata(params: {
    subject?: string;
    group_list?: string[];
    service_account?: string;
    client_id?: string;
    tenant?: string;
    environment?: string;
    cluster?: string;
  }): cluster_authorization_evaluation_result_t {
    return this.evaluate({
      context: {
        path: 'read_debug_metadata',
        capability_list: ['rpc.read.debug:*'],
        ...params
      }
    });
  }
}

export const full_admin_cluster_authorization_policy_fixture: cluster_authorization_policy_t =
  {
    policy_id: 'full_admin_prod',
    effect: 'allow',
    subject_match: {
      group: 'cluster-admins'
    },
    capabilities: [
      'rpc.call:*',
      'rpc.admin.mutate:*',
      'rpc.read.cluster:*',
      'rpc.read.debug:*'
    ],
    constraints: {
      tenant: '*',
      environment: ['staging', 'prod'],
      cluster: '*'
    }
  };

export const function_limited_cluster_authorization_policy_fixture: cluster_authorization_policy_t =
  {
    policy_id: 'billing_function_admin',
    effect: 'allow',
    subject_match: {
      service_account: 'svc_billing_release'
    },
    capabilities: [
      'rpc.call:function:BillingCharge',
      'rpc.admin.mutate:function:define',
      'rpc.admin.mutate:function:redefine',
      'rpc.admin.mutate:function:undefine',
      'rpc.read.cluster:*'
    ],
    constraints: {
      tenant: 'billing',
      environment: ['staging'],
      function_name_pattern: '^Billing.*$',
      node_selector: {
        labels: {
          service: 'billing'
        }
      },
      mutation_types: ['define_function', 'redefine_function', 'undefine_function']
    }
  };

export const call_only_cluster_authorization_policy_fixture: cluster_authorization_policy_t =
  {
    policy_id: 'call_only_client',
    effect: 'allow',
    subject_match: {
      client_id: 'client_portal_backend'
    },
    capabilities: ['rpc.call:function:GetUserProfile'],
    constraints: {
      tenant: 'customer_portal',
      environment: ['prod']
    }
  };
