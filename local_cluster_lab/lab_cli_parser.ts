import type {
  lab_cli_args_t,
  lab_cli_command_t,
  lab_profile_t,
  lab_scenario_name_t,
  lab_service_id_t
} from './lab_types';

const valid_command_list: lab_cli_command_t[] = [
  'up',
  'down',
  'status',
  'logs',
  'snapshot',
  'run-scenario'
];

const valid_profile_list: lab_profile_t[] = ['minimal', 'ingress', 'cluster', 'geo'];

const valid_service_id_list: lab_service_id_t[] = [
  'discovery_daemon',
  'control_plane',
  'geo_control_plane',
  'node_east',
  'node_west',
  'ingress_single',
  'ingress_regional_east',
  'ingress_regional_west',
  'ingress_global'
];

const valid_scenario_name_list: lab_scenario_name_t[] = [
  'happy_path_call',
  'ingress_failover',
  'discovery_membership_and_expiry',
  'control_plane_policy_sync',
  'geo_cross_region_failover',
  'auth_failure',
  'stale_control_fail_closed'
];

function IsFlagEnabled(params: { value: string | undefined }): boolean {
  return (
    params.value === undefined ||
    params.value === 'true' ||
    params.value === '1' ||
    params.value === 'yes' ||
    params.value === ''
  );
}

export function ParseLabCliArgs(params: {
  argv: string[];
}): lab_cli_args_t {
  const [command_raw, ...flag_list] = params.argv;

  if (!command_raw || !valid_command_list.includes(command_raw as lab_cli_command_t)) {
    throw new Error(
      `Unknown command "${String(command_raw)}". Valid commands: ${valid_command_list.join(', ')}`
    );
  }

  const parsed_args: lab_cli_args_t = {
    command: command_raw as lab_cli_command_t
  };

  for (const flag of flag_list) {
    if (!flag.startsWith('--')) {
      continue;
    }

    const trimmed_flag = flag.slice(2);
    const [flag_name, raw_value] = trimmed_flag.split('=');

    if (flag_name === 'profile') {
      if (!raw_value || !valid_profile_list.includes(raw_value as lab_profile_t)) {
        throw new Error(
          `Invalid profile "${String(raw_value)}". Valid profiles: ${valid_profile_list.join(', ')}`
        );
      }

      parsed_args.profile = raw_value as lab_profile_t;
      continue;
    }

    if (flag_name === 'service') {
      if (!raw_value || !valid_service_id_list.includes(raw_value as lab_service_id_t)) {
        throw new Error(
          `Invalid service "${String(raw_value)}". Valid services: ${valid_service_id_list.join(', ')}`
        );
      }

      parsed_args.service_id = raw_value as lab_service_id_t;
      continue;
    }

    if (flag_name === 'name') {
      if (!raw_value || !valid_scenario_name_list.includes(raw_value as lab_scenario_name_t)) {
        throw new Error(
          `Invalid scenario "${String(raw_value)}". Valid scenarios: ${valid_scenario_name_list.join(', ')}`
        );
      }

      parsed_args.scenario_name = raw_value as lab_scenario_name_t;
      continue;
    }

    if (flag_name === 'follow') {
      parsed_args.follow = IsFlagEnabled({
        value: raw_value
      });
      continue;
    }
  }

  return parsed_args;
}

export function BuildLabCliUsageText(): string {
  return [
    'Usage:',
    '  up --profile=<minimal|ingress|cluster|geo>',
    '  down',
    '  status',
    '  logs [--service=<service_id>] [--follow=true]',
    '  snapshot [--service=<service_id>]',
    '  run-scenario --name=<scenario_name>',
    '',
    `Profiles: ${valid_profile_list.join(', ')}`,
    `Services: ${valid_service_id_list.join(', ')}`,
    `Scenarios: ${valid_scenario_name_list.join(', ')}`
  ].join('\n');
}
