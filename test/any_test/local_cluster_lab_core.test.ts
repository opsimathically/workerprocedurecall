import assert from 'node:assert/strict';
import test from 'node:test';

import { GetLabProfileServiceIdList } from '../../local_cluster_lab/lab_config';
import { BuildLabCliUsageText, ParseLabCliArgs } from '../../local_cluster_lab/lab_cli_parser';

test('local cluster lab cli parser parses up command with profile', function () {
  const parsed_args = ParseLabCliArgs({
    argv: ['up', '--profile=geo']
  });

  assert.equal(parsed_args.command, 'up');
  assert.equal(parsed_args.profile, 'geo');
});

test('local cluster lab cli parser parses run-scenario command', function () {
  const parsed_args = ParseLabCliArgs({
    argv: ['run-scenario', '--name=stale_control_fail_closed']
  });

  assert.equal(parsed_args.command, 'run-scenario');
  assert.equal(parsed_args.scenario_name, 'stale_control_fail_closed');
});

test('local cluster lab profile plan includes expected geo services', function () {
  const service_id_list = GetLabProfileServiceIdList({
    profile: 'geo'
  });

  assert.equal(service_id_list.includes('geo_control_plane'), true);
  assert.equal(service_id_list.includes('ingress_global'), true);
  assert.equal(service_id_list[0], 'discovery_daemon');
});

test('local cluster lab cli usage text contains scenario list', function () {
  const usage_text = BuildLabCliUsageText();
  assert.equal(usage_text.includes('stale_control_fail_closed'), true);
  assert.equal(usage_text.includes('up --profile='), true);
});

