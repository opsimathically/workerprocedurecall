import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

import { GetLabProfileServiceIdList, GetLabServiceDefinition, lab_config } from './lab_config';
import { BuildLabCliUsageText, ParseLabCliArgs } from './lab_cli_parser';
import { HttpJsonRequest, WaitForCondition } from './lab_http';
import {
  EnsureLabDirectories,
  GetLabLogsDirectoryPath,
  GetLabRootDirectoryPath
} from './lab_paths';
import { RunLabScenario } from './lab_scenarios';
import { DeleteLabStateFile, ReadLabState, WriteLabState } from './lab_state';
import type {
  lab_profile_t,
  lab_service_id_t,
  lab_service_process_state_t,
  lab_state_t
} from './lab_types';

function IsProcessAlive(params: { pid: number }): boolean {
  try {
    process.kill(params.pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function WaitForProcessExit(params: {
  pid: number;
  timeout_ms: number;
}): Promise<boolean> {
  const started_unix_ms = Date.now();
  while (Date.now() - started_unix_ms <= params.timeout_ms) {
    if (!IsProcessAlive({ pid: params.pid })) {
      return true;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 100);
    });
  }

  return !IsProcessAlive({ pid: params.pid });
}

async function GetServiceHealth(params: {
  service_id: lab_service_id_t;
}): Promise<{
  ok: boolean;
  body?: unknown;
  error_message?: string;
}> {
  const service_definition = GetLabServiceDefinition({
    service_id: params.service_id
  });

  try {
    const response_body = await HttpJsonRequest({
      method: 'GET',
      host: service_definition.endpoint.host,
      port: service_definition.admin_port,
      path: '/health',
      timeout_ms: lab_config.timing.health_request_timeout_ms
    });

    return {
      ok: true,
      body: response_body
    };
  } catch (error) {
    return {
      ok: false,
      error_message: error instanceof Error ? error.message : String(error)
    };
  }
}

async function WaitForServiceHealthy(params: {
  service_id: lab_service_id_t;
}): Promise<void> {
  await WaitForCondition({
    timeout_ms: lab_config.timing.startup_timeout_ms,
    poll_interval_ms: lab_config.timing.startup_poll_interval_ms,
    condition_func: async (): Promise<boolean> => {
      const health = await GetServiceHealth({
        service_id: params.service_id
      });
      return health.ok;
    }
  });
}

function BuildServiceLogFilePath(params: { service_id: lab_service_id_t }): string {
  return path.resolve(GetLabLogsDirectoryPath(), `${params.service_id}.log`);
}

function BuildServiceProcessState(params: {
  service_id: lab_service_id_t;
  profile: lab_profile_t;
  pid: number;
}): lab_service_process_state_t {
  const service_definition = GetLabServiceDefinition({
    service_id: params.service_id
  });

  return {
    service_id: params.service_id,
    role: service_definition.role,
    profile: params.profile,
    pid: params.pid,
    admin_base_url: `https://${service_definition.endpoint.host}:${service_definition.admin_port}`,
    log_file_path: BuildServiceLogFilePath({
      service_id: params.service_id
    }),
    endpoint: service_definition.endpoint,
    started_unix_ms: Date.now()
  };
}

async function StartServiceProcess(params: {
  service_id: lab_service_id_t;
  profile: lab_profile_t;
  state: lab_state_t;
}): Promise<void> {
  const existing_process_state = params.state.service_process_state_by_id[params.service_id];
  if (existing_process_state && IsProcessAlive({ pid: existing_process_state.pid })) {
    return;
  }

  const log_file_path = BuildServiceLogFilePath({
    service_id: params.service_id
  });

  fs.writeFileSync(log_file_path, '', {
    encoding: 'utf8'
  });

  const log_file_descriptor = fs.openSync(log_file_path, 'a');
  const lab_root_directory_path = GetLabRootDirectoryPath();
  const repo_root_directory_path = path.resolve(lab_root_directory_path, '..');
  const service_process_entry_path = path.resolve(lab_root_directory_path, 'service_process.ts');

  const child_process = spawn(
    process.execPath,
    [
      '--require',
      'ts-node/register',
      '--require',
      'tsconfig-paths/register',
      service_process_entry_path,
      `--service=${params.service_id}`,
      `--profile=${params.profile}`
    ],
    {
      cwd: repo_root_directory_path,
      detached: true,
      stdio: ['ignore', log_file_descriptor, log_file_descriptor]
    }
  );

  fs.closeSync(log_file_descriptor);
  child_process.unref();

  params.state.service_process_state_by_id[params.service_id] = BuildServiceProcessState({
    service_id: params.service_id,
    profile: params.profile,
    pid: child_process.pid ?? -1
  });

  WriteLabState({
    state: params.state
  });

  try {
    await WaitForServiceHealthy({
      service_id: params.service_id
    });
  } catch (error) {
    const log_text = fs.existsSync(log_file_path)
      ? fs.readFileSync(log_file_path, 'utf8')
      : '';

    const recent_log_text = TailLines({
      text: log_text,
      line_limit: 30
    });

    throw new Error(
      [
        `Service "${params.service_id}" failed health checks.`,
        `Cause: ${error instanceof Error ? error.message : String(error)}`,
        `Log file: ${log_file_path}`,
        recent_log_text.length > 0 ? `Recent logs:\n${recent_log_text}` : 'Recent logs: (empty)'
      ].join('\n')
    );
  }
}

async function StopServiceProcess(params: {
  service_id: lab_service_id_t;
  state: lab_state_t;
  force_kill?: boolean;
}): Promise<void> {
  const process_state = params.state.service_process_state_by_id[params.service_id];
  if (!process_state) {
    return;
  }

  if (!IsProcessAlive({ pid: process_state.pid })) {
    delete params.state.service_process_state_by_id[params.service_id];
    WriteLabState({
      state: params.state
    });
    return;
  }

  const signal: NodeJS.Signals = params.force_kill ? 'SIGKILL' : 'SIGTERM';
  try {
    process.kill(process_state.pid, signal);
  } catch {
    // Ignore signal errors.
  }

  const exited = await WaitForProcessExit({
    pid: process_state.pid,
    timeout_ms: lab_config.timing.shutdown_timeout_ms
  });

  if (!exited && !params.force_kill) {
    try {
      process.kill(process_state.pid, 'SIGKILL');
    } catch {
      // Ignore signal errors.
    }

    await WaitForProcessExit({
      pid: process_state.pid,
      timeout_ms: 2_000
    });
  }

  delete params.state.service_process_state_by_id[params.service_id];
  WriteLabState({
    state: params.state
  });
}

function PrintStatusLine(params: {
  service_id: lab_service_id_t;
  pid: number;
  process_alive: boolean;
  health_ok: boolean;
  detail: string;
}): void {
  const status_text = params.process_alive
    ? params.health_ok
      ? 'ready'
      : 'alive_unhealthy'
    : 'stopped';

  process.stdout.write(
    `${params.service_id.padEnd(24)} pid=${String(params.pid).padEnd(8)} status=${status_text.padEnd(15)} ${params.detail}\n`
  );
}

function PrintJson(params: { value: unknown }): void {
  process.stdout.write(`${JSON.stringify(params.value, null, 2)}\n`);
}

function TailLines(params: { text: string; line_limit: number }): string {
  const line_list = params.text.split('\n');
  const sliced_line_list = line_list.slice(
    Math.max(0, line_list.length - params.line_limit)
  );
  return sliced_line_list.join('\n');
}

async function FollowLogFile(params: { log_file_path: string }): Promise<void> {
  let cursor = fs.existsSync(params.log_file_path)
    ? fs.statSync(params.log_file_path).size
    : 0;

  process.stdout.write(`Following ${params.log_file_path} (Ctrl+C to stop)...\n`);

  while (true) {
    if (!fs.existsSync(params.log_file_path)) {
      await new Promise<void>((resolve): void => {
        setTimeout(resolve, 200);
      });
      continue;
    }

    const next_size = fs.statSync(params.log_file_path).size;
    if (next_size > cursor) {
      const stream = fs.createReadStream(params.log_file_path, {
        start: cursor,
        end: next_size - 1,
        encoding: 'utf8'
      });

      await new Promise<void>((resolve, reject): void => {
        stream.on('data', (chunk): void => {
          process.stdout.write(String(chunk));
        });
        stream.on('error', reject);
        stream.on('end', (): void => {
          resolve();
        });
      });

      cursor = next_size;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, 200);
    });
  }
}

async function BringProfileUp(params: { profile: lab_profile_t }): Promise<void> {
  EnsureLabDirectories();

  const existing_state = ReadLabState();
  if (existing_state) {
    if (existing_state.profile !== params.profile) {
      await BringStackDown();
    } else {
      const service_id_list = GetLabProfileServiceIdList({
        profile: params.profile
      });
      let already_running = true;
      for (const service_id of service_id_list) {
        const process_state = existing_state.service_process_state_by_id[service_id];
        if (!process_state || !IsProcessAlive({ pid: process_state.pid })) {
          already_running = false;
          break;
        }
      }

      if (already_running) {
        process.stdout.write(
          `Profile "${params.profile}" is already running. Use status/logs/run-scenario.\n`
        );
        return;
      }

      await BringStackDown();
    }
  }

  const state: lab_state_t = {
    profile: params.profile,
    started_unix_ms: Date.now(),
    service_process_state_by_id: {}
  };

  const service_id_list = GetLabProfileServiceIdList({
    profile: params.profile
  });

  process.stdout.write(`Starting local cluster lab profile "${params.profile}"...\n`);
  try {
    for (const service_id of service_id_list) {
      process.stdout.write(`  -> starting ${service_id}\n`);
      await StartServiceProcess({
        service_id,
        profile: params.profile,
        state
      });
    }

    WriteLabState({
      state
    });

    process.stdout.write('Local cluster lab profile started successfully.\n');
  } catch (error) {
    process.stderr.write(
      `Failed to start profile "${params.profile}": ${
        error instanceof Error ? error.message : String(error)
      }\n`
    );

    await BringStackDown({
      state_override: state
    });

    process.exitCode = 1;
  }
}

async function BringStackDown(params?: { state_override?: lab_state_t }): Promise<void> {
  const state = params?.state_override ?? ReadLabState();
  if (!state) {
    process.stdout.write('Local cluster lab is not running.\n');
    return;
  }

  const service_id_list = Object.keys(state.service_process_state_by_id) as lab_service_id_t[];
  const sorted_service_id_list = [...service_id_list].sort(
    (left_service_id, right_service_id): number => {
      const left_order = GetLabServiceDefinition({
        service_id: left_service_id
      }).startup_order_index;
      const right_order = GetLabServiceDefinition({
        service_id: right_service_id
      }).startup_order_index;

      return right_order - left_order;
    }
  );

  for (const service_id of sorted_service_id_list) {
    await StopServiceProcess({
      service_id,
      state
    });
  }

  DeleteLabStateFile();
  process.stdout.write('Local cluster lab stopped.\n');
}

async function PrintStackStatus(): Promise<void> {
  const state = ReadLabState();
  if (!state) {
    process.stdout.write('Local cluster lab is not running.\n');
    return;
  }

  process.stdout.write(
    `Profile: ${state.profile}\nStarted: ${new Date(state.started_unix_ms).toISOString()}\n`
  );
  process.stdout.write(
    '--------------------------------------------------------------------------------\n'
  );

  const service_id_list = Object.keys(state.service_process_state_by_id) as lab_service_id_t[];

  for (const service_id of service_id_list) {
    const process_state = state.service_process_state_by_id[service_id];
    if (!process_state) {
      continue;
    }

    const process_alive = IsProcessAlive({
      pid: process_state.pid
    });

    if (!process_alive) {
      PrintStatusLine({
        service_id,
        pid: process_state.pid,
        process_alive: false,
        health_ok: false,
        detail: 'process is not alive'
      });
      continue;
    }

    const health = await GetServiceHealth({
      service_id
    });
    PrintStatusLine({
      service_id,
      pid: process_state.pid,
      process_alive: true,
      health_ok: health.ok,
      detail: health.ok ? 'healthy admin endpoint' : `health error: ${health.error_message}`
    });
  }
}

async function PrintLogs(params: {
  service_id?: lab_service_id_t;
  follow?: boolean;
}): Promise<void> {
  const state = ReadLabState();
  if (!state) {
    process.stdout.write('Local cluster lab is not running.\n');
    return;
  }

  const service_id_list = params.service_id
    ? [params.service_id]
    : (Object.keys(state.service_process_state_by_id) as lab_service_id_t[]);

  if (params.follow) {
    if (service_id_list.length !== 1) {
      throw new Error('Follow mode requires exactly one --service value.');
    }

    const service_id = service_id_list[0];
    const process_state = state.service_process_state_by_id[service_id];
    if (!process_state) {
      throw new Error(`Service "${service_id}" is not running.`);
    }

    await FollowLogFile({
      log_file_path: process_state.log_file_path
    });
    return;
  }

  for (const service_id of service_id_list) {
    const process_state = state.service_process_state_by_id[service_id];
    if (!process_state) {
      process.stdout.write(`\n=== ${service_id} ===\n(not running)\n`);
      continue;
    }

    process.stdout.write(`\n=== ${service_id} (${process_state.log_file_path}) ===\n`);
    if (!fs.existsSync(process_state.log_file_path)) {
      process.stdout.write('(log file not found)\n');
      continue;
    }

    const log_text = fs.readFileSync(process_state.log_file_path, 'utf8');
    const tailed_text = TailLines({
      text: log_text,
      line_limit: 120
    });
    process.stdout.write(`${tailed_text}\n`);
  }
}

async function PrintSnapshots(params: {
  service_id?: lab_service_id_t;
}): Promise<void> {
  const state = ReadLabState();
  if (!state) {
    process.stdout.write('Local cluster lab is not running.\n');
    return;
  }

  const service_id_list = params.service_id
    ? [params.service_id]
    : (Object.keys(state.service_process_state_by_id) as lab_service_id_t[]);

  for (const service_id of service_id_list) {
    const process_state = state.service_process_state_by_id[service_id];
    if (!process_state) {
      continue;
    }

    process.stdout.write(`\n=== snapshot:${service_id} ===\n`);
    try {
      const snapshot = await HttpJsonRequest({
        method: 'GET',
        host: process_state.endpoint.host,
        port: GetLabServiceDefinition({ service_id }).admin_port,
        path: '/snapshot',
        timeout_ms: lab_config.timing.health_request_timeout_ms
      });
      PrintJson({
        value: snapshot
      });
    } catch (error) {
      process.stdout.write(
        `snapshot error: ${error instanceof Error ? error.message : String(error)}\n`
      );
    }
  }
}

async function RunScenarioCommand(params: {
  scenario_name: NonNullable<ReturnType<typeof ParseLabCliArgs>['scenario_name']>;
}): Promise<void> {
  const state = ReadLabState();
  if (!state) {
    throw new Error('Local cluster lab is not running. Start a profile with `up --profile=...` first.');
  }

  const context = {
    state,
    stop_service_func: async (stop_params: {
      service_id: lab_service_id_t;
      force_kill?: boolean;
    }): Promise<void> => {
      const latest_state = ReadLabState();
      if (!latest_state) {
        throw new Error('Lab state file is missing.');
      }

      await StopServiceProcess({
        service_id: stop_params.service_id,
        state: latest_state,
        force_kill: stop_params.force_kill
      });
    },
    start_service_func: async (start_params: { service_id: lab_service_id_t }): Promise<void> => {
      const latest_state = ReadLabState();
      if (!latest_state) {
        throw new Error('Lab state file is missing.');
      }

      await StartServiceProcess({
        service_id: start_params.service_id,
        profile: latest_state.profile,
        state: latest_state
      });
    },
    get_service_snapshot_func: async (snapshot_params: {
      service_id: lab_service_id_t;
    }): Promise<unknown> => {
      const service_definition = GetLabServiceDefinition({
        service_id: snapshot_params.service_id
      });

      return await HttpJsonRequest({
        method: 'GET',
        host: service_definition.endpoint.host,
        port: service_definition.admin_port,
        path: '/snapshot',
        timeout_ms: lab_config.timing.health_request_timeout_ms
      });
    },
    call_service_action_func: async (action_params: {
      service_id: lab_service_id_t;
      action_name: string;
      payload?: Record<string, unknown>;
    }): Promise<unknown> => {
      const service_definition = GetLabServiceDefinition({
        service_id: action_params.service_id
      });

      return await HttpJsonRequest({
        method: 'POST',
        host: service_definition.endpoint.host,
        port: service_definition.admin_port,
        path: '/action',
        timeout_ms: lab_config.timing.health_request_timeout_ms,
        body: {
          action_name: action_params.action_name,
          ...(action_params.payload ?? {})
        }
      });
    }
  };

  const scenario_result = await RunLabScenario({
    scenario_name: params.scenario_name,
    context
  });

  process.stdout.write(`Scenario: ${scenario_result.scenario_name}\n`);
  process.stdout.write(`Trace ID: ${scenario_result.trace_id}\n`);
  process.stdout.write(`Result: ${scenario_result.ok ? 'PASS' : 'FAIL'}\n`);
  process.stdout.write(`${scenario_result.detail_message}\n`);
  process.stdout.write(`Where to look next: ${scenario_result.where_to_look_next}\n`);
}

async function Run(): Promise<void> {
  EnsureLabDirectories();

  const cli_args = ParseLabCliArgs({
    argv: process.argv.slice(2)
  });

  if (cli_args.command === 'up') {
    if (!cli_args.profile) {
      throw new Error('Missing --profile for up command.');
    }
    await BringProfileUp({
      profile: cli_args.profile
    });
    return;
  }

  if (cli_args.command === 'down') {
    await BringStackDown();
    return;
  }

  if (cli_args.command === 'status') {
    await PrintStackStatus();
    return;
  }

  if (cli_args.command === 'logs') {
    await PrintLogs({
      service_id: cli_args.service_id,
      follow: cli_args.follow
    });
    return;
  }

  if (cli_args.command === 'snapshot') {
    await PrintSnapshots({
      service_id: cli_args.service_id
    });
    return;
  }

  if (cli_args.command === 'run-scenario') {
    if (!cli_args.scenario_name) {
      throw new Error('Missing --name for run-scenario command.');
    }
    await RunScenarioCommand({
      scenario_name: cli_args.scenario_name
    });
    return;
  }
}

void Run().catch((error): void => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n\n`);
  process.stderr.write(`${BuildLabCliUsageText()}\n`);
  process.exit(1);
});
