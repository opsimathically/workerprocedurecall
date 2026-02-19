import { createHash } from 'node:crypto';
import { Worker } from 'node:worker_threads';

type lifecycle_state_t = 'stopped' | 'starting' | 'running' | 'stopping';
type control_command_t =
  | 'define_function'
  | 'undefine_function'
  | 'define_dependency'
  | 'undefine_dependency'
  | 'define_constant'
  | 'undefine_constant'
  | 'shutdown';

type runtime_options_t = {
  call_timeout_ms?: number;
  control_timeout_ms?: number;
  start_timeout_ms?: number;
  stop_timeout_ms?: number;
  restart_on_failure?: boolean;
  max_restarts_per_worker?: number;
  max_pending_calls_per_worker?: number;
  restart_base_delay_ms?: number;
  restart_max_delay_ms?: number;
  restart_jitter_ms?: number;
};

export type workerprocedurecall_constructor_params_t = runtime_options_t;

export type worker_function_handler_t<
  args_t extends unknown[] = [unknown],
  return_t = unknown
> = (...args: args_t) => return_t | Promise<return_t>;

export type define_worker_function_params_t<
  args_t extends unknown[] = [unknown],
  return_t = unknown
> = {
  name: string;
  worker_func: worker_function_handler_t<args_t, return_t>;
};

export type undefine_worker_function_params_t = {
  name: string;
};

export type define_worker_dependency_params_t = {
  alias: string;
  module_specifier: string;
  export_name?: string;
  is_default_export?: boolean;
};

export type undefine_worker_dependency_params_t = {
  alias: string;
};

export type define_worker_constant_params_t = {
  name: string;
  value: unknown;
};

export type undefine_worker_constant_params_t = {
  name: string;
};

export type start_workers_params_t = runtime_options_t & {
  count: number;
};

export type remote_error_t = {
  name: string;
  message: string;
  stack?: string;
};

export type remote_function_information_t = {
  name: string;
  parameter_signature: string | null;
  function_hash_sha1: string;
  installed_worker_count: number;
};

export type remote_dependency_information_t = {
  alias: string;
  module_specifier: string;
  export_name: string | null;
  is_default_export: boolean;
  installed_worker_count: number;
};

export type remote_constant_information_t = {
  name: string;
  installed_worker_count: number;
};

export type worker_health_state_t =
  | 'starting'
  | 'ready'
  | 'degraded'
  | 'restarting'
  | 'stopped';

export type worker_event_severity_t = 'info' | 'warn' | 'error';

export type worker_event_t = {
  event_id: string;
  worker_id: number;
  source: 'worker' | 'parent';
  event_name: string;
  severity: worker_event_severity_t;
  timestamp: string;
  correlation_id?: string;
  error?: remote_error_t;
  details?: Record<string, unknown>;
};

export type worker_event_listener_t = (worker_event: worker_event_t) => void;

export type worker_health_information_t = {
  worker_id: number;
  health_state: worker_health_state_t;
  pending_call_count: number;
  pending_control_count: number;
  restart_attempt: number;
};

export type worker_call_proxy_t = {
  [function_name: string]: (...args: unknown[]) => Promise<unknown>;
};

type worker_function_definition_t = {
  name: string;
  worker_func: worker_function_handler_t<any, any>;
  function_source: string;
  normalized_function_source: string;
  function_hash_sha1: string;
  parameter_signature: string | null;
  required_dependency_aliases: Set<string>;
  required_constant_names: Set<string>;
  installed_worker_ids: Set<number>;
};

type worker_dependency_definition_t = {
  alias: string;
  module_specifier: string;
  export_name: string | null;
  is_default_export: boolean;
  installed_worker_ids: Set<number>;
};

type worker_constant_definition_t = {
  name: string;
  value: unknown;
  installed_worker_ids: Set<number>;
};

type worker_state_t = {
  worker_id: number;
  worker_instance: Worker;
  health_state: worker_health_state_t;
  ready: boolean;
  shutting_down: boolean;
  restart_attempt: number;
  pending_call_request_ids: Set<string>;
  pending_control_request_ids: Set<string>;
};

type pending_call_record_t = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout_handle: NodeJS.Timeout;
  worker_id: number;
  function_name: string;
};

type pending_control_record_t = {
  resolve: () => void;
  reject: (error: Error) => void;
  timeout_handle: NodeJS.Timeout;
  worker_id: number;
  command: control_command_t;
};

type worker_ready_waiter_t = {
  resolve: () => void;
  reject: (error: Error) => void;
  timeout_handle: NodeJS.Timeout;
};

type worker_exit_waiter_t = {
  resolve: () => void;
  timeout_handle: NodeJS.Timeout;
};

type call_request_message_t = {
  message_type: 'call_request';
  request_id: string;
  function_name: string;
  call_args: unknown[];
};

type control_request_message_t = {
  message_type: 'control_request';
  control_request_id: string;
  command: control_command_t;
  payload?: Record<string, unknown>;
};

type parent_to_worker_message_t =
  | call_request_message_t
  | control_request_message_t;

type ready_message_t = {
  message_type: 'ready';
};

type call_response_message_t = {
  message_type: 'call_response';
  request_id: string;
  ok: boolean;
  return_value?: unknown;
  error?: remote_error_t;
};

type control_response_message_t = {
  message_type: 'control_response';
  control_request_id: string;
  ok: boolean;
  error?: remote_error_t;
};

type worker_event_message_t = {
  message_type: 'worker_event';
  event_id: string;
  worker_id: number;
  event_name: string;
  severity: worker_event_severity_t;
  timestamp: string;
  correlation_id?: string;
  error?: remote_error_t;
  details?: Record<string, unknown>;
};

type worker_to_parent_message_t =
  | ready_message_t
  | call_response_message_t
  | control_response_message_t
  | worker_event_message_t;

declare global {
  function wpc_import(alias: string): Promise<unknown>;
  function wpc_constant(name: string): unknown;
}

function ValidatePositiveInteger(params: { value: number; label: string }): void {
  const { value, label } = params;
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive integer.`);
  }
}

function ValidateNonNegativeInteger(params: {
  value: number;
  label: string;
}): void {
  const { value, label } = params;
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${label} must be a non-negative integer.`);
  }
}

function GetErrorMessage(params: { error: unknown }): string {
  const { error } = params;

  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === 'string') {
    return error;
  }

  return String(error);
}

function ToRemoteError(params: { error: unknown }): remote_error_t {
  const { error } = params;

  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack
    };
  }

  return {
    name: 'Error',
    message: GetErrorMessage({ error })
  };
}

function ToError(params: {
  remote_error: remote_error_t | undefined;
  fallback_message: string;
}): Error {
  const { remote_error, fallback_message } = params;

  if (!remote_error) {
    return new Error(fallback_message);
  }

  const error_to_return = new Error(remote_error.message);
  error_to_return.name = remote_error.name;

  if (typeof remote_error.stack === 'string' && remote_error.stack.length > 0) {
    error_to_return.stack = remote_error.stack;
  }

  return error_to_return;
}

function EnsureSerializable(params: { value: unknown; label: string }): void {
  const { value, label } = params;

  try {
    structuredClone(value);
  } catch (error) {
    throw new Error(
      `${label} is not serializable via structured clone: ${GetErrorMessage({ error })}`
    );
  }
}

function ParseParameterSignature(params: {
  function_source: string;
}): string | null {
  const { function_source } = params;

  const standard_parameter_match = function_source.match(/^[\s\S]*?\(([^)]*)\)/);
  if (standard_parameter_match) {
    const full_parameter_group = standard_parameter_match[1].trim();
    if (full_parameter_group.length === 0) {
      return null;
    }

    const parameter_name = full_parameter_group.split(',')[0]?.trim();
    if (parameter_name && parameter_name.length > 0) {
      return parameter_name;
    }
  }

  const arrow_parameter_match = function_source.match(
    /^\s*(?:async\s*)?([^\s=()]+)\s*=>/
  );

  if (!arrow_parameter_match) {
    return null;
  }

  return arrow_parameter_match[1].trim();
}

function NormalizeFunctionSourceForHash(params: { function_source: string }): string {
  const { function_source } = params;

  // Normalize line endings and boundary whitespace for deterministic hashing.
  return function_source.replace(/\r\n?/g, '\n').trim();
}

function ComputeFunctionHashSha1(params: { function_source: string }): string {
  const { function_source } = params;

  const normalized_function_source = NormalizeFunctionSourceForHash({
    function_source
  });

  return createHash('sha1').update(normalized_function_source, 'utf8').digest('hex');
}

function ValidateIdentifier(params: { value: string; label: string }): void {
  const { value, label } = params;
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`${label} must be a valid JavaScript identifier.`);
  }
}

function ParseStringLiteralDependencies(params: {
  function_source: string;
}): Set<string> {
  const { function_source } = params;

  const dependency_aliases = new Set<string>();
  const import_call_pattern = /wpc_import\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)/g;
  const context_dependency_pattern =
    /context\.dependencies\.([A-Za-z_][A-Za-z0-9_]*)/g;

  for (const pattern of [import_call_pattern, context_dependency_pattern]) {
    let match_result = pattern.exec(function_source);
    while (match_result) {
      dependency_aliases.add(match_result[1]);
      match_result = pattern.exec(function_source);
    }
  }

  return dependency_aliases;
}

function ParseStringLiteralConstants(params: {
  function_source: string;
}): Set<string> {
  const { function_source } = params;

  const constant_names = new Set<string>();
  const constant_call_pattern = /wpc_constant\(\s*['"]([A-Za-z_][A-Za-z0-9_]*)['"]\s*\)/g;
  const context_constant_pattern = /context\.constants\.([A-Za-z_][A-Za-z0-9_]*)/g;

  for (const pattern of [constant_call_pattern, context_constant_pattern]) {
    let match_result = pattern.exec(function_source);
    while (match_result) {
      constant_names.add(match_result[1]);
      match_result = pattern.exec(function_source);
    }
  }

  return constant_names;
}

function BuildWorkerRuntimeScript(): string {
  return String.raw`
const { parentPort, threadId } = require('node:worker_threads');

if (!parentPort) {
  throw new Error('Worker runtime started without parentPort.');
}

const worker_function_registry = new Map();
const worker_dependency_registry = new Map();
const worker_constant_registry = new Map();
let next_worker_event_id = 1;

function GetErrorMessage(error) {
  if (error instanceof Error) {
    return error.message;
  }

  if (typeof error === 'string') {
    return error;
  }

  return String(error);
}

function ToRemoteError(error) {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack
    };
  }

  return {
    name: 'Error',
    message: GetErrorMessage(error)
  };
}

function EmitWorkerEvent(params) {
  const event_message = {
    message_type: 'worker_event',
    event_id: 'worker_' + String(threadId) + '_' + String(next_worker_event_id),
    worker_id: threadId,
    event_name: params.event_name,
    severity: params.severity,
    timestamp: new Date().toISOString(),
    correlation_id: params.correlation_id,
    error: params.error,
    details: params.details
  };

  next_worker_event_id += 1;

  try {
    parentPort.postMessage(event_message);
  } catch {
    // Ignore - cannot safely report event transport failures.
  }
}

function SafePostMessage(message) {
  try {
    parentPort.postMessage(message);
    return true;
  } catch (error) {
    EmitWorkerEvent({
      event_name: 'post_message_failed',
      severity: 'error',
      error: ToRemoteError(error),
      details: {
        message_type: message && message.message_type ? message.message_type : null
      }
    });

    return false;
  }
}

function EnsureSerializable(value, label) {
  try {
    structuredClone(value);
  } catch (error) {
    throw new Error(label + ' is not serializable via structured clone: ' + GetErrorMessage(error));
  }
}

function ValidateIdentifier(value, label) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(label + ' must be a valid JavaScript identifier.');
  }
}

function InstallFunction(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Define payload must be an object.');
  }

  const name = payload.name;
  const function_source = payload.function_source;

  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('Function name must be a non-empty string.');
  }

  if (typeof function_source !== 'string' || function_source.length === 0) {
    throw new Error('Function source must be a non-empty string.');
  }

  const installed_function = new Function('"use strict"; return (' + function_source + ');')();

  if (typeof installed_function !== 'function') {
    throw new Error('Provided worker function source does not evaluate to a function.');
  }

  worker_function_registry.set(name, installed_function);
}

async function ResolveModule(module_specifier) {
  try {
    return require(module_specifier);
  } catch (require_error) {
    try {
      return await import(module_specifier);
    } catch (import_error) {
      throw new Error(
        'Failed to load module "' + module_specifier + '". require error: ' + GetErrorMessage(require_error) + '. import error: ' + GetErrorMessage(import_error)
      );
    }
  }
}

async function InstallDependency(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Dependency payload must be an object.');
  }

  const alias = payload.alias;
  const module_specifier = payload.module_specifier;
  const export_name = payload.export_name;
  const is_default_export = payload.is_default_export === true;

  if (typeof alias !== 'string' || alias.length === 0) {
    throw new Error('Dependency alias must be a non-empty string.');
  }

  ValidateIdentifier(alias, 'Dependency alias');

  if (typeof module_specifier !== 'string' || module_specifier.length === 0) {
    throw new Error('Dependency module_specifier must be a non-empty string.');
  }

  if (typeof export_name !== 'undefined' && typeof export_name !== 'string') {
    throw new Error('Dependency export_name must be a string when provided.');
  }

  const resolved_module = await ResolveModule(module_specifier);
  let resolved_dependency;

  if (is_default_export) {
    resolved_dependency = resolved_module && resolved_module.default;
  } else if (typeof export_name === 'string' && export_name.length > 0) {
    resolved_dependency = resolved_module && resolved_module[export_name];
  } else {
    resolved_dependency = resolved_module;
  }

  if (typeof resolved_dependency === 'undefined') {
    throw new Error(
      'Dependency alias "' + alias + '" could not resolve requested export from "' + module_specifier + '".'
    );
  }

  worker_dependency_registry.set(alias, resolved_dependency);
}

function InstallConstant(payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Constant payload must be an object.');
  }

  const name = payload.name;
  const value = payload.value;

  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('Constant name must be a non-empty string.');
  }

  ValidateIdentifier(name, 'Constant name');
  EnsureSerializable(value, 'Constant value');

  worker_constant_registry.set(name, value);
}

globalThis.wpc_import = async function(alias) {
  if (typeof alias !== 'string' || alias.length === 0) {
    throw new Error('wpc_import(alias) requires a non-empty string alias.');
  }

  if (!worker_dependency_registry.has(alias)) {
    throw new Error('Dependency alias "' + alias + '" is not defined on this worker.');
  }

  return worker_dependency_registry.get(alias);
};

globalThis.wpc_constant = function(name) {
  if (typeof name !== 'string' || name.length === 0) {
    throw new Error('wpc_constant(name) requires a non-empty string name.');
  }

  if (!worker_constant_registry.has(name)) {
    throw new Error('Constant "' + name + '" is not defined on this worker.');
  }

  return worker_constant_registry.get(name);
};

async function HandleControlRequest(message) {
  const control_request_id = message.control_request_id;

  if (typeof control_request_id !== 'string' || control_request_id.length === 0) {
    EmitWorkerEvent({
      event_name: 'malformed_control_request',
      severity: 'warn',
      details: {
        reason: 'control_request_id missing or invalid'
      }
    });

    return;
  }

  try {
    if (message.command === 'define_function') {
      InstallFunction(message.payload);
    } else if (message.command === 'undefine_function') {
      const function_name = message.payload && message.payload.name;
      if (typeof function_name !== 'string' || function_name.length === 0) {
        throw new Error('Function name is required for undefine.');
      }

      worker_function_registry.delete(function_name);
    } else if (message.command === 'define_dependency') {
      await InstallDependency(message.payload);
    } else if (message.command === 'undefine_dependency') {
      const dependency_alias = message.payload && message.payload.alias;
      if (typeof dependency_alias !== 'string' || dependency_alias.length === 0) {
        throw new Error('Dependency alias is required for undefine.');
      }

      worker_dependency_registry.delete(dependency_alias);
    } else if (message.command === 'define_constant') {
      InstallConstant(message.payload);
    } else if (message.command === 'undefine_constant') {
      const constant_name = message.payload && message.payload.name;
      if (typeof constant_name !== 'string' || constant_name.length === 0) {
        throw new Error('Constant name is required for undefine.');
      }

      worker_constant_registry.delete(constant_name);
    } else if (message.command === 'shutdown') {
      SafePostMessage({
        message_type: 'control_response',
        control_request_id,
        ok: true
      });

      EmitWorkerEvent({
        event_name: 'worker_shutdown_requested',
        severity: 'info',
        correlation_id: control_request_id
      });

      setImmediate(function() {
        process.exit(0);
      });

      return;
    } else {
      throw new Error('Unknown control command: ' + String(message.command));
    }

    SafePostMessage({
      message_type: 'control_response',
      control_request_id,
      ok: true
    });
  } catch (error) {
    EmitWorkerEvent({
      event_name: 'control_command_failed',
      severity: 'error',
      correlation_id: control_request_id,
      error: ToRemoteError(error),
      details: {
        command: message.command
      }
    });

    SafePostMessage({
      message_type: 'control_response',
      control_request_id,
      ok: false,
      error: ToRemoteError(error)
    });
  }
}

async function HandleCallRequest(message) {
  const request_id = message.request_id;
  const function_name = message.function_name;

  if (typeof request_id !== 'string' || request_id.length === 0) {
    EmitWorkerEvent({
      event_name: 'malformed_call_request',
      severity: 'warn',
      details: {
        reason: 'request_id missing or invalid'
      }
    });

    return;
  }

  if (typeof function_name !== 'string' || function_name.length === 0) {
    EmitWorkerEvent({
      event_name: 'malformed_call_request',
      severity: 'warn',
      correlation_id: request_id,
      details: {
        reason: 'function_name missing or invalid'
      }
    });

    SafePostMessage({
      message_type: 'call_response',
      request_id,
      ok: false,
      error: {
        name: 'Error',
        message: 'Malformed call request: function_name is required.'
      }
    });

    return;
  }

  const worker_function = worker_function_registry.get(function_name);

  if (!worker_function) {
    EmitWorkerEvent({
      event_name: 'call_target_missing',
      severity: 'warn',
      correlation_id: request_id,
      details: {
        function_name
      }
    });

    SafePostMessage({
      message_type: 'call_response',
      request_id,
      ok: false,
      error: {
        name: 'Error',
        message: 'Worker function "' + function_name + '" is not defined.'
      }
    });

    return;
  }

  try {
    const call_args = Array.isArray(message.call_args)
      ? message.call_args
      : [message.params];

    const function_context = {
      dependencies: Object.fromEntries(worker_dependency_registry.entries()),
      constants: Object.fromEntries(worker_constant_registry.entries())
    };

    const return_value = await worker_function(...call_args, function_context);
    EnsureSerializable(return_value, 'Worker return value');

    SafePostMessage({
      message_type: 'call_response',
      request_id,
      ok: true,
      return_value
    });
  } catch (error) {
    EmitWorkerEvent({
      event_name: 'call_execution_failed',
      severity: 'error',
      correlation_id: request_id,
      error: ToRemoteError(error),
      details: {
        function_name
      }
    });

    SafePostMessage({
      message_type: 'call_response',
      request_id,
      ok: false,
      error: ToRemoteError(error)
    });
  }
}

async function HandleParentMessage(message) {
  if (!message || typeof message !== 'object') {
    EmitWorkerEvent({
      event_name: 'malformed_parent_message',
      severity: 'warn',
      details: {
        reason: 'message is not an object'
      }
    });

    return;
  }

  const message_type = message.message_type;

  if (message_type === 'control_request') {
    await HandleControlRequest(message);
    return;
  }

  if (message_type === 'call_request') {
    await HandleCallRequest(message);
    return;
  }

  EmitWorkerEvent({
    event_name: 'unknown_message_type',
    severity: 'warn',
    details: {
      message_type: typeof message_type === 'string' ? message_type : null
    }
  });
}

parentPort.on('message', function(message) {
  void HandleParentMessage(message).catch(function(error) {
    EmitWorkerEvent({
      event_name: 'message_handler_failure',
      severity: 'error',
      error: ToRemoteError(error)
    });
  });
});

process.on('uncaughtException', function(error) {
  EmitWorkerEvent({
    event_name: 'uncaught_exception',
    severity: 'error',
    error: ToRemoteError(error)
  });
});

process.on('unhandledRejection', function(reason) {
  EmitWorkerEvent({
    event_name: 'unhandled_rejection',
    severity: 'error',
    error: ToRemoteError(reason)
  });
});

SafePostMessage({ message_type: 'ready' });
EmitWorkerEvent({
  event_name: 'worker_ready',
  severity: 'info',
  details: {
    pid: process.pid
  }
});
`;
}

export class WorkerProcedureCall {
  private lifecycle_state: lifecycle_state_t = 'stopped';
  private target_worker_count = 0;

  private worker_state_by_id = new Map<number, worker_state_t>();
  private function_definition_by_name = new Map<string, worker_function_definition_t>();
  private dependency_definition_by_alias = new Map<
    string,
    worker_dependency_definition_t
  >();
  private constant_definition_by_name = new Map<string, worker_constant_definition_t>();

  private pending_call_by_request_id = new Map<string, pending_call_record_t>();
  private pending_control_by_request_id = new Map<
    string,
    pending_control_record_t
  >();

  private ready_waiter_by_worker_id = new Map<number, worker_ready_waiter_t>();
  private exit_waiter_by_worker_id = new Map<number, worker_exit_waiter_t>();
  private last_error_by_worker_id = new Map<number, Error>();
  private worker_event_listener_by_id = new Map<number, worker_event_listener_t>();

  private next_worker_id = 1;
  private next_call_request_id = 1;
  private next_control_request_id = 1;
  private next_worker_event_id = 1;
  private next_worker_event_listener_id = 1;
  private round_robin_index = 0;

  private default_call_timeout_ms = 30_000;
  private default_control_timeout_ms = 10_000;
  private default_start_timeout_ms = 10_000;
  private default_stop_timeout_ms = 10_000;
  private restart_on_failure = true;
  private max_restarts_per_worker = 3;
  private max_pending_calls_per_worker = 10_000;
  private restart_base_delay_ms = 100;
  private restart_max_delay_ms = 5_000;
  private restart_jitter_ms = 100;

  private stop_in_progress_promise: Promise<void> | null = null;

  public readonly call: worker_call_proxy_t;

  constructor(params: workerprocedurecall_constructor_params_t = {}) {
    this.applyRuntimeOptions({ options: params });
    this.call = this.createCallProxy();
  }

  onWorkerEvent(params: { listener: worker_event_listener_t }): number {
    const { listener } = params;

    const listener_id = this.next_worker_event_listener_id;
    this.next_worker_event_listener_id += 1;

    this.worker_event_listener_by_id.set(listener_id, listener);
    return listener_id;
  }

  offWorkerEvent(params: { listener_id: number }): void {
    const { listener_id } = params;
    this.worker_event_listener_by_id.delete(listener_id);
  }

  getWorkerHealthStates(): worker_health_information_t[] {
    return Array.from(this.worker_state_by_id.values())
      .sort((left_worker_state, right_worker_state): number => {
        return left_worker_state.worker_id - right_worker_state.worker_id;
      })
      .map((worker_state): worker_health_information_t => {
        return {
          worker_id: worker_state.worker_id,
          health_state: worker_state.health_state,
          pending_call_count: worker_state.pending_call_request_ids.size,
          pending_control_count: worker_state.pending_control_request_ids.size,
          restart_attempt: worker_state.restart_attempt
        };
      });
  }

  async defineWorkerFunction<args_t extends unknown[] = [unknown], return_t = unknown>(
    params: define_worker_function_params_t<args_t, return_t>
  ): Promise<void> {
    const { name, worker_func } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Function name must be a non-empty string.');
    }

    ValidateIdentifier({ value: name, label: 'Function name' });

    if (typeof worker_func !== 'function') {
      throw new Error('worker_func must be a function.');
    }

    const function_source = worker_func.toString();
    if (function_source.length === 0) {
      throw new Error(`Function "${name}" could not be serialized.`);
    }

    const normalized_function_source = NormalizeFunctionSourceForHash({
      function_source
    });
    const function_hash_sha1 = ComputeFunctionHashSha1({ function_source });
    const parameter_signature = ParseParameterSignature({ function_source });
    const required_dependency_aliases = ParseStringLiteralDependencies({
      function_source
    });
    const required_constant_names = ParseStringLiteralConstants({
      function_source
    });

    const existing_definition = this.function_definition_by_name.get(name);
    if (
      existing_definition &&
      existing_definition.normalized_function_source === normalized_function_source &&
      existing_definition.function_hash_sha1 === function_hash_sha1
    ) {
      return;
    }

    const updated_definition: worker_function_definition_t =
      existing_definition ?? {
        name,
        worker_func,
        function_source,
        normalized_function_source,
        function_hash_sha1,
        parameter_signature,
        required_dependency_aliases,
        required_constant_names,
        installed_worker_ids: new Set<number>()
      };

    updated_definition.worker_func = worker_func;
    updated_definition.function_source = function_source;
    updated_definition.normalized_function_source = normalized_function_source;
    updated_definition.function_hash_sha1 = function_hash_sha1;
    updated_definition.parameter_signature = parameter_signature;
    updated_definition.required_dependency_aliases = required_dependency_aliases;
    updated_definition.required_constant_names = required_constant_names;
    updated_definition.installed_worker_ids.clear();

    this.function_definition_by_name.set(name, updated_definition);

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const installation_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.installFunctionOnWorker({
            worker_id: worker_state.worker_id,
            function_definition: updated_definition
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = installation_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to install function "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  async defineWorkerDependency(
    params: define_worker_dependency_params_t
  ): Promise<void> {
    const { alias, module_specifier } = params;
    const export_name =
      typeof params.export_name === 'string' && params.export_name.length > 0
        ? params.export_name
        : null;
    const is_default_export = params.is_default_export === true;

    if (typeof alias !== 'string' || alias.length === 0) {
      throw new Error('Dependency alias must be a non-empty string.');
    }

    ValidateIdentifier({ value: alias, label: 'Dependency alias' });

    if (typeof module_specifier !== 'string' || module_specifier.length === 0) {
      throw new Error('Dependency module_specifier must be a non-empty string.');
    }

    if (is_default_export && export_name !== null) {
      throw new Error(
        'Dependency cannot set both is_default_export and export_name simultaneously.'
      );
    }

    const existing_definition = this.dependency_definition_by_alias.get(alias);
    if (
      existing_definition &&
      existing_definition.module_specifier === module_specifier &&
      existing_definition.export_name === export_name &&
      existing_definition.is_default_export === is_default_export
    ) {
      if (this.lifecycle_state === 'running') {
        await this.installDependencyAcrossRunningWorkers({
          alias,
          dependency_definition: existing_definition
        });
      }
      return;
    }

    const dependency_definition: worker_dependency_definition_t =
      existing_definition ?? {
        alias,
        module_specifier,
        export_name,
        is_default_export,
        installed_worker_ids: new Set<number>()
      };

    dependency_definition.alias = alias;
    dependency_definition.module_specifier = module_specifier;
    dependency_definition.export_name = export_name;
    dependency_definition.is_default_export = is_default_export;
    dependency_definition.installed_worker_ids.clear();

    this.dependency_definition_by_alias.set(alias, dependency_definition);
    await this.installDependencyAcrossRunningWorkers({
      alias,
      dependency_definition
    });
  }

  async undefineWorkerDependency(
    params: undefine_worker_dependency_params_t
  ): Promise<void> {
    const { alias } = params;

    if (typeof alias !== 'string' || alias.length === 0) {
      throw new Error('Dependency alias must be a non-empty string.');
    }

    const existing_definition = this.dependency_definition_by_alias.get(alias);
    if (!existing_definition) {
      return;
    }

    this.dependency_definition_by_alias.delete(alias);

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const removal_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.removeDependencyOnWorker({
            worker_id: worker_state.worker_id,
            alias
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = removal_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to remove dependency "${alias}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  async getWorkerDependencies(): Promise<remote_dependency_information_t[]> {
    return Array.from(this.dependency_definition_by_alias.values())
      .sort((left_definition, right_definition): number => {
        return left_definition.alias.localeCompare(right_definition.alias);
      })
      .map((dependency_definition): remote_dependency_information_t => {
        return {
          alias: dependency_definition.alias,
          module_specifier: dependency_definition.module_specifier,
          export_name: dependency_definition.export_name,
          is_default_export: dependency_definition.is_default_export,
          installed_worker_count: dependency_definition.installed_worker_ids.size
        };
      });
  }

  async defineWorkerConstant(params: define_worker_constant_params_t): Promise<void> {
    const { name, value } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Constant name must be a non-empty string.');
    }

    ValidateIdentifier({ value: name, label: 'Constant name' });
    EnsureSerializable({ value, label: `Constant "${name}" value` });

    const existing_definition = this.constant_definition_by_name.get(name);
    if (existing_definition) {
      try {
        if (JSON.stringify(existing_definition.value) === JSON.stringify(value)) {
          if (this.lifecycle_state === 'running') {
            await this.installConstantAcrossRunningWorkers({
              name,
              constant_definition: existing_definition
            });
          }
          return;
        }
      } catch {
        // fall through to redefinition path when equality cannot be determined.
      }
    }

    const constant_definition: worker_constant_definition_t =
      existing_definition ?? {
        name,
        value,
        installed_worker_ids: new Set<number>()
      };

    constant_definition.name = name;
    constant_definition.value = value;
    constant_definition.installed_worker_ids.clear();

    this.constant_definition_by_name.set(name, constant_definition);
    await this.installConstantAcrossRunningWorkers({
      name,
      constant_definition
    });
  }

  async undefineWorkerConstant(
    params: undefine_worker_constant_params_t
  ): Promise<void> {
    const { name } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Constant name must be a non-empty string.');
    }

    const existing_definition = this.constant_definition_by_name.get(name);
    if (!existing_definition) {
      return;
    }

    this.constant_definition_by_name.delete(name);

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const removal_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.removeConstantOnWorker({
            worker_id: worker_state.worker_id,
            name
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = removal_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to remove constant "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  async getWorkerConstants(): Promise<remote_constant_information_t[]> {
    return Array.from(this.constant_definition_by_name.values())
      .sort((left_definition, right_definition): number => {
        return left_definition.name.localeCompare(right_definition.name);
      })
      .map((constant_definition): remote_constant_information_t => {
        return {
          name: constant_definition.name,
          installed_worker_count: constant_definition.installed_worker_ids.size
        };
      });
  }

  async startWorkers(params: start_workers_params_t): Promise<void> {
    const { count } = params;

    if (this.lifecycle_state !== 'stopped') {
      throw new Error('Workers are already started or currently transitioning.');
    }

    ValidatePositiveInteger({ value: count, label: 'count' });
    this.applyRuntimeOptions({ options: params });

    this.lifecycle_state = 'starting';
    this.target_worker_count = count;

    try {
      for (let index = 0; index < count; index += 1) {
        await this.createWorker({ restart_attempt: 0 });
      }

      this.lifecycle_state = 'running';
    } catch (error) {
      await this.stopWorkers();
      throw new Error(`Failed to start workers: ${GetErrorMessage({ error })}`);
    }
  }

  async getRemoteFunctions(): Promise<remote_function_information_t[]> {
    return Array.from(this.function_definition_by_name.values())
      .sort((left_definition, right_definition): number => {
        return left_definition.name.localeCompare(right_definition.name);
      })
      .map((function_definition): remote_function_information_t => {
        return {
          name: function_definition.name,
          parameter_signature: function_definition.parameter_signature,
          function_hash_sha1: function_definition.function_hash_sha1,
          installed_worker_count: function_definition.installed_worker_ids.size
        };
      });
  }

  async undefineWokerFunction(
    params: undefine_worker_function_params_t
  ): Promise<void> {
    const { name } = params;

    if (typeof name !== 'string' || name.length === 0) {
      throw new Error('Function name must be a non-empty string.');
    }

    const existing_definition = this.function_definition_by_name.get(name);
    if (!existing_definition) {
      return;
    }

    this.function_definition_by_name.delete(name);

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const removal_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.removeFunctionOnWorker({
            worker_id: worker_state.worker_id,
            function_name: name
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = removal_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to remove function "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  async undefineWorkerFunction(
    params: undefine_worker_function_params_t
  ): Promise<void> {
    await this.undefineWokerFunction(params);
  }

  async stopWorkers(): Promise<void> {
    if (this.stop_in_progress_promise) {
      await this.stop_in_progress_promise;
      return;
    }

    if (this.lifecycle_state === 'stopped') {
      return;
    }

    this.stop_in_progress_promise = this.stopWorkersInternal();

    try {
      await this.stop_in_progress_promise;
    } finally {
      this.stop_in_progress_promise = null;
    }
  }

  private async stopWorkersInternal(): Promise<void> {
    this.lifecycle_state = 'stopping';

    const worker_states = Array.from(this.worker_state_by_id.values());
    for (const worker_state of worker_states) {
      worker_state.shutting_down = true;
      this.setWorkerHealthState({
        worker_id: worker_state.worker_id,
        health_state: 'stopped',
        reason: 'stopWorkers requested'
      });
    }

    await Promise.all(
      worker_states.map(async (worker_state): Promise<void> => {
        if (worker_state.ready) {
          try {
            await this.sendControlCommand({
              worker_id: worker_state.worker_id,
              command: 'shutdown',
              payload: {},
              timeout_ms: Math.min(2_000, this.default_control_timeout_ms)
            });
          } catch {
            // Ignore and continue into forced termination path.
          }
        }

        await this.waitForWorkerExitOrTerminate({
          worker_id: worker_state.worker_id,
          timeout_ms: this.default_stop_timeout_ms
        });
      })
    );

    this.rejectAllPendingCalls({
      reason: new Error('Workers were stopped before call completion.')
    });

    this.rejectAllPendingControls({
      reason: new Error('Workers were stopped before control completion.')
    });

    for (const function_definition of this.function_definition_by_name.values()) {
      function_definition.installed_worker_ids.clear();
    }

    for (const dependency_definition of this.dependency_definition_by_alias.values()) {
      dependency_definition.installed_worker_ids.clear();
    }

    for (const constant_definition of this.constant_definition_by_name.values()) {
      constant_definition.installed_worker_ids.clear();
    }

    this.worker_state_by_id.clear();
    this.ready_waiter_by_worker_id.clear();
    this.exit_waiter_by_worker_id.clear();
    this.last_error_by_worker_id.clear();

    this.round_robin_index = 0;
    this.target_worker_count = 0;
    this.lifecycle_state = 'stopped';
  }

  private async callWorkerFunction(params: {
    function_name: string;
    call_args: unknown[];
  }): Promise<unknown> {
    const { function_name, call_args } = params;

    if (this.lifecycle_state !== 'running') {
      throw new Error('Workers are not running. Start workers before invoking calls.');
    }

    const function_definition = this.function_definition_by_name.get(function_name);
    if (!function_definition) {
      throw new Error(`Function "${function_name}" is not defined.`);
    }

    EnsureSerializable({ value: call_args, label: 'Call args' });

    const worker_state = this.selectWorkerForCall({
      function_definition,
      function_name
    });
    const request_id = this.generateCallRequestId();

    return await new Promise<unknown>((resolve, reject): void => {
      const timeout_handle = setTimeout((): void => {
        const pending_call = this.pending_call_by_request_id.get(request_id);
        if (!pending_call) {
          return;
        }

        this.pending_call_by_request_id.delete(request_id);
        const current_worker_state = this.worker_state_by_id.get(worker_state.worker_id);
        current_worker_state?.pending_call_request_ids.delete(request_id);
        if (current_worker_state) {
          this.setWorkerHealthState({
            worker_id: current_worker_state.worker_id,
            health_state: 'degraded',
            reason: `call timeout for ${function_name}`
          });
        }

        reject(
          new Error(
            `Call to "${function_name}" timed out after ${this.default_call_timeout_ms}ms.`
          )
        );
      }, this.default_call_timeout_ms);

      this.pending_call_by_request_id.set(request_id, {
        resolve,
        reject,
        timeout_handle,
        worker_id: worker_state.worker_id,
        function_name
      });

      worker_state.pending_call_request_ids.add(request_id);

      const message: call_request_message_t = {
        message_type: 'call_request',
        request_id,
        function_name,
        call_args
      };

      try {
        worker_state.worker_instance.postMessage(message);
      } catch (error) {
        clearTimeout(timeout_handle);
        worker_state.pending_call_request_ids.delete(request_id);
        this.pending_call_by_request_id.delete(request_id);
        this.setWorkerHealthState({
          worker_id: worker_state.worker_id,
          health_state: 'degraded',
          reason: `call dispatch failed for ${function_name}`
        });

        reject(
          new Error(
            `Failed to dispatch call "${function_name}": ${GetErrorMessage({ error })}`
          )
        );
      }
    });
  }

  private createCallProxy(): worker_call_proxy_t {
    const call_proxy_target: Record<string, unknown> = {};

    const call_proxy = new Proxy(call_proxy_target, {
      get: (_target, property_name: string | symbol): unknown => {
        if (typeof property_name !== 'string') {
          return undefined;
        }

        if (
          property_name === 'then' ||
          property_name === 'catch' ||
          property_name === 'finally'
        ) {
          return undefined;
        }

        return async (...call_args: unknown[]): Promise<unknown> => {
          return await this.callWorkerFunction({
            function_name: property_name,
            call_args
          });
        };
      }
    });

    return call_proxy as worker_call_proxy_t;
  }

  private applyRuntimeOptions(params: { options: runtime_options_t }): void {
    const { options } = params;

    if (typeof options.call_timeout_ms === 'number') {
      ValidatePositiveInteger({
        value: options.call_timeout_ms,
        label: 'call_timeout_ms'
      });
      this.default_call_timeout_ms = options.call_timeout_ms;
    }

    if (typeof options.control_timeout_ms === 'number') {
      ValidatePositiveInteger({
        value: options.control_timeout_ms,
        label: 'control_timeout_ms'
      });
      this.default_control_timeout_ms = options.control_timeout_ms;
    }

    if (typeof options.start_timeout_ms === 'number') {
      ValidatePositiveInteger({
        value: options.start_timeout_ms,
        label: 'start_timeout_ms'
      });
      this.default_start_timeout_ms = options.start_timeout_ms;
    }

    if (typeof options.stop_timeout_ms === 'number') {
      ValidatePositiveInteger({
        value: options.stop_timeout_ms,
        label: 'stop_timeout_ms'
      });
      this.default_stop_timeout_ms = options.stop_timeout_ms;
    }

    if (typeof options.restart_on_failure === 'boolean') {
      this.restart_on_failure = options.restart_on_failure;
    }

    if (typeof options.max_restarts_per_worker === 'number') {
      ValidateNonNegativeInteger({
        value: options.max_restarts_per_worker,
        label: 'max_restarts_per_worker'
      });
      this.max_restarts_per_worker = options.max_restarts_per_worker;
    }

    if (typeof options.max_pending_calls_per_worker === 'number') {
      ValidatePositiveInteger({
        value: options.max_pending_calls_per_worker,
        label: 'max_pending_calls_per_worker'
      });
      this.max_pending_calls_per_worker = options.max_pending_calls_per_worker;
    }

    if (typeof options.restart_base_delay_ms === 'number') {
      ValidatePositiveInteger({
        value: options.restart_base_delay_ms,
        label: 'restart_base_delay_ms'
      });
      this.restart_base_delay_ms = options.restart_base_delay_ms;
    }

    if (typeof options.restart_max_delay_ms === 'number') {
      ValidatePositiveInteger({
        value: options.restart_max_delay_ms,
        label: 'restart_max_delay_ms'
      });
      this.restart_max_delay_ms = options.restart_max_delay_ms;
    }

    if (typeof options.restart_jitter_ms === 'number') {
      ValidateNonNegativeInteger({
        value: options.restart_jitter_ms,
        label: 'restart_jitter_ms'
      });
      this.restart_jitter_ms = options.restart_jitter_ms;
    }
  }

  private generateCallRequestId(): string {
    const request_id = `call_${this.next_call_request_id}`;
    this.next_call_request_id += 1;
    return request_id;
  }

  private generateControlRequestId(): string {
    const request_id = `control_${this.next_control_request_id}`;
    this.next_control_request_id += 1;
    return request_id;
  }

  private generateWorkerEventId(): string {
    const event_id = `event_${this.next_worker_event_id}`;
    this.next_worker_event_id += 1;
    return event_id;
  }

  private emitWorkerEvent(params: { worker_event: worker_event_t }): void {
    const { worker_event } = params;

    for (const listener of this.worker_event_listener_by_id.values()) {
      try {
        listener(worker_event);
      } catch {
        // Listener exceptions must never impact worker supervision.
      }
    }
  }

  private emitParentWorkerEvent(params: {
    worker_id: number;
    event_name: string;
    severity: worker_event_severity_t;
    correlation_id?: string;
    error?: remote_error_t;
    details?: Record<string, unknown>;
  }): void {
    const { worker_id, event_name, severity, correlation_id, error, details } =
      params;

    this.emitWorkerEvent({
      worker_event: {
        event_id: this.generateWorkerEventId(),
        worker_id,
        source: 'parent',
        event_name,
        severity,
        timestamp: new Date().toISOString(),
        correlation_id,
        error,
        details
      }
    });
  }

  private setWorkerHealthState(params: {
    worker_id: number;
    health_state: worker_health_state_t;
    reason?: string;
  }): void {
    const { worker_id, health_state, reason } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      return;
    }

    if (worker_state.health_state === health_state) {
      return;
    }

    worker_state.health_state = health_state;

    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'worker_health_state_changed',
      severity: health_state === 'degraded' ? 'warn' : 'info',
      details: {
        health_state,
        reason: reason ?? null
      }
    });
  }

  private async waitForMs(params: { delay_ms: number }): Promise<void> {
    const { delay_ms } = params;

    if (delay_ms <= 0) {
      return;
    }

    await new Promise<void>((resolve): void => {
      setTimeout(resolve, delay_ms);
    });
  }

  private getRestartDelayMs(params: { restart_attempt: number }): number {
    const { restart_attempt } = params;

    const exponential_multiplier = Math.max(1, 2 ** Math.max(0, restart_attempt - 1));
    const base_delay_ms = this.restart_base_delay_ms * exponential_multiplier;
    const capped_delay_ms = Math.min(base_delay_ms, this.restart_max_delay_ms);

    if (this.restart_jitter_ms <= 0) {
      return capped_delay_ms;
    }

    const jitter_delta = Math.floor(Math.random() * (this.restart_jitter_ms + 1));
    return Math.min(capped_delay_ms + jitter_delta, this.restart_max_delay_ms);
  }

  private async createWorker(params: { restart_attempt: number }): Promise<number> {
    const worker_id = this.next_worker_id;
    this.next_worker_id += 1;

    const worker_instance = new Worker(BuildWorkerRuntimeScript(), { eval: true });

    const worker_state: worker_state_t = {
      worker_id,
      worker_instance,
      health_state: 'starting',
      ready: false,
      shutting_down: false,
      restart_attempt: params.restart_attempt,
      pending_call_request_ids: new Set<string>(),
      pending_control_request_ids: new Set<string>()
    };

    this.worker_state_by_id.set(worker_id, worker_state);

    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'worker_spawned',
      severity: 'info',
      details: {
        restart_attempt: params.restart_attempt
      }
    });

    worker_instance.on('message', (message: unknown): void => {
      this.handleWorkerMessage({ worker_id, message });
    });

    worker_instance.on('error', (error: Error): void => {
      this.handleWorkerError({ worker_id, error });
    });

    worker_instance.on('exit', (exit_code: number): void => {
      this.handleWorkerExit({ worker_id, exit_code });
    });

    await this.waitForWorkerReady({
      worker_id,
      timeout_ms: this.default_start_timeout_ms
    });

    await this.installAllDependenciesOnWorker({ worker_id });
    await this.installAllConstantsOnWorker({ worker_id });
    await this.installAllFunctionsOnWorker({ worker_id });

    return worker_id;
  }

  private async installAllFunctionsOnWorker(params: {
    worker_id: number;
  }): Promise<void> {
    const { worker_id } = params;

    const function_definitions = Array.from(
      this.function_definition_by_name.values()
    ).sort((left_definition, right_definition): number => {
      return left_definition.name.localeCompare(right_definition.name);
    });

    for (const function_definition of function_definitions) {
      await this.installFunctionOnWorker({ worker_id, function_definition });
    }
  }

  private async installAllDependenciesOnWorker(params: {
    worker_id: number;
  }): Promise<void> {
    const { worker_id } = params;

    const dependency_definitions = Array.from(
      this.dependency_definition_by_alias.values()
    ).sort((left_definition, right_definition): number => {
      return left_definition.alias.localeCompare(right_definition.alias);
    });

    for (const dependency_definition of dependency_definitions) {
      await this.installDependencyOnWorker({ worker_id, dependency_definition });
    }
  }

  private async installAllConstantsOnWorker(params: {
    worker_id: number;
  }): Promise<void> {
    const { worker_id } = params;

    const constant_definitions = Array.from(
      this.constant_definition_by_name.values()
    ).sort((left_definition, right_definition): number => {
      return left_definition.name.localeCompare(right_definition.name);
    });

    for (const constant_definition of constant_definitions) {
      await this.installConstantOnWorker({ worker_id, constant_definition });
    }
  }

  private async installDependencyAcrossRunningWorkers(params: {
    alias: string;
    dependency_definition: worker_dependency_definition_t;
  }): Promise<void> {
    const { alias, dependency_definition } = params;

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const install_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.installDependencyOnWorker({
            worker_id: worker_state.worker_id,
            dependency_definition
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = install_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to install dependency "${alias}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  private async installConstantAcrossRunningWorkers(params: {
    name: string;
    constant_definition: worker_constant_definition_t;
  }): Promise<void> {
    const { name, constant_definition } = params;

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const worker_states = this.getReadyWorkerStates();
    const install_results = await Promise.allSettled(
      worker_states.map(async (worker_state): Promise<void> => {
        try {
          await this.installConstantOnWorker({
            worker_id: worker_state.worker_id,
            constant_definition
          });
        } catch (error) {
          throw new Error(
            `Worker ${worker_state.worker_id}: ${GetErrorMessage({ error })}`
          );
        }
      })
    );

    const errors = install_results
      .filter((result): result is PromiseRejectedResult => {
        return result.status === 'rejected';
      })
      .map((result): string => {
        return GetErrorMessage({ error: result.reason });
      });

    if (errors.length > 0) {
      throw new Error(
        `Failed to install constant "${name}" on one or more workers: ${errors.join('; ')}`
      );
    }
  }

  private async installFunctionOnWorker(params: {
    worker_id: number;
    function_definition: worker_function_definition_t;
  }): Promise<void> {
    const { worker_id, function_definition } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'define_function',
      payload: {
        name: function_definition.name,
        function_source: function_definition.function_source
      }
    });

    function_definition.installed_worker_ids.add(worker_id);
  }

  private async removeFunctionOnWorker(params: {
    worker_id: number;
    function_name: string;
  }): Promise<void> {
    const { worker_id, function_name } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'undefine_function',
      payload: {
        name: function_name
      }
    });
  }

  private async installDependencyOnWorker(params: {
    worker_id: number;
    dependency_definition: worker_dependency_definition_t;
  }): Promise<void> {
    const { worker_id, dependency_definition } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'define_dependency',
      payload: {
        alias: dependency_definition.alias,
        module_specifier: dependency_definition.module_specifier,
        export_name: dependency_definition.export_name ?? undefined,
        is_default_export: dependency_definition.is_default_export
      }
    });

    dependency_definition.installed_worker_ids.add(worker_id);
  }

  private async removeDependencyOnWorker(params: {
    worker_id: number;
    alias: string;
  }): Promise<void> {
    const { worker_id, alias } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'undefine_dependency',
      payload: {
        alias
      }
    });
  }

  private async installConstantOnWorker(params: {
    worker_id: number;
    constant_definition: worker_constant_definition_t;
  }): Promise<void> {
    const { worker_id, constant_definition } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'define_constant',
      payload: {
        name: constant_definition.name,
        value: constant_definition.value
      }
    });

    constant_definition.installed_worker_ids.add(worker_id);
  }

  private async removeConstantOnWorker(params: {
    worker_id: number;
    name: string;
  }): Promise<void> {
    const { worker_id, name } = params;

    await this.sendControlCommand({
      worker_id,
      command: 'undefine_constant',
      payload: {
        name
      }
    });
  }

  private async sendControlCommand(params: {
    worker_id: number;
    command: control_command_t;
    payload?: Record<string, unknown>;
    timeout_ms?: number;
  }): Promise<void> {
    const { worker_id, command, payload, timeout_ms } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      throw new Error(`Worker ${worker_id} is not available.`);
    }

    const control_request_id = this.generateControlRequestId();
    const control_timeout_ms = timeout_ms ?? this.default_control_timeout_ms;

    await new Promise<void>((resolve, reject): void => {
      const timeout_handle = setTimeout((): void => {
        const pending_control = this.pending_control_by_request_id.get(control_request_id);
        if (!pending_control) {
          return;
        }

        this.pending_control_by_request_id.delete(control_request_id);
        worker_state.pending_control_request_ids.delete(control_request_id);
        this.setWorkerHealthState({
          worker_id,
          health_state: 'degraded',
          reason: `control timeout for ${command}`
        });

        reject(
          new Error(
            `Control command "${command}" timed out after ${control_timeout_ms}ms on worker ${worker_id}.`
          )
        );
      }, control_timeout_ms);

      this.pending_control_by_request_id.set(control_request_id, {
        resolve,
        reject,
        timeout_handle,
        worker_id,
        command
      });

      worker_state.pending_control_request_ids.add(control_request_id);

      const message: control_request_message_t = {
        message_type: 'control_request',
        control_request_id,
        command,
        payload
      };

      try {
        worker_state.worker_instance.postMessage(message);
      } catch (error) {
        clearTimeout(timeout_handle);
        worker_state.pending_control_request_ids.delete(control_request_id);
        this.pending_control_by_request_id.delete(control_request_id);
        this.setWorkerHealthState({
          worker_id,
          health_state: 'degraded',
          reason: `control dispatch failed for ${command}`
        });

        reject(
          new Error(
            `Failed to send control command "${command}" to worker ${worker_id}: ${GetErrorMessage({ error })}`
          )
        );
      }
    });
  }

  private async waitForWorkerReady(params: {
    worker_id: number;
    timeout_ms: number;
  }): Promise<void> {
    const { worker_id, timeout_ms } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      throw new Error(`Worker ${worker_id} is not available.`);
    }

    if (worker_state.ready) {
      return;
    }

    await new Promise<void>((resolve, reject): void => {
      const timeout_handle = setTimeout((): void => {
        this.ready_waiter_by_worker_id.delete(worker_id);
        this.setWorkerHealthState({
          worker_id,
          health_state: 'degraded',
          reason: 'ready timeout'
        });
        reject(
          new Error(`Worker ${worker_id} did not report ready within ${timeout_ms}ms.`)
        );
      }, timeout_ms);

      const waiter: worker_ready_waiter_t = {
        resolve: (): void => {
          clearTimeout(timeout_handle);
          resolve();
        },
        reject: (error: Error): void => {
          clearTimeout(timeout_handle);
          reject(error);
        },
        timeout_handle
      };

      this.ready_waiter_by_worker_id.set(worker_id, waiter);

      if (!this.worker_state_by_id.has(worker_id)) {
        this.ready_waiter_by_worker_id.delete(worker_id);
        clearTimeout(timeout_handle);
        reject(new Error(`Worker ${worker_id} exited before becoming ready.`));
      }
    });
  }

  private async waitForWorkerExitOrTerminate(params: {
    worker_id: number;
    timeout_ms: number;
  }): Promise<void> {
    const { worker_id, timeout_ms } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      return;
    }

    await new Promise<void>((resolve): void => {
      const timeout_handle = setTimeout((): void => {
        void worker_state.worker_instance.terminate().finally((): void => {
          this.exit_waiter_by_worker_id.delete(worker_id);
          resolve();
        });
      }, timeout_ms);

      const waiter: worker_exit_waiter_t = {
        resolve: (): void => {
          clearTimeout(timeout_handle);
          resolve();
        },
        timeout_handle
      };

      this.exit_waiter_by_worker_id.set(worker_id, waiter);

      if (!this.worker_state_by_id.has(worker_id)) {
        this.exit_waiter_by_worker_id.delete(worker_id);
        clearTimeout(timeout_handle);
        resolve();
      }
    });
  }

  private selectWorkerForCall(params: {
    function_definition: worker_function_definition_t;
    function_name: string;
  }): worker_state_t {
    const { function_definition, function_name } = params;

    for (const required_alias of function_definition.required_dependency_aliases) {
      if (!this.dependency_definition_by_alias.has(required_alias)) {
        throw new Error(
          `Function "${function_name}" requires dependency alias "${required_alias}" which is not defined.`
        );
      }
    }

    for (const required_name of function_definition.required_constant_names) {
      if (!this.constant_definition_by_name.has(required_name)) {
        throw new Error(
          `Function "${function_name}" requires constant "${required_name}" which is not defined.`
        );
      }
    }

    const eligible_worker_states = this.getReadyWorkerStates().filter(
      (worker_state): boolean => {
        if (!function_definition.installed_worker_ids.has(worker_state.worker_id)) {
          return false;
        }

        for (const required_alias of function_definition.required_dependency_aliases) {
          const dependency_definition =
            this.dependency_definition_by_alias.get(required_alias);
          if (
            !dependency_definition ||
            !dependency_definition.installed_worker_ids.has(worker_state.worker_id)
          ) {
            return false;
          }
        }

        for (const required_name of function_definition.required_constant_names) {
          const constant_definition =
            this.constant_definition_by_name.get(required_name);
          if (
            !constant_definition ||
            !constant_definition.installed_worker_ids.has(worker_state.worker_id)
          ) {
            return false;
          }
        }

        return true;
      }
    );

    if (eligible_worker_states.length === 0) {
      throw new Error(
        `No ready workers with function "${function_name}" installed are available.`
      );
    }

    const search_start_index = this.round_robin_index % eligible_worker_states.length;

    let minimum_pending_call_count = Number.POSITIVE_INFINITY;
    for (const worker_state of eligible_worker_states) {
      const pending_call_count = worker_state.pending_call_request_ids.size;
      if (pending_call_count < minimum_pending_call_count) {
        minimum_pending_call_count = pending_call_count;
      }
    }

    if (minimum_pending_call_count >= this.max_pending_calls_per_worker) {
      throw new Error(
        `All workers are saturated for "${function_name}" (${this.max_pending_calls_per_worker} pending call limit per worker reached).`
      );
    }

    let selected_worker_index = -1;
    for (
      let offset_index = 0;
      offset_index < eligible_worker_states.length;
      offset_index += 1
    ) {
      const candidate_index =
        (search_start_index + offset_index) % eligible_worker_states.length;
      const candidate_worker_state = eligible_worker_states[candidate_index];

      if (
        candidate_worker_state.pending_call_request_ids.size ===
        minimum_pending_call_count
      ) {
        selected_worker_index = candidate_index;
        break;
      }
    }

    if (selected_worker_index < 0) {
      throw new Error('Failed to select a worker for call dispatch.');
    }

    this.round_robin_index =
      (selected_worker_index + 1) % eligible_worker_states.length;

    return eligible_worker_states[selected_worker_index];
  }

  private getReadyWorkerStates(): worker_state_t[] {
    return Array.from(this.worker_state_by_id.values())
      .filter((worker_state): boolean => {
        return (
          worker_state.ready &&
          !worker_state.shutting_down &&
          worker_state.health_state === 'ready'
        );
      })
      .sort((left_worker_state, right_worker_state): number => {
        return left_worker_state.worker_id - right_worker_state.worker_id;
      });
  }

  private handleWorkerMessage(params: {
    worker_id: number;
    message: unknown;
  }): void {
    const { worker_id, message } = params;

    if (typeof message !== 'object' || message === null) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'malformed_worker_message',
        severity: 'warn',
        details: {
          reason: 'message is not an object'
        }
      });
      return;
    }

    const message_data = message as Record<string, unknown>;
    const message_type = message_data.message_type;

    if (message_type === 'ready') {
      this.handleReadyMessage({ worker_id });
      return;
    }

    if (message_type === 'call_response') {
      this.handleCallResponseMessage({
        worker_id,
        message: message as worker_to_parent_message_t
      });
      return;
    }

    if (message_type === 'control_response') {
      this.handleControlResponseMessage({
        worker_id,
        message: message as worker_to_parent_message_t
      });
      return;
    }

    if (message_type === 'worker_event') {
      this.handleWorkerEventMessage({
        worker_id,
        message: message as worker_to_parent_message_t
      });
      return;
    }

    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'unknown_worker_message',
      severity: 'warn',
      details: {
        message_type: typeof message_type === 'string' ? message_type : null
      }
    });
  }

  private handleReadyMessage(params: { worker_id: number }): void {
    const { worker_id } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      return;
    }

    worker_state.ready = true;
    this.setWorkerHealthState({
      worker_id,
      health_state: 'ready',
      reason: 'worker sent ready message'
    });

    const ready_waiter = this.ready_waiter_by_worker_id.get(worker_id);
    if (!ready_waiter) {
      return;
    }

    this.ready_waiter_by_worker_id.delete(worker_id);
    clearTimeout(ready_waiter.timeout_handle);
    ready_waiter.resolve();
  }

  private handleWorkerEventMessage(params: {
    worker_id: number;
    message: worker_to_parent_message_t;
  }): void {
    const { worker_id, message } = params;

    if (message.message_type !== 'worker_event') {
      return;
    }

    const normalized_worker_event: worker_event_t = {
      event_id: message.event_id,
      worker_id,
      source: 'worker',
      event_name: message.event_name,
      severity: message.severity,
      timestamp: message.timestamp,
      correlation_id: message.correlation_id,
      error: message.error,
      details: message.details
    };

    const worker_state = this.worker_state_by_id.get(worker_id);
    const degraded_event_names = new Set<string>([
      'uncaught_exception',
      'message_handler_failure',
      'post_message_failed'
    ]);

    if (
      worker_state &&
      message.severity === 'error' &&
      degraded_event_names.has(message.event_name)
    ) {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'degraded',
        reason: message.event_name
      });
    }

    this.emitWorkerEvent({
      worker_event: normalized_worker_event
    });
  }

  private handleCallResponseMessage(params: {
    worker_id: number;
    message: worker_to_parent_message_t;
  }): void {
    const { worker_id, message } = params;

    if (message.message_type !== 'call_response') {
      return;
    }

    const pending_call = this.pending_call_by_request_id.get(message.request_id);
    if (!pending_call) {
      return;
    }

    this.pending_call_by_request_id.delete(message.request_id);

    const worker_state = this.worker_state_by_id.get(worker_id);
    worker_state?.pending_call_request_ids.delete(message.request_id);

    clearTimeout(pending_call.timeout_handle);

    if (message.ok) {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'ready',
        reason: 'call response ok'
      });
      pending_call.resolve(message.return_value);
      return;
    }

    pending_call.reject(
      ToError({
        remote_error: message.error,
        fallback_message: `Call to "${pending_call.function_name}" failed.`
      })
    );
  }

  private handleControlResponseMessage(params: {
    worker_id: number;
    message: worker_to_parent_message_t;
  }): void {
    const { worker_id, message } = params;

    if (message.message_type !== 'control_response') {
      return;
    }

    const pending_control = this.pending_control_by_request_id.get(
      message.control_request_id
    );

    if (!pending_control) {
      return;
    }

    this.pending_control_by_request_id.delete(message.control_request_id);

    const worker_state = this.worker_state_by_id.get(worker_id);
    worker_state?.pending_control_request_ids.delete(message.control_request_id);

    clearTimeout(pending_control.timeout_handle);

    if (message.ok) {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'ready',
        reason: 'control response ok'
      });
      pending_control.resolve();
      return;
    }

    pending_control.reject(
      ToError({
        remote_error: message.error,
        fallback_message: `Control command "${pending_control.command}" failed.`
      })
    );
  }

  private handleWorkerError(params: { worker_id: number; error: Error }): void {
    const { worker_id, error } = params;
    this.last_error_by_worker_id.set(worker_id, error);
    this.setWorkerHealthState({
      worker_id,
      health_state: 'degraded',
      reason: error.message
    });
    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'worker_error',
      severity: 'error',
      error: ToRemoteError({ error }),
      details: {
        message: error.message
      }
    });
  }

  private handleWorkerExit(params: { worker_id: number; exit_code: number }): void {
    const { worker_id, exit_code } = params;

    const worker_state = this.worker_state_by_id.get(worker_id);
    if (!worker_state) {
      const exit_waiter = this.exit_waiter_by_worker_id.get(worker_id);
      if (exit_waiter) {
        this.exit_waiter_by_worker_id.delete(worker_id);
        clearTimeout(exit_waiter.timeout_handle);
        exit_waiter.resolve();
      }

      return;
    }

    if (!worker_state.shutting_down && this.restart_on_failure) {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'restarting',
        reason: 'worker exited unexpectedly'
      });
    } else {
      this.setWorkerHealthState({
        worker_id,
        health_state: 'stopped',
        reason: 'worker exited'
      });
    }

    this.worker_state_by_id.delete(worker_id);

    for (const function_definition of this.function_definition_by_name.values()) {
      function_definition.installed_worker_ids.delete(worker_id);
    }

    for (const dependency_definition of this.dependency_definition_by_alias.values()) {
      dependency_definition.installed_worker_ids.delete(worker_id);
    }

    for (const constant_definition of this.constant_definition_by_name.values()) {
      constant_definition.installed_worker_ids.delete(worker_id);
    }

    const last_worker_error = this.last_error_by_worker_id.get(worker_id);
    this.last_error_by_worker_id.delete(worker_id);

    const ready_waiter = this.ready_waiter_by_worker_id.get(worker_id);
    if (ready_waiter) {
      this.ready_waiter_by_worker_id.delete(worker_id);
      clearTimeout(ready_waiter.timeout_handle);
      ready_waiter.reject(
        new Error(`Worker ${worker_id} exited before it became ready.`)
      );
    }

    const exit_waiter = this.exit_waiter_by_worker_id.get(worker_id);
    if (exit_waiter) {
      this.exit_waiter_by_worker_id.delete(worker_id);
      clearTimeout(exit_waiter.timeout_handle);
      exit_waiter.resolve();
    }

    const failure_parts = [`Worker ${worker_id} exited with code ${exit_code}.`];
    if (last_worker_error) {
      failure_parts.push(`Last worker error: ${last_worker_error.message}`);
    }

    const failure_reason = failure_parts.join(' ');

    this.emitParentWorkerEvent({
      worker_id,
      event_name: 'worker_exited',
      severity: exit_code === 0 ? 'info' : 'error',
      details: {
        exit_code,
        failure_reason
      }
    });

    this.rejectPendingCallsForWorker({ worker_state, reason: failure_reason });
    this.rejectPendingControlsForWorker({ worker_state, reason: failure_reason });

    if (
      this.lifecycle_state !== 'running' ||
      worker_state.shutting_down ||
      !this.restart_on_failure
    ) {
      return;
    }

    if (this.worker_state_by_id.size >= this.target_worker_count) {
      return;
    }

    if (worker_state.restart_attempt >= this.max_restarts_per_worker) {
      this.emitParentWorkerEvent({
        worker_id,
        event_name: 'worker_restart_exhausted',
        severity: 'error',
        details: {
          restart_attempt: worker_state.restart_attempt,
          max_restarts_per_worker: this.max_restarts_per_worker
        }
      });
      return;
    }

    void this.restartWorker({
      restart_attempt: worker_state.restart_attempt + 1,
      failed_worker_id: worker_id
    });
  }

  private async restartWorker(params: {
    restart_attempt: number;
    failed_worker_id: number;
  }): Promise<void> {
    const { restart_attempt, failed_worker_id } = params;

    if (this.lifecycle_state !== 'running') {
      return;
    }

    const restart_delay_ms = this.getRestartDelayMs({ restart_attempt });
    this.emitParentWorkerEvent({
      worker_id: failed_worker_id,
      event_name: 'worker_restart_scheduled',
      severity: 'warn',
      details: {
        restart_attempt,
        restart_delay_ms
      }
    });

    await this.waitForMs({ delay_ms: restart_delay_ms });

    if (this.lifecycle_state !== 'running') {
      return;
    }

    try {
      await this.createWorker({ restart_attempt });
      const replacement_worker = Array.from(this.worker_state_by_id.values()).find(
        (worker_state): boolean => {
          return worker_state.restart_attempt === restart_attempt;
        }
      );

      this.emitParentWorkerEvent({
        worker_id: replacement_worker?.worker_id ?? failed_worker_id,
        event_name: 'worker_restart_completed',
        severity: 'info',
        details: {
          restart_attempt
        }
      });
    } catch {
      if (restart_attempt >= this.max_restarts_per_worker) {
        this.emitParentWorkerEvent({
          worker_id: failed_worker_id,
          event_name: 'worker_restart_failed',
          severity: 'error',
          details: {
            restart_attempt,
            exhausted: true
          }
        });
        return;
      }

      if (this.lifecycle_state !== 'running') {
        return;
      }

      this.emitParentWorkerEvent({
        worker_id: failed_worker_id,
        event_name: 'worker_restart_failed',
        severity: 'warn',
        details: {
          restart_attempt,
          exhausted: false
        }
      });

      void this.restartWorker({
        restart_attempt: restart_attempt + 1,
        failed_worker_id
      });
    }
  }

  private rejectPendingCallsForWorker(params: {
    worker_state: worker_state_t;
    reason: string;
  }): void {
    const { worker_state, reason } = params;

    for (const request_id of worker_state.pending_call_request_ids) {
      const pending_call = this.pending_call_by_request_id.get(request_id);
      if (!pending_call) {
        continue;
      }

      this.pending_call_by_request_id.delete(request_id);
      clearTimeout(pending_call.timeout_handle);
      pending_call.reject(new Error(reason));
    }

    worker_state.pending_call_request_ids.clear();
  }

  private rejectPendingControlsForWorker(params: {
    worker_state: worker_state_t;
    reason: string;
  }): void {
    const { worker_state, reason } = params;

    for (const control_request_id of worker_state.pending_control_request_ids) {
      const pending_control = this.pending_control_by_request_id.get(control_request_id);
      if (!pending_control) {
        continue;
      }

      this.pending_control_by_request_id.delete(control_request_id);
      clearTimeout(pending_control.timeout_handle);
      pending_control.reject(new Error(reason));
    }

    worker_state.pending_control_request_ids.clear();
  }

  private rejectAllPendingCalls(params: { reason: Error }): void {
    const { reason } = params;

    for (const [request_id, pending_call] of this.pending_call_by_request_id.entries()) {
      clearTimeout(pending_call.timeout_handle);
      pending_call.reject(reason);
      this.pending_call_by_request_id.delete(request_id);
    }
  }

  private rejectAllPendingControls(params: { reason: Error }): void {
    const { reason } = params;

    for (const [request_id, pending_control] of this.pending_control_by_request_id.entries()) {
      clearTimeout(pending_control.timeout_handle);
      pending_control.reject(reason);
      this.pending_control_by_request_id.delete(request_id);
    }
  }
}
