import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

export type fuzz_process_event_t = {
  event_type: 'unhandled_rejection' | 'uncaught_exception';
  timestamp_unix_ms: number;
  error_message: string;
  stack: string | null;
};

type fuzz_failure_artifact_t = {
  suite_name: string;
  seed: string;
  case_index: number;
  generated_payload: unknown;
  error_message: string;
  stack: string | null;
  timestamp_unix_ms: number;
};

type fuzz_run_case_context_t = {
  suite_name: string;
  seed: string;
  case_index: number;
  random_func: () => number;
};

type fuzz_run_case_result_t =
  | void
  | {
      generated_payload?: unknown;
    };

export type fuzz_run_suite_params_t = {
  suite_name: string;
  default_iterations: number;
  default_case_timeout_ms: number;
  run_case_func: (params: fuzz_run_case_context_t) => Promise<fuzz_run_case_result_t>;
};

export type fuzz_process_crash_guard_t = {
  dispose: () => fuzz_process_event_t[];
};

function HashStringToUint32(params: { value: string }): number {
  let hash_value = 0x811c9dc5;
  for (let index = 0; index < params.value.length; index += 1) {
    hash_value ^= params.value.charCodeAt(index);
    hash_value = Math.imul(hash_value, 0x01000193);
  }

  return hash_value >>> 0;
}

export function BuildDeterministicRandomFunction(params: {
  seed: string;
  suite_name: string;
  case_index: number;
}): () => number {
  let state = HashStringToUint32({
    value: `${params.seed}:${params.suite_name}:${params.case_index}`
  });

  return function (): number {
    state = (state + 0x6d2b79f5) >>> 0;
    let next_value = Math.imul(state ^ (state >>> 15), state | 1);
    next_value ^= next_value + Math.imul(next_value ^ (next_value >>> 7), next_value | 61);
    return ((next_value ^ (next_value >>> 14)) >>> 0) / 4294967296;
  };
}

export function RandomIntegerInRange(params: {
  random_func: () => number;
  minimum_value: number;
  maximum_value: number;
}): number {
  const span_value = params.maximum_value - params.minimum_value + 1;
  return params.minimum_value + Math.floor(params.random_func() * span_value);
}

export function RandomBoolean(params: { random_func: () => number }): boolean {
  return params.random_func() >= 0.5;
}

export function RandomAlphaNumericString(params: {
  random_func: () => number;
  minimum_length: number;
  maximum_length: number;
}): string {
  const alphabet =
    'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.';
  const string_length = RandomIntegerInRange({
    random_func: params.random_func,
    minimum_value: params.minimum_length,
    maximum_value: params.maximum_length
  });

  let result = '';
  for (let index = 0; index < string_length; index += 1) {
    const alphabet_index = RandomIntegerInRange({
      random_func: params.random_func,
      minimum_value: 0,
      maximum_value: alphabet.length - 1
    });
    result += alphabet[alphabet_index];
  }

  return result;
}

export function BuildRandomJsonValue(params: {
  random_func: () => number;
  depth: number;
  max_depth: number;
  max_array_length: number;
  max_object_keys: number;
  max_string_length: number;
}): unknown {
  if (params.depth >= params.max_depth) {
    const terminal_type_index = RandomIntegerInRange({
      random_func: params.random_func,
      minimum_value: 0,
      maximum_value: 3
    });
    if (terminal_type_index === 0) {
      return null;
    }
    if (terminal_type_index === 1) {
      return RandomBoolean({
        random_func: params.random_func
      });
    }
    if (terminal_type_index === 2) {
      return RandomIntegerInRange({
        random_func: params.random_func,
        minimum_value: -10_000,
        maximum_value: 10_000
      });
    }

    return RandomAlphaNumericString({
      random_func: params.random_func,
      minimum_length: 0,
      maximum_length: params.max_string_length
    });
  }

  const node_type_index = RandomIntegerInRange({
    random_func: params.random_func,
    minimum_value: 0,
    maximum_value: 5
  });

  if (node_type_index === 0) {
    return null;
  }

  if (node_type_index === 1) {
    return RandomBoolean({
      random_func: params.random_func
    });
  }

  if (node_type_index === 2) {
    return RandomIntegerInRange({
      random_func: params.random_func,
      minimum_value: -100_000,
      maximum_value: 100_000
    });
  }

  if (node_type_index === 3) {
    return RandomAlphaNumericString({
      random_func: params.random_func,
      minimum_length: 0,
      maximum_length: params.max_string_length
    });
  }

  if (node_type_index === 4) {
    const array_length = RandomIntegerInRange({
      random_func: params.random_func,
      minimum_value: 0,
      maximum_value: params.max_array_length
    });
    const value_list: unknown[] = [];
    for (let index = 0; index < array_length; index += 1) {
      value_list.push(
        BuildRandomJsonValue({
          ...params,
          depth: params.depth + 1
        })
      );
    }
    return value_list;
  }

  const key_count = RandomIntegerInRange({
    random_func: params.random_func,
    minimum_value: 0,
    maximum_value: params.max_object_keys
  });
  const value_object: Record<string, unknown> = {};
  for (let index = 0; index < key_count; index += 1) {
    const key_name = RandomAlphaNumericString({
      random_func: params.random_func,
      minimum_length: 1,
      maximum_length: 12
    });
    value_object[key_name] = BuildRandomJsonValue({
      ...params,
      depth: params.depth + 1
    });
  }

  return value_object;
}

export function ResolveFuzzSeed(): string {
  const env_seed = process.env.FUZZ_SEED;
  if (typeof env_seed === 'string' && env_seed.length > 0) {
    return env_seed;
  }

  return `${Date.now()}`;
}

export function ResolveFuzzIterations(params: {
  default_iterations: number;
}): number {
  const env_iterations = process.env.FUZZ_ITERS;
  if (typeof env_iterations !== 'string' || env_iterations.length === 0) {
    return params.default_iterations;
  }

  const parsed_iterations = Number.parseInt(env_iterations, 10);
  if (!Number.isFinite(parsed_iterations) || parsed_iterations <= 0) {
    return params.default_iterations;
  }

  return parsed_iterations;
}

export function ResolveFuzzCaseTimeoutMs(params: {
  default_timeout_ms: number;
}): number {
  const env_timeout = process.env.FUZZ_CASE_TIMEOUT_MS;
  if (typeof env_timeout !== 'string' || env_timeout.length === 0) {
    return params.default_timeout_ms;
  }

  const parsed_timeout = Number.parseInt(env_timeout, 10);
  if (!Number.isFinite(parsed_timeout) || parsed_timeout <= 0) {
    return params.default_timeout_ms;
  }

  return parsed_timeout;
}

export function ResolveFuzzSingleCaseIndex(): number | null {
  const env_case = process.env.FUZZ_CASE;
  if (typeof env_case !== 'string' || env_case.length === 0) {
    return null;
  }

  const parsed_case_index = Number.parseInt(env_case, 10);
  if (!Number.isFinite(parsed_case_index) || parsed_case_index < 0) {
    return null;
  }

  return parsed_case_index;
}

export function InstallProcessCrashGuard(params: {
  suite_name: string;
}): fuzz_process_crash_guard_t {
  const process_event_list: fuzz_process_event_t[] = [];

  const unhandled_rejection_listener = function (error: unknown): void {
    process_event_list.push({
      event_type: 'unhandled_rejection',
      timestamp_unix_ms: Date.now(),
      error_message: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack ?? null : null
    });
  };

  const uncaught_exception_listener = function (error: Error): void {
    process_event_list.push({
      event_type: 'uncaught_exception',
      timestamp_unix_ms: Date.now(),
      error_message: error.message,
      stack: error.stack ?? null
    });
  };

  process.on('unhandledRejection', unhandled_rejection_listener);
  process.on('uncaughtException', uncaught_exception_listener);

  return {
    dispose: (): fuzz_process_event_t[] => {
      process.off('unhandledRejection', unhandled_rejection_listener);
      process.off('uncaughtException', uncaught_exception_listener);

      if (process_event_list.length > 0) {
        process.stdout.write(
          `[fuzz] suite=${params.suite_name} captured_process_events=${process_event_list.length}\n`
        );
      }
      return [...process_event_list];
    }
  };
}

async function RunWithTimeout<T>(params: {
  timeout_ms: number;
  promise_func: () => Promise<T>;
}): Promise<T> {
  const timeout_promise = new Promise<T>((_, reject): void => {
    const timeout_handle = setTimeout((): void => {
      clearTimeout(timeout_handle);
      reject(new Error(`Fuzz case timed out after ${params.timeout_ms}ms.`));
    }, params.timeout_ms);
  });

  return await Promise.race([params.promise_func(), timeout_promise]);
}

function EnsureFailureDirectoryPath(): string {
  const directory_path = path.resolve('/tmp', 'workerprocedurecall_fuzz_failures');
  fs.mkdirSync(directory_path, {
    recursive: true
  });
  return directory_path;
}

function WriteFailureArtifact(params: fuzz_failure_artifact_t): string {
  const directory_path = EnsureFailureDirectoryPath();
  const file_name = `${params.suite_name}_${params.seed}_${params.case_index}.json`;
  const file_path = path.resolve(directory_path, file_name);

  fs.writeFileSync(file_path, JSON.stringify(params, null, 2), {
    encoding: 'utf8'
  });

  return file_path;
}

export async function RunFuzzSuite(params: fuzz_run_suite_params_t): Promise<void> {
  const seed = ResolveFuzzSeed();
  const case_timeout_ms = ResolveFuzzCaseTimeoutMs({
    default_timeout_ms: params.default_case_timeout_ms
  });
  const max_iterations = ResolveFuzzIterations({
    default_iterations: params.default_iterations
  });
  const specific_case_index = ResolveFuzzSingleCaseIndex();

  const case_index_list: number[] =
    specific_case_index === null
      ? Array.from({ length: max_iterations }, (_, case_index): number => case_index)
      : [specific_case_index];

  process.stdout.write(
    `[fuzz] suite=${params.suite_name} seed=${seed} cases=${case_index_list.length} timeout_ms=${case_timeout_ms}\n`
  );

  for (const case_index of case_index_list) {
    const random_func = BuildDeterministicRandomFunction({
      seed,
      suite_name: params.suite_name,
      case_index
    });
    let case_generated_payload: unknown = null;

    try {
      await RunWithTimeout({
        timeout_ms: case_timeout_ms,
        promise_func: async (): Promise<void> => {
          const case_result = await params.run_case_func({
            suite_name: params.suite_name,
            seed,
            case_index,
            random_func
          });
          case_generated_payload = case_result?.generated_payload ?? null;
        }
      });
    } catch (error) {
      const artifact_path = WriteFailureArtifact({
        suite_name: params.suite_name,
        seed,
        case_index,
        generated_payload: case_generated_payload,
        error_message: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack ?? null : null,
        timestamp_unix_ms: Date.now()
      });

      assert.fail(
        [
          `[fuzz] suite=${params.suite_name} failed at case=${case_index} seed=${seed}`,
          `artifact=${artifact_path}`,
          `replay: FUZZ_SEED=${seed} FUZZ_CASE=${case_index} npm run test:fuzz:replay`,
          `error=${error instanceof Error ? error.message : String(error)}`
        ].join('\n')
      );
    }
  }
}
