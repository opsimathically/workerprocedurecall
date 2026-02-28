/**
 * Run with:
 * npm run benchmark:worker-vs-single-sha1-heavy
 */
import { createHash } from 'node:crypto';
import * as os from 'node:os';
import { performance } from 'node:perf_hooks';
import { WorkerProcedureCall } from '../src/index';

type benchmark_result_t = {
  benchmark_name: string;
  total_operations: number;
  total_duration_ms: number;
  operations_per_second: number;
  average_latency_microseconds: number;
  average_latency_nanoseconds: number;
};

type run_worker_benchmark_params_t = {
  total_operations: number;
  warmup_operations: number;
  worker_count: number;
  worker_parallelism: number;
  expected_digest: string;
};

type run_local_benchmark_params_t = {
  total_operations: number;
  warmup_operations: number;
  expected_digest: string;
};

const BENCHMARK_TOTAL_OPERATIONS = 100;
const BENCHMARK_WARMUP_OPERATIONS = 2;
const SHA1_LOOP_ITERATIONS_PER_CALL = 1_000_000;
const SHA1_BENCHMARK_INPUT = 'PLEASEHASHTHISVALUEOVERANDOVER';

function GetLogicalCpuCount(): number {
  if (typeof os.availableParallelism === 'function') {
    return os.availableParallelism();
  }

  return os.cpus().length;
}

function BuildBenchmarkResult(params: {
  benchmark_name: string;
  total_operations: number;
  start_time_ms: number;
  end_time_ms: number;
}): benchmark_result_t {
  const {
    benchmark_name,
    total_operations,
    start_time_ms,
    end_time_ms
  } = params;
  const total_duration_ms = end_time_ms - start_time_ms;
  const total_duration_seconds = total_duration_ms / 1000;
  const operations_per_second = total_operations / total_duration_seconds;
  const average_latency_microseconds =
    (total_duration_ms * 1000) / total_operations;
  const average_latency_nanoseconds =
    (total_duration_ms * 1_000_000) / total_operations;

  return {
    benchmark_name,
    total_operations,
    total_duration_ms,
    operations_per_second,
    average_latency_microseconds,
    average_latency_nanoseconds
  };
}

function ComputeSha1Repeated(): string {
  let current_digest = '';

  for (
    let hash_iteration_index = 0;
    hash_iteration_index < SHA1_LOOP_ITERATIONS_PER_CALL;
    hash_iteration_index += 1
  ) {
    current_digest = createHash('sha1')
      .update(SHA1_BENCHMARK_INPUT)
      .digest('hex');
  }

  return current_digest;
}

function ValidateDigestResult(params: {
  value: unknown;
  expected_digest: string;
  label: string;
  operation_index: number;
}): void {
  const { value, expected_digest, label, operation_index } = params;
  if (value !== expected_digest) {
    throw new Error(
      `${label} returned unexpected digest at operation ${operation_index}: ${String(value)}`
    );
  }
}

function FormatNumber(params: {
  value: number;
  fraction_digits: number;
}): string {
  const { value, fraction_digits } = params;
  return value.toLocaleString('en-US', {
    maximumFractionDigits: fraction_digits,
    minimumFractionDigits: fraction_digits
  });
}

function PrintBenchmarkResult(params: { benchmark_result: benchmark_result_t }): void {
  const { benchmark_result } = params;

  console.log('');
  console.log(`Benchmark: ${benchmark_result.benchmark_name}`);
  console.log(
    `- Total operations: ${benchmark_result.total_operations.toLocaleString('en-US')}`
  );
  console.log(
    `- Total duration: ${FormatNumber({
      value: benchmark_result.total_duration_ms,
      fraction_digits: 3
    })} ms`
  );
  console.log(
    `- Throughput: ${FormatNumber({
      value: benchmark_result.operations_per_second,
      fraction_digits: 2
    })} ops/sec`
  );
  console.log(
    `- Average latency: ${FormatNumber({
      value: benchmark_result.average_latency_microseconds,
      fraction_digits: 3
    })} us (${FormatNumber({
      value: benchmark_result.average_latency_nanoseconds,
      fraction_digits: 0
    })} ns)`
  );
}

type run_parallel_operations_params_t = {
  total_operations: number;
  max_in_flight: number;
  operation_func: (operation_index: number) => Promise<void>;
};

async function RunParallelOperations(
  params: run_parallel_operations_params_t
): Promise<void> {
  const { total_operations, max_in_flight, operation_func } = params;
  const bounded_max_in_flight = Math.max(1, Math.floor(max_in_flight));
  let next_operation_index = 0;
  let has_failed = false;

  async function RunOperationLoop(): Promise<void> {
    while (true) {
      if (has_failed) {
        return;
      }

      const current_operation_index = next_operation_index;
      next_operation_index += 1;

      if (current_operation_index >= total_operations) {
        return;
      }

      try {
        await operation_func(current_operation_index);
      } catch (error: unknown) {
        has_failed = true;
        throw error;
      }
    }
  }

  const runner_count = Math.min(total_operations, bounded_max_in_flight);
  const runner_promises: Promise<void>[] = [];

  for (let runner_index = 0; runner_index < runner_count; runner_index += 1) {
    runner_promises.push(RunOperationLoop());
  }

  await Promise.all(runner_promises);
}

function RunLocalBenchmark(params: run_local_benchmark_params_t): benchmark_result_t {
  const { total_operations, warmup_operations, expected_digest } = params;

  for (
    let operation_index = 0;
    operation_index < warmup_operations;
    operation_index += 1
  ) {
    const return_value = ComputeSha1Repeated();
    ValidateDigestResult({
      value: return_value,
      expected_digest,
      label: 'Local warmup',
      operation_index
    });
  }

  const start_time_ms = performance.now();
  for (
    let operation_index = 0;
    operation_index < total_operations;
    operation_index += 1
  ) {
    const return_value = ComputeSha1Repeated();
    ValidateDigestResult({
      value: return_value,
      expected_digest,
      label: 'Local benchmark',
      operation_index
    });
  }
  const end_time_ms = performance.now();

  return BuildBenchmarkResult({
    benchmark_name: 'Single-thread In-process SHA-1 Heavy Function',
    total_operations,
    start_time_ms,
    end_time_ms
  });
}

async function RunWorkerBenchmark(
  params: run_worker_benchmark_params_t
): Promise<benchmark_result_t> {
  const {
    total_operations,
    warmup_operations,
    worker_count,
    worker_parallelism,
    expected_digest
  } = params;

  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.defineWorkerDependency({
      alias: 'crypto_dep',
      module_specifier: 'node:crypto'
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCBenchmarkSha1Heavy',
      worker_func: async function (): Promise<string> {
        const crypto_module = await wpc_import<{
          createHash: typeof createHash;
        }>({
          alias: 'crypto_dep'
        });
        const sha1_benchmark_input = 'PLEASEHASHTHISVALUEOVERANDOVER';
        const sha1_loop_iterations_per_call = 1_000_000;
        let current_digest = '';

        for (
          let hash_iteration_index = 0;
          hash_iteration_index < sha1_loop_iterations_per_call;
          hash_iteration_index += 1
        ) {
          current_digest = crypto_module
            .createHash('sha1')
            .update(sha1_benchmark_input)
            .digest('hex');
        }

        return current_digest;
      }
    });

    await workerprocedurecall.startWorkers({
      count: worker_count
    });

    await RunParallelOperations({
      total_operations: warmup_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (operation_index: number): Promise<void> {
        const return_value = await workerprocedurecall.call.WPCBenchmarkSha1Heavy();
        ValidateDigestResult({
          value: return_value,
          expected_digest,
          label: 'Worker warmup',
          operation_index
        });
      }
    });

    const start_time_ms = performance.now();
    await RunParallelOperations({
      total_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (operation_index: number): Promise<void> {
        const return_value = await workerprocedurecall.call.WPCBenchmarkSha1Heavy();
        ValidateDigestResult({
          value: return_value,
          expected_digest,
          label: 'Worker benchmark',
          operation_index
        });
      }
    });
    const end_time_ms = performance.now();

    return BuildBenchmarkResult({
      benchmark_name: 'WorkerProcedureCall SHA-1 Heavy RPC',
      total_operations,
      start_time_ms,
      end_time_ms
    });
  } finally {
    await workerprocedurecall.stopWorkers();
  }
}

function PrintWinner(params: {
  worker_benchmark_result: benchmark_result_t;
  local_benchmark_result: benchmark_result_t;
}): void {
  const { worker_benchmark_result, local_benchmark_result } = params;

  const worker_duration_ms = worker_benchmark_result.total_duration_ms;
  const local_duration_ms = local_benchmark_result.total_duration_ms;
  const faster_benchmark =
    worker_duration_ms < local_duration_ms
      ? worker_benchmark_result
      : local_benchmark_result;
  const slower_benchmark =
    worker_duration_ms < local_duration_ms
      ? local_benchmark_result
      : worker_benchmark_result;
  const duration_difference_ms =
    slower_benchmark.total_duration_ms - faster_benchmark.total_duration_ms;

  console.log('');
  console.log(
    `Winner: ${faster_benchmark.benchmark_name} (${FormatNumber({
      value: duration_difference_ms,
      fraction_digits: 3
    })} ms faster)`
  );
}

async function RunBenchmarkSuite(): Promise<void> {
  const logical_cpu_count = GetLogicalCpuCount();
  const worker_parallelism = logical_cpu_count;
  const expected_digest = createHash('sha1')
    .update(SHA1_BENCHMARK_INPUT)
    .digest('hex');

  console.log('Worker Vs Single-thread SHA-1 Heavy Benchmark');
  console.log(
    `Configuration: workers=${logical_cpu_count}, operations=${BENCHMARK_TOTAL_OPERATIONS.toLocaleString(
      'en-US'
    )}, warmup=${BENCHMARK_WARMUP_OPERATIONS.toLocaleString(
      'en-US'
    )}, sha1_iterations_per_call=${SHA1_LOOP_ITERATIONS_PER_CALL.toLocaleString(
      'en-US'
    )}, worker_execution_mode=parallel, worker_parallelism=${worker_parallelism.toLocaleString(
      'en-US'
    )}`
  );

  const worker_benchmark_result = await RunWorkerBenchmark({
    total_operations: BENCHMARK_TOTAL_OPERATIONS,
    warmup_operations: BENCHMARK_WARMUP_OPERATIONS,
    worker_count: logical_cpu_count,
    worker_parallelism,
    expected_digest
  });

  const local_benchmark_result = RunLocalBenchmark({
    total_operations: BENCHMARK_TOTAL_OPERATIONS,
    warmup_operations: BENCHMARK_WARMUP_OPERATIONS,
    expected_digest
  });

  PrintBenchmarkResult({
    benchmark_result: worker_benchmark_result
  });
  PrintBenchmarkResult({
    benchmark_result: local_benchmark_result
  });

  PrintWinner({
    worker_benchmark_result,
    local_benchmark_result
  });
}

void RunBenchmarkSuite().catch((error: unknown) => {
  console.error('Benchmark failed:', error);
  process.exitCode = 1;
});
