/**
 * Run with:
 * npm run benchmark:shared-vs-ipc
 */
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

type run_parallel_operations_params_t = {
  total_operations: number;
  max_in_flight: number;
  operation_func: (operation_index: number) => Promise<void>;
};

type run_worker_shared_benchmark_params_t = {
  total_operations: number;
  warmup_operations: number;
  worker_count: number;
  worker_parallelism: number;
};

type run_worker_ipc_benchmark_params_t = {
  total_operations: number;
  warmup_operations: number;
  worker_count: number;
  worker_parallelism: number;
};

const BENCHMARK_TOTAL_OPERATIONS = 20_000;
const BENCHMARK_WARMUP_OPERATIONS = 1_000;

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

  return {
    benchmark_name,
    total_operations,
    total_duration_ms,
    operations_per_second: total_operations / total_duration_seconds,
    average_latency_microseconds: (total_duration_ms * 1000) / total_operations,
    average_latency_nanoseconds: (total_duration_ms * 1_000_000) / total_operations
  };
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

async function RunWorkerSharedBenchmark(
  params: run_worker_shared_benchmark_params_t
): Promise<benchmark_result_t> {
  const { total_operations, warmup_operations, worker_count, worker_parallelism } =
    params;

  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({
      count: worker_count
    });

    await workerprocedurecall.sharedCreate({
      id: 'shared_benchmark_counter_1',
      type: 'number',
      content: 0
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCSharedIncrementBenchmark',
      worker_func: async function (params: { id: string }): Promise<number> {
        const current_value = await wpc_shared_access<number>({
          id: params.id
        });

        await wpc_shared_write<number>({
          id: params.id,
          content: current_value + 1
        });

        await wpc_shared_release({
          id: params.id
        });

        return current_value + 1;
      }
    });

    await RunParallelOperations({
      total_operations: warmup_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (): Promise<void> {
        const next_value = await workerprocedurecall.call.WPCSharedIncrementBenchmark({
          id: 'shared_benchmark_counter_1'
        });

        if (typeof next_value !== 'number') {
          throw new Error('shared warmup returned non-number value.');
        }
      }
    });

    const start_time_ms = performance.now();
    await RunParallelOperations({
      total_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (): Promise<void> {
        const next_value = await workerprocedurecall.call.WPCSharedIncrementBenchmark({
          id: 'shared_benchmark_counter_1'
        });

        if (typeof next_value !== 'number') {
          throw new Error('shared benchmark returned non-number value.');
        }
      }
    });
    const end_time_ms = performance.now();

    const final_value = await workerprocedurecall.sharedAccess<number>({
      id: 'shared_benchmark_counter_1'
    });
    await workerprocedurecall.sharedRelease({
      id: 'shared_benchmark_counter_1'
    });

    const expected_total = total_operations + warmup_operations;
    if (final_value !== expected_total) {
      throw new Error(
        `shared counter final value mismatch. expected ${expected_total}, got ${String(final_value)}.`
      );
    }

    return BuildBenchmarkResult({
      benchmark_name: 'Worker Shared Memory Increment (parallel)',
      total_operations,
      start_time_ms,
      end_time_ms
    });
  } finally {
    await workerprocedurecall.stopWorkers();
  }
}

async function RunWorkerIpcBenchmark(
  params: run_worker_ipc_benchmark_params_t
): Promise<benchmark_result_t> {
  const { total_operations, warmup_operations, worker_count, worker_parallelism } =
    params;

  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({
      count: worker_count
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCIpcPayloadBenchmark',
      worker_func: async function (params: {
        value: number;
        label: string;
      }): Promise<{ value: number; label: string }> {
        return {
          value: params.value + 1,
          label: params.label
        };
      }
    });

    await RunParallelOperations({
      total_operations: warmup_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (operation_index: number): Promise<void> {
        const return_value = await workerprocedurecall.call.WPCIpcPayloadBenchmark({
          value: operation_index,
          label: 'warmup'
        });

        if (
          !return_value ||
          typeof return_value !== 'object' ||
          (return_value as { value?: unknown }).value !== operation_index + 1
        ) {
          throw new Error('ipc warmup returned invalid payload.');
        }
      }
    });

    const start_time_ms = performance.now();
    await RunParallelOperations({
      total_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (operation_index: number): Promise<void> {
        const return_value = await workerprocedurecall.call.WPCIpcPayloadBenchmark({
          value: operation_index,
          label: 'benchmark'
        });

        if (
          !return_value ||
          typeof return_value !== 'object' ||
          (return_value as { value?: unknown }).value !== operation_index + 1
        ) {
          throw new Error('ipc benchmark returned invalid payload.');
        }
      }
    });
    const end_time_ms = performance.now();

    return BuildBenchmarkResult({
      benchmark_name: 'Worker IPC Structured Clone Payload (parallel)',
      total_operations,
      start_time_ms,
      end_time_ms
    });
  } finally {
    await workerprocedurecall.stopWorkers();
  }
}

function PrintWinner(params: {
  benchmark_a: benchmark_result_t;
  benchmark_b: benchmark_result_t;
}): void {
  const { benchmark_a, benchmark_b } = params;

  const faster_benchmark =
    benchmark_a.total_duration_ms < benchmark_b.total_duration_ms
      ? benchmark_a
      : benchmark_b;
  const slower_benchmark =
    benchmark_a.total_duration_ms < benchmark_b.total_duration_ms
      ? benchmark_b
      : benchmark_a;

  console.log('');
  console.log(
    `Winner: ${faster_benchmark.benchmark_name} (${FormatNumber({
      value: slower_benchmark.total_duration_ms - faster_benchmark.total_duration_ms,
      fraction_digits: 3
    })} ms faster)`
  );
}

async function RunBenchmarkSuite(): Promise<void> {
  const logical_cpu_count = GetLogicalCpuCount();
  const worker_parallelism = logical_cpu_count;

  console.log('Shared Memory Vs IPC Worker Benchmark');
  console.log(
    `Configuration: workers=${logical_cpu_count}, operations=${BENCHMARK_TOTAL_OPERATIONS.toLocaleString(
      'en-US'
    )}, warmup=${BENCHMARK_WARMUP_OPERATIONS.toLocaleString(
      'en-US'
    )}, worker_execution_mode=parallel, worker_parallelism=${worker_parallelism.toLocaleString('en-US')}`
  );

  const shared_memory_benchmark = await RunWorkerSharedBenchmark({
    total_operations: BENCHMARK_TOTAL_OPERATIONS,
    warmup_operations: BENCHMARK_WARMUP_OPERATIONS,
    worker_count: logical_cpu_count,
    worker_parallelism
  });

  const ipc_benchmark = await RunWorkerIpcBenchmark({
    total_operations: BENCHMARK_TOTAL_OPERATIONS,
    warmup_operations: BENCHMARK_WARMUP_OPERATIONS,
    worker_count: logical_cpu_count,
    worker_parallelism
  });

  PrintBenchmarkResult({
    benchmark_result: shared_memory_benchmark
  });
  PrintBenchmarkResult({
    benchmark_result: ipc_benchmark
  });

  PrintWinner({
    benchmark_a: shared_memory_benchmark,
    benchmark_b: ipc_benchmark
  });
}

void RunBenchmarkSuite().catch((error: unknown) => {
  console.error('Benchmark failed:', error);
  process.exitCode = 1;
});
