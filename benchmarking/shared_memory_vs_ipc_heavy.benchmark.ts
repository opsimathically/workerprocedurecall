/**
 * Run with:
 * npm run benchmark:shared-vs-ipc-heavy
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
  shard_count: number;
  cpu_loop_iterations: number;
};

type run_worker_ipc_benchmark_params_t = {
  total_operations: number;
  warmup_operations: number;
  worker_count: number;
  worker_parallelism: number;
  payload_bytes: Uint8Array;
  cpu_loop_iterations: number;
};

const BENCHMARK_TOTAL_OPERATIONS = 1_000;
const BENCHMARK_WARMUP_OPERATIONS = 50;
const SHARED_CHUNK_SHARD_COUNT = 128;
const IPC_PAYLOAD_SIZE_BYTES = 1 * 1024 * 1024;
const CPU_LOOP_ITERATIONS = 2_000;

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

function BuildDeterministicPayload(params: { payload_size_bytes: number }): Uint8Array {
  const { payload_size_bytes } = params;
  const payload_bytes = new Uint8Array(payload_size_bytes);

  for (let byte_index = 0; byte_index < payload_bytes.length; byte_index += 1) {
    payload_bytes[byte_index] = (byte_index * 31 + 17) % 256;
  }

  return payload_bytes;
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

async function RunWorkerSharedHeavyBenchmark(
  params: run_worker_shared_benchmark_params_t
): Promise<benchmark_result_t> {
  const {
    total_operations,
    warmup_operations,
    worker_count,
    worker_parallelism,
    shard_count,
    cpu_loop_iterations
  } = params;

  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({
      count: worker_count
    });

    for (let shard_index = 0; shard_index < shard_count; shard_index += 1) {
      await workerprocedurecall.sharedCreate({
        id: `shared_heavy_shard_${shard_index}`,
        type: 'number',
        note: 'heavy shared benchmark shard counter',
        content: 0
      });
    }

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCSharedHeavyShardIncrement',
      worker_func: async function (params: {
        shard_id: number;
        cpu_loop_iterations: number;
      }): Promise<number> {
        const shared_chunk_id = `shared_heavy_shard_${params.shard_id}`;

        const current_value = await wpc_shared_access<number>({
          id: shared_chunk_id
        });

        let scratch_value = 0;
        for (
          let loop_index = 0;
          loop_index < params.cpu_loop_iterations;
          loop_index += 1
        ) {
          scratch_value = (scratch_value + ((loop_index + params.shard_id) & 7)) %
            1_000_000_007;
        }

        const next_value = current_value + 1 + (scratch_value % 2) - (scratch_value % 2);

        await wpc_shared_write<number>({
          id: shared_chunk_id,
          content: next_value
        });

        await wpc_shared_release({
          id: shared_chunk_id
        });

        return next_value;
      }
    });

    await RunParallelOperations({
      total_operations: warmup_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (operation_index: number): Promise<void> {
        const shard_id = operation_index % shard_count;
        const next_value = await workerprocedurecall.call.WPCSharedHeavyShardIncrement({
          shard_id,
          cpu_loop_iterations
        });

        if (typeof next_value !== 'number') {
          throw new Error('shared heavy warmup returned non-number value.');
        }
      }
    });

    const start_time_ms = performance.now();
    await RunParallelOperations({
      total_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (operation_index: number): Promise<void> {
        const shard_id = operation_index % shard_count;
        const next_value = await workerprocedurecall.call.WPCSharedHeavyShardIncrement({
          shard_id,
          cpu_loop_iterations
        });

        if (typeof next_value !== 'number') {
          throw new Error('shared heavy benchmark returned non-number value.');
        }
      }
    });
    const end_time_ms = performance.now();

    let total_shard_value = 0;
    for (let shard_index = 0; shard_index < shard_count; shard_index += 1) {
      const shard_value = await workerprocedurecall.sharedAccess<number>({
        id: `shared_heavy_shard_${shard_index}`
      });

      total_shard_value += shard_value;

      await workerprocedurecall.sharedRelease({
        id: `shared_heavy_shard_${shard_index}`
      });
    }

    const expected_total = total_operations + warmup_operations;
    if (total_shard_value !== expected_total) {
      throw new Error(
        `shared heavy shard total mismatch. expected ${expected_total}, got ${String(total_shard_value)}.`
      );
    }

    return BuildBenchmarkResult({
      benchmark_name: 'Worker Shared Memory Sharded Counters (heavy parallel)',
      total_operations,
      start_time_ms,
      end_time_ms
    });
  } finally {
    await workerprocedurecall.stopWorkers();
  }
}

async function RunWorkerIpcHeavyBenchmark(
  params: run_worker_ipc_benchmark_params_t
): Promise<benchmark_result_t> {
  const {
    total_operations,
    warmup_operations,
    worker_count,
    worker_parallelism,
    payload_bytes,
    cpu_loop_iterations
  } = params;

  const workerprocedurecall = new WorkerProcedureCall();

  try {
    await workerprocedurecall.startWorkers({
      count: worker_count
    });

    await workerprocedurecall.defineWorkerFunction({
      name: 'WPCIpcHeavyPayload',
      worker_func: async function (params: {
        payload_bytes: Uint8Array;
        shard_id: number;
        cpu_loop_iterations: number;
      }): Promise<number> {
        let checksum = 0;

        for (
          let byte_index = 0;
          byte_index < params.payload_bytes.length;
          byte_index += 64
        ) {
          checksum =
            (checksum + params.payload_bytes[byte_index] + params.shard_id) %
            1_000_000_007;
        }

        for (
          let loop_index = 0;
          loop_index < params.cpu_loop_iterations;
          loop_index += 1
        ) {
          checksum = (checksum + (loop_index & 7)) % 1_000_000_007;
        }

        return checksum;
      }
    });

    await RunParallelOperations({
      total_operations: warmup_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (operation_index: number): Promise<void> {
        const return_value = await workerprocedurecall.call.WPCIpcHeavyPayload({
          payload_bytes,
          shard_id: operation_index % SHARED_CHUNK_SHARD_COUNT,
          cpu_loop_iterations
        });

        if (typeof return_value !== 'number') {
          throw new Error('ipc heavy warmup returned non-number checksum.');
        }
      }
    });

    const start_time_ms = performance.now();
    await RunParallelOperations({
      total_operations,
      max_in_flight: worker_parallelism,
      operation_func: async function (operation_index: number): Promise<void> {
        const return_value = await workerprocedurecall.call.WPCIpcHeavyPayload({
          payload_bytes,
          shard_id: operation_index % SHARED_CHUNK_SHARD_COUNT,
          cpu_loop_iterations
        });

        if (typeof return_value !== 'number') {
          throw new Error('ipc heavy benchmark returned non-number checksum.');
        }
      }
    });
    const end_time_ms = performance.now();

    return BuildBenchmarkResult({
      benchmark_name: 'Worker IPC Large Payload (heavy parallel)',
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
  const payload_bytes = BuildDeterministicPayload({
    payload_size_bytes: IPC_PAYLOAD_SIZE_BYTES
  });

  console.log('Shared Memory Vs IPC Worker Benchmark (Heavy)');
  console.log(
    `Configuration: workers=${logical_cpu_count}, operations=${BENCHMARK_TOTAL_OPERATIONS.toLocaleString(
      'en-US'
    )}, warmup=${BENCHMARK_WARMUP_OPERATIONS.toLocaleString(
      'en-US'
    )}, worker_execution_mode=parallel, worker_parallelism=${worker_parallelism.toLocaleString(
      'en-US'
    )}, shard_count=${SHARED_CHUNK_SHARD_COUNT.toLocaleString(
      'en-US'
    )}, ipc_payload_bytes=${IPC_PAYLOAD_SIZE_BYTES.toLocaleString(
      'en-US'
    )}, cpu_loop_iterations=${CPU_LOOP_ITERATIONS.toLocaleString('en-US')}`
  );

  const shared_memory_benchmark = await RunWorkerSharedHeavyBenchmark({
    total_operations: BENCHMARK_TOTAL_OPERATIONS,
    warmup_operations: BENCHMARK_WARMUP_OPERATIONS,
    worker_count: logical_cpu_count,
    worker_parallelism,
    shard_count: SHARED_CHUNK_SHARD_COUNT,
    cpu_loop_iterations: CPU_LOOP_ITERATIONS
  });

  const ipc_benchmark = await RunWorkerIpcHeavyBenchmark({
    total_operations: BENCHMARK_TOTAL_OPERATIONS,
    warmup_operations: BENCHMARK_WARMUP_OPERATIONS,
    worker_count: logical_cpu_count,
    worker_parallelism,
    payload_bytes,
    cpu_loop_iterations: CPU_LOOP_ITERATIONS
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
