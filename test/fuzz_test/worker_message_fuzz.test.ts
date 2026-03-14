import assert from 'node:assert/strict';
import test from 'node:test';

import { WorkerProcedureCall } from '../../src/index';
import {
  BuildRandomJsonValue,
  InstallProcessCrashGuard,
  RandomAlphaNumericString,
  RandomIntegerInRange,
  RunFuzzSuite
} from './fuzz_harness';

type worker_message_fuzz_worker_state_t = {
  worker_id: number;
  worker_instance: {
    postMessage: (message: unknown) => void;
  };
};

function BuildFuzzWorkerMessage(params: {
  random_func: () => number;
  case_index: number;
}): unknown {
  const duplicate_call_request_id = `fuzz_call_${Math.max(0, params.case_index - 1)}`;
  const duplicate_control_request_id = `fuzz_control_${Math.max(0, params.case_index - 1)}`;
  const duplicate_shared_request_id = `fuzz_shared_${Math.max(0, params.case_index - 1)}`;

  const mode = RandomIntegerInRange({
    random_func: params.random_func,
    minimum_value: 0,
    maximum_value: 7
  });

  if (mode === 0) {
    return {
      message_type: 'control_request',
      control_request_id: duplicate_control_request_id,
      command: RandomAlphaNumericString({
        random_func: params.random_func,
        minimum_length: 1,
        maximum_length: 24
      }),
      payload: BuildRandomJsonValue({
        random_func: params.random_func,
        depth: 0,
        max_depth: 4,
        max_array_length: 5,
        max_object_keys: 5,
        max_string_length: 64
      })
    };
  }

  if (mode === 1) {
    return {
      message_type: 'call_request',
      call_request_id: duplicate_call_request_id,
      function_name: RandomAlphaNumericString({
        random_func: params.random_func,
        minimum_length: 1,
        maximum_length: 32
      }),
      args: new Array(8).fill(0).map((): unknown => {
        return BuildRandomJsonValue({
          random_func: params.random_func,
          depth: 0,
          max_depth: 3,
          max_array_length: 4,
          max_object_keys: 4,
          max_string_length: 48
        });
      })
    };
  }

  if (mode === 2) {
    return {
      message_type: 'shared_response',
      shared_request_id: duplicate_shared_request_id,
      ok: false,
      error: {
        message: RandomAlphaNumericString({
          random_func: params.random_func,
          minimum_length: 0,
          maximum_length: 80
        })
      }
    };
  }

  if (mode === 3) {
    return {
      message_type: 'unknown_fuzz_message',
      payload: {
        case_index: params.case_index
      }
    };
  }

  if (mode === 4) {
    return {
      message_type: 'call_request',
      call_request_id: `fuzz_call_${params.case_index}`,
      function_name: 'FuzzPing',
      args: [
        {
          value: RandomAlphaNumericString({
            random_func: params.random_func,
            minimum_length: 0,
            maximum_length: 128
          })
        }
      ]
    };
  }

  if (mode === 5) {
    return BuildRandomJsonValue({
      random_func: params.random_func,
      depth: 0,
      max_depth: 4,
      max_array_length: 8,
      max_object_keys: 8,
      max_string_length: 256
    });
  }

  if (mode === 6) {
    return {
      message_type: 'shared_response',
      shared_request_id: 42,
      ok: 'true',
      data: {
        nested: {
          payload: new Array(40).fill('x').join('')
        }
      }
    };
  }

  return {
    message_type: 'control_request',
    control_request_id: `fuzz_control_${params.case_index}`,
    command: null,
    payload: 'bad_payload_shape'
  };
}

function GetWorkerStateList(params: {
  workerprocedurecall: WorkerProcedureCall;
}): worker_message_fuzz_worker_state_t[] {
  const worker_state_by_id = (
    params.workerprocedurecall as unknown as {
      worker_state_by_id: Map<number, worker_message_fuzz_worker_state_t>;
    }
  ).worker_state_by_id;

  return Array.from(worker_state_by_id.values());
}

test('worker parent-to-worker message fuzz stability', async function (): Promise<void> {
  const process_guard = InstallProcessCrashGuard({
    suite_name: 'worker_message_fuzz'
  });
  const workerprocedurecall = new WorkerProcedureCall();
  const captured_worker_event_name_list: string[] = [];

  await workerprocedurecall.defineWorkerFunction({
    name: 'FuzzPing',
    worker_func: async function (params?: { value?: string }): Promise<string> {
      return `PING:${params?.value ?? 'none'}`;
    }
  });

  const worker_event_listener_id = workerprocedurecall.onWorkerEvent({
    listener: (worker_event): void => {
      captured_worker_event_name_list.push(worker_event.event_name);
    }
  });

  try {
    await workerprocedurecall.startWorkers({
      count: 2
    });

    await RunFuzzSuite({
      suite_name: 'worker_message_fuzz',
      default_iterations: 140,
      default_case_timeout_ms: 300,
      run_case_func: async (params): Promise<void> => {
        const worker_state_list = GetWorkerStateList({
          workerprocedurecall
        });
        assert.equal(worker_state_list.length >= 1, true);

        const selected_worker_index = RandomIntegerInRange({
          random_func: params.random_func,
          minimum_value: 0,
          maximum_value: worker_state_list.length - 1
        });
        const selected_worker_state = worker_state_list[selected_worker_index];
        const fuzz_message = BuildFuzzWorkerMessage({
          random_func: params.random_func,
          case_index: params.case_index
        });

        selected_worker_state.worker_instance.postMessage(fuzz_message);

        if (params.case_index % 4 === 0) {
          const call_result = await workerprocedurecall.call.FuzzPing({
            value: `case_${params.case_index}`
          });
          assert.equal(typeof call_result, 'string');
          assert.equal((call_result as string).startsWith('PING:'), true);
        }

        await new Promise<void>((resolve): void => {
          setTimeout(resolve, 2);
        });

        const health_state_list = workerprocedurecall.getWorkerHealthStates();
        const has_stopped_worker = health_state_list.some((health_state): boolean => {
          return health_state.health_state === 'stopped';
        });
        assert.equal(has_stopped_worker, false);

        const has_worker_crash_event = captured_worker_event_name_list.some(
          (event_name): boolean => {
            return event_name === 'worker_error' || event_name === 'worker_exited';
          }
        );
        assert.equal(has_worker_crash_event, false);
      }
    });
  } finally {
    workerprocedurecall.offWorkerEvent({
      listener_id: worker_event_listener_id
    });
    await workerprocedurecall.stopWorkers().catch((): void => {});

    const process_event_list = process_guard.dispose();
    assert.equal(process_event_list.length, 0);
  }
});
