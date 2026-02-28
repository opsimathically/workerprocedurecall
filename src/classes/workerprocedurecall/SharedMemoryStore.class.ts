export type shared_chunk_type_t = 'json' | 'text' | 'number' | 'binary';

export type shared_chunk_lifecycle_state_t = 'active' | 'freed';

export type shared_lock_owner_kind_t = 'parent' | 'worker';

export type shared_lock_owner_context_t = {
  owner_kind: shared_lock_owner_kind_t;
  owner_id: string;
  worker_id?: number;
  call_request_id?: string;
};

export type shared_chunk_create_params_t<content_t = unknown> = {
  id: string;
  note?: string;
  type: shared_chunk_type_t;
  content: content_t;
};

export type shared_chunk_access_params_t = {
  id: string;
  timeout_ms?: number;
};

export type shared_chunk_write_params_t<content_t = unknown> = {
  id: string;
  content: content_t;
};

export type shared_chunk_release_params_t = {
  id: string;
};

export type shared_chunk_free_params_t = {
  id: string;
  require_unlocked?: boolean;
};

export type shared_lock_auto_release_reason_t =
  | 'call_complete_auto_release'
  | 'call_timeout_auto_release'
  | 'worker_exit_auto_release'
  | 'call_rejection_auto_release';

export type shared_lock_auto_release_result_t = {
  released_lock_count: number;
  released_chunk_ids: string[];
};

export type shared_lock_event_t = {
  timestamp: string;
  event_name: string;
  chunk_id: string;
  owner_id: string | null;
  details?: Record<string, unknown>;
};

export type shared_lock_debug_entry_t = {
  id: string;
  type: shared_chunk_type_t;
  note: string | null;
  lifecycle_state: shared_chunk_lifecycle_state_t;
  version: number;
  byte_offset: number;
  allocated_length_bytes: number;
  content_length_bytes: number;
  is_locked: boolean;
  lock_owner_kind: shared_lock_owner_kind_t | null;
  lock_owner_id: string | null;
  lock_owner_worker_id: number | null;
  lock_owner_call_request_id: string | null;
  lock_acquired_timestamp: string | null;
  lock_held_ms: number | null;
  waiter_count: number;
  contention_count: number;
  timeout_count: number;
  free_conflict_count: number;
  created_timestamp: string;
  last_write_timestamp: string;
  freed_timestamp: string | null;
};

export type shared_lock_debug_info_params_t = {
  include_history?: boolean;
  min_held_ms?: number;
};

export type shared_lock_debug_information_t = {
  snapshot_timestamp: string;
  total_chunk_count: number;
  locked_chunk_count: number;
  waiting_chunk_count: number;
  total_waiter_count: number;
  chunks: shared_lock_debug_entry_t[];
  recent_events?: shared_lock_event_t[];
};

export type shared_memory_store_constructor_params_t = {
  heap_size_bytes?: number;
  max_event_history_count?: number;
};

type shared_heap_block_t = {
  offset: number;
  length: number;
};

type shared_lock_waiter_t = {
  owner_context: shared_lock_owner_context_t;
  enqueued_timestamp_ms: number;
  timeout_handle: NodeJS.Timeout;
  settled: boolean;
  resolve: () => void;
  reject: (error: Error) => void;
};

type shared_chunk_record_t = {
  id: string;
  type: shared_chunk_type_t;
  note: string | null;
  lifecycle_state: shared_chunk_lifecycle_state_t;
  version: number;
  allocation_offset: number;
  allocation_length_bytes: number;
  content_length_bytes: number;
  created_timestamp_ms: number;
  last_write_timestamp_ms: number;
  freed_timestamp_ms: number | null;
  contention_count: number;
  timeout_count: number;
  free_conflict_count: number;
  lock_state_buffer: SharedArrayBuffer;
  lock_state_view: Int32Array;
  lock_owner: shared_lock_owner_context_t | null;
  lock_acquired_timestamp_ms: number | null;
  lock_waiter_queue: shared_lock_waiter_t[];
};

function ValidateNonEmptyString(params: { value: unknown; label: string }): string {
  const { value, label } = params;

  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${label} must be a non-empty string.`);
  }

  return value;
}

function ValidatePositiveInteger(params: { value: number; label: string }): void {
  const { value, label } = params;

  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive integer.`);
  }
}

function ValidateNonNegativeInteger(params: { value: number; label: string }): void {
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

function ToIsoTimestamp(params: { timestamp_ms: number | null }): string | null {
  const { timestamp_ms } = params;
  if (timestamp_ms === null) {
    return null;
  }

  return new Date(timestamp_ms).toISOString();
}

function EncodeSharedContent(params: {
  type: shared_chunk_type_t;
  content: unknown;
}): Uint8Array {
  const { type, content } = params;
  const text_encoder = new TextEncoder();

  if (type === 'json') {
    let serialized_json: string;
    try {
      serialized_json = JSON.stringify(content);
    } catch (error) {
      throw new Error(
        `json content could not be serialized: ${GetErrorMessage({ error })}`
      );
    }

    if (typeof serialized_json !== 'string') {
      throw new Error('json content could not be serialized.');
    }

    return text_encoder.encode(serialized_json);
  }

  if (type === 'text') {
    if (typeof content !== 'string') {
      throw new Error('text content must be a string.');
    }

    return text_encoder.encode(content);
  }

  if (type === 'number') {
    if (typeof content !== 'number' || !Number.isFinite(content)) {
      throw new Error('number content must be a finite number.');
    }

    const number_buffer = new ArrayBuffer(8);
    const number_view = new DataView(number_buffer);
    number_view.setFloat64(0, content, true);
    return new Uint8Array(number_buffer);
  }

  if (content instanceof Uint8Array) {
    return new Uint8Array(content);
  }

  if (ArrayBuffer.isView(content)) {
    return new Uint8Array(content.buffer, content.byteOffset, content.byteLength);
  }

  if (content instanceof ArrayBuffer) {
    return new Uint8Array(content);
  }

  throw new Error(
    'binary content must be Uint8Array, ArrayBuffer, or another ArrayBuffer view.'
  );
}

function DecodeSharedContent(params: {
  type: shared_chunk_type_t;
  content_bytes: Uint8Array;
}): unknown {
  const { type, content_bytes } = params;
  const text_decoder = new TextDecoder();

  if (type === 'json') {
    const serialized_json = text_decoder.decode(content_bytes);
    try {
      return JSON.parse(serialized_json);
    } catch (error) {
      throw new Error(
        `json content could not be parsed: ${GetErrorMessage({ error })}`
      );
    }
  }

  if (type === 'text') {
    return text_decoder.decode(content_bytes);
  }

  if (type === 'number') {
    if (content_bytes.byteLength !== 8) {
      throw new Error('number content is not 8 bytes.');
    }

    const number_view = new DataView(
      content_bytes.buffer,
      content_bytes.byteOffset,
      content_bytes.byteLength
    );
    return number_view.getFloat64(0, true);
  }

  return new Uint8Array(content_bytes);
}

function BuildLockOwnerMatches(params: {
  expected_owner: shared_lock_owner_context_t;
  actual_owner: shared_lock_owner_context_t | null;
}): boolean {
  const { expected_owner, actual_owner } = params;

  if (!actual_owner) {
    return false;
  }

  return (
    expected_owner.owner_kind === actual_owner.owner_kind &&
    expected_owner.owner_id === actual_owner.owner_id
  );
}

export class WorkerProcedureCallSharedMemoryStore {
  private heap_size_bytes: number;
  private heap_buffer: SharedArrayBuffer;
  private heap_view: Uint8Array;
  private free_heap_block_list: shared_heap_block_t[];
  private chunk_by_id = new Map<string, shared_chunk_record_t>();
  private recent_lock_event_history: shared_lock_event_t[] = [];
  private max_event_history_count: number;

  constructor(params: shared_memory_store_constructor_params_t = {}) {
    const heap_size_bytes =
      typeof params.heap_size_bytes === 'number'
        ? params.heap_size_bytes
        : 32 * 1024 * 1024;
    ValidatePositiveInteger({
      value: heap_size_bytes,
      label: 'heap_size_bytes'
    });

    const max_event_history_count =
      typeof params.max_event_history_count === 'number'
        ? params.max_event_history_count
        : 200;
    ValidatePositiveInteger({
      value: max_event_history_count,
      label: 'max_event_history_count'
    });

    this.heap_size_bytes = heap_size_bytes;
    this.heap_buffer = new SharedArrayBuffer(heap_size_bytes);
    this.heap_view = new Uint8Array(this.heap_buffer);
    this.free_heap_block_list = [{ offset: 0, length: heap_size_bytes }];
    this.max_event_history_count = max_event_history_count;
  }

  async createChunk<content_t = unknown>(params: {
    chunk: shared_chunk_create_params_t<content_t>;
  }): Promise<void> {
    const { chunk } = params;
    const id = ValidateNonEmptyString({ value: chunk.id, label: 'id' });
    const note =
      typeof chunk.note === 'string' && chunk.note.length > 0 ? chunk.note : null;

    if (this.chunk_by_id.has(id)) {
      throw new Error(`Shared chunk "${id}" already exists.`);
    }

    const encoded_content = EncodeSharedContent({
      type: chunk.type,
      content: chunk.content
    });
    const required_length_bytes = Math.max(1, encoded_content.byteLength);
    const allocation = this.allocateHeapRange({
      length_bytes: required_length_bytes
    });

    this.heap_view.fill(
      0,
      allocation.offset,
      allocation.offset + allocation.length
    );
    this.heap_view.set(encoded_content, allocation.offset);

    const now_ms = Date.now();
    const lock_state_buffer = new SharedArrayBuffer(
      Int32Array.BYTES_PER_ELEMENT * 2
    );
    const lock_state_view = new Int32Array(lock_state_buffer);
    Atomics.store(lock_state_view, 0, 0);
    Atomics.store(lock_state_view, 1, 0);

    const chunk_record: shared_chunk_record_t = {
      id,
      type: chunk.type,
      note,
      lifecycle_state: 'active',
      version: 1,
      allocation_offset: allocation.offset,
      allocation_length_bytes: allocation.length,
      content_length_bytes: encoded_content.byteLength,
      created_timestamp_ms: now_ms,
      last_write_timestamp_ms: now_ms,
      freed_timestamp_ms: null,
      contention_count: 0,
      timeout_count: 0,
      free_conflict_count: 0,
      lock_state_buffer,
      lock_state_view,
      lock_owner: null,
      lock_acquired_timestamp_ms: null,
      lock_waiter_queue: []
    };

    this.chunk_by_id.set(id, chunk_record);
    this.recordLockEvent({
      event_name: 'chunk_created',
      chunk_id: id,
      owner_id: null,
      details: {
        type: chunk.type,
        allocated_length_bytes: allocation.length,
        content_length_bytes: encoded_content.byteLength
      }
    });
  }

  async accessChunk<content_t = unknown>(params: {
    access: shared_chunk_access_params_t;
    owner_context: shared_lock_owner_context_t;
  }): Promise<content_t> {
    const { access, owner_context } = params;
    const id = ValidateNonEmptyString({ value: access.id, label: 'id' });
    const chunk = this.getActiveChunkById({ id });

    await this.acquireChunkLock({
      chunk,
      owner_context,
      timeout_ms: access.timeout_ms
    });

    const content_bytes = this.getChunkContentBytes({
      chunk
    });
    return DecodeSharedContent({
      type: chunk.type,
      content_bytes
    }) as content_t;
  }

  async writeChunk<content_t = unknown>(params: {
    write: shared_chunk_write_params_t<content_t>;
    owner_context: shared_lock_owner_context_t;
  }): Promise<void> {
    const { write, owner_context } = params;
    const id = ValidateNonEmptyString({ value: write.id, label: 'id' });
    const chunk = this.getActiveChunkById({ id });

    this.ensureOwnerHasChunkLock({
      chunk,
      owner_context
    });

    const encoded_content = EncodeSharedContent({
      type: chunk.type,
      content: write.content
    });
    const required_length_bytes = Math.max(1, encoded_content.byteLength);

    if (required_length_bytes > chunk.allocation_length_bytes) {
      const new_allocation = this.allocateHeapRange({
        length_bytes: required_length_bytes
      });

      this.heap_view.fill(
        0,
        new_allocation.offset,
        new_allocation.offset + new_allocation.length
      );
      this.heap_view.set(encoded_content, new_allocation.offset);

      this.freeHeapRange({
        offset: chunk.allocation_offset,
        length: chunk.allocation_length_bytes
      });

      chunk.allocation_offset = new_allocation.offset;
      chunk.allocation_length_bytes = new_allocation.length;
    } else {
      this.heap_view.fill(
        0,
        chunk.allocation_offset,
        chunk.allocation_offset + chunk.allocation_length_bytes
      );
      this.heap_view.set(encoded_content, chunk.allocation_offset);
    }

    chunk.content_length_bytes = encoded_content.byteLength;
    chunk.version += 1;
    chunk.last_write_timestamp_ms = Date.now();

    this.recordLockEvent({
      event_name: 'chunk_written',
      chunk_id: chunk.id,
      owner_id: owner_context.owner_id,
      details: {
        version: chunk.version,
        content_length_bytes: chunk.content_length_bytes
      }
    });
  }

  async releaseChunk(params: {
    release: shared_chunk_release_params_t;
    owner_context: shared_lock_owner_context_t;
  }): Promise<void> {
    const { release, owner_context } = params;
    const id = ValidateNonEmptyString({ value: release.id, label: 'id' });
    const chunk = this.getActiveChunkById({ id });

    this.ensureOwnerHasChunkLock({
      chunk,
      owner_context
    });

    this.releaseChunkLockInternal({
      chunk
    });
  }

  async freeChunk(params: {
    free: shared_chunk_free_params_t;
    owner_context: shared_lock_owner_context_t;
  }): Promise<void> {
    const { free, owner_context } = params;
    const id = ValidateNonEmptyString({ value: free.id, label: 'id' });
    const chunk = this.getActiveChunkById({ id });
    const require_unlocked = free.require_unlocked !== false;

    const has_lock_owner = chunk.lock_owner !== null;
    if (has_lock_owner) {
      chunk.free_conflict_count += 1;
      this.recordLockEvent({
        event_name: 'chunk_free_conflict_locked',
        chunk_id: id,
        owner_id: owner_context.owner_id,
        details: {
          lock_owner_id: chunk.lock_owner?.owner_id ?? null
        }
      });

      if (require_unlocked) {
        throw new Error(
          `Shared chunk "${id}" is locked and cannot be freed while require_unlocked is true.`
        );
      }

      if (
        !BuildLockOwnerMatches({
          expected_owner: owner_context,
          actual_owner: chunk.lock_owner
        })
      ) {
        throw new Error(
          `Shared chunk "${id}" is locked by a different owner and cannot be force-freed.`
        );
      }
    }

    if (chunk.lock_waiter_queue.length > 0) {
      chunk.free_conflict_count += 1;
      this.recordLockEvent({
        event_name: 'chunk_free_conflict_waiters',
        chunk_id: id,
        owner_id: owner_context.owner_id,
        details: {
          waiter_count: chunk.lock_waiter_queue.length
        }
      });

      if (require_unlocked) {
        throw new Error(
          `Shared chunk "${id}" has waiting lock requests and cannot be freed while require_unlocked is true.`
        );
      }
    }

    this.rejectAllChunkWaiters({
      chunk,
      reason: `Shared chunk "${id}" was freed before lock acquisition completed.`
    });

    Atomics.store(chunk.lock_state_view, 0, 0);
    Atomics.notify(chunk.lock_state_view, 0, 1);

    chunk.lock_owner = null;
    chunk.lock_acquired_timestamp_ms = null;
    chunk.lifecycle_state = 'freed';
    chunk.freed_timestamp_ms = Date.now();

    this.chunk_by_id.delete(id);
    this.freeHeapRange({
      offset: chunk.allocation_offset,
      length: chunk.allocation_length_bytes
    });

    this.recordLockEvent({
      event_name: 'chunk_freed',
      chunk_id: id,
      owner_id: owner_context.owner_id
    });
  }

  getLockDebugInfo(params: {
    options?: shared_lock_debug_info_params_t;
  }): shared_lock_debug_information_t {
    const { options } = params;
    const min_held_ms =
      typeof options?.min_held_ms === 'number' ? options.min_held_ms : 0;
    ValidateNonNegativeInteger({
      value: Math.floor(min_held_ms),
      label: 'min_held_ms'
    });

    const now_ms = Date.now();
    const chunks = Array.from(this.chunk_by_id.values())
      .sort((left_chunk, right_chunk): number => {
        return left_chunk.id.localeCompare(right_chunk.id);
      })
      .map((chunk): shared_lock_debug_entry_t => {
        const lock_held_ms =
          chunk.lock_owner && chunk.lock_acquired_timestamp_ms !== null
            ? now_ms - chunk.lock_acquired_timestamp_ms
            : null;

        return {
          id: chunk.id,
          type: chunk.type,
          note: chunk.note,
          lifecycle_state: chunk.lifecycle_state,
          version: chunk.version,
          byte_offset: chunk.allocation_offset,
          allocated_length_bytes: chunk.allocation_length_bytes,
          content_length_bytes: chunk.content_length_bytes,
          is_locked: chunk.lock_owner !== null,
          lock_owner_kind: chunk.lock_owner?.owner_kind ?? null,
          lock_owner_id: chunk.lock_owner?.owner_id ?? null,
          lock_owner_worker_id: chunk.lock_owner?.worker_id ?? null,
          lock_owner_call_request_id: chunk.lock_owner?.call_request_id ?? null,
          lock_acquired_timestamp: ToIsoTimestamp({
            timestamp_ms: chunk.lock_acquired_timestamp_ms
          }),
          lock_held_ms,
          waiter_count: Atomics.load(chunk.lock_state_view, 1),
          contention_count: chunk.contention_count,
          timeout_count: chunk.timeout_count,
          free_conflict_count: chunk.free_conflict_count,
          created_timestamp: new Date(chunk.created_timestamp_ms).toISOString(),
          last_write_timestamp: new Date(chunk.last_write_timestamp_ms).toISOString(),
          freed_timestamp: ToIsoTimestamp({
            timestamp_ms: chunk.freed_timestamp_ms
          })
        };
      })
      .filter((chunk_debug_entry): boolean => {
        if (!chunk_debug_entry.is_locked) {
          return true;
        }

        if (chunk_debug_entry.lock_held_ms === null) {
          return true;
        }

        return chunk_debug_entry.lock_held_ms >= min_held_ms;
      });

    const locked_chunk_count = chunks.filter((chunk): boolean => {
      return chunk.is_locked;
    }).length;
    const waiting_chunk_count = chunks.filter((chunk): boolean => {
      return chunk.waiter_count > 0;
    }).length;
    const total_waiter_count = chunks.reduce((accumulator, chunk): number => {
      return accumulator + chunk.waiter_count;
    }, 0);

    const debug_information: shared_lock_debug_information_t = {
      snapshot_timestamp: new Date(now_ms).toISOString(),
      total_chunk_count: chunks.length,
      locked_chunk_count,
      waiting_chunk_count,
      total_waiter_count,
      chunks
    };

    if (options?.include_history === true) {
      debug_information.recent_events = [...this.recent_lock_event_history];
    }

    return debug_information;
  }

  releaseLocksByOwnerId(params: {
    owner_id: string;
    release_reason: shared_lock_auto_release_reason_t;
    worker_id?: number;
    call_request_id?: string;
  }): shared_lock_auto_release_result_t {
    const { owner_id, release_reason, worker_id, call_request_id } = params;
    const released_chunk_ids: string[] = [];

    for (const chunk of this.chunk_by_id.values()) {
      if (chunk.lock_owner?.owner_id !== owner_id) {
        continue;
      }

      const lock_owner = chunk.lock_owner;
      const event_worker_id =
        typeof worker_id === 'number' ? worker_id : lock_owner?.worker_id ?? null;
      const event_call_request_id =
        typeof call_request_id === 'string'
          ? call_request_id
          : lock_owner?.call_request_id ?? null;

      released_chunk_ids.push(chunk.id);
      this.recordLockEvent({
        event_name: release_reason,
        chunk_id: chunk.id,
        owner_id,
        details: {
          worker_id: event_worker_id,
          call_request_id: event_call_request_id,
          lock_count_released: 1
        }
      });

      this.releaseChunkLockInternal({
        chunk
      });
    }

    return {
      released_lock_count: released_chunk_ids.length,
      released_chunk_ids
    };
  }

  releaseLocksByWorkerId(params: {
    worker_id: number;
    release_reason?: shared_lock_auto_release_reason_t;
  }): shared_lock_auto_release_result_t {
    const { worker_id, release_reason = 'worker_exit_auto_release' } = params;
    const released_chunk_ids: string[] = [];

    for (const chunk of this.chunk_by_id.values()) {
      if (chunk.lock_owner?.owner_kind !== 'worker') {
        continue;
      }

      if (chunk.lock_owner.worker_id !== worker_id) {
        continue;
      }

      const lock_owner = chunk.lock_owner;
      released_chunk_ids.push(chunk.id);
      this.recordLockEvent({
        event_name: release_reason,
        chunk_id: chunk.id,
        owner_id: lock_owner.owner_id,
        details: {
          worker_id,
          call_request_id: lock_owner.call_request_id ?? null,
          lock_count_released: 1
        }
      });

      this.releaseChunkLockInternal({
        chunk
      });
    }

    return {
      released_lock_count: released_chunk_ids.length,
      released_chunk_ids
    };
  }

  private getActiveChunkById(params: { id: string }): shared_chunk_record_t {
    const { id } = params;

    const chunk = this.chunk_by_id.get(id);
    if (!chunk || chunk.lifecycle_state !== 'active') {
      throw new Error(`Shared chunk "${id}" was not found.`);
    }

    return chunk;
  }

  private allocateHeapRange(params: {
    length_bytes: number;
  }): { offset: number; length: number } {
    const { length_bytes } = params;
    ValidatePositiveInteger({
      value: length_bytes,
      label: 'length_bytes'
    });

    for (let block_index = 0; block_index < this.free_heap_block_list.length; block_index += 1) {
      const free_block = this.free_heap_block_list[block_index];
      if (free_block.length < length_bytes) {
        continue;
      }

      const allocation_offset = free_block.offset;
      free_block.offset += length_bytes;
      free_block.length -= length_bytes;

      if (free_block.length === 0) {
        this.free_heap_block_list.splice(block_index, 1);
      }

      return {
        offset: allocation_offset,
        length: length_bytes
      };
    }

    throw new Error(
      `Shared heap allocation failed for ${length_bytes} bytes. Heap size is ${this.heap_size_bytes} bytes.`
    );
  }

  private freeHeapRange(params: { offset: number; length: number }): void {
    const { offset, length } = params;
    ValidateNonNegativeInteger({ value: offset, label: 'offset' });
    ValidatePositiveInteger({ value: length, label: 'length' });

    if (offset + length > this.heap_size_bytes) {
      throw new Error('Attempted to free a heap range outside of bounds.');
    }

    this.free_heap_block_list.push({
      offset,
      length
    });
    this.free_heap_block_list.sort((left_block, right_block): number => {
      return left_block.offset - right_block.offset;
    });

    const merged_block_list: shared_heap_block_t[] = [];

    for (const free_block of this.free_heap_block_list) {
      const previous_block = merged_block_list[merged_block_list.length - 1];
      if (!previous_block) {
        merged_block_list.push({ ...free_block });
        continue;
      }

      if (previous_block.offset + previous_block.length === free_block.offset) {
        previous_block.length += free_block.length;
        continue;
      }

      merged_block_list.push({ ...free_block });
    }

    this.free_heap_block_list = merged_block_list;
  }

  private ensureOwnerHasChunkLock(params: {
    chunk: shared_chunk_record_t;
    owner_context: shared_lock_owner_context_t;
  }): void {
    const { chunk, owner_context } = params;

    if (
      !BuildLockOwnerMatches({
        expected_owner: owner_context,
        actual_owner: chunk.lock_owner
      })
    ) {
      throw new Error(
        `Shared chunk "${chunk.id}" is not locked by owner "${owner_context.owner_id}".`
      );
    }
  }

  private getChunkContentBytes(params: { chunk: shared_chunk_record_t }): Uint8Array {
    const { chunk } = params;
    return new Uint8Array(
      this.heap_buffer,
      chunk.allocation_offset,
      chunk.content_length_bytes
    );
  }

  private async acquireChunkLock(params: {
    chunk: shared_chunk_record_t;
    owner_context: shared_lock_owner_context_t;
    timeout_ms?: number;
  }): Promise<void> {
    const { chunk, owner_context } = params;
    const timeout_ms =
      typeof params.timeout_ms === 'number' ? params.timeout_ms : 30_000;

    ValidatePositiveInteger({
      value: timeout_ms,
      label: 'timeout_ms'
    });

    if (
      BuildLockOwnerMatches({
        expected_owner: owner_context,
        actual_owner: chunk.lock_owner
      })
    ) {
      throw new Error(
        `Shared chunk "${chunk.id}" is already locked by owner "${owner_context.owner_id}".`
      );
    }

    const lock_was_unlocked = Atomics.compareExchange(chunk.lock_state_view, 0, 0, 1) === 0;
    if (lock_was_unlocked && chunk.lock_owner === null) {
      chunk.lock_owner = owner_context;
      chunk.lock_acquired_timestamp_ms = Date.now();
      this.recordLockEvent({
        event_name: 'chunk_lock_acquired',
        chunk_id: chunk.id,
        owner_id: owner_context.owner_id
      });
      return;
    }

    chunk.contention_count += 1;
    this.recordLockEvent({
      event_name: 'chunk_lock_contention',
      chunk_id: chunk.id,
      owner_id: owner_context.owner_id,
      details: {
        current_owner_id: chunk.lock_owner?.owner_id ?? null
      }
    });

    await new Promise<void>((resolve, reject): void => {
      const waiter: shared_lock_waiter_t = {
        owner_context,
        enqueued_timestamp_ms: Date.now(),
        timeout_handle: setTimeout((): void => {
          if (waiter.settled) {
            return;
          }

          waiter.settled = true;

          const waiter_index = chunk.lock_waiter_queue.indexOf(waiter);
          if (waiter_index >= 0) {
            chunk.lock_waiter_queue.splice(waiter_index, 1);
            Atomics.sub(chunk.lock_state_view, 1, 1);
          }

          chunk.timeout_count += 1;
          this.recordLockEvent({
            event_name: 'chunk_lock_timeout',
            chunk_id: chunk.id,
            owner_id: owner_context.owner_id,
            details: {
              timeout_ms
            }
          });

          reject(
            new Error(
              `Timed out waiting for shared chunk "${chunk.id}" lock after ${timeout_ms}ms.`
            )
          );
        }, timeout_ms),
        settled: false,
        resolve: (): void => {
          if (waiter.settled) {
            return;
          }

          waiter.settled = true;
          clearTimeout(waiter.timeout_handle);
          resolve();
        },
        reject: (error: Error): void => {
          if (waiter.settled) {
            return;
          }

          waiter.settled = true;
          clearTimeout(waiter.timeout_handle);
          reject(error);
        }
      };

      chunk.lock_waiter_queue.push(waiter);
      Atomics.add(chunk.lock_state_view, 1, 1);
    });
  }

  private releaseChunkLockInternal(params: { chunk: shared_chunk_record_t }): void {
    const { chunk } = params;

    chunk.lock_owner = null;
    chunk.lock_acquired_timestamp_ms = null;

    while (chunk.lock_waiter_queue.length > 0) {
      const next_waiter = chunk.lock_waiter_queue.shift();
      if (!next_waiter || next_waiter.settled) {
        continue;
      }

      Atomics.sub(chunk.lock_state_view, 1, 1);
      chunk.lock_owner = next_waiter.owner_context;
      chunk.lock_acquired_timestamp_ms = Date.now();
      Atomics.store(chunk.lock_state_view, 0, 1);
      Atomics.notify(chunk.lock_state_view, 0, 1);

      this.recordLockEvent({
        event_name: 'chunk_lock_granted_from_wait_queue',
        chunk_id: chunk.id,
        owner_id: next_waiter.owner_context.owner_id,
        details: {
          queued_for_ms: Date.now() - next_waiter.enqueued_timestamp_ms
        }
      });

      next_waiter.resolve();
      return;
    }

    Atomics.store(chunk.lock_state_view, 0, 0);
    Atomics.notify(chunk.lock_state_view, 0, 1);

    this.recordLockEvent({
      event_name: 'chunk_lock_released',
      chunk_id: chunk.id,
      owner_id: null
    });
  }

  private rejectAllChunkWaiters(params: {
    chunk: shared_chunk_record_t;
    reason: string;
  }): void {
    const { chunk, reason } = params;

    while (chunk.lock_waiter_queue.length > 0) {
      const next_waiter = chunk.lock_waiter_queue.shift();
      if (!next_waiter || next_waiter.settled) {
        continue;
      }

      Atomics.sub(chunk.lock_state_view, 1, 1);
      next_waiter.reject(new Error(reason));
    }
  }

  private recordLockEvent(params: {
    event_name: string;
    chunk_id: string;
    owner_id: string | null;
    details?: Record<string, unknown>;
  }): void {
    const { event_name, chunk_id, owner_id, details } = params;
    this.recent_lock_event_history.push({
      timestamp: new Date().toISOString(),
      event_name,
      chunk_id,
      owner_id,
      details
    });

    if (this.recent_lock_event_history.length > this.max_event_history_count) {
      this.recent_lock_event_history.splice(
        0,
        this.recent_lock_event_history.length - this.max_event_history_count
      );
    }
  }
}
