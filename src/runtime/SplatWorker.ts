import { WASM_MODULE } from "./wasm";

import { getTransferable } from "./transferable";
import type { RpcHandlers } from "./worker";
import BundledWorker from "./worker?worker&inline";

type PromiseRecord = {
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
  onStatus?: (data: unknown) => void | Promise<void>;
  statusQueue: Promise<void>;
};

export class SplatWorker {
  worker: Worker;
  messages: Record<number, PromiseRecord> = {};
  peakWasmMemoryBytes = 0;
  disposed = false;
  static currentId = 0;

  constructor() {
    this.worker = new BundledWorker();
    this.worker.onmessage = (event) => this.onMessage(event);
    this.worker.onerror = (event) => this.dispose(new Error(event.message));
    this.worker.onmessageerror = () =>
      this.dispose(new Error("Invalid worker message"));
    void WASM_MODULE.then((module) => {
      if (!this.disposed)
        this.worker.postMessage({ name: "init-wasm", module });
    }).catch((error) => this.dispose(error));
  }

  onMessage(event: MessageEvent) {
    const { id, result, error, status, wasmMemoryBytes } = event.data;
    if (Number.isSafeInteger(wasmMemoryBytes) && wasmMemoryBytes >= 0) {
      this.peakWasmMemoryBytes = Math.max(
        this.peakWasmMemoryBytes,
        wasmMemoryBytes,
      );
    }
    const promise = this.messages[id];
    if (!promise) return;

    if (status !== undefined) {
      promise.statusQueue = promise.statusQueue.then(() => {
        if (this.messages[id] === promise) {
          return promise.onStatus?.(status);
        }
      });
      void promise.statusQueue.catch((error) => this.dispose(error));
      return;
    }

    void promise.statusQueue
      .then(() => {
        if (error !== undefined) throw error;
        return result;
      })
      .finally(() => {
        delete this.messages[id];
      })
      .then(promise.resolve, promise.reject);
  }

  async call<Name extends keyof RpcHandlers>(
    name: Name,
    args: Parameters<RpcHandlers[Name]>[0],
    options: {
      onStatus?: (data: unknown) => void | Promise<void>;
    } = {},
  ): Promise<Awaited<ReturnType<RpcHandlers[Name]>>> {
    type Result = Awaited<ReturnType<RpcHandlers[Name]>>;
    if (this.disposed) throw new Error("Worker terminated");
    const id = ++SplatWorker.currentId;
    const promise = new Promise<Result>((resolve, reject) => {
      this.messages[id] = {
        resolve: (value) => resolve(value as Result),
        reject,
        onStatus: options.onStatus,
        statusQueue: Promise.resolve(),
      };
    });
    try {
      this.worker.postMessage(
        { id, name, args },
        { transfer: getTransferable(args) },
      );
    } catch (error) {
      this.messages[id].reject(error);
      delete this.messages[id];
    }
    return promise;
  }

  dispose(reason: unknown = new Error("Worker terminated")) {
    if (this.disposed) return;
    this.disposed = true;
    this.worker.terminate();

    const messages = Object.values(this.messages);
    this.messages = {};
    for (const message of messages) {
      message.reject(reason);
    }
  }
}

const MEBIBYTE = 1024 * 1024;

const SMALL_WORKER_MEMORY_BYTES = 64 * MEBIBYTE;
const LARGE_WORKER_MEMORY_BYTES = 256 * MEBIBYTE;
const SMALL_WORKER_IDLE_TIMEOUT_MS = 3 * 60 * 1000;
const LARGE_WORKER_IDLE_TIMEOUT_MS = 3 * 1000;

/**
 * Keep small workers around for reuse, while releasing workers whose WASM
 * linear memory has grown large much sooner.
 */
function getWorkerIdleTimeoutMs(peakWasmMemoryBytes: number): number {
  const clampedMemoryBytes = Math.min(
    LARGE_WORKER_MEMORY_BYTES,
    Math.max(SMALL_WORKER_MEMORY_BYTES, peakWasmMemoryBytes),
  );
  const memoryRatio =
    (clampedMemoryBytes - SMALL_WORKER_MEMORY_BYTES) /
    (LARGE_WORKER_MEMORY_BYTES - SMALL_WORKER_MEMORY_BYTES);

  return Math.round(
    SMALL_WORKER_IDLE_TIMEOUT_MS +
      memoryRatio *
        (LARGE_WORKER_IDLE_TIMEOUT_MS - SMALL_WORKER_IDLE_TIMEOUT_MS),
  );
}

/**
 * Prefer the smallest idle worker so workers with large WASM heaps can reach
 * their shorter expiry timers. Equal-sized workers retain LIFO behavior.
 */
function getWorkerReuseIndex(
  workers: readonly { peakWasmMemoryBytes: number }[],
): number {
  let selectedIndex = -1;
  let selectedMemoryBytes = Number.POSITIVE_INFINITY;

  for (let index = workers.length - 1; index >= 0; index -= 1) {
    const memoryBytes = workers[index].peakWasmMemoryBytes;
    if (memoryBytes < selectedMemoryBytes) {
      selectedIndex = index;
      selectedMemoryBytes = memoryBytes;
    }
  }

  return selectedIndex;
}

class SplatWorkerPool {
  private heavyJobs: Promise<void> = Promise.resolve();
  maxWorkers;
  numWorkers = 0;
  freelist: SplatWorker[] = [];
  idleWorkerTimeouts = new Map<SplatWorker, ReturnType<typeof setTimeout>>();
  queue: ((worker: SplatWorker) => void)[] = [];

  constructor(maxWorkers = 4) {
    this.maxWorkers = maxWorkers;
  }

  async withWorker<T>(
    callback: (worker: SplatWorker) => Promise<T>,
    memoryHeavy = false,
  ): Promise<T> {
    if (memoryHeavy) {
      const next = this.heavyJobs.then(() => this.withWorker(callback));
      this.heavyJobs = next.then(
        () => {},
        () => {},
      );
      return next;
    }

    const worker = await this.allocWorker();
    try {
      return await callback(worker);
    } finally {
      this.freeWorker(worker);
    }
  }

  async allocWorker(): Promise<SplatWorker> {
    for (let index = this.freelist.length - 1; index >= 0; index--) {
      const worker = this.freelist[index];
      if (!worker.disposed) continue;
      clearTimeout(this.idleWorkerTimeouts.get(worker));
      this.idleWorkerTimeouts.delete(worker);
      this.freelist.splice(index, 1);
      this.numWorkers -= 1;
    }
    const workerIndex = getWorkerReuseIndex(this.freelist);
    if (workerIndex !== -1) {
      const worker = this.freelist.splice(workerIndex, 1)[0];
      const timeout = this.idleWorkerTimeouts.get(worker);
      if (timeout !== undefined) {
        clearTimeout(timeout);
        this.idleWorkerTimeouts.delete(worker);
      }
      return worker;
    }

    if (this.numWorkers < this.maxWorkers) {
      const worker = new SplatWorker();
      this.numWorkers += 1;
      return worker;
    }

    return new Promise((resolve) => {
      this.queue.push(resolve);
    });
  }

  freeWorker(worker: SplatWorker) {
    if (worker.disposed) {
      this.numWorkers -= 1;
      const waiter = this.queue.shift();
      if (waiter) {
        this.numWorkers += 1;
        waiter(new SplatWorker());
      }
      return;
    }
    if (this.numWorkers > this.maxWorkers) {
      // Worker no longer needed
      worker.dispose();
      this.numWorkers -= 1;
      return;
    }

    const waiter = this.queue.shift();
    if (waiter) {
      waiter(worker);
      return;
    }

    this.freelist.push(worker);
    const timeout = setTimeout(() => {
      this.idleWorkerTimeouts.delete(worker);
      const index = this.freelist.indexOf(worker);
      if (index === -1) return;

      this.freelist.splice(index, 1);
      worker.dispose();
      this.numWorkers -= 1;
    }, getWorkerIdleTimeoutMs(worker.peakWasmMemoryBytes));
    this.idleWorkerTimeouts.set(worker, timeout);
  }
}

export const workerPool = new SplatWorkerPool();
