import init_wasm, {
  type ChunkDecoder,
  decode_to_splats,
  set_sort_center_state,
  sort32_centers,
} from "gaussian-splat-rs";
import type { SplatResult } from "../data/defines";
import type { SerializedSplatPostDecode } from "../loaders/postDecode";
import {
  type PostDecodeSplatData,
  applySplatPostDecode,
} from "../loaders/postDecodeRuntime";
import { isSogPrefix, loadSog } from "../loaders/sog";
import { getTransferable } from "./transferable";

const rpcHandlers = {
  setSortCenterState,
  sortCenters32,
  loadSplats,
  resolveAsset,
};
export type RpcHandlers = typeof rpcHandlers;

let wasmMemory: WebAssembly.Memory | undefined;

function getWasmMemoryBytes() {
  return wasmMemory?.buffer.byteLength ?? 0;
}

function setSortCenterState({
  centerUpdateRangeIndices,
  updateCenters,
  matrixUpdateRangeIndices,
  updateMatrices,
  rangeMeshIds,
  rangeBases,
  rangeCounts,
}: {
  centerUpdateRangeIndices: Uint32Array;
  updateCenters: Float32Array;
  matrixUpdateRangeIndices: Uint32Array;
  updateMatrices: Float64Array;
  rangeMeshIds: Uint32Array;
  rangeBases: Uint32Array;
  rangeCounts: Uint32Array;
}) {
  set_sort_center_state(
    centerUpdateRangeIndices,
    updateCenters,
    matrixUpdateRangeIndices,
    updateMatrices,
    rangeMeshIds,
    rangeBases,
    rangeCounts,
  );
}

function sortCenters32({
  numSplats,
  cameraPosition,
  direction,
  radial,
  ordering,
}: {
  numSplats: number;
  cameraPosition: [number, number, number];
  direction: [number, number, number];
  radial: boolean;
  ordering: Uint32Array;
}) {
  const activeSplats = sort32_centers(
    numSplats,
    cameraPosition[0],
    cameraPosition[1],
    cameraPosition[2],
    direction[0],
    direction[1],
    direction[2],
    radial,
    ordering,
  );
  return { activeSplats, ordering };
}

async function onMessage(event: MessageEvent) {
  const {
    id,
    name,
    args,
  }: { id: unknown; name: keyof typeof rpcHandlers; args: unknown } =
    event.data;
  try {
    const handler = rpcHandlers[name] as (
      args: unknown,
      options: { sendStatus: (data: unknown) => void },
    ) => unknown | Promise<unknown>;
    if (!handler) {
      throw new Error(`Unknown worker RPC: ${name}`);
    }

    const sendStatus = (data: unknown) => {
      self.postMessage(
        { id, status: data },
        { transfer: getTransferable(data) },
      );
    };
    const result = await handler(args, { sendStatus });
    self.postMessage(
      { id, result, wasmMemoryBytes: getWasmMemoryBytes() },
      { transfer: getTransferable(result) },
    );
  } catch (error) {
    console.warn(`Worker error: ${error}`);
    self.postMessage(
      { id, error, wasmMemoryBytes: getWasmMemoryBytes() },
      { transfer: getTransferable(error) },
    );
  }
}

type LoadArgs = {
  url?: string;
  requestHeader?: Record<string, string>;
  withCredentials?: boolean;
  file?: Blob;
  fileBytes?: Uint8Array;
  fileType?: string;
  pathName?: string;
  baseUrl?: string;
  postDecode?: SerializedSplatPostDecode;
};

export type SplatLoadStatus =
  | { loaded: number; total: number }
  | { assetRequest: number; url: string };

type DecodeArgs = LoadArgs & {
  sendStatus: (data: SplatLoadStatus) => void;
  resolveAsset: (url: string) => Promise<string>;
};

async function decodeInput(args: DecodeArgs) {
  const {
    file,
    fileType,
    pathName,
    baseUrl,
    url,
    requestHeader,
    withCredentials,
    sendStatus,
    resolveAsset,
  } = args;
  let { fileBytes } = args;
  if (
    fileType === "sog" ||
    (!fileType &&
      (/(?:\.sog|(?:^|\/)meta\.json)(?:[?#]|$)/i.test(pathName ?? url ?? "") ||
        (fileBytes && isSogPrefix(fileBytes)) ||
        (file &&
          isSogPrefix(
            new Uint8Array(await file.slice(0, 4096).arrayBuffer()),
          ))))
  ) {
    return loadSog(args);
  }

  let streamLength = fileBytes?.length ?? file?.size ?? 0;
  let expectedInputLength = streamLength;
  let responseBody = file?.stream();
  let responseUrl = url;

  if (!fileBytes && url) {
    const request = new Request(url, {
      headers: requestHeader ? new Headers(requestHeader) : undefined,
      credentials: withCredentials ? "include" : "same-origin",
    });

    const response = await fetch(request);
    if (!response.ok || !response.body) {
      throw new Error(
        `Failed to fetch "${url}": ${response.status} ${response.statusText}`,
      );
    }
    responseBody = response.body;
    responseUrl = response.url;
    const contentLength = Number(response.headers.get("Content-Length") || "0");
    const responseLength =
      Number.isSafeInteger(contentLength) && contentLength > 0
        ? contentLength
        : 0;
    streamLength ||= responseLength;
    const contentEncoding = response.headers.get("Content-Encoding");
    // A CORS-filtered response can hide Content-Encoding while exposing
    // Content-Length, so only use the response length for decoder validation
    // when every response header is visible.
    const hasIdentityEncoding =
      !contentEncoding || contentEncoding.toLowerCase() === "identity";
    if (response.type === "basic" && hasIdentityEncoding) {
      expectedInputLength = responseLength;
    }
  } else if (!fileBytes && !file) {
    throw new Error("No url, file, or fileBytes provided");
  }

  const streamReader = responseBody?.getReader();
  const readInputChunk = async () => {
    if (fileBytes) {
      const chunk = fileBytes;
      fileBytes = undefined;
      return chunk;
    }
    if (streamReader) {
      for (;;) {
        const { done, value } = await streamReader.read();
        if (done) return undefined;
        if (value.length) return value;
      }
    }
  };

  let loaded = 0;
  let decoder: ChunkDecoder | undefined;
  try {
    // Keep the sniffed prefix for either decoder, including tiny stream chunks.
    const pending: Uint8Array[] = [];
    const prefix = new Uint8Array(4096);
    let prefixSize = 0;
    if (!fileType && !file && !fileBytes) {
      while (prefixSize < prefix.length) {
        const chunk = await readInputChunk();
        if (!chunk) break;
        pending.push(chunk);
        const count = Math.min(chunk.length, prefix.length - prefixSize);
        prefix.set(chunk.subarray(0, count), prefixSize);
        prefixSize += count;
        if (
          prefixSize >= 4 &&
          new TextDecoder().decode(prefix.subarray(0, prefixSize)).trim()
        )
          break;
      }
      if (isSogPrefix(prefix.subarray(0, prefixSize))) {
        const crossOrigin =
          url &&
          responseUrl &&
          new URL(url).origin !== new URL(responseUrl).origin;
        return await loadSog({
          readChunk: async () => pending.shift() ?? readInputChunk(),
          baseUrl: responseUrl ?? baseUrl,
          requestHeader: crossOrigin ? undefined : requestHeader,
          withCredentials: !crossOrigin && withCredentials,
          sendStatus,
          resolveAsset,
        });
      }
    }
    decoder = decode_to_splats(fileType, pathName ?? url);
    if (expectedInputLength > 0) {
      decoder.set_expected_input_size(expectedInputLength);
    }

    while (true) {
      const value = pending.shift() ?? (await readInputChunk());
      if (!value) break;

      loaded += value.length;
      if (expectedInputLength > 0 && loaded > expectedInputLength) {
        throw new Error(
          `Input length exceeds the expected ${expectedInputLength} bytes`,
        );
      }
      sendStatus({ loaded, total: streamLength });
      decoder.push(value);
    }

    if (expectedInputLength > 0 && loaded !== expectedInputLength) {
      throw new Error(
        `Input length mismatch: expected ${expectedInputLength} bytes, received ${loaded}`,
      );
    }

    if (streamLength === 0) {
      sendStatus({ loaded, total: loaded });
    }

    const complete = decoder;
    decoder = undefined;
    return complete.finish();
  } catch (error) {
    try {
      await streamReader?.cancel(error);
    } catch {
      // Preserve the decoding error if stream cancellation itself fails.
    }
    throw error;
  } finally {
    decoder?.free();
    streamReader?.releaseLock();
  }
}

async function loadSplats(
  args: LoadArgs,
  { sendStatus }: { sendStatus: (data: SplatLoadStatus) => void },
): Promise<SplatResult> {
  const decoded = (await decodeInput({
    ...args,
    sendStatus,
    resolveAsset: (url) =>
      new Promise<string>((resolve) => {
        const requestId = ++assetRequestId;
        assetRequests.set(requestId, resolve);
        sendStatus({ assetRequest: requestId, url });
      }),
  })) as PostDecodeSplatData;
  if (args.postDecode) applySplatPostDecode(decoded, args.postDecode);
  return {
    numSplats: decoded.numSplats,
    splatArrays: [decoded.splat0, decoded.splat1],
    sortCenters: decoded.sortCenters,
    extra: {
      sh1: decoded.sh1,
      sh2: decoded.sh2,
      sh3a: decoded.sh3a,
      sh3b: decoded.sh3b,
    },
  };
}

let assetRequestId = 0;
const assetRequests = new Map<number, (url: string) => void>();

function resolveAsset({ requestId, url }: { requestId: number; url: string }) {
  assetRequests.get(requestId)?.(url);
  assetRequests.delete(requestId);
}

async function initialize() {
  let resolveWaitForModule: (value: WebAssembly.Module) => void;
  const waitForModule = new Promise<WebAssembly.Module>((resolve) => {
    resolveWaitForModule = resolve;
  });

  const pending: MessageEvent[] = [];
  const bufferMessage = (event: MessageEvent) => {
    if (event.data.name === "init-wasm") {
      resolveWaitForModule(event.data.module as WebAssembly.Module);
      return;
    }
    pending.push(event);
  };
  self.addEventListener("message", bufferMessage);

  const wasm = await init_wasm({ module_or_path: await waitForModule });
  wasmMemory = wasm.memory;

  self.removeEventListener("message", bufferMessage);
  self.addEventListener("message", onMessage);

  for (const event of pending) {
    onMessage(event);
  }
  pending.length = 0;
}

void initialize().catch((error) => {
  setTimeout(() => {
    throw error;
  });
});
