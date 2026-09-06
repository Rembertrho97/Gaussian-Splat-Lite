import { Loader } from "three";
import { Splats, type SplatsOptions } from "../data/Splats";
import { workerPool } from "../runtime/SplatWorker";
import type { SplatLoadStatus } from "../runtime/worker";
import { SplatMesh } from "../scene/SplatMesh";
import { serializeSplatPostDecode } from "./postDecode";

type SplatLoadOptions = Pick<
  SplatsOptions,
  | "url"
  | "file"
  | "fileBytes"
  | "fileType"
  | "fileName"
  | "postDecode"
  | "onProgress"
> & {
  splats?: Splats;
  onLoad?: (decoded: Splats) => void;
  onError?: (error: unknown) => void;
};

// SplatLoader implements the THREE.Loader interface for PLY, SPZ and SOG.
export class SplatLoader extends Loader {
  load(
    url: string,
    onLoad?: (decoded: Splats) => void,
    onProgress?: (event: ProgressEvent) => void,
    onError?: (error: unknown) => void,
  ) {
    return this.loadInternal({ url, onLoad, onProgress, onError });
  }

  loadAsync(
    url: string,
    onProgress?: (event: ProgressEvent) => void,
  ): Promise<Splats> {
    return this.loadInternalAsync({ url, onProgress });
  }

  parse(splats: Splats): SplatMesh {
    return new SplatMesh({ splats });
  }

  loadInternal(options: SplatLoadOptions) {
    void this.loadInternalAsync(options).catch(() => {});
  }

  async loadInternalAsync({
    splats,
    url,
    file,
    fileBytes,
    fileType,
    fileName,
    postDecode,
    onLoad,
    onProgress,
    onError,
  }: SplatLoadOptions): Promise<Splats> {
    let resolvedURL: string | undefined;
    let started = false;
    try {
      if (
        [url, file, fileBytes].filter((input) => input !== undefined).length !==
        1
      ) {
        throw new Error("Provide exactly one of url, file, or fileBytes");
      }
      fileName ??= (file as File | undefined)?.name;
      const byteArray =
        fileBytes instanceof ArrayBuffer
          ? new Uint8Array(fileBytes)
          : fileBytes;
      resolvedURL =
        url === undefined
          ? undefined
          : this.manager.resolveURL((this.path ?? "") + url);
      started = true;
      this.manager.itemStart(resolvedURL ?? "");

      const pathName = resolvedURL || fileName;
      const baseUrl = new URL(pathName || "", window.location.href).href;
      const memoryHeavy =
        fileType === "sog" ||
        (!fileType && !/\.(ply|spz)(?:[?#]|$)/i.test(pathName ?? ""));
      const decoded = await workerPool.withWorker(
        (worker) =>
          worker.call(
            "loadSplats",
            {
              url: resolvedURL ? baseUrl : undefined,
              requestHeader: this.requestHeader,
              withCredentials: this.withCredentials,
              file,
              fileBytes: byteArray?.slice(),
              fileType,
              pathName,
              baseUrl,
              postDecode: postDecode
                ? serializeSplatPostDecode(postDecode)
                : undefined,
            },
            {
              onStatus: (data) => {
                const status = data as SplatLoadStatus;
                if ("assetRequest" in status) {
                  return worker.call("resolveAsset", {
                    requestId: status.assetRequest,
                    url: new URL(
                      this.manager.resolveURL(status.url),
                      window.location.href,
                    ).href,
                  });
                }
                if (onProgress) {
                  try {
                    onProgress(
                      new ProgressEvent("progress", {
                        lengthComputable: status.total !== 0,
                        ...status,
                      }),
                    );
                  } catch (error) {
                    console.error("Progress callback failed", error);
                  }
                }
              },
            },
          ),
        memoryHeavy,
      );
      const result = splats ?? new Splats();
      result.initialize(decoded as SplatsOptions);
      onLoad?.(result);
      return result;
    } catch (error) {
      if (started) this.manager.itemError(resolvedURL ?? "");
      onError?.(error);
      throw error;
    } finally {
      if (started) this.manager.itemEnd(resolvedURL ?? "");
    }
  }
}
