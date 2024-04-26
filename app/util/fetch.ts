import { decode, encode } from "@msgpack/msgpack";
import { Validator } from "@wzlin/valid";
import Dict from "@xtjs/lib/Dict";
import { useCallback, useEffect, useRef, useState } from "react";

export class BadStatusError extends Error {
  constructor(
    readonly url: string,
    readonly status: number,
  ) {
    super(`GET ${url} failed with status ${status}`);
  }
}

type CachedFetchResult = {
  status: number;
  headers: Headers;
  body: ArrayBuffer;
  throwIfBadStatus: () => void;
};
export const CACHED_FETCH_404 = new ArrayBuffer(0);
const fetches = new Dict<string, Promise<CachedFetchResult>>();
export const cachedFetch = (
  url: string,
  signal?: AbortSignal,
  throwOnBadStatus: boolean | "except-404" = false,
) =>
  fetches.computeIfAbsent(url, async () => {
    try {
      const res = await fetch(url, { signal });
      if (res.status >= 500) {
        throw new BadStatusError(url, res.status);
      }
      const throwIfBadStatus = () => {
        if (res.status < 200 || res.status > 299) {
          throw new BadStatusError(url, res.status);
        }
      };
      if (
        throwOnBadStatus === true ||
        (throwOnBadStatus === "except-404" && res.status !== 404)
      ) {
        throwIfBadStatus();
      }
      return {
        status: res.status,
        headers: res.headers,
        body:
          throwOnBadStatus === "except-404" && res.status === 404
            ? CACHED_FETCH_404
            : await res.arrayBuffer(),
        throwIfBadStatus,
      };
    } catch (err) {
      fetches.delete(url);
      throw err;
    }
  });

export const useRequest = <T>(endpoint: string, response: Validator<T>) => {
  const cur = useRef<AbortController | undefined>();
  const [data, setData] = useState<T | undefined>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  const clear = useCallback(() => {
    cur.current?.abort();
    setData(undefined);
    setLoading(false);
    setError(undefined);
  }, []);

  const go = useCallback(
    async (requestBody: any) => {
      clear();
      setLoading(true);
      const ac = (cur.current = new AbortController());
      try {
        const res = await fetch(`https://api-hndr.wilsonl.in/${endpoint}`, {
          signal: ac.signal,
          method: "POST",
          headers: {
            "content-type": "application/msgpack",
          },
          body: encode(requestBody),
        });
        if (res.headers.get("content-type") !== "application/msgpack") {
          return setError(
            [res.status, await res.text()].filter((l) => l).join(": "),
          );
        }
        const raw = await res.arrayBuffer();
        const data = await decode(new Uint8Array(raw));
        setData(response.parseRoot(data));
      } catch (err) {
        if (!ac.signal.aborted) {
          setError(["Fetch", err.message].join(": "));
        }
      } finally {
        if (!ac.signal.aborted) {
          setLoading(false);
        }
      }
    },
    [endpoint],
  );

  useEffect(() => {
    return () => cur.current?.abort();
  }, []);

  return {
    clear,
    data,
    error,
    go,
    loading,
  };
};
