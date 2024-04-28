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

export const usePromise = <T>() => {
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

  const set = useCallback(async (cb: (signal: AbortSignal) => Promise<T>) => {
    clear();
    setLoading(true);
    const ac = (cur.current = new AbortController());
    try {
      setData(await cb(ac.signal));
    } catch (err) {
      if (!ac.signal.aborted) {
        setError(err.message);
      }
      // In case the caller of this set() awaits it.
      throw err;
    } finally {
      if (!ac.signal.aborted) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    return () => cur.current?.abort();
  }, []);

  return {
    clear,
    data,
    error,
    loading,
    set,
  };
};
