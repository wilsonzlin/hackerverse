import { decode, encode } from "@msgpack/msgpack";
import { Validator } from "@wzlin/valid";
import { useCallback, useEffect, useRef, useState } from "react";

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
        const res = await fetch("https://api-hndr.wilsonl.in/" + endpoint, {
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
