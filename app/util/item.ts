import { decode } from "@msgpack/msgpack";
import { Item, fetchHnItem } from "@wzlin/crawler-toolkit-hn";
import { VBoolean, VInteger, VString, VStruct } from "@wzlin/valid";
import { useEffect, useRef, useState } from "react";
import { CACHED_FETCH_404, cachedFetch } from "./fetch";

const vEdgePost = new VStruct({
  author: new VString(),
  ts: new VInteger(),
  title: new VString(),
  url: new VString(),
  proto: new VString(),
  found_in_archive: new VBoolean(),
});

// Use fast edge.
export const cachedFetchEdgePost = async (
  signal: AbortSignal,
  edge: string,
  id: number,
) => {
  const res = await cachedFetch(
    `https://${edge}.edge-hndr.wilsonl.in/post/${id}`,
    signal,
    // Some posts don't exist if they're missing a title or author.
    "except-404",
  );
  return res.body === CACHED_FETCH_404
    ? undefined
    : vEdgePost.parseRoot(decode(res.body));
};

export const useHnItems = (ids: number[]) => {
  const fetchStarted = useRef(new Set<number>());
  const [items, setItems] = useState<Record<number, Item>>({});
  useEffect(() => {
    (async () => {
      await Promise.all(
        ids.map(async (id) => {
          if (!fetchStarted.current.has(id)) {
            fetchStarted.current.add(id);
            const item = await fetchHnItem(id);
            setItems((items) => ({ ...items, [id]: item }));
          }
        }),
      );
    })();
  }, [ids]);
  return items;
};
