import { decode, encode } from "@msgpack/msgpack";
import { Item, fetchHnItem } from "@wzlin/crawler-toolkit-hn";
import {
  VArray,
  VBoolean,
  VInteger,
  VOptional,
  VString,
  VStruct,
  VUnixSecTimestamp,
  Valid,
} from "@wzlin/valid";
import { createContext, useContext, useEffect, useRef, useState } from "react";
import { CACHED_FETCH_404, cachedFetch } from "./fetch";

const EDGES = [
  "ap-sydney-1",
  "uk-london-1",
  "us-ashburn-1",
  "us-sanjose-1",
] as const;

export const DEFAULT_EDGE = "us-ashburn-1";

// Use Promise.race so we don't wait for the slow ones.
export const findClosestEdge = (signal: AbortSignal | undefined) =>
  Promise.race(
    EDGES.map(async (edge) => {
      // Run a few times to avoid potential cold start biases.
      for (let i = 0; i < 3; i++) {
        await fetch(`https://${edge}.edge-hndr.wilsonl.in/healthz`, { signal });
      }
      return edge;
    }),
  );

export const EdgeContext = createContext(DEFAULT_EDGE);

const vEdgePost = new VStruct({
  author: new VString(),
  score: new VInteger(),
  ts: new VUnixSecTimestamp(),
  title: new VString(),
  url: new VString(),
  proto: new VString(),
  found_in_archive: new VBoolean(),
});
export type EdgePost = Valid<typeof vEdgePost>;

// Use fast edge.
export const cachedFetchEdgePost = async (
  signal: AbortSignal | undefined,
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

export const useEdgePosts = (ids: number[]) => {
  const edge = useContext(EdgeContext);
  const fetchStarted = useRef(new Set<number>());
  const [items, setItems] = useState<Record<number, EdgePost>>({});
  useEffect(() => {
    (async () => {
      await Promise.all(
        ids.map(async (id) => {
          if (!fetchStarted.current.has(id)) {
            fetchStarted.current.add(id);
            const item = await cachedFetchEdgePost(undefined, edge, id);
            if (item) {
              setItems((items) => ({ ...items, [id]: item }));
            }
          }
        }),
      );
    })();
  }, [ids]);
  return items;
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

export const useHnCommentPosts = (ids: number[]) => {
  const fetchStarted = useRef(new Set<number>());
  const [items, setItems] = useState<Record<number, Item | undefined>>({});
  useEffect(() => {
    (async () => {
      await Promise.all(
        ids.map(async (id) => {
          if (!fetchStarted.current.has(id)) {
            fetchStarted.current.add(id);
            let curId = id;
            let cur: Item | undefined;
            while (true) {
              cur = await fetchHnItem(curId);
              if (cur?.type === "story") {
                break;
              }
              if (cur?.type !== "comment" || !cur.parent) {
                cur = undefined;
                break;
              }
              curId = cur.parent;
            }
            setItems((items) => ({ ...items, [id]: cur }));
          }
        }),
      );
    })();
  }, [ids]);
  return items;
};

const vEdgeUrlMeta = new VStruct({
  description: new VString(),
  image_url: new VString(),
  lang: new VString(),
  snippet: new VString(),
  timestamp: new VUnixSecTimestamp(),
  timestamp_modified: new VUnixSecTimestamp(),
  title: new VString(),
});
export type EdgeUrlMeta = Valid<typeof vEdgeUrlMeta>;

export const fetchEdgeUrlMetas = async (
  signal: AbortSignal | undefined,
  edge: string,
  normalizedUrls: string[],
) => {
  const res = await fetch(`https://${edge}.edge-hndr.wilsonl.in/url-metas`, {
    signal,
    method: "POST",
    headers: {
      "Content-Type": "application/msgpack",
    },
    body: encode(normalizedUrls),
  });
  if (!res.ok) {
    const raw = await res.text();
    throw new Error(
      `Failed to fetch URL metas from edge with ${res.status}: ${raw}`,
    );
  }
  const raw = await res.arrayBuffer();
  return new VArray(new VOptional(vEdgeUrlMeta)).parseRoot(decode(raw));
};

export const useEdgeUrlMetas = (normalizedUrls: string[]) => {
  const edge = useContext(EdgeContext);
  const fetchStarted = useRef(new Set<string>());
  const [items, setItems] = useState<Record<string, EdgeUrlMeta>>({});
  useEffect(() => {
    (async () => {
      const pending = Array<string>();
      for (const url of normalizedUrls) {
        if (!fetchStarted.current.has(url)) {
          fetchStarted.current.add(url);
          pending.push(url);
        }
      }
      const res = await fetchEdgeUrlMetas(undefined, edge, pending);
      for (const [i, urlMeta] of res.entries()) {
        if (urlMeta) {
          setItems((items) => ({ ...items, [pending[i]]: urlMeta }));
        }
      }
    })();
  }, [normalizedUrls]);
  return items;
};
