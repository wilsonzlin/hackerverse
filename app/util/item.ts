import { decode } from "@msgpack/msgpack";
import { fetchHnItem } from "@wzlin/crawler-toolkit-hn";
import { VBoolean, VInteger, VString, VStruct } from "@wzlin/valid";
import mapExists from "@xtjs/lib/mapExists";

export const fetchItem = async (id: number) => {
  const cacheKey = `hndr:hn-item:${id}`;
  const cached = mapExists(localStorage.getItem(cacheKey), (raw) =>
    JSON.parse(raw),
  );
  if (cached) {
    return cached;
  }
  const item = await fetchHnItem(id);
  localStorage.setItem(cacheKey, JSON.stringify(item));
  return item;
};

const vEdgePost = new VStruct({
  author: new VString(),
  ts: new VInteger(),
  title: new VString(),
  url: new VString(),
  proto: new VString(),
  found_in_archive: new VBoolean(),
});

// Use fast edge.
export const fetchEdgePost = async (edge: string, id: number) => {
  const cacheKey = `hndr:edge-post:${id}`;
  const cached = mapExists(localStorage.getItem(cacheKey), (raw) =>
    JSON.parse(raw),
  );
  if (cached) {
    return cached;
  }
  const res = await fetch(`https://${edge}.edge-hndr.wilsonl.in/post/${id}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch post ${id} with status ${res.status}`);
  }
  const raw = await res.arrayBuffer();
  const post = vEdgePost.parseRoot(decode(raw));
  localStorage.setItem(cacheKey, JSON.stringify(post));
  return post;
};
