import { decode } from "@msgpack/msgpack";
import { VBoolean, VInteger, VString, VStruct } from "@wzlin/valid";
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
