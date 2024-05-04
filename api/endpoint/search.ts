import {
  VFiniteNumber,
  VHumanString,
  VInteger,
  VMember,
  VStruct,
  Valid,
} from "@wzlin/valid";
import serialiseToQueryString from "@xtjs/lib/serialiseToQueryString";

const input = new VStruct({
  query: new VHumanString(1, 512),
  limit: new VInteger(1, 128),
  dataset: new VMember(["posts", "posts_bgem3", "comments"]),
  weightSimilarity: new VFiniteNumber(),
  weightScore: new VFiniteNumber(),
  weightTimestamp: new VFiniteNumber(),
  decayTimestamp: new VFiniteNumber(),
});

export const endpointSearch = {
  input,
  handler: async ({
    limit,
    dataset,
    decayTimestamp,
    query,
    weightScore,
    weightSimilarity,
    weightTimestamp,
  }: Valid<typeof input>) => {
    const results = await fetch(
      `http://127.0.0.1:7001/${serialiseToQueryString({
        query,
        limit: 128,
        dataset,
        w_sim: weightSimilarity,
        w_score: weightScore,
        w_ts: weightTimestamp,
        decay_ts: decayTimestamp,
      })}`,
    ).then((r) => r.json());
    return { results: results.slice(0, limit) };
  },
};
