import {
  VFiniteNumber,
  VHumanString,
  VInteger,
  VMember,
  VStruct,
  Valid,
} from "@wzlin/valid";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import { QueryItemsOutput, makeQuery } from "../query";

const input = new VStruct({
  query: new VHumanString(1, 512),
  limit: new VInteger(1, 128),
  dataset: new VMember(["post", "toppost"] as const),
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
    const res = await makeQuery({
      dataset,
      queries: [query],
      ts_decay: decayTimestamp,
      scales: {
        sim: {
          post: { min: 0.7, max: 1 },
          toppost: { min: 0.55, max: 1 },
        }[dataset],
      },
      weights: {
        sim_scaled: weightSimilarity,
        ts_norm: weightTimestamp,
        votes_norm: weightScore,
      },
      outputs: [
        {
          items: {
            cols: ["id", "x", "y", "sim", "final_score"],
            limit,
          },
        },
      ],
    });
    const data = assertInstanceOf(res[0], QueryItemsOutput);
    return [
      ...data.items({
        id: new VInteger(1),
        x: new VFiniteNumber(),
        y: new VFiniteNumber(),
        sim: new VFiniteNumber(),
        final_score: new VFiniteNumber(),
      }),
    ];
  },
};
