import {
  VFiniteNumber,
  VHumanString,
  VInteger,
  VString,
  VStruct,
  Valid,
} from "@wzlin/valid";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import { QueryGroupByOutput, makeQuery } from "../query";

const input = new VStruct({
  query: new VHumanString(1, 512),
  limit: new VInteger(1, 20),
  simMinHundredths: new VInteger(80, 100),
});

export const endpointTopUsers = {
  input,
  handler: async ({ query, limit, simMinHundredths }: Valid<typeof input>) => {
    const simThreshold = simMinHundredths / 100;
    const res = await makeQuery({
      dataset: "comment",
      queries: [query],
      scales: {
        sim: {
          min: simThreshold,
          max: 1,
        },
      },
      weights: {
        // We can't multiply by votes, because the HN API does not expose votes for anything except posts.
        sim_scaled: 1,
      },
      outputs: [
        {
          group_by: {
            by: "user",
            cols: [["final_score", "sum"]],
            order_by: "final_score",
            order_asc: false,
            limit,
          },
        },
      ],
    });
    const data = assertInstanceOf(res[0], QueryGroupByOutput);
    return [
      ...data.entries(new VString(), {
        final_score: new VFiniteNumber(),
      }),
    ].map((e) => ({
      user: e[0],
      score: e[1].final_score,
    }));
  },
};
