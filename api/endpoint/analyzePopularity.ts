import {
  VFiniteNumber,
  VHumanString,
  VInteger,
  VStruct,
  Valid,
} from "@wzlin/valid";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import { QueryGroupByOutput, makeQuery } from "../query";

const input = new VStruct({
  query: new VHumanString(1, 512),
  simMinHundredths: new VInteger(80, 100),
});

export const endpointAnalyzePopularity = {
  input,
  handler: async ({ query, simMinHundredths }: Valid<typeof input>) => {
    const simThreshold = simMinHundredths / 100;
    const res = await makeQuery({
      dataset: "post",
      queries: [query],
      scales: {
        sim: { min: simThreshold, max: 1.0 },
      },
      post_filter_clip: {
        sim: { min: simThreshold, max: 1.0 },
        // Some posts have UNIX timestamp 0.
        ts_day: { min: 1, max: Number.MAX_SAFE_INTEGER },
      },
      weights: {
        votes: "sim",
      },
      outputs: [
        {
          group_by: {
            by: "ts_day",
            bucket: 7,
            cols: [["final_score", "sum"]],
          },
        },
      ],
    });
    const data = assertInstanceOf(res[0], QueryGroupByOutput);
    return {
      timestamps: [...data.groups(new VInteger())].map(
        (d) => new Date(d * 7 * 24 * 60 * 60 * 1000),
      ),
      scores: [...data.column("final_score", new VFiniteNumber())],
    };
  },
};
