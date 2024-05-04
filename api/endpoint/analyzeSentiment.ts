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

export const endpointAnalyzeSentiment = {
  input,
  handler: async ({ query, simMinHundredths }: Valid<typeof input>) => {
    const res = await makeQuery({
      dataset: "comment",
      queries: [query],
      thresholds: {
        sim: simMinHundredths / 100,
        sent_pos: 0.5,
        sent_neg: 0.5,
      },
      post_filter_clip: {
        sim_thresh: { min: 1.0, max: 1.0 },
      },
      outputs: [
        {
          group_by: {
            by: "ts_day",
            bucket: 7,
            cols: [
              ["sent_pos_thresh", "sum"],
              ["sent_neg_thresh", "sum"],
            ],
          },
        },
      ],
    });
    const data = assertInstanceOf(res[0], QueryGroupByOutput);
    return {
      timestamps: [...data.groups(new VInteger())].map(
        (d) => new Date(d * 7 * 24 * 60 * 60 * 1000),
      ),
      positives: [...data.column("sent_pos_thresh", new VFiniteNumber())],
      negatives: [...data.column("sent_neg_thresh", new VFiniteNumber())],
    };
  },
};
