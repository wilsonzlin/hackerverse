import {
  VHumanString,
  VInteger,
  VMember,
  VStruct,
  VTuple,
  Valid,
} from "@wzlin/valid";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import { QueryHeatmapOutput, makeQuery } from "../query";

const input = new VStruct({
  query: new VHumanString(1, 512),
  dataset: new VMember(["post", "toppost"] as const),
  color: new VTuple([
    new VInteger(0, 255),
    new VInteger(0, 255),
    new VInteger(0, 255),
  ] as const),
});

export const endpointHeatmap = {
  input,
  handler: async ({ dataset, query, color }: Valid<typeof input>) => {
    const res = await makeQuery({
      dataset,
      queries: [query],
      scales: {
        sim: {
          post: { min: 0.7, max: 1 },
          toppost: { min: 0.55, max: 1 },
        }[dataset],
      },
      post_filter_clip: {
        scaled: { min: 0.01, max: 1 },
      },
      weights: {
        sim_scaled: 1,
      },
      outputs: [
        {
          heatmap: {
            alpha_scale: 2, // TODO This is really a hack, investigate distribution of scores.
            density: 25,
            color,
            upscale: 2,
            sigma: 4,
          },
        },
      ],
    });
    const data = assertInstanceOf(res[0], QueryHeatmapOutput);
    return new Uint8Array(data.raw);
  },
};
