import {
  VFiniteNumber,
  VHumanString,
  VInteger,
  VStruct,
  Valid,
} from "@wzlin/valid";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import { QueryItemsOutput, makeQuery } from "../query";

const input = new VStruct({
  query: new VHumanString(1, 512),
  limit: new VInteger(1, 20),
  simMinHundredths: new VInteger(80, 100),
});

export const endpointTopPosts = {
  input,
  handler: async ({ query, limit, simMinHundredths }: Valid<typeof input>) => {
    const simThreshold = simMinHundredths / 100;
    const res = await makeQuery({
      dataset: "post",
      queries: [query],
      post_filter_clip: {
        sim: { min: simThreshold, max: 1 },
      },
      outputs: [
        {
          items: {
            cols: ["id", "sim"],
            limit,
            order_asc: false,
            order_by: "votes",
          },
        },
      ],
    });
    const data = assertInstanceOf(res[0], QueryItemsOutput);
    return [
      ...data.items({
        id: new VInteger(1),
        sim: new VFiniteNumber(),
      }),
    ];
  },
};
