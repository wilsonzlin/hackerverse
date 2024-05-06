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
  limit: new VInteger(1, 500),
  simMinHundredths: new VInteger(80, 100),
  orderBy: new VMember(["votes", "ts"] as const),
});

export const endpointPosts = {
  input,
  handler: async ({
    query,
    limit,
    simMinHundredths,
    orderBy,
  }: Valid<typeof input>) => {
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
            order_by: orderBy,
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
