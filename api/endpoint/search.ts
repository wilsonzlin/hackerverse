import { VHumanString, VInteger, VMember, VStruct, Valid } from "@wzlin/valid";
import serialiseToQueryString from "@xtjs/lib/serialiseToQueryString";

const input = new VStruct({
  query: new VHumanString(1, 512),
  limit: new VInteger(1, 128),
  dataset: new VMember(["posts", "comments"]),
});

export const endpointSearch = {
  input,
  handler: async (req: Valid<typeof input>) => {
    const results = await fetch(
      `http://127.0.0.1:7001/${serialiseToQueryString(req)}`,
    ).then((r) => r.json());
    return { results };
  },
};
