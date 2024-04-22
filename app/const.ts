import { VInstanceOf, VInteger, VStruct, VTagged } from "@wzlin/valid";

export const vWorkerPointMapMessage = new VTagged("$type", {
  render: new VStruct({
    canvas: new VInstanceOf(OffscreenCanvas),
    lod: new VInteger(0),
  }),
});
