import { createPyIpcQueue, spawnPyIpc } from "@msgpipe/nodejs";
import { VArray, VFiniteNumber, VObjectMap, VStruct } from "@wzlin/valid";

export const createEmbedWorker = async () => {
  const worker = await spawnPyIpc({
    script: `${__dirname}/worker_embed.py`,
    rootDir: `${__dirname}/../`,
  });
  const queue = createPyIpcQueue(worker);
  return {
    embed: async (texts: string[]) =>
      (
        await queue.request(
          "embed",
          { texts },
          new VStruct({
            embeddings: new VArray(
              new VStruct({
                dense: new VArray(new VFiniteNumber(), 1024, 1024),
                sparse: new VObjectMap(new VFiniteNumber()),
              }),
            ),
          }),
        )
      ).embeddings,
  };
};
