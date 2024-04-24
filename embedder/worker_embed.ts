import { createPyIpcQueue, spawnPyIpc } from "@msgpipe/nodejs";
import {
  VArray,
  VBytes,
  VFiniteNumber,
  VObjectMap,
  VOptional,
  VStruct,
} from "@wzlin/valid";

export const createEmbedWorker = async (dim: number) => {
  const worker = await spawnPyIpc({
    script: `${__dirname}/worker_embed.py`,
    rootDir: `${__dirname}/../`,
  });
  const queue = createPyIpcQueue(worker);
  return {
    embed: async (texts: string[]) => {
      const embLenRaw = dim * 4;
      const rawLen = embLenRaw * texts.length;
      const res = await queue.request(
        "embed",
        { texts },
        new VStruct({
          embeddings_raw: new VBytes(rawLen, rawLen),
          lexical_weights: new VOptional(
            new VArray(
              new VObjectMap(new VFiniteNumber()),
              texts.length,
              texts.length,
            ),
          ),
        }),
      );
      return texts.map((_, i) => ({
        dense: res.embeddings_raw.slice(i * embLenRaw, (i + 1) * embLenRaw),
        sparse: res.lexical_weights?.at(i),
      }));
    },
  };
};
