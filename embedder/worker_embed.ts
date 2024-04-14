import { createPyIpcQueue, spawnPyIpc } from "@msgpipe/nodejs";
import { VBytes, VStruct } from "@wzlin/valid";

export const createEmbedWorker = async () => {
  const worker = await spawnPyIpc({
    script: `${__dirname}/worker_embed.py`,
    rootDir: `${__dirname}/../`,
  });
  const queue = createPyIpcQueue(worker);
  return {
    embed: async (texts: string[]) => {
      const embLenRaw = 512 * 4;
      const rawLen = embLenRaw * texts.length;
      const { embeddings_raw: raw } = await queue.request(
        "embed",
        { texts },
        new VStruct({
          embeddings_raw: new VBytes(rawLen, rawLen),
        }),
      );
      return texts.map((_, i) => raw.slice(i * embLenRaw, (i + 1) * embLenRaw));
    },
  };
};
