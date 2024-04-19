import { createPyIpcQueue, spawnPyIpc } from "@msgpipe/nodejs";
import { VArray, VFiniteNumber, VStruct } from "@wzlin/valid";

export const createModel = async () => {
  const worker = await spawnPyIpc({
    script: `${__dirname}/model.py`,
    rootDir: `${__dirname}/../`,
  });
  const queue = createPyIpcQueue(worker);
  return {
    execute: async (texts: string[]) => {
      const { scores } = await queue.request(
        "model",
        { texts },
        new VStruct({
          scores: new VArray(
            new VStruct({
              negative: new VFiniteNumber(),
              neutral: new VFiniteNumber(),
              positive: new VFiniteNumber(),
            }),
            texts.length,
            texts.length,
          ),
        }),
      );
      return texts.map((_, i) => scores[i]);
    },
  };
};
