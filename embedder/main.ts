import { encode } from "@msgpack/msgpack";
import { setUpUncaughtExceptionHandler } from "@wzlin/service-toolkit";
import Batcher from "@xtjs/lib/js/Batcher";
import Pipeline from "@xtjs/lib/js/Pipeline";
import decodeUtf8 from "@xtjs/lib/js/decodeUtf8";
import { Duration } from "luxon";
import {
  QUEUE_EMBED,
  getKvRow,
  lg,
  measureMs,
  upsertKvRow,
  vQueueEmbedTask,
} from "../common/res";
import { createEmbedWorker } from "./worker_embed";

setUpUncaughtExceptionHandler();

const rawBytes = (t: ArrayBufferView) =>
  Buffer.from(t.buffer, t.byteOffset, t.byteLength);

(async () => {
  const embedWorker = await createEmbedWorker();
  const embedBatcher = new Batcher((texts: string[]) =>
    embedWorker.embed(texts),
  );

  await Pipeline.from(async () => {
    const [msg] = await QUEUE_EMBED.pollMessages(
      1,
      Duration.fromObject({ minutes: 30 }).as("seconds"),
    );
    if (!msg) {
      return;
    }
    const task = vQueueEmbedTask.parseRoot(msg.contents);
    return { msg, task };
  })
    .then({
      concurrency: 512,
      costBuffer: 1024,
      inputCost: () => 1,
      handler: async ({ msg, task }) => {
        const embInput = decodeUtf8(await getKvRow.execute(task.inputKey));
        return { msg, task, embInput };
      },
    })
    .finally(async ({ embInput, msg, task }) => {
      const textEmb = await measureMs("embed_text", () =>
        embedBatcher.execute(embInput),
      );
      const keyPfx = task.outputKey;
      await Promise.all([
        upsertKvRow.execute({
          k: `${keyPfx}/dense`,
          v: rawBytes(new Float32Array(textEmb.dense)),
        }),
        upsertKvRow.execute({
          k: `${keyPfx}/sparse`,
          v: encode(textEmb.sparse),
        }),
      ]);
      await QUEUE_EMBED.deleteMessages([msg]);
    });

  // Don't idle with an expensive GPU.
  lg.info("no more tasks, stopping");
  process.exit(0);
})();
