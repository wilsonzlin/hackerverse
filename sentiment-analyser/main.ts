import { encode } from "@msgpack/msgpack";
import { elementToText } from "@wzlin/crawler-toolkit-web";
import { setUpUncaughtExceptionHandler } from "@wzlin/service-toolkit";
import Batcher from "@xtjs/lib/Batcher";
import WorkerPool from "@xtjs/lib/WorkerPool";
import assertExists from "@xtjs/lib/assertExists";
import decodeUtf8 from "@xtjs/lib/decodeUtf8";
import { load } from "cheerio";
import { Duration } from "luxon";
import {
  QUEUE_ANALYSE_SENTIMENT,
  QUEUE_EMBED,
  getKvRow,
  upsertKvRow,
  vQueueAnalyseSentimentTask,
} from "../common/res";
import { createModel } from "./model";

setUpUncaughtExceptionHandler();

new WorkerPool(__filename, 256)
  .leaderState(async () => {
    const model = await createModel();
    const modelBatcher = new Batcher((texts: string[]) => model.execute(texts));
    return {
      modelBatcher,
    };
  })
  .leaderTask("model", async (input: string, ctx) => {
    return await ctx.state.modelBatcher.execute(input);
  })
  .worker(async (pool) => {
    while (true) {
      const [msg] = await QUEUE_EMBED.pollMessages(
        1,
        Duration.fromObject({ minutes: 10 }).as("seconds"),
      );
      if (!msg) {
        break;
      }
      const task = vQueueAnalyseSentimentTask.parseRoot(msg.contents);
      const inputHtml = decodeUtf8(
        assertExists(await getKvRow.execute(`comment/${task.comment}/text`)),
      );
      // Use elementToText over .text() to handle block elements properly.
      // The model was trained with links removed.
      const input = elementToText(load(inputHtml)("body")[0], {
        emitLinkHrefs: false,
      }).replaceAll(/^https?:\/\/\S+/, "http");
      const output = await pool.execute("model", input);
      await upsertKvRow.execute({
        k: `comment/${task.comment}/sentiment`,
        v: encode(output),
      });
      await QUEUE_ANALYSE_SENTIMENT.deleteMessages([msg]);
    }
  })
  .go();
