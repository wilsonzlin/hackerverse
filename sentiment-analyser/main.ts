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
  getKvRow,
  upsertKvRow,
  vQueueAnalyseSentimentTask,
} from "../common/res";
import { createModel } from "./model";

setUpUncaughtExceptionHandler();

// Do not use WorkerPool where each worker is running one of these poll-parse-infer-upsert loops, as we want to reuse and multiplex DB queries/requests/connections.
(async () => {
  // Have two loops so that while one is waiting for the model, the other can fetch from the DB and have some more inputs ready as soon as the GPU is done, to minimise GPU idle time.
  const model = await createModel();
  const modelBatcher = new Batcher((inputs: string[]) => model.execute(inputs));
  const loop  = async () => {
    while (true) {
      const msgs = await QUEUE_ANALYSE_SENTIMENT.pollMessages(
        1024,
        Duration.fromObject({ minutes: 10 }).as("seconds"),
      );
      if (!msgs.length) {
        break;
      }
      await Promise.all(msgs.map(async (msg) => {
        const task = vQueueAnalyseSentimentTask.parseRoot(msg.contents);
        const inputHtml = decodeUtf8(
          assertExists(await getKvRow.execute(`comment/${task.comment}/text`)),
        );
        // Use elementToText over .text() to handle block elements properly.
        // The model was trained with links replaced with "http": https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment-latest#full-classification-example.
        const input = elementToText(load(inputHtml)("body")[0], {
          emitLinkHrefs: false,
        })
          .replace(/^https?:\/\/\S+/g, "http")
          .trim();
        if (input) {
          const output = await modelBatcher.execute(input);
          await upsertKvRow.execute({
            k: `comment/${task.comment}/sentiment`,
            v: encode(output),
          });
        };
      }));
      await QUEUE_ANALYSE_SENTIMENT.deleteMessages(msgs);
    }
  };
  await Promise.all([loop(), loop()]);
})();
