import { encode } from "@msgpack/msgpack";
import { crawlHn } from "@wilsonzlin/crawler-toolkit";
import Batcher from "@xtjs/lib/js/Batcher";
import asyncTimeout from "@xtjs/lib/js/asyncTimeout";
import { load } from "cheerio";
import { Duration } from "luxon";
import {
  QUEUE_HN_CRAWL,
  lg,
  upsertDbRow,
  vQueueHnCrawlTask,
} from "../common/res";
import { createEmbedWorker } from "./worker_embed";

const rawBytes = (t: ArrayBufferView) =>
  Buffer.from(t.buffer, t.byteOffset, t.byteLength);

(async () => {
  const embedWorker = await createEmbedWorker();
  const embedBatcher = new Batcher((texts: string[]) =>
    embedWorker.embed(texts),
  );

  while (true) {
    const [t] = await QUEUE_HN_CRAWL.pollMessages(
      1,
      Duration.fromObject({ minutes: 15 }).as("millisecond"),
    );
    if (!t) {
      await asyncTimeout(3000);
      continue;
    }
    const { startId, endId } = vQueueHnCrawlTask.parseRoot(t.contents);

    const promises = Array<Promise<unknown>>();
    for await (const { comment: c, post: p } of crawlHn({
      concurrency: 80,
      logger: lg,
      nextId: startId,
      maxId: endId,
    })) {
      promises.push(
        (async () => {
          if (c) {
            const text = load(c.textHtml).text();
            const textEmb = await embedBatcher.execute(text);
            await upsertDbRow({
              table: "hn_comment",
              row: {
                id: c.id,
                deleted: c.deleted,
                dead: c.dead,
                score: c.score,
                text,
                author: c.author,
                ts: c.timestamp,
                post: c.post,
                emb_dense_text: rawBytes(new Float32Array(textEmb.dense)),
                emb_sparse_text: encode(textEmb.sparse),
              },
              keyColumns: ["id"],
            });
          }
          if (p) {
            const title = load(p.titleHtml).text();
            const text = load(p.textHtml).text();
            const [titleEmb, textEmb] = await Promise.all(
              [title, text].map((t) => embedBatcher.execute(t)),
            );
            await upsertDbRow({
              table: "hn_post",
              row: {
                id: p.id,
                deleted: p.deleted,
                dead: p.dead,
                score: p.score,
                title,
                text,
                author: p.author,
                ts: p.timestamp,
                parent: p.parent,
                url: p.url,
                emb_dense_title: rawBytes(new Float32Array(titleEmb.dense)),
                emb_sparse_title: encode(titleEmb.sparse),
                emb_dense_text: rawBytes(new Float32Array(textEmb.dense)),
                emb_sparse_text: encode(textEmb.sparse),
              },
              keyColumns: ["id"],
            });
          }
        })(),
      );
    }
    await Promise.all(promises);
  }
})();
