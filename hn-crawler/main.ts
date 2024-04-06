import { encode } from "@msgpack/msgpack";
import { crawlHn } from "@wilsonzlin/crawler-toolkit";
import Batcher from "@xtjs/lib/js/Batcher";
import asyncTimeout from "@xtjs/lib/js/asyncTimeout";
import mapNonEmpty from "@xtjs/lib/js/mapNonEmpty";
import { load } from "cheerio";
import { StatsD } from "hot-shots";
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

const statsd = new StatsD({
  host: "127.0.0.1",
  port: 8125,
  prefix: "hn_crawler.",
});

const measureMs = async <T>(
  metric: string,
  fn: () => Promise<T>,
  tags?: Record<string, string>,
) => {
  const started = Date.now();
  try {
    return await fn();
  } finally {
    statsd.timing(metric, Date.now() - started, tags);
  }
};

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
            const text = load(c.textHtml).text().trim();
            const textEmb = await mapNonEmpty(text, (t) =>
              measureMs("embed_comment_text", () => embedBatcher.execute(t)),
            );
            await measureMs("upsert_comment", () =>
              upsertDbRow({
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
                  emb_dense_text:
                    textEmb && rawBytes(new Float32Array(textEmb.dense)),
                  emb_sparse_text: textEmb && encode(textEmb.sparse),
                },
                keyColumns: ["id"],
              }),
            );
          }
          if (p) {
            const title = load(p.titleHtml).text().trim();
            const text = load(p.textHtml).text().trim();
            const [titleEmb, textEmb] = await Promise.all([
              mapNonEmpty(title, (t) =>
                measureMs("embed_post_title", () => embedBatcher.execute(t)),
              ),
              mapNonEmpty(text, (t) =>
                measureMs("embed_post_text", () => embedBatcher.execute(t)),
              ),
            ]);
            await measureMs("upsert_post", () =>
              upsertDbRow({
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
                  emb_dense_title:
                    titleEmb && rawBytes(new Float32Array(titleEmb.dense)),
                  emb_sparse_title: titleEmb && encode(titleEmb.sparse),
                  emb_dense_text:
                    textEmb && rawBytes(new Float32Array(textEmb.dense)),
                  emb_sparse_text: textEmb && encode(textEmb.sparse),
                },
                keyColumns: ["id"],
              }),
            );
          }
        })(),
      );
    }
    await Promise.all(promises);
  }
})();
