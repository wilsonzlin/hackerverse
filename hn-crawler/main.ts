import { encode } from "@msgpack/msgpack";
import { fetchHnItem } from "@wilsonzlin/crawler-toolkit";
import { setUpUncaughtExceptionHandler } from "@wzlin/service-toolkit";
import Batcher from "@xtjs/lib/js/Batcher";
import PromiseQueue from "@xtjs/lib/js/PromiseQueue";
import map from "@xtjs/lib/js/map";
import mapNonEmpty from "@xtjs/lib/js/mapNonEmpty";
import numberGenerator from "@xtjs/lib/js/numberGenerator";
import { load } from "cheerio";
import { StatsD } from "hot-shots";
import { Duration } from "luxon";
import {
  QUEUE_HN_CRAWL,
  lg,
  upsertDbRowBatch,
  vQueueHnCrawlTask,
} from "../common/res";
import { createEmbedWorker } from "./worker_embed";

setUpUncaughtExceptionHandler();

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
  type PostRow = {
    id: number;
    deleted: boolean;
    dead: boolean;
    score: number;
    title: string;
    text: string;
    author: string | undefined;
    ts: Date | undefined;
    parent: number | undefined;
    url: string | undefined;
    emb_dense_title: Buffer | undefined;
    emb_sparse_title: Uint8Array | undefined;
    emb_dense_text: Buffer | undefined;
    emb_sparse_text: Uint8Array | undefined;
  };
  type CommentRow = {
    id: number;
    deleted: boolean;
    dead: boolean;
    score: number;
    text: string;
    author: string | undefined;
    ts: Date | undefined;
    post: number | undefined;
    emb_dense_text: Buffer | undefined;
    emb_sparse_text: Uint8Array | undefined;
  };
  const upsertPostBatcher = new Batcher(async (rows: PostRow[]) => {
    await upsertDbRowBatch({
      table: "hn_post",
      rows,
      keyColumns: ["id"],
    });
    return Array(rows.length).fill(0);
  });
  const upsertCommentBatcher = new Batcher(async (rows: CommentRow[]) => {
    await upsertDbRowBatch({
      table: "hn_comment",
      rows,
      keyColumns: ["id"],
    });
    return Array(rows.length).fill(0);
  });

  while (true) {
    const [t] = await QUEUE_HN_CRAWL.pollMessages(
      1,
      Duration.fromObject({ minutes: 15 }).as("seconds"),
    );
    if (!t) {
      lg.info("no more tasks, stopping");
      // Don't idle with an expensive GPU.
      process.exit(0);
    }
    const { startId, endId } = vQueueHnCrawlTask.parseRoot(t.contents);
    const q = new PromiseQueue(512);

    await Promise.all(
      map(numberGenerator(startId, endId + 1), (id) =>
        q.add(async () => {
          const { comment: c, post: p } = await measureMs("fetch_item", () =>
            fetchHnItem(id, {
              onRetry: () => statsd.increment("item_fetch_error"),
            }),
          );
          if (c) {
            const text = load(c.textHtml).text().trim();
            const textEmb = await mapNonEmpty(text, (t) =>
              measureMs("embed_comment_text", () => embedBatcher.execute(t)),
            );
            await measureMs("upsert_comment", () =>
              upsertCommentBatcher.execute({
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
              upsertPostBatcher.execute({
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
              }),
            );
          }
        }),
      ),
    );

    await QUEUE_HN_CRAWL.deleteMessages([t]);
  }
})();
