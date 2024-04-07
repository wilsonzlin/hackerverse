import { fetchHnMaxId } from "@wilsonzlin/crawler-toolkit";
import { VInteger, VStruct, Valid } from "@wzlin/valid";
import {
  QUEUE_HN_CRAWL,
  db,
  lg,
  upsertDbRowBatch,
  vQueueHnCrawlTask,
} from "../common/res";

(async () => {
  const nextId = await db
    .query(
      "select v from cfg where k = 'hn_crawler_next_id'",
      [],
      new VStruct({
        v: new VInteger(0),
      }),
    )
    .then((r) => r.at(0)?.v ?? 0);

  const maxId = await fetchHnMaxId();
  lg.info({ nextId, maxId }, "found range");

  const BATCH_SIZE = 1024;
  const msgs = Array<Valid<typeof vQueueHnCrawlTask>>();
  for (let start = nextId; start <= maxId; start += BATCH_SIZE) {
    msgs.push({
      startId: start,
      endId: Math.min(maxId, start + BATCH_SIZE - 1),
    });
  }
  await QUEUE_HN_CRAWL.pushMessages(
    msgs.map((contents) => ({
      contents,
      visibilityTimeoutSecs: 0,
    })),
  );
  lg.info({ messages: msgs.length }, "enqueued tasks");
  // Need to upsert as the first run won't have any row to update.
  await upsertDbRowBatch({
    table: "cfg",
    rows: [
      {
        k: "hn_crawler_next_id",
        v: `${maxId + 1}`,
      },
    ],
    keyColumns: ["k"],
  });
  lg.info("all done!");
})();
