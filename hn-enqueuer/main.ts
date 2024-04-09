import { fetchHnMaxId } from "@wilsonzlin/crawler-toolkit";
import { VInteger, Valid } from "@wzlin/valid";
import {
  QUEUE_HN_CRAWL,
  getCfg,
  lg,
  setCfg,
  vQueueHnCrawlTask,
} from "../common/res";

(async () => {
  const nextId = (await getCfg("enqueuer_next_id", new VInteger(0))) ?? 0;

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
  await setCfg("enqueuer_next_id", maxId + 1);
  lg.info("all done!");
})();
