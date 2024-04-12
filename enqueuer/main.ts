import {
  Item,
  crawlHn,
  fetchHnItem,
  fetchHnMaxId,
} from "@wzlin/crawler-toolkit-hn";
import {
  elementToText,
  normaliseUrl,
  normaliseUrlToParts,
} from "@wzlin/crawler-toolkit-web";
import { VInteger, VString, VStruct, VUtf8Bytes, Valid } from "@wzlin/valid";
import Batcher from "@xtjs/lib/js/Batcher";
import WorkerPool from "@xtjs/lib/js/WorkerPool";
import assertExists from "@xtjs/lib/js/assertExists";
import defined from "@xtjs/lib/js/defined";
import encodeUtf8 from "@xtjs/lib/js/encodeUtf8";
import mapExists from "@xtjs/lib/js/mapExists";
import mapNonEmpty from "@xtjs/lib/js/mapNonEmpty";
import waitGroup from "@xtjs/lib/js/waitGroup";
import { load } from "cheerio";
import { StatsD } from "hot-shots";
import { decode } from "html-entities";
import { Duration } from "luxon";
import { cpus } from "node:os";
import {
  QUEUE_CRAWL,
  QUEUE_EMBED,
  db,
  getCfg,
  lg,
  setCfg,
  upsertDbRowBatch,
  upsertKvRow,
  vQueueCrawlTask,
  vQueueEmbedTask,
} from "../common/res";

const DUR_MS_48H = Duration.fromObject({ hours: 48 }).as("milliseconds");

const statsd = new StatsD({
  host: "telegraf",
  port: 8125,
  prefix: "enqueuer.",
});

const parsePostTitle = (html: string) =>
  decode(html) // Titles shouldn't contain any elements, so merely decoding should be enough.
    .trim()
    // Prevent embedding and other models from clustering/grouping/etc. by these common prefixes.
    .replace(/^(Ask|Show) HN:?/i, "")
    .trim();

// Use elementToText over .text() to handle block elements properly.
const extractText = (html: string) =>
  elementToText(load(html)("body")[0], {
    emitLinkHrefs: true,
  });

const insertAndGetUserId = new Batcher(async (usernames: string[]) => {
  await db.batch(
    "insert into usr (username) values (?) on duplicate key update id = id",
    usernames.map((name) => [name]),
  );
  const rows = await db.query(
    `select id, username from usr where username in (${usernames.map(() => "?").join(",")})`,
    [...usernames],
    new VStruct({
      id: new VInteger(1),
      username: new VUtf8Bytes(new VString()),
    }),
  );
  const map = Object.fromEntries(rows.map((r) => [r.username, r.id]));
  return usernames.map((n) => map[n]);
});

const insertAndGetUrlId = new Batcher(
  async (
    urls: Array<{
      proto: string;
      url: string;
    }>,
  ) => {
    const changes = await db.batch(
      "insert into url (url, proto) values (?, ?) on duplicate key update id = id",
      urls.map((u) => [encodeUtf8(u.url), u.proto]),
    );
    const rows = await db.query(
      `select id, url from url where url in (${urls.map(() => "?").join(",")})`,
      [...urls.map((u) => encodeUtf8(u.url))],
      new VStruct({
        id: new VInteger(1),
        url: new VUtf8Bytes(new VString()),
      }),
    );
    const map = Object.fromEntries(rows.map((r) => [r.url, r.id]));
    return urls.map((u, i) => ({
      id: map[u.url],
      didInsert: changes[i].affectedRows > 0,
    }));
  },
);

type CommentRow = {
  id: number;
  deleted: boolean;
  dead: boolean;
  score: number;
  parent: number;
  author: number | undefined;
  ts: Date | undefined;
  post: number | undefined;
};

const upsertCommentRow = new Batcher(async (rows: CommentRow[]) => {
  await upsertDbRowBatch({
    table: "comment",
    rows,
    keyColumns: ["id"],
  });
  return Array(rows.length).fill(true);
});

type PostRow = {
  id: number;
  deleted: boolean;
  dead: boolean;
  score: number;
  author: number | undefined;
  ts: Date | undefined;
  url: number | undefined;
};

const upsertPostRow = new Batcher(async (rows: PostRow[]) => {
  await upsertDbRowBatch({
    table: "post",
    rows,
    keyColumns: ["id"],
  });
  return Array(rows.length).fill(true);
});

const processItem = async (item: Item) => {
  let embInput: string | undefined;
  let root: number | undefined;
  let url: ReturnType<typeof normaliseUrlToParts> | undefined;
  if (item?.type === "comment") {
    const chain = ["# Reply", extractText(item.text ?? "")];
    // All comments have a parent.
    let parentId = assertExists(item.parent);
    while (true) {
      const p = await fetchHnItem(parentId);
      if (p?.type === "story") {
        chain.unshift(
          "# Post",
          parsePostTitle(p.title ?? ""),
          normaliseUrl(p.url ?? "")?.replace(/^https?:\/\//, "") ?? "",
        );
        root = p.id;
        break;
      } else if (p?.type === "comment") {
        chain.unshift("# Comment", extractText(p.text ?? ""));
        parentId = assertExists(p.parent);
      } else {
        // We are likely in a dead post or comment subtree, but let's still keep the chain.
        break;
      }
    }
    embInput = chain.join("\n\n");
  }
  // Skip fetching and/or generating embedding if post is dead or deleted. Low scores (including negative) are fine; they may just be controversial (spam are usually marked as dead).
  if (item?.type === "story" && !item.dead && !item.deleted) {
    // Sometimes the `url` value is an empty string, likely because the post has been deleted.
    // normaliseUrl() will remove any hash component, and normalise percent escapes and query params.
    url = mapNonEmpty(item.url ?? "", (u) => normaliseUrlToParts(u, true));
    const title = parsePostTitle(item.title ?? "");
    embInput = (
      url
        ? [
            title,
            `${url.domain}${url.pathname}${url.query}`,
            "<<<REPLACE_WITH_PAGE_TITLE>>>",
            "<<<REPLACE_WITH_PAGE_DESCRIPTION>>>",
            "<<<REPLACE_WITH_PAGE_TEXT>>>",
          ]
        : [title, extractText(item.text ?? "")]
    )
      .filter(defined)
      .join("\n\n");
    let addedSep = false;
    const topComments = item.kids?.slice() ?? [];
    const MAX_LEN = 1024 * 64; // 64 KiB.
    while (topComments.length && embInput.length < MAX_LEN) {
      // Embelish with top-level top comments (item.children are ranked already). This is useful if the page isn't primarily text, could not be fetched, etc.
      const i = await fetchHnItem(topComments.shift()!);
      // We don't want to include negative comments as part of the post's text representation.
      if (!i || i.type !== "comment" || i.dead || i.deleted || i.score! < 0) {
        continue;
      }
      const text = extractText(i.text ?? "");
      if (!addedSep) {
        // Use Markdown syntax, colon, and ASCII border to really emphasise separation.
        embInput += "\n\n# Comments:\n==========";
        addedSep = true;
      } else {
        embInput += "\n\n----------";
      }
      embInput += "\n\n" + text;
    }
    embInput = embInput.slice(0, MAX_LEN);
  }

  let keyPfx: string | undefined;
  if (item?.type === "comment") {
    keyPfx = `comment/${item.id}`;
    await Promise.all([
      upsertCommentRow.execute({
        id: item.id,
        deleted: item.deleted ?? false,
        dead: item.dead ?? false,
        score: item.score ?? 0,
        parent: assertExists(item.parent),
        author: await mapExists(item.by, (n) => insertAndGetUserId.execute(n)),
        ts: item.time,
        post: root,
      }),
      upsertKvRow.execute({
        k: `${keyPfx}/text`,
        v: encodeUtf8(item.text ?? ""),
      }),
    ]);
  } else if (item?.type === "story") {
    keyPfx = `post/${item.id}`;
    const urlObj = mapExists(url, (u) => ({
      proto: u.protocol,
      url: `${u.domain}${u.pathname}${u.query}`,
    }));
    const urlRow = await mapExists(urlObj, (u) => insertAndGetUrlId.execute(u));
    if (urlRow?.didInsert) {
      const msg: Valid<typeof vQueueCrawlTask> = {
        id: urlRow.id,
        proto: urlObj!.proto,
        url: urlObj!.url,
      };
      await QUEUE_CRAWL.pushMessages([
        { contents: msg, visibilityTimeoutSecs: 0 },
      ]);
    }
    await Promise.all([
      upsertPostRow.execute({
        id: item.id,
        deleted: item.deleted ?? false,
        dead: item.dead ?? false,
        score: item.score ?? 0,
        author: await mapExists(item.by, (n) => insertAndGetUserId.execute(n)),
        ts: item.time,
        url: urlRow?.id,
      }),
      upsertKvRow.execute({
        k: `${keyPfx}/title`,
        v: encodeUtf8(item.title ?? ""),
      }),
      upsertKvRow.execute({
        k: `${keyPfx}/text`,
        v: encodeUtf8(item.text ?? ""),
      }),
    ]);
  }
  if (embInput) {
    await upsertKvRow.execute({
      k: `${keyPfx}/emb_input`,
      v: encodeUtf8(embInput),
    });
    // For convenience, we enqueue this now so we don't need another service/loop to do so, however embedding must not start until URLs have been crawled.
    const msg: Valid<typeof vQueueEmbedTask> = {
      inputKey: `${keyPfx}/emb_input`,
      outputKey: `${keyPfx}/emb`,
    };
    await QUEUE_EMBED.pushMessages([
      {
        contents: msg,
        visibilityTimeoutSecs: 0,
      },
    ]);
  }
};

new WorkerPool(__filename, cpus().length)
  .workerTask("process", async (item: Item) => {
    await processItem(item);
  })
  .leader(async (pool) => {
    let nextId = (await getCfg("enqueuer_next_id", new VInteger(0))) ?? 0;
    const maxId = await fetchHnMaxId();
    lg.info({ nextId, maxId }, "found range");
    let nextIdToCommit = nextId;
    const idsPendingCommit = new Set<number>();

    let flushing = false;
    const maybeFlushId = async () => {
      if (flushing) {
        return;
      }
      flushing = true;
      let didChange = false;
      while (idsPendingCommit.has(nextIdToCommit)) {
        idsPendingCommit.delete(nextIdToCommit);
        nextIdToCommit++;
        didChange = true;
      }
      if (didChange) {
        await setCfg("enqueuer_next_id", nextIdToCommit);
        statsd.increment("commit_next_id");
      }
      flushing = false;
    };

    const wg = waitGroup();
    for await (const { id, item } of crawlHn({
      concurrency: 512,
      nextId,
      // Posts may change a lot within 48 hours (score, top comments, moderator changes to parent/title/URL).
      stopOnItemWithinDurationMs: DUR_MS_48H,
      onItemFetchRetry: () => statsd.increment("item_fetch_error"),
    })) {
      wg.add(1);
      pool.execute("process", item).then(() => {
        idsPendingCommit.add(id);
        maybeFlushId();
        wg.done();
      });
    }
    await wg;
    lg.info("all done!");
  })
  .go();
