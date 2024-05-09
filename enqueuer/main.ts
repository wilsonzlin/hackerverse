import { Item, fetchHnItem, fetchHnMaxId } from "@wzlin/crawler-toolkit-hn";
import {
  elementToText,
  normaliseUrl,
  normaliseUrlToParts,
} from "@wzlin/crawler-toolkit-web";
import { VInteger, VString, VStruct, VUtf8Bytes, Valid } from "@wzlin/valid";
import Batcher from "@xtjs/lib/Batcher";
import WorkerPool from "@xtjs/lib/WorkerPool";
import assertExists from "@xtjs/lib/assertExists";
import encodeUtf8 from "@xtjs/lib/encodeUtf8";
import mapExists from "@xtjs/lib/mapExists";
import mapNonEmpty from "@xtjs/lib/mapNonEmpty";
import { load } from "cheerio";
import { decode } from "html-entities";
import { Duration } from "luxon";
import { cpus } from "node:os";
import {
  QUEUE_CRAWL,
  QUEUE_EMBED,
  db,
  getCfg,
  lg,
  measureMs,
  setCfg,
  statsd,
  upsertDbRowBatch,
  upsertKvRow,
  vQueueCrawlTask,
  vQueueEmbedTask,
} from "../common/res";

const DUR_MS_48H = Duration.fromObject({ hours: 48 }).as("milliseconds");

const fetchItem = async (id: number) =>
  measureMs("item_fetch_ms", () =>
    fetchHnItem(id, {
      onRetry: () => statsd.increment("item_fetch_error"),
    }),
  );

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
    // Make sure to filter out empty comments, as otherwise the embedding basically becomes the same as the parent.
    const text = extractText(item.text ?? "").trim();
    if (text) {
      const chain = ["# Reply", text];
      // All comments have a parent.
      let parentId = assertExists(item.parent);
      while (true) {
        const p = await fetchItem(parentId);
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
  }
  // Skip fetching and/or generating embedding if post is dead or deleted. Low scores (including negative) are fine; they may just be controversial (spam are usually marked as dead).
  if (item?.type === "story" && !item.dead && !item.deleted) {
    // Sometimes the `url` value is an empty string, likely because the post has been deleted.
    // normaliseUrl() will remove any hash component, and normalise percent escapes and query params.
    url = mapNonEmpty(item.url ?? "", (u) => normaliseUrlToParts(u, true));
    const urlNoProto = url && `${url.domain}${url.pathname}${url.query}`;
    // Database limitation.
    if (urlNoProto && encodeUtf8(urlNoProto).byteLength > 3000) {
      url = undefined;
    }
    const title = parsePostTitle(item.title ?? "");
    // Don't put placeholders if there's no URL, as otherwise the embedder will have to do a lookup for every single post to see if it has a URL.
    embInput = (
      url
        ? [
            title,
            urlNoProto,
            "<<<REPLACE_WITH_PAGE_TITLE>>>",
            "<<<REPLACE_WITH_PAGE_DESCRIPTION>>>",
            "<<<REPLACE_WITH_PAGE_TEXT>>>",
          ]
        : [title, extractText(item.text ?? "")]
    )
      .filter((l) => l)
      .join("\n\n");
    let addedSep = false;
    const topComments = item.kids?.slice() ?? [];
    const MAX_LEN = 1024 * 64; // 64 KiB.
    while (topComments.length && embInput.length < MAX_LEN) {
      // Embellish with top-level top comments (`item.kids` are ranked already). This is useful if the page isn't primarily text, could not be fetched, etc.
      const i = await fetchItem(topComments.shift()!);
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
  embInput = embInput?.trim();

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
  .workerTask("process", async (id: number) => {
    const item = await fetchItem(id);
    if (!item) {
      return;
    }
    // Posts may change a lot within 48 hours (score, top comments, moderator changes to parent/title/URL).
    if (item.time && Date.now() - item.time.getTime() < DUR_MS_48H) {
      return;
    }
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

    const CONCURRENCY = cpus().length * 16;
    await Promise.all(
      Array.from({ length: CONCURRENCY }, async () => {
        while (nextId <= maxId) {
          const id = nextId++;
          await pool.execute("process", id);
          idsPendingCommit.add(id);
          maybeFlushId();
        }
      }),
    );
    lg.info("all done!");
  })
  .go();
