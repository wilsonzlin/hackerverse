import { encode } from "@msgpack/msgpack";
import { fetchHnItem, fetchHnMaxId } from "@wzlin/crawler-toolkit-hn";
import {
  elementToText,
  normaliseUrl,
  parseHtml,
} from "@wzlin/crawler-toolkit-web";
import { VInteger, VString, VStruct, VUtf8Bytes, Valid } from "@wzlin/valid";
import Batcher from "@xtjs/lib/js/Batcher";
import Pipeline from "@xtjs/lib/js/Pipeline";
import WorkerPool from "@xtjs/lib/js/WorkerPool";
import assertExists from "@xtjs/lib/js/assertExists";
import decodeUtf8 from "@xtjs/lib/js/decodeUtf8";
import defined from "@xtjs/lib/js/defined";
import encodeUtf8 from "@xtjs/lib/js/encodeUtf8";
import filterValue from "@xtjs/lib/js/filterValue";
import mapExists from "@xtjs/lib/js/mapExists";
import { load } from "cheerio";
import { StatsD } from "hot-shots";
import { decode } from "html-entities";
import { Duration } from "luxon";
import { cpus } from "node:os";
import { Agent, fetch, setGlobalDispatcher } from "undici";
import {
  QUEUE_EMBED,
  db,
  getCfg,
  lg,
  measureMs,
  setCfg,
  upsertDbRowBatch,
  upsertKvRow,
  vQueueEmbedTask,
} from "../common/res";

// https://github.com/nodejs/undici/issues/1531#issuecomment-1178869993
setGlobalDispatcher(
  new Agent({
    connect: {
      // The default of 5 seconds causes way too many UND_ERR_CONNECT_TIMEOUT.
      timeout: 1000 * 60,
    },
  }),
);

const DUR_MS_48H = Duration.fromObject({ hours: 48 }).as("milliseconds");

const statsd = new StatsD({
  host: "telegraf",
  port: 8125,
  prefix: "enqueuer.",
});

const handleContentFetchError = (id: number, url: string, err: any) => {
  const code =
    err.cause?.code ||
    err.code ||
    err.cause?.constructor?.name ||
    err.constructor?.name;
  lg.warn(
    {
      id,
      url,
      code,
      message: err.message,
      data: { ...err },
      causeMessage: err.cause?.message,
      causeData: mapExists(err.cause, (c) => ({ ...c })),
    },
    "failed to fetch content",
  );
  statsd.increment("content_fetch_error", {
    error: code,
  });
  // Ignore errors.
  return undefined;
};

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

const fetchItem = (id: number) =>
  measureMs(statsd, "item_fetch_ms", () =>
    fetchHnItem(id, {
      onRetry: () => statsd.increment("item_fetch_error"),
    }),
  );

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
  url: string | undefined;
  page_fetched: boolean;
};

const upsertPostRow = new Batcher(async (rows: PostRow[]) => {
  await upsertDbRowBatch({
    table: "post",
    rows,
    keyColumns: ["id"],
  });
  return Array(rows.length).fill(true);
});

const CONTENT_CRAWL_CONCURRENCY = 512;
const WORKER_COUNT = cpus().length;
new WorkerPool(__filename, WORKER_COUNT)
  .workerTask("parseHtml", async (html: string) => {
    const $ = load(html);
    const p = parseHtml($);
    return {
      description: p.description,
      imageUrl: p.imageUrl,
      lang: p.ogLocale || p.htmlLang,
      snippet: p.snippet,
      text: p.mainArticleText || p.pageText,
      timestamp: p.timestamp,
      timestampModified: p.timestampModified,
      title: p.title,
    };
  })
  .leader(async (pool) => {
    let nextId = (await getCfg("enqueuer_next_id", new VInteger(0))) ?? 0;
    const maxId = await fetchHnMaxId();
    lg.info({ nextId, maxId }, "found range");
    let earlyStop = false;
    let nextIdToCommit = nextId;
    const idsPendingCommit = new Set<number>();

    await Pipeline.from(async () => {
      if (earlyStop || nextId >= maxId) {
        lg.info({ nextId, earlyStop }, "stopping");
        return undefined;
      }
      return nextId++;
    })
      .then({
        inputCost: () => 1,
        concurrency: 384,
        costBuffer: 1,
        handler: async (id) => {
          const item = await fetchItem(id);
          const ts = item?.time;
          // Posts may change a lot within 48 hours (score, top comments, moderator changes to parent/title/URL).
          if (ts && Date.now() - ts.getTime() < DUR_MS_48H) {
            earlyStop = true;
            return;
          }
          // Even if this item has neither a comment nor a post, we must still pass it through the pipeline in order to commit the ID.
          return {
            id,
            item,
          };
        },
      })
      .then({
        inputCost: (i) => (i.item?.url ? 1 : 0),
        concurrency: CONTENT_CRAWL_CONCURRENCY,
        costBuffer: 1024,
        handler: async ({ id, item }) => {
          const abortController = new AbortController();
          const timeout = setTimeout(() => abortController.abort(), 1000 * 60);
          const url = mapExists(
            filterValue(
              item?.url,
              // Only fetch if the post is not dead or deleted.
              // Sometimes the `url` value is an empty string, likely because the post has been deleted.
              (url) => url && !item?.dead && !item?.deleted,
            ),
            (url) => {
              // normaliseUrl() will remove any hash component, and normalise percent escapes and query params.
              const norm = normaliseUrl(url, true);
              if (!norm) {
                lg.warn({ id, url }, "invalid URL");
                return;
              }
              return norm;
            },
          );
          return {
            id,
            item,
            timeout,
            url,
            fetch: await mapExists(url, (url) =>
              measureMs(statsd, "content_fetch_ms", () =>
                fetch(url, {
                  headers: {
                    accept: "text/html,application/xhtml+xml",
                    "accept-language": "en-US,en;q=0.5",
                  },
                  signal: abortController.signal,
                }).catch((err) => handleContentFetchError(id, url, err)),
              ),
            ),
          };
        },
      })
      .then({
        inputCost: (i) => (i.fetch ? 1 : 0),
        concurrency: CONTENT_CRAWL_CONCURRENCY,
        costBuffer: 1,
        handler: async ({ id, item, fetch, timeout, url }) => {
          clearTimeout(timeout);
          return {
            id,
            item,
            url,
            html: await mapExists(fetch, async (f) => {
              if (!f.ok) {
                lg.warn(
                  { status: f.status, url },
                  "failed to fetch content with bad status",
                );
                statsd.increment("content_fetch_bad_status", {
                  status: `${f.status}`,
                });
                // Ignore.
                return undefined;
              }
              const ct = f.headers.get("content-type");
              if (ct && !ct.startsWith("text/html")) {
                lg.warn({ id, url }, "not text/html");
                return undefined;
              }
              const raw = await measureMs(
                statsd,
                "content_fetch_response_body_ms",
                () =>
                  f
                    .arrayBuffer()
                    .catch((err) => handleContentFetchError(id, url!, err)),
              );
              statsd.increment(
                "content_fetch_response_body_bytes",
                raw?.byteLength ?? 0,
              );
              return raw && decodeUtf8(new Uint8Array(raw));
            }),
          };
        },
      })
      .then({
        inputCost: (i) => (i.html ? 1 : 0),
        concurrency: WORKER_COUNT,
        costBuffer: 128,
        handler: async ({ html, id, item, url }) => {
          return {
            id,
            item,
            url,
            parsed: await mapExists(html, (html) =>
              measureMs(statsd, "content_parse", () =>
                pool.execute("parseHtml", html),
              ),
            ),
          };
        },
      })
      .then({
        inputCost: () => 1,
        concurrency: 384,
        costBuffer: 128,
        handler: async ({ id, item, parsed, url }) => {
          let embInput: string | undefined;
          let root: number | undefined;
          const [articleText, meta] =
            mapExists(parsed, ({ text, ...meta }) => [text, meta]) ?? [];
          // Skip generating embedding if item is dead or deleted. Low scores (including negative) are fine; they may just be controversial (spam are usually marked as dead).
          if (item && !item.dead && !item.deleted) {
            if (item.type === "story") {
              const title = parsePostTitle(item.title ?? "");
              const text = articleText || extractText(item.text ?? "");
              embInput = [
                title,
                url?.replace(/^https?:\/\//, ""), // Doesn't exist for text posts.
                meta?.title, // Doesn't exist if fetch failed.
                meta?.description, // Doesn't exist if fetch failed.
                text,
              ]
                .filter(defined)
                .join("\n\n");
              let addedSep = false;
              const topComments = item.kids?.slice() ?? [];
              const MAX_LEN = 1024 * 64; // 64 KiB.
              while (topComments.length && embInput.length < MAX_LEN) {
                // Embelish with top-level top comments (item.children are ranked already). This is useful if the page isn't primarily text, could not be fetched, etc.
                const i = await fetchItem(topComments.shift()!);
                // We don't want to include negative comments as part of the post's text representation.
                if (
                  !i ||
                  i.type !== "comment" ||
                  i.dead ||
                  i.deleted ||
                  i.score! < 0
                ) {
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
            } else if (item.type === "comment") {
              const chain = ["# Reply", extractText(item.text ?? "")];
              // All comments have a parent.
              let parentId = assertExists(item.parent);
              while (true) {
                const p = await fetchItem(parentId);
                if (p?.type === "story") {
                  chain.unshift(
                    "# Post",
                    parsePostTitle(p.title ?? ""),
                    normaliseUrl(p.url ?? "") ?? "",
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
          statsd.increment("process_item");
          return {
            embInput,
            id,
            item,
            meta,
            parsed,
            root,
            url,
          };
        },
      })
      .finally(async ({ embInput, id, item, meta, parsed, root, url }) => {
        idsPendingCommit.add(id);
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
              author: await mapExists(item.by, (n) =>
                insertAndGetUserId.execute(n),
              ),
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
          await Promise.all([
            upsertPostRow.execute({
              id: item.id,
              deleted: item.deleted ?? false,
              dead: item.dead ?? false,
              score: item.score ?? 0,
              author: await mapExists(item.by, (n) =>
                insertAndGetUserId.execute(n),
              ),
              ts: item.time,
              url,
              page_fetched: item.url ? !!parsed : true,
            }),
            upsertKvRow.execute({
              k: `${keyPfx}/title`,
              v: encodeUtf8(item.title ?? ""),
            }),
            upsertKvRow.execute({
              k: `${keyPfx}/text`,
              v: encodeUtf8(item.text ?? ""),
            }),
            meta &&
              upsertKvRow.execute({
                k: `${keyPfx}/page_meta`,
                v: encode(meta),
              }),
          ]);
        }
        if (embInput) {
          await upsertKvRow.execute({
            k: `${keyPfx}/emb_input`,
            v: encodeUtf8(embInput),
          });
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
        while (idsPendingCommit.has(nextIdToCommit)) {
          idsPendingCommit.delete(nextIdToCommit);
          await setCfg("enqueuer_next_id", nextIdToCommit + 1);
          nextIdToCommit++;
          statsd.increment("commit_next_id");
        }
      });

    lg.info("all done!");
  })
  .go();
