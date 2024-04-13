import { encode } from "@msgpack/msgpack";
import { parseHtml } from "@wzlin/crawler-toolkit-web";
import { setUpUncaughtExceptionHandler } from "@wzlin/service-toolkit";
import decodeUtf8 from "@xtjs/lib/js/decodeUtf8";
import encodeUtf8 from "@xtjs/lib/js/encodeUtf8";
import withoutUndefined from "@xtjs/lib/js/withoutUndefined";
import { load } from "cheerio";
import { Duration } from "luxon";
import { randomInt } from "node:crypto";
import { Agent, fetch, setGlobalDispatcher } from "undici";
import {
  QUEUE_CRAWL,
  db,
  lg,
  measureMs,
  statsd,
  upsertKvRow,
  vQueueCrawlTask,
} from "../common/res";

setUpUncaughtExceptionHandler();

// https://github.com/nodejs/undici/issues/1531#issuecomment-1178869993
setGlobalDispatcher(
  new Agent({
    connect: {
      // The default of 5 seconds causes way too many UND_ERR_CONNECT_TIMEOUT.
      timeout: 1000 * 60,
    },
  }),
);

class BadStatusError extends Error {
  readonly code: string;

  constructor(readonly status: number) {
    super(`Failed to fetch with bad status: ${status}`);
    this.code = `BadStatus:${status}`;
  }
}

class BadContentTypeError extends Error {
  readonly code: string;

  constructor(contentType: string) {
    super(`Fetch returned non-HTML content type: ${contentType}`);
    this.code = `BadContentType:${contentType}`;
  }
}

// Designed to run on 1 CPU core and 4 GB RAM.
const CONTENT_CRAWL_CONCURRENCY = 256;
(async () => {
  await Promise.all(
    Array.from({ length: CONTENT_CRAWL_CONCURRENCY }, async () => {
      while (true) {
        const [t] = await QUEUE_CRAWL.pollMessages(
          1,
          Duration.fromObject({ minutes: 20 }).as("seconds"),
        );
        if (!t) {
          return;
        }
        const { id, proto, url } = vQueueCrawlTask.parseRoot(t.contents);

        const fetchStarted = new Date();
        let fetchEnded: Date | undefined;
        let fetchErr: any;
        const abortController = new AbortController();
        const timeout = setTimeout(() => abortController.abort(), 1000 * 60);
        let raw: ArrayBuffer | undefined;
        try {
          const f = await fetch(`${proto}//${url}`, {
            headers: withoutUndefined({
              accept: "text/html,application/xhtml+xml",
              "accept-language": "en-US,en;q=0.5",
              "user-agent": process.env["USER_AGENT"],
            }),
            signal: abortController.signal,
          }).finally(() => (fetchEnded = new Date()));
          if (!f.ok) {
            throw new BadStatusError(f.status);
          }
          const ct = f.headers.get("content-type");
          if (ct && !ct.startsWith("text/html")) {
            throw new BadContentTypeError(ct);
          }
          raw = await measureMs("fetch_response_ms", () => f.arrayBuffer());
          statsd.increment("fetch_bytes", raw?.byteLength);
        } catch (err) {
          // If `fetchErr` is `20`, it's due to the AbortSignal. `DOMException.ABORT_ERR === 20`.
          fetchErr =
            err.cause?.code ||
            err.code ||
            err.cause?.constructor?.name ||
            err.constructor?.name ||
            "Unknown";
          statsd.increment("fetch_error", {
            error: fetchErr,
          });
          if (
            fetchErr === "EAI_AGAIN" ||
            (err instanceof BadStatusError && err.status === 429)
          ) {
            // Don't update the DB row, we're not finished.
            // Don't instead create a new message, as that could cause exponential explosion if two workers polled the same message somehow.
            await QUEUE_CRAWL.updateMessage(t, randomInt(60 * 15));
            continue;
          }
        } finally {
          clearTimeout(timeout);
          if (fetchEnded) {
            statsd.timing(
              "fetch_ms",
              fetchEnded.getTime() - fetchStarted.getTime(),
              {
                result:
                  fetchErr == DOMException.ABORT_ERR
                    ? "timeout"
                    : fetchErr != undefined
                      ? "error"
                      : "ok",
              },
            );
          }
        }
        let text, meta;
        if (raw) {
          const html = decodeUtf8(raw);
          const p = await measureMs("parse", async () => parseHtml(load(html)));
          text = (p.mainArticleText || p.pageText).slice(0, 64 * 1024);
          meta = {
            description: p.description,
            imageUrl: p.imageUrl,
            lang: p.ogLocale || p.htmlLang,
            snippet: p.snippet,
            timestamp: p.timestamp,
            timestampModified: p.timestampModified,
            title: p.title,
          };
        }
        await Promise.all([
          db.exec("update url set fetched = ?, fetch_err = ? where url = ?", [
            fetchStarted,
            fetchErr,
            url,
          ]),
          // Do not overwrite or delete existing text/meta if this crawl has failed.
          text &&
            upsertKvRow.execute({
              k: `url/${id}/text`,
              v: encodeUtf8(text),
            }),
          meta &&
            upsertKvRow.execute({
              k: `url/${id}/meta`,
              v: encode(meta),
            }),
        ]);
        await QUEUE_CRAWL.deleteMessages([t]);
      }
    }),
  );
  lg.info("all done!");
})();
