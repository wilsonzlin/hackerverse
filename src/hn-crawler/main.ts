import { crawlHn } from "@wilsonzlin/crawler-toolkit";
import { VInteger, VOptional, VStruct } from "@wzlin/valid";
import Database from "better-sqlite3";
import { Duration } from "luxon";
import pino from "pino";

(async () => {
  const logger = pino();

  const db = new Database("/data/db.sqlite3");

  const nextId =
    new VOptional(
      new VStruct({
        v: new VInteger(0),
      }),
    ).parseRoot(
      db.prepare("select v from cfg where k = 'hn_crawler_next_id'").get(),
    )?.v ?? 0;

  for await (const { comment: c, post: p, nextId: nextIdToPersist } of crawlHn({
    concurrency: 16,
    logger,
    nextId,
    stopOnItemWithinDurationMs: Duration.fromObject({ days: 2 }).as(
      "milliseconds",
    ),
  })) {
    if (c) {
      db.prepare(
        `
          insert or replace into hn_comment (id, deleted, dead, score, text, author, ts, post)
          values (?, ?, ?, ?, ?, ?, ?, ?)
        `,
      )
        .bind(
          c.id,
          +c.deleted,
          +c.dead,
          c.score,
          c.textHtml,
          c.author ?? null,
          c.timestamp?.toISOString() ?? null,
          c.post ?? null,
        )
        .run();
    }
    if (p) {
      db.prepare(
        `
          insert or replace into hn_post (id, deleted, dead, score, title, text, author, ts, parent, url)
          values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
      )
        .bind(
          p.id,
          +p.deleted,
          +p.dead,
          p.score,
          p.titleHtml,
          p.textHtml,
          p.author,
          p.timestamp?.toISOString() ?? null,
          p.parent ?? null,
          p.url ?? null,
        )
        .run();
    }
    // Need to upsert as the first run won't have any row to update.
    db.prepare(
      `
        insert or replace into cfg (k, v)
        values ('hn_crawler_next_id', ?)
      `,
    )
      .bind(`${nextIdToPersist}`)
      .run();
  }
})();
