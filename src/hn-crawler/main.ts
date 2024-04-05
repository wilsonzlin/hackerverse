import { crawlHn } from "@wilsonzlin/crawler-toolkit";
import { VInteger, VOptional, VStruct } from "@wzlin/valid";
import Database from "better-sqlite3";

(async () => {
  const db = new Database("/data/db.sqlite3");

  const nextId =
    new VOptional(
      new VStruct({
        v: new VInteger(0),
      }),
    ).parseRoot(
      db.prepare("select v from cfg where k = 'hn_crawler_next_id'").get(),
    )?.v ?? 0;
  console.log("Starting from", nextId);

  await crawlHn({
    concurrency: 128,
    nextId,
    onBatch: async ({ comments, nextId, posts }) => {
      for (const c of comments) {
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
      for (const p of posts) {
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
        .bind(nextId)
        .run();
    },
  });
})();
