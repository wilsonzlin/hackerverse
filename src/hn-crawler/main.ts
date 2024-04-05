import { crawlHn } from "@wilsonzlin/crawler-toolkit";
import { VInteger, VOptional, VStruct } from "@wzlin/valid";
import Database from "better-sqlite3";

(async () => {
  const db = new Database("/data/db.sqlite3");

  const nextId = new VOptional(new VStruct({
    v: new VInteger(0),
  })).parseRoot(
    db.prepare("select v from cfg where k = 'hn_crawler_next_id'")
      .get()
  )?.v ?? 0;

  const updateNextId = db.prepare(
    `
      update cfg set v = ? where k = 'hn_crawler_next_id'
    `
  );
  const insertComment = db.prepare(
    `
      insert into hn_comment (id, deleted, dead, score, text, author, ts, post)
      values (?, ?, ?, ?, ?, ?, ?, ?)
    `
  );
  const insertPost = db.prepare(
    `
      insert into hn_post (id, deleted, dead, score, title, text, author, ts, parent, url)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
  );

  await crawlHn({
    concurrency: 128,
    nextId,
    onBatch: async ({ comments, nextId, posts }) => {
      for (const c of comments) {
        insertComment.bind(
          c.id, c.deleted, c.dead, c.score, c.textHtml, c.author ?? null, c.timestamp?.toISOString() ?? null, c.post ?? null,
        ).run();
      }
      for (const p of posts) {
        insertPost.bind(
          p.id, p.deleted, p.dead, p.score, p.titleHtml, p.textHtml, p.author, p.timestamp?.toISOString() ?? null, p.parent ?? null, p.url ?? null,
        ).run();
      }
      updateNextId.bind(nextId).run();
    },
  });
})();
