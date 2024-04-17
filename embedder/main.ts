import { decode } from "@msgpack/msgpack";
import { setUpUncaughtExceptionHandler } from "@wzlin/service-toolkit";
import {
  VDate,
  VInteger,
  VOptional,
  VString,
  VStruct,
  VUtf8Bytes,
  Valid,
  ValuePath,
} from "@wzlin/valid";
import Batcher from "@xtjs/lib/js/Batcher";
import UnreachableError from "@xtjs/lib/js/UnreachableError";
import WorkerPool from "@xtjs/lib/js/WorkerPool";
import assertExists from "@xtjs/lib/js/assertExists";
import decodeUtf8 from "@xtjs/lib/js/decodeUtf8";
import mapExists from "@xtjs/lib/js/mapExists";
import parseInteger from "@xtjs/lib/js/parseInteger";
import { Duration } from "luxon";
import {
  QUEUE_EMBED,
  db,
  getKvRow,
  statsd,
  upsertKvRow,
  vQueueEmbedTask,
} from "../common/res";
import { createEmbedWorker } from "./worker_embed";

setUpUncaughtExceptionHandler();

const vMeta = new VStruct({
  description: new VOptional(new VString()),
  imageUrl: new VOptional(new VString()),
  lang: new VOptional(new VString()),
  snippet: new VOptional(new VString()),
  timestamp: new VOptional(new VDate()),
  timestampModified: new VOptional(new VDate()),
  title: new VOptional(new VString()),
});

const postUrlIdFetcher = new Batcher(async (ids: number[]) => {
  const rows = await db.query(
    `
      select id, url
      from post
      where id in (${ids.map(() => "?").join(",")})
    `,
    ids,
    new VStruct({
      id: new VInteger(1),
      url: new VOptional(new VInteger(1)),
    }),
  );
  const map = Object.fromEntries(rows.map((r) => [r.id, r.url]));
  return ids.map((id) => map[id]);
});

const urlFetchStateFetcher = new Batcher(async (ids: number[]) => {
  const rows = await db.query(
    `
      select id, fetched as ts, fetch_err as err
      from url
      where id in (${ids.map(() => "?").join(",")})
    `,
    ids,
    new VStruct({
      id: new VInteger(1),
      ts: new VOptional(new VDate()),
      err: new VOptional(new VUtf8Bytes(new VString())),
    }),
  );
  const map = Object.fromEntries(rows.map(({ id, ...r }) => [id, r]));
  return ids.map((id) => map[id]);
});

const pageFetcher = new Batcher(async (urlIds: number[]) => {
  const rows = await Promise.all(
    urlIds.flatMap((id) =>
      [`url/${id}/text`, `url/${id}/meta`].map((k) =>
        getKvRow.execute(k).then((v) => [k, v] as const),
      ),
    ),
  );
  const texts: Record<number, string | undefined> = {};
  const metas: Record<number, Valid<typeof vMeta> | undefined> = {};
  for (const [k, v] of rows) {
    if (!v) {
      continue;
    }
    const [_, idRaw, typ] = assertExists(
      /^url\/([0-9]+)\/(text|meta)$/.exec(k),
    );
    const id = parseInteger(idRaw);
    switch (typ) {
      case "text":
        texts[id] = decodeUtf8(v);
        break;
      case "meta":
        metas[id] = vMeta.parse(new ValuePath([k]), decode(v));
        break;
      default:
        throw new UnreachableError();
    }
  }
  return urlIds.map((id) => ({
    text: texts[id],
    meta: metas[id],
  }));
});

new WorkerPool(__filename, 256)
  .leaderState(async () => {
    const embedWorker = await createEmbedWorker();
    const embedBatcher = new Batcher((texts: string[]) =>
      embedWorker.embed(texts),
    );
    return {
      embedBatcher,
    };
  })
  .leaderTask("embed", async (input: string, ctx) => {
    return await ctx.state.embedBatcher.execute(input);
  })
  .worker(async (pool) => {
    while (true) {
      const [msg] = await QUEUE_EMBED.pollMessages(
        1,
        Duration.fromObject({ minutes: 30 }).as("seconds"),
      );
      if (!msg) {
        break;
      }
      const task = vQueueEmbedTask.parseRoot(msg.contents);
      let embInput = decodeUtf8(
        assertExists(await getKvRow.execute(task.inputKey)),
      );
      if (embInput.includes("<<<REPLACE_WITH_PAGE_TITLE>>>")) {
        const postId = parseInteger(
          /^post\/([0-9]+)\/emb_input$/.exec(task.inputKey)![1],
        );
        const urlId = await postUrlIdFetcher.execute(postId);
        const fetchState = await mapExists(urlId, (id) =>
          urlFetchStateFetcher.execute(id),
        );
        const { text, meta } =
          (await mapExists(urlId, (id) => pageFetcher.execute(id))) ?? {};
        // The item hasn't been fetched yet.
        // Do not delete queue task. Do not update queue task, let the existing visibility timeout postpone its processing. Do not continue.
        if (!fetchState?.ts) {
          statsd.increment("skipped");
          continue;
        }
        // We mark if the fetch failed or the text/meta is (partially) missing. This ensures we can decide whether to fix the input and regenerate to ensure high quality embeddings, or just go ahead anyway and use the potentially poor embeddings with little to no input.
        const embMissingPage = !!(fetchState.err || !text || !meta);
        await db.exec("update post set emb_missing_page = ? where id = ?", [
          embMissingPage,
          postId,
        ]);
        embInput = embInput
          .replace("<<<REPLACE_WITH_PAGE_TITLE>>>", meta?.title ?? "")
          .replace(
            "<<<REPLACE_WITH_PAGE_DESCRIPTION>>>",
            meta?.description ?? "",
          )
          .replace("<<<REPLACE_WITH_PAGE_TEXT>>>", text ?? "");
      }
      const textEmb = await pool.execute("embed", embInput);
      await upsertKvRow.execute({
        k: task.outputKey,
        v: textEmb,
      });
      await QUEUE_EMBED.deleteMessages([msg]);
    }
  })
  .go();
