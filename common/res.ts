import { VInteger, VStruct } from "@wzlin/valid";
import { DbRpcClient, MsgPackValue } from "db-rpc-client-js";
import pino from "pino";
import { QueuedClient } from "queued-client-js";

export const lg = pino();

export const db = new DbRpcClient({
  endpoint: "https://db-rpc.posh.wilsonl.in",
  apiKey: process.env["DB_RPC_API_KEY"],
}).database("hndr");

export const upsertDbRow = async <R extends Record<string, MsgPackValue>>({
  keyColumns,
  row,
  table,
}: {
  keyColumns: (keyof R)[];
  row: R;
  table: string;
}) => {
  const ents = Object.entries(row);
  const sql = `
    insert into ${table} (${ents.map((e) => e[0]).join(", ")})
    values (${ents.map(() => "?").join(", ")})
    on duplicate key update
      ${ents
        .filter((e) => !keyColumns.includes(e[0] as any))
        .map((e) => `${e[0]} = values(${e[0]})`)
        .join(", ")}
  `;
  return await db.exec(
    sql,
    ents.map((e) => e[1]),
  );
};

export const queued = new QueuedClient({
  endpoint: "https://queued.posh.wilsonl.in",
  apiKey: process.env["QUEUED_API_KEY"],
});

export const QUEUE_HN_CRAWL = queued.queue("hndr:hn_crawl");

export const vQueueHnCrawlTask = new VStruct({
  startId: new VInteger(0),
  // Inclusive.
  endId: new VInteger(0),
});
