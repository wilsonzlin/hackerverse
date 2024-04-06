import { VInteger, VStruct } from "@wzlin/valid";
import { DbRpcClient, MsgPackValue } from "db-rpc-client-js";
import pino from "pino";
import { QueuedClient } from "queued-client-js";

export const lg = pino();

export const db = new DbRpcClient({
  endpoint: "https://db-rpc.posh.wilsonl.in",
  apiKey: process.env["DB_RPC_API_KEY"],
}).database("hndr");

export const upsertDbRowBatch = async <R extends Record<string, MsgPackValue>>({
  keyColumns,
  rows,
  table,
}: {
  keyColumns: (keyof R)[];
  rows: R[];
  table: string;
}) => {
  const cols = Object.keys(rows[0]);
  const sql = `
    insert into ${table} (${cols.join(", ")})
    values (${cols.map(() => "?").join(", ")})
    on duplicate key update
      ${cols
        .filter((c) => !keyColumns.includes(c))
        .map((c) => `${c} = values(${c})`)
        .join(", ")}
  `;
  return await db.batch(
    sql,
    rows.map((r) => cols.map((c) => r[c])),
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
