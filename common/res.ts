import { VInteger, VStruct, VUtf8Bytes, Validator } from "@wzlin/valid";
import { DbRpcClient, MsgPackValue } from "db-rpc-client-js";
import pino from "pino";
import { QueuedClient } from "queued-client-js";

export const lg = pino();

export const db = new DbRpcClient({
  endpoint: "https://db-rpc.posh.wilsonl.in",
  apiKey: process.env["DB_RPC_API_KEY"],
}).database("hndr");

export const getCfg = async <V>(
  k: string,
  v: Validator<V>,
): Promise<V | undefined> => {
  const [row] = await db.query(
    "select v from cfg where k = ?",
    [k],
    new VStruct({
      // MariaDB returns `v` as UTF-8 bytes for some reason.
      v: new VUtf8Bytes(v),
    }),
  );
  return row.v;
};

export const setCfg = async (k: string, v: string | number) => {
  // Need to upsert as the first setCfg call won't have any row to update.
  await upsertDbRowBatch({
    table: "cfg",
    rows: [
      {
        k,
        v: String(v),
      },
    ],
    keyColumns: ["k"],
  });
};

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
