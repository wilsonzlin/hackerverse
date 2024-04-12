import { VBytes, VString, VStruct, VUtf8Bytes, Validator } from "@wzlin/valid";
import Batcher from "@xtjs/lib/js/Batcher";
import { DbRpcClient, MsgPackValue } from "db-rpc-client-js";
import { StatsD } from "hot-shots";
import pino from "pino";
import { QueuedClient } from "queued-client-js";

export const measureMs = async <T>(
  statsd: StatsD,
  metric: string,
  fn: () => Promise<T>,
  tags?: Record<string, string>,
) => {
  const started = Date.now();
  try {
    return await fn();
  } finally {
    statsd.timing(metric, Date.now() - started, tags);
  }
};

export const lg = pino({
  base: undefined,
  timestamp: () => `,"timestamp":"${new Date().toISOString()}"`,
  formatters: {
    level: (label) => ({ level: label }),
  },
});

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
  return row?.v;
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

export type KvRow = {
  k: string;
  v: Uint8Array;
};

export const getKvRow = new Batcher(async (keys: string[]) => {
  const rows = await db.query(
    `select k, v from kv where k in (${keys.map(() => "?").join(",")})`,
    [...keys],
    new VStruct({
      k: new VString(),
      v: new VBytes(),
    }),
  );
  const map = Object.fromEntries(rows.map((r) => [r.k, r.v]));
  return keys.map((k) => map[k]);
});

export const upsertKvRow = new Batcher(async (rows: KvRow[]) => {
  await upsertDbRowBatch({
    table: "kv",
    rows,
    keyColumns: ["k"],
  });
  return Array(rows.length).fill(true);
});

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

export const QUEUE_EMBED = queued.queue("hndr:embed");

export const vQueueEmbedTask = new VStruct({
  inputKey: new VString(),
  outputKey: new VString(),
});
