import { DbRpcClient } from "db-rpc-client-js";
import { QueuedClient } from "queued-client-js";

export const db = new DbRpcClient({
  endpoint: "https://db-rpc.posh.wilsonl.in",
  apiKey: process.env["DB_RPC_API_KEY"],
}).database("hndr");

export const queued = new QueuedClient({
  endpoint: "https://queued.posh.wilsonl.in",
  apiKey: process.env["QUEUED_API_KEY"],
});
