import { decode, encode } from "@msgpack/msgpack";
import {
  VArray,
  VInteger,
  VOptional,
  VString,
  VStruct,
  VUnknown,
  Valid,
} from "@wzlin/valid";
import Dict from "@xtjs/lib/Dict";
import assertExists from "@xtjs/lib/assertExists";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import assertState from "@xtjs/lib/assertState";
import findAndRemove from "@xtjs/lib/findAndRemove";
import randomPick from "@xtjs/lib/randomPick";
import readBufferStream from "@xtjs/lib/readBufferStream";
import http from "http";
import https from "https";
import { WebSocket, WebSocketServer } from "ws";
import { lg } from "../common/res";

const TOKEN = assertExists(process.env["API_WORKER_NODE_TOKEN"]);

let nextReqId = 0;
const reqs = new Dict<
  number,
  {
    resolve: (
      res:
        | {
            output: any;
          }
        | {
            error: any;
          },
    ) => void;
    reject: (err: Error) => void;
  }
>();
const channelToConns: Record<string, WebSocket[]> = {};
const connStates = new WeakMap<
  WebSocket,
  {
    requests: Set<number>;
    channels: Set<string>;
  }
>();

const vNodeInitMessage = new VStruct({
  ip: new VString(),
  token: new VString(),
  channels: new VArray(new VString(1), 1),
});

const vMessageToNode = new VStruct({
  id: new VInteger(0),
  input: new VUnknown(),
});

const vMessageToBroker = new VStruct({
  id: new VInteger(0),
  output: new VOptional(new VUnknown()),
  error: new VOptional(new VUnknown()),
});

const wsServer = https.createServer({
  key: Buffer.from(
    assertExists(process.env["API_WORKER_NODE_KEY_B64"]),
    "base64",
  ),
  cert: Buffer.from(
    assertExists(process.env["API_WORKER_NODE_CERT_B64"]),
    "base64",
  ),
});
wsServer.listen(6000, () => lg.info("WS server started"));
const ws = new WebSocketServer({
  server: wsServer,
});
ws.on("connection", (conn) => {
  lg.info("node connected");
  const verifyTimeout = setTimeout(() => {
    lg.warn("did not receive token within reasonable time");
    conn.close();
  }, 1000 * 15);
  conn.once("message", (raw, isBinary) => {
    assertState(isBinary);
    const msg = vNodeInitMessage.parseRoot(
      decode(assertInstanceOf(raw, Buffer)),
    );
    if (msg.token !== TOKEN) {
      lg.warn("received invalid token");
      conn.close();
      return;
    }
    clearTimeout(verifyTimeout);
    lg.info({ ip: msg.ip, channels: msg.channels }, "node verified");
    // Set up connection.
    const connState = {
      requests: new Set<number>(),
      channels: new Set(msg.channels),
    };
    connStates.set(conn, connState);
    for (const ch of msg.channels) {
      (channelToConns[ch] ??= []).push(conn);
    }
    conn.on("message", (raw, isBinary) => {
      assertState(isBinary);
      const { id, error, output } = vMessageToBroker.parseRoot(
        decode(assertInstanceOf(raw, Buffer)),
      );
      connState.requests.delete(id);
      const prom = reqs.remove(id);
      prom?.resolve({ error, output });
    });
  });
  conn.on("close", () => {
    lg.info("node disconnected");
    const connState = connStates.get(conn);
    if (connState) {
      for (const id of connState.requests) {
        reqs.remove(id)?.reject(new Error("Node disconnected"));
      }
      for (const ch of connState.channels) {
        findAndRemove(channelToConns[ch], (oc) => oc === conn);
      }
    }
  });
});

const sendToNode = (channel: string, input: any) =>
  new Promise<{ error?: any; output?: any }>((resolve, reject) => {
    const id = nextReqId++;
    const conn = randomPick(channelToConns[channel] ?? []);
    if (!conn) {
      return reject(new Error("No node available"));
    }
    reqs.set(id, { resolve, reject });
    connStates.get(conn)!.requests.add(id);
    const msg: Valid<typeof vMessageToNode> = { id, input };
    conn.send(JSON.stringify(msg));
  });

http
  .createServer(async (req, res) => {
    if (req.method !== "POST") {
      return res.writeHead(405).end();
    }
    const channel = req.url?.slice(1) ?? "";
    let input;
    try {
      input = decode(await readBufferStream(req));
    } catch (err) {
      return res.writeHead(400).end(err.message);
    }
    let resBody;
    try {
      resBody = await sendToNode(channel, input);
    } catch (err) {
      return res.writeHead(500).end(err.message);
    }
    res
      .writeHead(resBody.error ? 502 : 200, {
        "content-type": "application/msgpack",
      })
      .end(encode(resBody.error ?? resBody.output));
  })
  .listen(6050, () => lg.info("API server started"));
