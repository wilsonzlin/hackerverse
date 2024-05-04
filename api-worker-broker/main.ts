import { decode, encode } from "@msgpack/msgpack";
import { VInteger, VString, VStruct, VUnknown, Valid } from "@wzlin/valid";
import Dict from "@xtjs/lib/Dict";
import assertExists from "@xtjs/lib/assertExists";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import assertState from "@xtjs/lib/assertState";
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
    resolve: (res: any) => void;
    reject: (err: any) => void;
  }
>();
const connToReq = new WeakMap<WebSocket, Set<number>>();

const vNodeInitMessage = new VStruct({
  ip: new VString(),
  token: new VString(),
});

const vMessageToNode = new VStruct({
  id: new VInteger(0),
  input: new VUnknown(),
});

const vMessageToBroker = new VStruct({
  id: new VInteger(0),
  output: new VUnknown(),
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
  connToReq.set(conn, new Set());
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
    lg.info({ ip: msg.ip }, "node verified");
    clearTimeout(verifyTimeout);
    conn.on("message", (raw, isBinary) => {
      assertState(isBinary);
      const msg = vMessageToBroker.parseRoot(
        decode(assertInstanceOf(raw, Buffer)),
      );
      connToReq.get(conn)!.delete(msg.id);
      reqs.remove(msg.id)?.resolve(msg.output);
    });
  });
  conn.on("close", () => {
    lg.info("node disconnected");
    for (const id of connToReq.get(conn)!) {
      reqs.remove(id)?.reject(new Error("Node disconnected"));
    }
  });
});

const sendToNode = (input: any) =>
  new Promise((resolve, reject) => {
    const id = nextReqId++;
    const conn = randomPick([...ws.clients]);
    if (!conn) {
      return reject(new Error("No node available"));
    }
    reqs.set(id, { resolve, reject });
    connToReq.get(conn)!.add(id);
    const msg: Valid<typeof vMessageToNode> = { id, input };
    conn.send(encode(msg), { binary: true });
  });

http
  .createServer(async (req, res) => {
    if (req.method !== "POST") {
      return res.writeHead(405).end();
    }
    let input;
    try {
      input = decode(await readBufferStream(req));
    } catch (err) {
      return res.writeHead(400).end(err.message);
    }
    let resBody;
    try {
      resBody = await sendToNode(input);
    } catch (err) {
      return res.writeHead(500).end(err.message);
    }
    res
      .writeHead(200, {
        "content-type": "application/msgpack",
      })
      .end(encode(resBody));
  })
  .listen(6050, () => lg.info("API server started"));
