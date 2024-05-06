import { decode, encode } from "@msgpack/msgpack";
import { Validator, ValuePath } from "@wzlin/valid";
import assertExists from "@xtjs/lib/assertExists";
import decodeBase64 from "@xtjs/lib/decodeBase64";
import parseInteger from "@xtjs/lib/parseInteger";
import readBufferStream from "@xtjs/lib/readBufferStream";
import uint8ArrayToBuffer from "@xtjs/lib/uint8ArrayToBuffer";
import { createSecureServer } from "http2";
import { lg } from "../common/res";
import { endpointAnalyzePopularity } from "./endpoint/analyzePopularity";
import { endpointAnalyzeSentiment } from "./endpoint/analyzeSentiment";
import { endpointHeatmap } from "./endpoint/heatmap";
import { endpointPosts } from "./endpoint/posts";
import { endpointSearch } from "./endpoint/search";
import { endpointTopUsers } from "./endpoint/topUsers";
import { endpointUrlMetas } from "./endpoint/urlMetas";

const getPemEnv = (name: string) =>
  uint8ArrayToBuffer(
    decodeBase64(assertExists(process.env[`SSL_${name}_BASE64`])),
  );

type Endpoint = {
  input: Validator<any>;
  handler: (req: any) => Promise<any>;
};

const ENDPOINTS: Record<string, Endpoint> = {
  analyzePopularity: endpointAnalyzePopularity,
  analyzeSentiment: endpointAnalyzeSentiment,
  heatmap: endpointHeatmap,
  posts: endpointPosts,
  search: endpointSearch,
  topUsers: endpointTopUsers,
  urlMetas: endpointUrlMetas,
};

createSecureServer(
  {
    allowHTTP1: true,
    key: getPemEnv("KEY"),
    cert: getPemEnv("CERT"),
    ca: getPemEnv("CA"),
    rejectUnauthorized: true,
    requestCert: true,
  },
  async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "*");
    res.setHeader("Access-Control-Allow-Headers", "*");
    if (req.method === "OPTIONS") {
      return res.writeHead(200).end();
    }
    if (req.method !== "POST") {
      return res.writeHead(405).end();
    }
    const endpointName = req.url?.slice(1);
    const endpoint = ENDPOINTS[endpointName];
    if (!endpoint) {
      return res.writeHead(404).end();
    }
    let reqBodyRaw;
    try {
      reqBodyRaw = decode(await readBufferStream(req));
    } catch {
      return res.writeHead(400).end();
    }
    let reqBody;
    try {
      reqBody = endpoint.input.parse(
        new ValuePath(["request body"]),
        reqBodyRaw,
      );
    } catch (err) {
      return res.writeHead(400).end(err.message);
    }
    let resBody;
    try {
      resBody = await endpoint.handler(reqBody);
    } catch (error) {
      lg.error(
        {
          error: {
            trace: error.stack,
            message: error.message,
            type: error.constructor?.name,
            name: error.name,
            data: { ...error },
          },
          endpoint: endpointName,
        },
        "endpoint error",
      );
      return res.writeHead(500).end();
    }
    return res
      .writeHead(200, {
        "content-type": "application/msgpack",
      })
      .end(encode(resBody));
  },
)
  .on("error", (error) => lg.error({ error }, "server error"))
  .listen(parseInteger(process.env["PORT"]!), () => lg.info("server started"));
