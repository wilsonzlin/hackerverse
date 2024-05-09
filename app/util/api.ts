import { decode, encode } from "@msgpack/msgpack";
import {
  VArray,
  VBytes,
  VDate,
  VFiniteNumber,
  VString,
  VStruct,
  Validator,
} from "@wzlin/valid";

export const apiCall = async <R>(
  signal: AbortSignal,
  endpoint: string,
  req: any,
  vRes: Validator<R>,
): Promise<R> => {
  const res = await fetch(`https://api-hndr.wilsonl.in/${endpoint}`, {
    signal,
    method: "POST",
    headers: {
      "content-type": "application/msgpack",
    },
    body: encode(req),
  });
  if (!res.ok) {
    const msg = await res.text();
    throw new Error(`${endpoint} API call failed with ${res.status}: ${msg}`);
  }
  const raw = decode(await res.arrayBuffer());
  return vRes.parseRoot(raw);
};

export const analyzePopularityApiCall = async (
  signal: AbortSignal,
  req: {
    query: string;
    simMinHundredths: number;
  },
) =>
  apiCall(
    signal,
    "analyzePopularity",
    req,
    new VStruct({
      timestamps: new VArray(new VDate()),
      scores: new VArray(new VFiniteNumber()),
    }),
  );

export const analyzeSentimentApiCall = async (
  signal: AbortSignal,
  req: {
    query: string;
    simMinHundredths: number;
  },
) =>
  apiCall(
    signal,
    "analyzeSentiment",
    req,
    new VStruct({
      timestamps: new VArray(new VDate()),
      positives: new VArray(new VFiniteNumber()),
      negatives: new VArray(new VFiniteNumber()),
    }),
  );

export const heatmapApiCall = async (
  signal: AbortSignal,
  req: {
    query: string;
    dataset: "post" | "toppost";
    color: readonly [number, number, number];
  },
) => apiCall(signal, "heatmap", req, new VBytes());

export const searchApiCall = async (
  signal: AbortSignal,
  req: {
    query: string;
    limit: number;
    dataset: "post" | "toppost";
    weightSimilarity: number;
    weightScore: number;
    weightTimestamp: number;
    decayTimestamp: number;
  },
) =>
  apiCall(
    signal,
    "search",
    req,
    new VArray(
      new VStruct({
        id: new VFiniteNumber(),
        x: new VFiniteNumber(),
        y: new VFiniteNumber(),
        sim: new VFiniteNumber(),
        final_score: new VFiniteNumber(),
      }),
    ),
  );

export const itemsApiCall = async (
  signal: AbortSignal,
  req: {
    dataset: "comment" | "post";
    query: string;
    limit: number;
    simMinHundredths: number;
    orderBy: "votes" | "ts";
  },
) =>
  apiCall(
    signal,
    "items",
    req,
    new VArray(
      new VStruct({
        id: new VFiniteNumber(),
        sim: new VFiniteNumber(),
      }),
    ),
  );

export const topUsersApiCall = async (
  signal: AbortSignal,
  req: {
    query: string;
    limit: number;
    simMinHundredths: number;
  },
) =>
  apiCall(
    signal,
    "topUsers",
    req,
    new VArray(
      new VStruct({
        user: new VString(),
        score: new VFiniteNumber(),
      }),
    ),
  );
