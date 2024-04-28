import { Item, fetchHnItem } from "@wzlin/crawler-toolkit-hn";
import Dict from "@xtjs/lib/Dict";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import assertState from "@xtjs/lib/assertState";
import mapExists from "@xtjs/lib/mapExists";
import { DateTime } from "luxon";
import { useEffect, useMemo, useRef, useState } from "react";
import { useMeasure } from "../util/dom";
import { usePromise } from "../util/fetch";
import "./App.css";
import { Ico } from "./Ico";
import { Loading } from "./Loading";
import { PointMap } from "./PointMap";

type Clip = { min: number; max: number };

class ApiRowsOutput {
  constructor(
    private readonly keysArray: Uint32Array,
    private readonly valuesArray: Float32Array,
  ) {
    assertState(keysArray.length === valuesArray.length);
  }

  get length() {
    return this.keysArray.length;
  }

  *keys() {
    yield* this.keysArray;
  }

  *entries() {
    for (let i = 0; i < this.length; i++) {
      yield [this.keysArray[i], this.valuesArray[i]] as const;
    }
  }

  object() {
    return Object.fromEntries(this.entries());
  }

  dict() {
    return new Dict(this.entries());
  }
}

class ApiHeatmapOutput {
  constructor(private readonly rawWebp: ArrayBuffer) {}

  blob() {
    return new Blob([this.rawWebp], { type: "image/webp" });
  }

  url() {
    return URL.createObjectURL(this.blob());
  }
}

const apiCall = async (
  signal: AbortSignal,
  req: {
    dataset: string;
    queries: string[];
    sim_scale: Clip;
    sim_agg?: "mean" | "min" | "max";
    ts_weight_decay?: number;
    filter_hnsw?: number;
    filter_clip?: Record<string, Clip>;
    weights: Record<string, number>;
    outputs: Array<
      | {
          group_by: {
            group_by: string;
            group_bucket?: number;
            group_final_score_agg?: "mean" | "min" | "max" | "sum" | "count";
          };
        }
      | {
          heatmap: {
            width: number;
            height: number;
            color: [number, number, number];
            alpha_min: number;
            alpha_max: number;
            sigma: number;
            upscale: number;
          };
        }
      | {
          items: {
            order_by?: string;
            order_asc?: boolean;
            limit?: number;
          };
        }
    >;
  },
) => {
  const res = await fetch("https://api-hndr.wilsonl.in/", {
    signal,
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(req),
  });
  if (!res.ok) {
    throw new Error(`Bad status ${res.status}: ${await res.text()}`);
  }
  const payload = await res.arrayBuffer();
  const dv = new DataView(payload);
  let i = 0;
  return req.outputs.map((out) => {
    if ("group_by" in out || "items" in out) {
      const count = dv.getUint32(i, true);
      i += 4;
      const keys = new Uint32Array(payload, i, count);
      i += count * 4;
      const values = new Float32Array(payload, i, count);
      i += count * 4;
      return new ApiRowsOutput(keys, values);
    }
    if ("heatmap" in out) {
      const rawLen = dv.getUint32(i, true);
      i += 4;
      const raw = payload.slice(i, (i += rawLen));
      return new ApiHeatmapOutput(raw);
    }
    throw new UnreachableError();
  });
};
type ApiResponse = Awaited<ReturnType<typeof apiCall>>;

export const App = () => {
  const [$root, setRootElem] = useState<HTMLDivElement | null>(null);
  const rootDim = useMeasure($root);

  const $form = useRef<HTMLFormElement | null>(null);

  const [query, setQuery] = useState<{
    query: string;
    weightSimilarity: number;
    weightScore: number;
    weightTimestamp: number;
    decayTimestamp: number;
  }>();
  const queryReq = usePromise<ApiResponse>();
  useEffect(() => {
    if (!query) {
      queryReq.clear();
    } else {
      const timeout = setTimeout(() => {
        queryReq.set((signal) =>
          apiCall(signal, {
            dataset: "posts_bgem3",
            queries: [query.query],
            sim_scale: { min: 0, max: 1 },
            ts_weight_decay: query.decayTimestamp,
            outputs: [
              {
                items: {
                  order_by: "final_score",
                  order_asc: false,
                  limit: 10,
                },
              },
            ],
            weights: {
              sim: query.weightSimilarity,
              ts: query.weightTimestamp,
              vote: query.weightScore,
            },
          }),
        );
      }, 250);
      return () => clearTimeout(timeout);
    }
  }, [query]);
  const results = useMemo(
    () =>
      mapExists(queryReq.data, (data) => [
        ...assertInstanceOf(data[0], ApiRowsOutput).entries(),
      ]),
    [queryReq.data],
  );

  const [items, setItems] = useState<Record<number, Item>>({});
  useEffect(() => {
    if (!results) {
      return;
    }
    (async () => {
      await Promise.all(
        results.map(async ([id]) => {
          const item = await fetchHnItem(id);
          setItems((items) => ({ ...items, [id]: item }));
        }),
      );
    })();
  }, [results]);

  return (
    <div ref={setRootElem} className="App">
      <PointMap height={rootDim?.height ?? 0} width={rootDim?.width ?? 0} />

      <form
        ref={$form}
        className="search"
        onSubmit={(e) => {
          e.preventDefault();
          const form = e.currentTarget;
          const elems = form.elements;
          const getInput = (name: string) =>
            assertInstanceOf(elems.namedItem(name), HTMLInputElement);
          setQuery({
            query: getInput("query").value.trim(),
            weightSimilarity: getInput("w_sim").valueAsNumber,
            weightScore: getInput("w_score").valueAsNumber,
            weightTimestamp: getInput("w_ts").valueAsNumber,
            decayTimestamp: getInput("decay_ts").valueAsNumber,
          });
        }}
      >
        <div className="main">
          <input name="query" placeholder="Search or ask" />
          {queryReq.loading ? (
            <Loading size={24} />
          ) : results ? (
            <button
              type="button"
              onClick={() => {
                setQuery(undefined);
                assertInstanceOf(
                  $form.current!.elements.namedItem("query"),
                  HTMLInputElement,
                ).value = "";
              }}
            >
              <Ico i="close" />
            </button>
          ) : (
            <button disabled={queryReq.loading} type="submit">
              <Ico i="search" />
            </button>
          )}
        </div>
        <div className="params">
          <label>
            <span>
              W<sub>sim</sub>
            </span>
            <input
              name="w_sim"
              type="number"
              defaultValue={0.4}
              step={0.00001}
            />
          </label>
          <label>
            <span>
              W<sub>score</sub>
            </span>
            <input
              name="w_score"
              type="number"
              defaultValue={0.4}
              step={0.00001}
            />
          </label>
          <label>
            <span>
              W<sub>ts</sub>
            </span>
            <input
              name="w_ts"
              type="number"
              defaultValue={0.2}
              step={0.00001}
            />
          </label>
          <label>
            <span>
              λ<sub>ts</sub>
            </span>
            <input
              name="decay_ts"
              type="number"
              defaultValue={0.1}
              step={0.00001}
            />
          </label>
        </div>
      </form>

      <div className="results">
        {mapExists(queryReq.error, (error) => (
          <p className="err">{error}</p>
        ))}

        {results?.map(([id, score]) => {
          const item = items[id];
          if (!item || !item.time || !item.by || !item.title) {
            return;
          }
          const hnUrl = `https://news.ycombinator.com/item?id=${id}`;
          let url, site;
          if (item.url) {
            try {
              const parsed = new URL(item.url);
              url = item.url;
              site = parsed.hostname.replace(/^www\./, "");
            } catch {
              return;
            }
          } else {
            url = hnUrl;
            site = "news.ycombinator.com";
          }
          const ts = DateTime.fromJSDate(item.time);
          const ago = DateTime.now()
            .diff(ts)
            .rescale()
            .toHuman({ unitDisplay: "long" })
            .split(",")[0];
          return (
            <div key={id} className="result">
              <p className="site">{site}</p>
              <a href={url} target="_blank" rel="noopener noreferrer">
                <h1 dangerouslySetInnerHTML={{ __html: item.title ?? "" }} />
              </a>
              <p>
                <a href={hnUrl} target="_blank" rel="noopener noreferrer">
                  {item.score} point{item.score == 1 ? "" : "s"}
                </a>{" "}
                by{" "}
                <a
                  href={`https://news.ycombinator.com/user?id=${item.by}`}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  {item.by}
                </a>{" "}
                {ago} ago
              </p>
            </div>
          );
        })}
      </div>
    </div>
  );
};
