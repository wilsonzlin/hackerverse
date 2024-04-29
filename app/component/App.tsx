import { Item, fetchHnItem } from "@wzlin/crawler-toolkit-hn";
import Dict from "@xtjs/lib/Dict";
import UnreachableError from "@xtjs/lib/UnreachableError";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import assertState from "@xtjs/lib/assertState";
import defined from "@xtjs/lib/defined";
import findAndRemove from "@xtjs/lib/findAndRemove";
import hexToRgb from "@xtjs/lib/hexToRgb";
import propertyComparator from "@xtjs/lib/propertyComparator";
import reversedComparator from "@xtjs/lib/reversedComparator";
import rgbToHex from "@xtjs/lib/rgbToHex";
import { produce } from "immer";
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
            density: number;
            color: [number, number, number];
            alpha_scale?: number;
            sigma?: number;
            upscale?: number;
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
  const out = req.outputs.map((out) => {
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
  assertState(i === payload.byteLength);
  return out;
};
type ApiResponse = Awaited<ReturnType<typeof apiCall>>;

type QueryParams = {
  query: string;
  weightSimilarity: number;
  weightScore: number;
  weightTimestamp: number;
  decayTimestamp: number;
};

type QueryResults = {
  items: Array<{ id: number; score: number }>;
  heatmap: ImageBitmap;
};

const QueryForm = ({
  canDelete,
  color,
  onChangeColor,
  onResults,
  onDelete,
}: {
  canDelete: boolean;
  color: [number, number, number];
  onChangeColor: (c: [number, number, number]) => void;
  onResults: (results: QueryResults | undefined) => void;
  onDelete: () => void;
}) => {
  const [queryRaw, setQueryRaw] = useState("");
  const [weightSimilarity, setWeightSimilarity] = useState(0.7);
  const [weightScore, setWeightScore] = useState(0.1);
  const [weightTimestamp, setWeightTimestamp] = useState(0.2);
  const [decayTimestamp, setDecayTimestamp] = useState(0.1);

  const [showParams, setShowParams] = useState(false);

  const queryReq = usePromise<QueryResults>();

  return (
    <form
      className="QueryForm"
      onSubmit={(e) => {
        e.preventDefault();
        const query = queryRaw.trim();
        if (!query) {
          queryReq.clear();
          onResults(undefined);
          return;
        }
        queryReq.set(async (signal) => {
          const data = await apiCall(signal, {
            dataset: "posts-bgem3",
            queries: [query],
            sim_scale: { min: 0.55, max: 0.75 },
            ts_weight_decay: decayTimestamp,
            outputs: [
              {
                items: {
                  limit: 10,
                },
              },
              {
                heatmap: {
                  alpha_scale: 2, // TODO This is really a hack, investigate distribution of scores.
                  density: 25,
                  color,
                  upscale: 2,
                  sigma: 4,
                },
              },
            ],
            weights: {
              sim: weightSimilarity,
              ts: weightTimestamp,
              vote: weightScore,
            },
          });
          const results = {
            items: [...assertInstanceOf(data[0], ApiRowsOutput).entries()].map(
              ([id, score]) => ({ id, score }),
            ),
            heatmap: await createImageBitmap(
              assertInstanceOf(data[1], ApiHeatmapOutput).blob(),
            ),
          };
          onResults(results);
          return results;
        });
      }}
    >
      <div className="main">
        <label className="color">
          <input
            hidden
            type="color"
            value={rgbToHex(...color)}
            onChange={(e) => onChangeColor(hexToRgb(e.currentTarget.value))}
          />
          <div
            style={{
              backgroundColor: `rgb(${color.join(",")})`,
            }}
          />
        </label>
        <input
          className="query"
          placeholder="Search, ask, visualize"
          value={queryRaw}
          onChange={(e) => setQueryRaw(e.currentTarget.value)}
        />
        <button
          type="button"
          className="toggle-params"
          onClick={() => setShowParams(!showParams)}
        >
          <Ico i="more_horiz" size={20} />
        </button>
        {!queryRaw.trim() ? (
          canDelete && (
            <button
              type="button"
              onClick={() => {
                onDelete();
              }}
            >
              <Ico i="delete" size={20} />
            </button>
          )
        ) : (
          <button
            type="button"
            onClick={() => {
              setQueryRaw("");
              queryReq.clear();
              onResults(undefined);
            }}
          >
            <Ico i="close" size={20} />
          </button>
        )}
        {queryReq.loading ? (
          <Loading size={18} />
        ) : (
          <button type="submit">
            <Ico i="search" size={20} />
          </button>
        )}
        {queryReq.error && (
          <button
            type="button"
            className="error"
            onClick={() => alert(queryReq.error)}
          >
            <Ico i="error" />
          </button>
        )}
      </div>
      {showParams && (
        <div className="params">
          <label>
            <span>
              W<sub>sim</sub>
            </span>
            <input
              type="number"
              step={0.00001}
              value={weightSimilarity}
              onChange={(e) => setWeightSimilarity(e.target.valueAsNumber)}
            />
          </label>
          <label>
            <span>
              W<sub>score</sub>
            </span>
            <input
              type="number"
              step={0.00001}
              value={weightScore}
              onChange={(e) => setWeightScore(e.target.valueAsNumber)}
            />
          </label>
          <label>
            <span>
              W<sub>ts</sub>
            </span>
            <input
              type="number"
              step={0.00001}
              value={weightTimestamp}
              onChange={(e) => setWeightTimestamp(e.target.valueAsNumber)}
            />
          </label>
          <label>
            <span>
              λ<sub>ts</sub>
            </span>
            <input
              type="number"
              step={0.00001}
              value={decayTimestamp}
              onChange={(e) => setDecayTimestamp(e.target.valueAsNumber)}
            />
          </label>
        </div>
      )}
    </form>
  );
};

type QueryState = {
  id: number;
  color: [number, number, number];
  results: QueryResults | undefined;
};

export const App = () => {
  const [$root, setRootElem] = useState<HTMLDivElement | null>(null);
  const rootDim = useMeasure($root);

  // We want to preserve other query states (i.e. don't unmount the existing React component) when deleting one, so we need some identifier and not just the ordinal which shifts.
  const nextQueryId = useRef(1);
  const [queries, setQueries] = useState<Array<QueryState>>([
    {
      id: 0,
      color: [49, 185, 235],
      results: undefined,
    },
  ]);
  const results = useMemo(
    () =>
      queries
        .flatMap((q) => q.results?.items ?? [])
        .sort(reversedComparator(propertyComparator("score"))),
    [queries],
  );
  const heatmaps = useMemo(
    () => queries.map((q) => q.results?.heatmap).filter(defined),
    [queries],
  );

  const [items, setItems] = useState<Record<number, Item>>({});
  useEffect(() => {
    (async () => {
      // TODO Don't refetch if already existing or in the process of fetching.
      await Promise.all(
        results.map(async ({ id }) => {
          const item = await fetchHnItem(id);
          setItems((items) => ({ ...items, [id]: item }));
        }),
      );
    })();
  }, [results]);

  return (
    <div ref={setRootElem} className="App">
      <PointMap
        heatmaps={heatmaps}
        height={rootDim?.height ?? 0}
        width={rootDim?.width ?? 0}
      />

      <div className="panel">
        <div className="queries">
          {queries.map((q, i) => {
            const mutQ = (fn: (q: QueryState) => unknown) => {
              // Always use setQueries in callback mode, and always find ID, since `queries` may have changed since we last created and passed the on* callbacks.
              setQueries((queries) =>
                produce(queries, (queries) => {
                  const found = queries.find((oq) => oq.id === q.id);
                  if (found) {
                    fn(found);
                  }
                }),
              );
            };

            return (
              <QueryForm
                key={q.id}
                canDelete={queries.length > 1}
                color={q.color}
                onChangeColor={(color) => mutQ((q) => (q.color = color))}
                onDelete={() =>
                  setQueries((queries) =>
                    produce(
                      queries,
                      (queries) =>
                        void findAndRemove(queries, (oq) => oq.id === q.id),
                    ),
                  )
                }
                onResults={(results) => mutQ((q) => (q.results = results))}
              />
            );
          })}

          <button
            className="add-query"
            onClick={() => {
              setQueries([
                ...queries,
                {
                  id: nextQueryId.current++,
                  color: [
                    Math.floor(Math.random() * 255),
                    Math.floor(Math.random() * 255),
                    Math.floor(Math.random() * 255),
                  ],
                  results: undefined,
                },
              ]);
            }}
          >
            <Ico i="add" size={18} />
            <span>Add query</span>
          </button>
        </div>

        <div className="results">
          {results?.map(({ id, score }) => {
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
    </div>
  );
};
