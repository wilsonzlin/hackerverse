import assertExists from "@xtjs/lib/assertExists";
import defined from "@xtjs/lib/defined";
import findAndRemove from "@xtjs/lib/findAndRemove";
import hexToRgb from "@xtjs/lib/hexToRgb";
import mapExists from "@xtjs/lib/mapExists";
import rgbToHex from "@xtjs/lib/rgbToHex";
import { produce } from "immer";
import { DateTime } from "luxon";
import { useEffect, useMemo, useRef, useState } from "react";
import { Ico } from "../component/Ico";
import { Loading } from "../component/Loading";
import { PageSwitcher } from "../component/PageSwitcher";
import { PointMap, PointMapController } from "../component/PointMap";
import { heatmapApiCall, searchApiCall } from "../util/api";
import { useMeasure } from "../util/dom";
import { usePromise } from "../util/fetch";
import { useHnItems } from "../util/item";
import "./Search.css";

type QueryResults = Array<{
  id: number;
  x: number;
  y: number;
  sim: number;
  final_score: number;
}>;

const QueryForm = ({
  onResults,
}: {
  onResults: (results: QueryResults) => void;
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
      onSubmit={async (e) => {
        e.preventDefault();
        const query = queryRaw.trim();
        if (!query) {
          queryReq.clear();
          onResults([]);
          return;
        }
        const results = await queryReq.set((signal) =>
          searchApiCall(signal, {
            query,
            limit: 10,
            dataset: "toppost",
            weightSimilarity,
            weightScore,
            weightTimestamp,
            decayTimestamp,
          }),
        );
        onResults(assertExists(results));
        return results;
      }}
    >
      <div className="main">
        <input
          className="query"
          placeholder="Search or ask"
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
        {!!queryRaw.trim() && (
          <button
            type="button"
            onClick={() => {
              setQueryRaw("");
              queryReq.clear();
              onResults([]);
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

type HeatmapQueryState = {
  id: number;
  color: [number, number, number];
  heatmap: ImageBitmap | undefined;
};

const HeatmapForm = ({
  canDelete,
  color,
  onChangeColor,
  onHeatmap,
  onDelete,
}: {
  canDelete: boolean;
  color: [number, number, number];
  onChangeColor: (c: [number, number, number]) => void;
  onHeatmap: (hm: ImageBitmap | undefined) => void;
  onDelete: () => void;
}) => {
  const [queryRaw, setQueryRaw] = useState("");

  const queryReq = usePromise<ImageBitmap>();
  useEffect(() => onHeatmap(queryReq.data), [queryReq.data]);

  return (
    <form
      className="HeatmapForm"
      onSubmit={(e) => {
        e.preventDefault();
        queryReq.set(async (signal) => {
          const query = queryRaw.trim();
          if (!query) {
            return;
          }
          const heatmapRaw = await heatmapApiCall(signal, {
            color,
            dataset: "toppost",
            query,
          });
          const heatmap = await createImageBitmap(
            new Blob([heatmapRaw], { type: "image/webp" }),
          );
          signal.throwIfAborted();
          return heatmap;
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
          placeholder="Search or ask"
          value={queryRaw}
          onChange={(e) => setQueryRaw(e.currentTarget.value)}
        />
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
    </form>
  );
};

export const SearchPage = () => {
  const [$root, setRootElem] = useState<HTMLDivElement | null>(null);
  const rootDim = useMeasure($root);

  const mapCtl = useRef<PointMapController>();

  // We want to preserve other query states (i.e. don't unmount the existing React component) when deleting one, so we need some identifier and not just the ordinal which shifts.
  const nextHeatmapQueryId = useRef(0);
  const [heatmapQueries, setHeatmapQueries] =
    useState(Array<HeatmapQueryState>());
  const heatmaps = useMemo(
    () => heatmapQueries.map((q) => q.heatmap).filter(defined),
    [heatmapQueries],
  );

  const [results, setResults] = useState<QueryResults>([]);
  const [shouldAnimateToResults, setShouldAnimateToResults] = useState(false);
  useEffect(() => {
    if (!shouldAnimateToResults) {
      return;
    }
    setShouldAnimateToResults(false);
    let xMinPt = Infinity;
    let xMaxPt = -Infinity;
    let yMinPt = Infinity;
    let yMaxPt = -Infinity;
    for (const p of results) {
      xMinPt = Math.min(xMinPt, p.x);
      xMaxPt = Math.max(xMaxPt, p.x);
      yMinPt = Math.min(yMinPt, p.y);
      yMaxPt = Math.max(yMaxPt, p.y);
    }
    const ANIM_MS = 700;
    mapCtl.current?.animate(
      {
        x0Pt: xMinPt,
        x1Pt: xMaxPt,
        y0Pt: yMinPt,
        y1Pt: yMaxPt,
      },
      ANIM_MS,
    );
  }, [shouldAnimateToResults]);
  const items = useHnItems(results.map((r) => r.id));

  return (
    <div ref={setRootElem} className="SearchPage">
      <PointMap
        controllerRef={mapCtl}
        heatmaps={heatmaps}
        height={rootDim?.height ?? 0}
        resultPoints={results}
        width={rootDim?.width ?? 0}
      />

      <PageSwitcher />

      <div className="heatmaps">
        {heatmapQueries.map((q) => {
          const mutQ = (fn: (q: HeatmapQueryState) => unknown) => {
            // Always use setQueries in callback mode, and always find ID, since `queries` may have changed since we last created and passed the on* callbacks.
            setHeatmapQueries((queries) =>
              produce(queries, (queries) => {
                mapExists(
                  queries.find((oq) => oq.id === q.id),
                  fn,
                );
              }),
            );
          };

          return (
            <HeatmapForm
              key={q.id}
              canDelete={heatmapQueries.length > 1}
              color={q.color}
              onChangeColor={(color) => mutQ((q) => (q.color = color))}
              onDelete={() =>
                setHeatmapQueries((queries) =>
                  produce(
                    queries,
                    (queries) =>
                      void findAndRemove(queries, (oq) => oq.id === q.id),
                  ),
                )
              }
              onHeatmap={(heatmap) => mutQ((q) => (q.heatmap = heatmap))}
            />
          );
        })}
        <button
          className="add-heatmap"
          onClick={() => {
            setHeatmapQueries([
              ...heatmapQueries,
              {
                id: nextHeatmapQueryId.current++,
                color: [
                  Math.floor(Math.random() * 255),
                  Math.floor(Math.random() * 255),
                  Math.floor(Math.random() * 255),
                ],
                heatmap: undefined,
              },
            ]);
          }}
        >
          <Ico i="add" size={18} />
          <span>Add</span>
        </button>
      </div>

      <div className="panel">
        <div className="queries">
          <QueryForm
            onResults={(results) => {
              setResults(results);
              // Only animate when results come in, not for any other reason that `results` changes (e.g. clearing, deleting).
              if (results.length) {
                setShouldAnimateToResults(true);
              }
            }}
          />
        </div>

        <div className="results">
          {results?.map(({ id, sim, final_score }) => {
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
                <div>
                  <p className="site">{site}</p>
                  <div>
                    {sim} {final_score}
                  </div>
                </div>
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
