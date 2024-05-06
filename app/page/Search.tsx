import defined from "@xtjs/lib/defined";
import findAndRemove from "@xtjs/lib/findAndRemove";
import mapExists from "@xtjs/lib/mapExists";
import randomPick from "@xtjs/lib/randomPick";
import shuffleArray from "@xtjs/lib/shuffleArray";
import { produce } from "immer";
import { DateTime } from "luxon";
import { useEffect, useMemo, useRef, useState } from "react";
import { ColorInput } from "../component/ColorInput";
import { Ico } from "../component/Ico";
import { Loading } from "../component/Loading";
import { PageSwitcher } from "../component/PageSwitcher";
import { PointMap, PointMapController } from "../component/PointMap";
import { RouteLink } from "../component/RouteLink";
import { heatmapApiCall, searchApiCall } from "../util/api";
import { City, Point } from "../util/const";
import { useBrowserDimensions, useMeasure } from "../util/dom";
import { usePromise } from "../util/fetch";
import { EdgePost, useEdgePosts } from "../util/item";
import { resultPointColor } from "../util/map";
import "./Search.css";

const EXAMPLE_QUERIES = [
  // Tech skills.
  [
    "how to write good documentation",
    "entering the tech industry",
    "getting into open source",
    "why learn rust",
    "why learn functional programming",
    "lessons learnt from typescript",
    "learning ffmpeg",
    "video codecs, how do they work?",
    "tabs vs spaces",
    "bash flaws",
    "books on distributed systems",
    "writing a programming language",
  ],
  // Entrepreneurship skills.
  [
    "lessons as a solo founder",
    "marketing for developers",
    "open source business model",
    "self bootstrapping",
    "startup postmortems",
  ],
  // History.
  [
    "what happened to wework",
    "google search result quality",
    "personal data leaks",
  ],
  // Life.
  [
    "using time wisely",
    "career growth",
    "organizing life",
    "good writing skills",
    "communicating well",
    "taking good photos",
    "what makes good music",
  ],
  // Random.
  [
    "concept art",
    "cool terminal things",
    "cool things with css",
    "linus rants",
    "pixel art",
  ],
];

type QueryResults = Array<{
  id: number;
  x: number;
  y: number;
  sim: number;
  final_score: number;
}>;

const QueryForm = ({
  onChangeInputFocused,
  onChangeQuery,
  onResults,
  query,
}: {
  onChangeInputFocused: (focused: boolean) => void;
  onChangeQuery: (query: string) => void;
  onResults: (results: QueryResults | undefined) => void;
  query: string;
}) => {
  const [queryRaw, setQueryRaw] = useState("");
  useEffect(() => setQueryRaw(query), [query]);

  const [weightSimilarity, setWeightSimilarity] = useState(0.7);
  const [weightScore, setWeightScore] = useState(0.1);
  const [weightTimestamp, setWeightTimestamp] = useState(0.2);
  const [decayTimestamp, setDecayTimestamp] = useState(0.1);

  const [showParams, setShowParams] = useState(false);

  const queryReq = usePromise<QueryResults>();
  useEffect(() => onResults(queryReq.data), [queryReq.data]);
  useEffect(() => {
    queryReq.set(async (signal) => {
      if (!query) {
        return;
      }
      return await searchApiCall(signal, {
        query,
        limit: 10,
        dataset: "toppost",
        weightSimilarity,
        weightScore,
        weightTimestamp,
        decayTimestamp,
      });
    });
  }, [query]);

  return (
    <form
      className="QueryForm"
      onSubmit={(e) => {
        e.preventDefault();
        onChangeQuery(queryRaw.trim());
      }}
    >
      <div className="main">
        <input
          className="query"
          onBlur={() => onChangeInputFocused(false)}
          onChange={(e) => setQueryRaw(e.currentTarget.value)}
          onFocus={() => onChangeInputFocused(true)}
          placeholder="Search or ask"
          value={queryRaw}
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
              onChangeQuery("");
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
            <Ico i="error" size={20} />
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
        <ColorInput
          color={color}
          onChange={(color) => onChangeColor(color)}
          size={24}
        />
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

const Result = ({ id, item }: { id: number; item: EdgePost }) => {
  if (!item || !item.ts || !item.author || !item.title) {
    return null;
  }
  const hnUrl = `https://news.ycombinator.com/item?id=${id}`;
  let url, site;
  if (item.url) {
    url = `${item.proto}//${item.url}`;
    site = item.url.split("/")[0];
  } else {
    url = hnUrl;
    site = "news.ycombinator.com";
  }
  const ts = DateTime.fromJSDate(item.ts);
  const ago = ts.toRelative();
  return (
    <div key={id} className="result">
      <div className="sup">
        <div
          className="color"
          style={{
            background: resultPointColor(id),
          }}
        />
        <div className="site">{site}</div>
      </div>
      <a href={url} target="_blank" rel="noopener noreferrer">
        <h1 dangerouslySetInnerHTML={{ __html: item.title ?? "" }} />
      </a>
      <p className="sub">
        <a href={hnUrl} target="_blank" rel="noopener noreferrer">
          {item.score} point{item.score == 1 ? "" : "s"}
        </a>{" "}
        by{" "}
        <a
          href={`https://news.ycombinator.com/user?id=${item.author}`}
          target="_blank"
          rel="noopener noreferrer"
        >
          {item.author}
        </a>{" "}
        {ago}
      </p>
    </div>
  );
};

export const SearchPage = () => {
  const onMobile = useBrowserDimensions().width < 800;

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

  const [query, setQuery] = useState("");
  const [queryInputFocused, setQueryInputFocused] = useState(false);

  const generateExampleQueries = () =>
    shuffleArray(EXAMPLE_QUERIES.map((set) => randomPick(set)));
  const [exampleQueries, setExampleQueries] = useState(generateExampleQueries);

  const [queryResults, setQueryResults] = useState<QueryResults>();
  const [shouldAnimateToResults, setShouldAnimateToResults] = useState(false);
  useEffect(() => {
    if (!shouldAnimateToResults) {
      return;
    }
    setShouldAnimateToResults(false);
    if (!queryResults?.length) {
      return;
    }
    let xMinPt = Infinity;
    let xMaxPt = -Infinity;
    let yMinPt = Infinity;
    let yMaxPt = -Infinity;
    for (const p of queryResults) {
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
  const [nearbyQuery, setNearbyQuery] = useState<{ x: number; y: number }>();
  const [nearbyResults, setNearbyResults] = useState<Array<Point | City>>();
  const itemIds = useMemo(
    () => [
      ...(queryResults?.map((r) => r.id) ?? []),
      ...(nearbyResults
        ?.filter((r): r is Point => "id" in r)
        .map((r) => r.id) ?? []),
    ],
    [queryResults, nearbyResults],
  );
  const items = useEdgePosts(itemIds);
  const results = nearbyResults ?? queryResults;

  return (
    <div ref={setRootElem} className="SearchPage">
      <PointMap
        controllerRef={mapCtl}
        heatmaps={heatmaps}
        height={rootDim?.height ?? 0}
        nearbyQuery={nearbyQuery}
        onNearbyQuery={setNearbyQuery}
        onNearbyQueryResults={setNearbyResults}
        resultPoints={nearbyQuery ? undefined : queryResults}
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
        <div className="query-container">
          <QueryForm
            query={query}
            onChangeInputFocused={setQueryInputFocused}
            onChangeQuery={(query) => {
              setQuery(query);
              setNearbyQuery(undefined);
              setNearbyResults(undefined);
            }}
            onResults={(results) => {
              setQueryResults(results);
              if (!results) {
                setExampleQueries(generateExampleQueries());
              }
              // Only animate when results come in, not for any other reason that `results` changes (e.g. clearing, deleting).
              if (results?.length && !nearbyQuery) {
                setShouldAnimateToResults(true);
              }
            }}
          />

          {!query && !nearbyQuery && (!onMobile || queryInputFocused) && (
            <div className="example-queries">
              <div className="header">
                <h2>Example queries</h2>
                <button
                  onMouseDown={(e) => {
                    // Don't lose focus on input, which dismisses these example queries on mobile.
                    e.preventDefault();
                    setExampleQueries(generateExampleQueries());
                  }}
                >
                  <Ico i="refresh" size={20} />
                </button>
              </div>
              <div className="list">
                {exampleQueries.map((query, i) => (
                  <button key={query} onMouseDown={() => setQuery(query)}>
                    {query}
                  </button>
                ))}
              </div>
            </div>
          )}

          {nearbyResults && queryResults && (
            <button
              className="back-to-search"
              onClick={() => {
                setNearbyQuery(undefined);
                setNearbyResults(undefined);
                setShouldAnimateToResults(true);
              }}
            >
              <Ico i="close" size={20} />
              <span>Back to search</span>
            </button>
          )}
        </div>

        <div className="results">
          {results?.map((r) =>
            "id" in r ? (
              mapExists(items[r.id], (item) => (
                <Result key={r.id} id={r.id} item={item} />
              ))
            ) : (
              <div key={r.label} className="result city-result">
                <RouteLink href={`/c/${encodeURIComponent(r.label)}`}>
                  <h1>{r.label}</h1>
                </RouteLink>
                <p>
                  <Ico i="open_in_new" size={24} />
                  <span>Visit community</span>
                </p>
              </div>
            ),
          )}
        </div>
      </div>
    </div>
  );
};
