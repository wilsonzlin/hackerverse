import bounded from "@xtjs/lib/bounded";
import defined from "@xtjs/lib/defined";
import mapExists from "@xtjs/lib/mapExists";
import mapNonEmpty from "@xtjs/lib/mapNonEmpty";
import maybeParseNumber from "@xtjs/lib/maybeParseNumber";
import shuffleArray from "@xtjs/lib/shuffleArray";
import { useEffect, useMemo, useState } from "react";
import Plot from "react-plotly.js";
import seedrandom from "seedrandom";
import { Ico } from "../component/Ico";
import { Loading } from "../component/Loading";
import { PageSwitcher } from "../component/PageSwitcher";
import {
  analyzePopularityApiCall,
  analyzeSentimentApiCall,
  postsApiCall,
} from "../util/api";
import { useMeasure } from "../util/dom";
import { usePromise } from "../util/fetch";
import { useEdgePosts } from "../util/item";
import { router } from "../util/router";
import "./Analysis.css";

const SIM_THRESHOLD_MIN = 0.8;
const SIM_THRESHOLD_MAX = 1;

const queryColor = (query: string) => {
  const rng = seedrandom(query);
  return `hsl(${Math.floor(rng() * 360)}, 92%, 50%)`;
};

type PopularityQueryResults = {
  timestamps: Date[];
  scores: number[];
};

type SentimentQueryResults = {
  timestamps: Date[];
  positives: number[];
  negatives: number[];
};

type TopPost = {
  id: number;
  sim: number;
  title: string;
  ts: Date;
  url?: string;
};

type QueryResults = {
  popularity?: PopularityQueryResults;
  sentiment?: SentimentQueryResults;
  topPosts?: TopPost[];
};

const Query = ({
  onChangeQuery,
  onChangeSimThreshold,
  onDelete,
  onResults,
  query,
  simThreshold = SIM_THRESHOLD_MIN,
}: {
  onChangeQuery: (q: string) => void;
  onChangeSimThreshold: (v: number) => void;
  onDelete: () => void;
  onResults: (results: QueryResults) => void;
  query: string;
  simThreshold?: number;
}) => {
  const toSimThresholdRaw = (v: number) => v.toFixed(2);
  const [simThresholdRaw, setSimThresholdRaw] = useState(
    toSimThresholdRaw(simThreshold),
  );
  useEffect(
    () => setSimThresholdRaw(toSimThresholdRaw(simThreshold)),
    [simThreshold],
  );
  const simMinHundredths = Math.ceil(simThreshold * 100);

  const [queryRaw, setQueryRaw] = useState(query);
  useEffect(() => setQueryRaw(query), [query]);

  const popReq = usePromise<{
    timestamps: Date[];
    scores: number[];
  }>();
  useEffect(() => {
    popReq.set(async (signal) => {
      if (!query) {
        return;
      }
      return await analyzePopularityApiCall(signal, {
        query,
        simMinHundredths,
      });
    });
  }, [query, simMinHundredths]);

  const sentReq = usePromise<{
    timestamps: Date[];
    positives: number[];
    negatives: number[];
  }>();
  useEffect(() => {
    sentReq.set(async (signal) => {
      if (!query) {
        return;
      }
      return await analyzeSentimentApiCall(signal, {
        query,
        simMinHundredths,
      });
    });
  }, [query, simMinHundredths]);

  const [showTopPosts, setShowTopPosts] = useState(true);
  const topPostsReq = usePromise<Array<{ id: number; sim: number }>>();
  const items = useEdgePosts(topPostsReq.data?.map((i) => i.id) ?? []);
  const topPosts = useMemo(
    () =>
      topPostsReq.data
        ?.map(({ id, sim }) =>
          mapExists(items[id], (item) =>
            mapNonEmpty(item.title ?? "", () => ({ ...item, id, sim })),
          ),
        )
        .filter(defined),
    [topPostsReq.data, items],
  );
  useEffect(() => {
    topPostsReq.set(async (signal) => {
      if (!query) {
        return;
      }
      return await postsApiCall(signal, {
        limit: 8,
        query,
        simMinHundredths,
        orderBy: "votes",
      });
    });
  }, [query, simMinHundredths]);

  useEffect(() => {
    onResults({
      popularity: popReq.data,
      sentiment: sentReq.data,
      topPosts: showTopPosts ? topPosts : undefined,
    });
  }, [popReq.data, sentReq.data, topPosts, showTopPosts]);

  const loading = popReq.loading || sentReq.loading;
  const error = popReq.error || sentReq.error;

  return (
    <form
      className="query"
      onSubmit={(e) => {
        e.preventDefault();
        onChangeQuery(queryRaw.trim());
      }}
    >
      {error ? (
        <p className="err" onClick={() => alert(error)}>
          <Ico i="error" />
        </p>
      ) : query ? (
        <div
          className="color"
          style={{
            background: queryColor(query),
          }}
        />
      ) : null}
      <input
        className="value"
        value={queryRaw}
        placeholder="Query"
        onChange={(e) => setQueryRaw(e.currentTarget.value)}
      />
      <button
        type="button"
        onClick={() => setShowTopPosts(!showTopPosts)}
        style={{
          opacity: showTopPosts ? 1 : 0.4,
        }}
      >
        <Ico i="history" size={22} />
      </button>
      <input
        className="sim"
        type="text"
        min={SIM_THRESHOLD_MIN}
        max={SIM_THRESHOLD_MAX}
        step={0.01}
        value={simThresholdRaw}
        onChange={(e) => setSimThresholdRaw(e.currentTarget.value)}
        onBlur={(e) => {
          const parsed = maybeParseNumber(e.currentTarget.value.trim());
          if (!parsed || !Number.isFinite(parsed)) {
            setSimThresholdRaw(toSimThresholdRaw(simThreshold));
          } else {
            onChangeSimThreshold(
              bounded(parsed, SIM_THRESHOLD_MIN, SIM_THRESHOLD_MAX),
            );
          }
        }}
      />
      {loading && <Loading size={18} />}
      <button type="button" onClick={onDelete}>
        <Ico i="delete" size={22} />
      </button>
    </form>
  );
};

const SentimentSection = ({
  onChangeSimThreshold,
  results,
  topPosts,
}: {
  onChangeSimThreshold: (query: string, v: number) => void;
  results: {
    data: SentimentQueryResults;
    query: string;
  }[];
  topPosts?: (TopPost & {
    query: string;
  })[];
}) => {
  const [$chartContainer, setChartContainerElem] =
    useState<HTMLDivElement | null>(null);
  const chartContainerRect = useMeasure($chartContainer);

  const chartData = useMemo(
    () =>
      results.flatMap(
        ({ query, data: { positives, negatives, timestamps } }) => {
          const common = {
            x: timestamps,
            type: "scatter" as const,
            line: {
              width: 1,
            },
            hoverlabel: {
              font: {
                family: "InterVariable",
              },
            },
          };
          return [
            {
              ...common,
              name: `${query} (positive)`,
              y: positives,
              marker: { color: queryColor(query) },
            },
            {
              ...common,
              name: `${query} (negative)`,
              y: negatives.map((v) => -v),
              marker: { color: queryColor(query) },
            },
          ];
        },
      ),
    [results],
  );
  const chartLayout = useMemo(
    () => ({
      autosize: false,
      barmode: "stack" as const,
      height: 640,
      hovermode: "x" as const,
      showlegend: false,
      width: chartContainerRect?.width,
      annotations: topPosts?.map((item, i) => ({
        align: "right" as const,
        bgcolor: "rgba(255, 255, 255, 0.9)",
        captureevents: true,
        showarrow: false,
        text: [
          `<b>${item.title}</b>`,
          mapNonEmpty(
            item.url ?? "",
            (url) => `<br><sub>${url.split("/")[0]}</sub>`,
          ),
        ].join(""),
        x: item.ts.toISOString(),
        xanchor: "right" as const,
        xref: "x" as const,
        y: (10 - (i % 10)) / 10,
        yref: "paper" as const,
      })),
      font: {
        family: "InterVariable",
        size: 12,
      },
      margin: {
        b: 28,
        l: 70,
        pad: 0,
        r: 14,
        t: 14,
      },
      shapes: topPosts?.map((item) => ({
        type: "line" as const,
        x0: item.ts.toISOString(),
        x1: item.ts.toISOString(),
        xref: "x" as const,
        y0: 0,
        y1: 1,
        yref: "paper" as const,
        line: {
          color: "rgba(0, 0, 0, 0.5)",
          width: 1,
          dash: "dot" as const,
        },
      })),
      xaxis: {
        showgrid: false,
      },
      yaxis: {
        showgrid: false,
        title: {
          text: "Comments",
          size: 14,
        },
      },
    }),
    [chartContainerRect, topPosts],
  );

  return (
    <section>
      <h2>
        <Ico i="sentiment_satisfied" size={24} />
        <span>Sentiment over time</span>
      </h2>
      <div className="info">
        <Ico i="info" size={20} />
        <p>
          If any labelled post isn't relevant, select it to dismiss it and tune
          the similarity threshold.
        </p>
      </div>
      <div ref={setChartContainerElem}>
        <Plot
          data={chartData}
          layout={chartLayout}
          onClickAnnotation={(a) => {
            const post = topPosts?.at(a.index);
            if (post) {
              onChangeSimThreshold(post.query, post.sim + 0.01);
            }
          }}
        />
      </div>
    </section>
  );
};

const PopularitySection = ({
  results,
}: {
  results: {
    query: string;
    data: PopularityQueryResults;
  }[];
}) => {
  const [$chartContainer, setChartContainerElem] =
    useState<HTMLDivElement | null>(null);
  const chartContainerRect = useMeasure($chartContainer);

  const chartData = useMemo(
    () =>
      results.map(({ query, data: { scores, timestamps } }) => ({
        x: timestamps,
        line: {
          width: 1,
        },
        hoverlabel: {
          font: {
            family: "InterVariable",
          },
        },
        name: query,
        y: scores,
        marker: {
          color: queryColor(query),
        },
      })),
    [results],
  );
  const chartLayout = useMemo(
    () => ({
      autosize: false,
      height: 500,
      hovermode: "x" as const,
      showlegend: true,
      width: chartContainerRect?.width,
      font: {
        family: "InterVariable",
        size: 12,
      },
      legend: {
        orientation: "h" as const,
      },
      margin: {
        b: 28,
        l: 56,
        pad: 0,
        r: 14,
        t: 14,
      },
      xaxis: {
        showgrid: false,
      },
      yaxis: {
        showgrid: false,
        title: {
          text: "Post votes",
          size: 14,
        },
      },
    }),
    [chartContainerRect],
  );

  return (
    <section className="PopularitySection">
      <h2>
        <Ico i="trending_up" size={24} />
        <span>Popularity over time</span>
      </h2>
      <div ref={setChartContainerElem}>
        <Plot data={chartData} layout={chartLayout} />
      </div>
    </section>
  );
};

export const AnalysisPage = ({ params }: { params: string[] }) => {
  const queries = useMemo(() => params.filter((q) => q), [params]);

  const [simThresholds, setSimThresholds] = useState<Record<string, number>>(
    {},
  );
  useEffect(() => {
    const newThresholds: Record<string, number> = {};
    for (const q of queries) {
      newThresholds[q] = simThresholds[q] ?? {};
    }
    setSimThresholds(simThresholds);
  }, [queries]);

  const [queryResults, setQueryResults] = useState<
    Record<string, QueryResults>
  >({});
  useEffect(() => {
    const newResults: Record<string, QueryResults> = {};
    for (const q of queries) {
      newResults[q] = queryResults[q] ?? {};
    }
    setQueryResults(newResults);
  }, [queries]);

  const sentResults = useMemo(
    () =>
      queries
        .map((q) =>
          mapExists(queryResults[q]?.sentiment, (data) => ({ query: q, data })),
        )
        .filter(defined),
    [queryResults],
  );
  const popResults = useMemo(
    () =>
      queries
        .map((q) =>
          mapExists(queryResults[q]?.popularity, (data) => ({
            query: q,
            data,
          })),
        )
        .filter(defined),
    [queryResults],
  );
  const topPosts = useMemo(
    () =>
      shuffleArray(
        queries.flatMap(
          (q) =>
            queryResults[q]?.topPosts?.map((p) => ({ ...p, query: q })) ?? [],
        ),
      ).slice(0, 12),
    [queryResults],
  );

  const [newQuery, setNewQuery] = useState("");

  const go = (newQueries: string[]) => {
    router.change(
      "/a/" +
        newQueries
          .sort()
          .map((q) => encodeURIComponent(q))
          .join("/"),
    );
  };

  return (
    <div className="AnalysisPage">
      <div className="queries">
        {queries.map((q) => (
          <Query
            key={q}
            onChangeQuery={(newQuery) =>
              go(queries.filter((oq) => oq !== q).concat(newQuery))
            }
            onDelete={() => go(queries.filter((oq) => oq !== q))}
            onResults={(results) =>
              setQueryResults((qr) => ({
                ...qr,
                [q]: results,
              }))
            }
            onChangeSimThreshold={(v) =>
              setSimThresholds((st) => ({
                ...st,
                [q]: v,
              }))
            }
            simThreshold={simThresholds[q]}
            query={q}
          />
        ))}

        <form
          className="query"
          onSubmit={(e) => {
            e.preventDefault();
            const q = newQuery.trim();
            if (q) {
              go(queries.concat(q));
            }
            setNewQuery("");
          }}
        >
          <input
            className="new-query"
            placeholder="Query"
            value={newQuery}
            onChange={(e) => setNewQuery(e.currentTarget.value)}
          />
        </form>
      </div>

      <SentimentSection
        results={sentResults}
        topPosts={topPosts}
        onChangeSimThreshold={(query, v) =>
          setSimThresholds((st) => ({
            ...st,
            [query]: v,
          }))
        }
      />

      <PopularitySection results={popResults} />

      <PageSwitcher />
    </div>
  );
};
