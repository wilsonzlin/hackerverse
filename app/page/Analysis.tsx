import { VFiniteNumber, VInteger, VString } from "@wzlin/valid";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import bounded from "@xtjs/lib/bounded";
import defined from "@xtjs/lib/defined";
import mapExists from "@xtjs/lib/mapExists";
import mapNonEmpty from "@xtjs/lib/mapNonEmpty";
import maybeParseNumber from "@xtjs/lib/maybeParseNumber";
import withoutUndefined from "@xtjs/lib/withoutUndefined";
import { produce } from "immer";
import { useEffect, useMemo, useRef, useState } from "react";
import Plot from "react-plotly.js";
import { Ico } from "../component/Ico";
import { Loading } from "../component/Loading";
import { PageSwitcher } from "../component/PageSwitcher";
import { ApiGroupByOutput, ApiItemsOutput, apiCall } from "../util/api";
import { useMeasure } from "../util/dom";
import { usePromise } from "../util/fetch";
import { useHnItems } from "../util/item";
import "./Analysis.css";

const SIM_THRESHOLD_MIN = 0.8;
const SIM_THRESHOLD_MAX = 1;

const SentimentSection = ({
  areaMode,
  query,
  simThreshold,
  onChangeSimThreshold,
}: {
  areaMode: boolean;
  query: string;
  simThreshold: number;
  onChangeSimThreshold: (v: number) => void;
}) => {
  const [$chartContainer, setChartContainerElem] =
    useState<HTMLDivElement | null>(null);
  const chartContainerRect = useMeasure($chartContainer);

  const sentReq = usePromise<{
    timestamps: Date[];
    positives: number[];
    negatives: number[];
  }>();
  const topPostsReq = usePromise<Array<{ id: number; sim: number }>>();
  const items = useHnItems(topPostsReq.data?.map((i) => i.id) ?? []);
  const topPosts = topPostsReq.data
    ?.map(({ id, sim }) =>
      mapExists(items[id], (item) =>
        mapNonEmpty(item.title ?? "", () => ({ ...item, sim })),
      ),
    )
    .filter(defined);

  useEffect(() => {
    sentReq.set(async (signal) => {
      const res = await apiCall(signal, {
        dataset: "comments",
        queries: [query].filter(defined),
        thresholds: withoutUndefined({
          sim: mapExists(query, () => simThreshold),
          sent_pos: 0.5,
          sent_neg: 0.5,
        }),
        post_filter_clip: mapExists(query, () => ({
          sim_thresh: { min: 1.0, max: 1.0 },
        })),
        outputs: [
          {
            group_by: {
              by: "ts_day",
              bucket: 7,
              cols: [
                ["sent_pos_thresh", "sum"],
                ["sent_neg_thresh", "sum"],
              ],
            },
          },
        ],
      });
      const data = assertInstanceOf(res[0], ApiGroupByOutput);
      return {
        timestamps: [...data.groups(new VInteger())].map(
          (d) => new Date(d * 7 * 24 * 60 * 60 * 1000),
        ),
        positives: [...data.column("sent_pos_thresh", new VFiniteNumber())],
        negatives: [...data.column("sent_neg_thresh", new VFiniteNumber())],
      };
    });

    topPostsReq.set(async (signal) => {
      if (!query) {
        return;
      }
      const res = await apiCall(signal, {
        dataset: "posts",
        queries: [query],
        post_filter_clip: {
          sim: { min: simThreshold, max: 1 },
        },
        outputs: [
          {
            items: {
              cols: ["id", "sim"],
              limit: 8,
              order_asc: false,
              order_by: "votes",
            },
          },
        ],
      });
      const data = assertInstanceOf(res[0], ApiItemsOutput);
      return [
        ...data.items({
          id: new VInteger(1),
          sim: new VFiniteNumber(),
        }),
      ];
    });
  }, [query, simThreshold]);

  const chartData = useMemo(
    () =>
      mapExists(sentReq.data, ({ negatives, positives, timestamps }) => {
        const common = {
          x: timestamps,
          stackgroup: areaMode ? ("one" as const) : undefined,
          groupnorm: areaMode ? ("percent" as const) : undefined,
          type: areaMode ? ("scatter" as const) : ("bar" as const),
          line: areaMode ? { width: 0 } : undefined,
          hoverlabel: {
            font: {
              family: "InterVariable",
            },
          },
        };
        return [
          {
            ...common,
            name: "Positive",
            y: positives,
            marker: { color: "#30D5C8" },
          },
          {
            ...common,
            name: "Negative",
            y: negatives,
            marker: { color: "#800020" },
          },
        ];
      }),
    [sentReq.data, areaMode],
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
            (url) => `<br><sub>${new URL(url).hostname}</sub>`,
          ),
        ].join(""),
        x: item.time?.toISOString(),
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
        x0: item.time?.toISOString(),
        x1: item.time?.toISOString(),
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
          text: areaMode ? "Percentage of comments" : "Comments",
          size: 14,
        },
      },
    }),
    [chartContainerRect, areaMode, topPostsReq.data, items],
  );

  return (
    <section>
      <h2>
        <Ico i="sentiment_satisfied" size={24} />
        <span>Sentiment over time</span>
      </h2>
      {!!query && (
        <div className="info">
          <Ico i="info" size={20} />
          <p>
            If any labelled post isn't relevant, select it to dismiss it and
            tune the similarity threshold.
          </p>
        </div>
      )}
      {sentReq.loading && <Loading size={24} />}
      {sentReq.error && <p className="err">{sentReq.error}</p>}
      {chartData && (
        <div ref={setChartContainerElem}>
          <Plot
            data={chartData}
            layout={chartLayout}
            onClickAnnotation={(a) => {
              const post = topPosts?.at(a.index);
              if (post) {
                onChangeSimThreshold(post.sim + 0.01);
              }
            }}
          />
        </div>
      )}
    </section>
  );
};

const TopUsersSection = ({
  query,
  simThreshold,
}: {
  query: string;
  simThreshold: number;
}) => {
  const req = usePromise<Array<{ user: string; score: number }>>();
  useEffect(() => {
    if (!query) {
      req.clear();
      return;
    }
    req.set(async (signal) => {
      const res = await apiCall(signal, {
        dataset: "comments",
        queries: [query],
        scales: {
          sim: {
            min: simThreshold,
            max: 1,
          },
        },
        weights: {
          // We can't multiply by votes, because the HN API does not expose votes for anything except posts.
          sim_scaled: 1,
        },
        outputs: [
          {
            group_by: {
              by: "user",
              cols: [["final_score", "sum"]],
              order_by: "final_score",
              order_asc: false,
              limit: 20,
            },
          },
        ],
      });
      const data = assertInstanceOf(res[0], ApiGroupByOutput);
      return [
        ...data.entries(new VString(), {
          final_score: new VFiniteNumber(),
        }),
      ].map((e) => ({
        user: e[0],
        score: e[1].final_score,
      }));
    });
  }, [query, simThreshold]);

  if (!query) {
    return null;
  }

  return (
    <section>
      <h2>
        <Ico i="social_leaderboard" size={24} />
        <span>Top users</span>
      </h2>
      {req.loading && <Loading size={24} />}
      {req.error && <p className="err">{req.error}</p>}
      {req.data && (
        <table>
          <thead>
            <tr>
              <th>Rank</th>
              <th>User</th>
              <th>Score</th>
            </tr>
          </thead>
          <tbody>
            {req.data.map((r, i) => (
              <tr key={i}>
                <th>{i + 1}</th>
                <td>
                  <a
                    href={`https://news.ycombinator.com/user?id=${encodeURIComponent(r.user)}`}
                    target="_blank"
                    rel="noopener"
                  >
                    {r.user}
                  </a>
                </td>
                <td>{r.score}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
};

type PopularitySectionQueryResults = {
  timestamps: Date[];
  scores: number[];
};

const PopularitySectionQuery = ({
  onChangeQuery,
  onDelete,
  onResults,
  query,
  readonly,
  simThreshold,
}: {
  onChangeQuery: (q: string) => void;
  onDelete: () => void;
  onResults: (results: PopularitySectionQueryResults | undefined) => void;
  query: string;
  readonly: boolean;
  simThreshold: number;
}) => {
  const [queryRaw, setQueryRaw] = useState(query);
  useEffect(() => setQueryRaw(query), [query]);

  const req = usePromise<{
    timestamps: Date[];
    scores: number[];
  }>();
  useEffect(() => {
    req.set(async (signal) => {
      if (!query) {
        onResults(undefined);
        return;
      }
      const res = await apiCall(signal, {
        dataset: "posts",
        queries: [query],
        scales: {
          sim: { min: simThreshold, max: 1.0 },
        },
        post_filter_clip: {
          sim: { min: simThreshold, max: 1.0 },
          // Some posts have UNIX timestamp 0.
          ts_day: { min: 1, max: Number.MAX_SAFE_INTEGER },
        },
        weights: {
          votes: "sim",
        },
        outputs: [
          {
            group_by: {
              by: "ts_day",
              bucket: 7,
              cols: [["final_score", "sum"]],
            },
          },
        ],
      });
      const data = assertInstanceOf(res[0], ApiGroupByOutput);
      const results = {
        timestamps: [...data.groups(new VInteger())].map(
          (d) => new Date(d * 7 * 24 * 60 * 60 * 1000),
        ),
        scores: [...data.column("final_score", new VFiniteNumber())],
      };
      onResults(results);
      return results;
    });
  }, [query, simThreshold]);

  return (
    <div className="PopularitySectionQuery">
      {req.error && <p className="err">{req.error}</p>}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          onChangeQuery(queryRaw.trim());
        }}
      >
        {req.loading && <Loading size={16} />}
        <input
          value={queryRaw}
          onChange={(e) => setQueryRaw(e.currentTarget.value)}
          readOnly={readonly}
        />
        {!readonly && (
          <button type="button" onClick={onDelete}>
            <Ico i="delete" />
          </button>
        )}
      </form>
    </div>
  );
};

const PopularitySection = ({
  query,
  simThreshold,
}: {
  query: string;
  simThreshold: number;
}) => {
  const [$chartContainer, setChartContainerElem] =
    useState<HTMLDivElement | null>(null);
  const chartContainerRect = useMeasure($chartContainer);

  // ID -1 is reserved for main query.
  const nextQueryId = useRef(0);
  type QueryState = {
    id: number;
    query: string;
    results: PopularitySectionQueryResults | undefined;
  };
  const [queries, setQueries] = useState<Array<QueryState>>([
    { id: -1, query, results: undefined },
  ]);
  useEffect(
    () =>
      setQueries((queries) =>
        produce(queries, (queries) => {
          for (const q of queries) {
            if (q.id === -1) {
              q.query = query;
              break;
            }
          }
        }),
      ),
    [query],
  );

  const chartData = useMemo(
    () =>
      queries
        .map((q) =>
          mapExists(q.results, ({ scores, timestamps }) => ({
            x: timestamps,
            hoverlabel: {
              font: {
                family: "InterVariable",
              },
            },
            name: q.query,
            y: scores,
          })),
        )
        .filter(defined),
    [queries],
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

  if (!query) {
    return null;
  }

  return (
    <section className="PopularitySection">
      <h2>
        <Ico i="trending_up" size={24} />
        <span>Popularity over time</span>
      </h2>
      <div className="queries">
        {queries.map((q) => {
          const id = q.id;
          const mutQ = (fn: (q: QueryState) => unknown) =>
            setQueries((queries) =>
              produce(queries, (queries) => {
                for (const q2 of queries) {
                  if (q2.id === id) {
                    fn(q2);
                    break;
                  }
                }
              }),
            );
          return (
            <PopularitySectionQuery
              key={id}
              onChangeQuery={(query) => mutQ((q) => (q.query = query))}
              onDelete={() =>
                setQueries((queries) => queries.filter((q2) => q2.id !== id))
              }
              onResults={(results) => mutQ((q) => (q.results = results))}
              query={q.query}
              readonly={id === -1}
              simThreshold={simThreshold}
            />
          );
        })}
        <button
          className="add-comparison"
          type="button"
          onClick={() => {
            setQueries((queries) => [
              ...queries,
              { id: nextQueryId.current++, query: "", results: undefined },
            ]);
          }}
        >
          <Ico i="add" size={20} />
          <span>Add comparison</span>
        </button>
      </div>
      {chartData && (
        <div ref={setChartContainerElem}>
          <Plot data={chartData} layout={chartLayout} />
        </div>
      )}
    </section>
  );
};

export const AnalysisPage = () => {
  const [query, setQuery] = useState("");
  const [simThreshold, setSimThreshold_doNotUse] = useState(SIM_THRESHOLD_MIN);
  const setSimThreshold = (v: number) => {
    v = bounded(v, SIM_THRESHOLD_MIN, SIM_THRESHOLD_MAX);
    setSimThresholdRaw(toSimThresholdRaw(v));
    setSimThreshold_doNotUse(v);
  };

  const toSimThresholdRaw = (v: number) => v.toFixed(2);
  const [queryRaw, setQueryRaw] = useState("");
  const [simThresholdRaw, setSimThresholdRaw] = useState(
    toSimThresholdRaw(simThreshold),
  );
  const [areaMode, setAreaMode] = useState(false);

  return (
    <div className="AnalysisPage">
      <section>
        <form
          className="query-form"
          onSubmit={(e) => {
            e.preventDefault();
            setQuery(queryRaw.trim());
          }}
        >
          <input
            placeholder="Query"
            value={queryRaw}
            onChange={(e) => setQueryRaw(e.currentTarget.value)}
          />
          <button type="submit">
            <Ico i="query_stats" size={20} />
          </button>
        </form>

        <div className="controls">
          <label>
            <input
              type="checkbox"
              checked={areaMode}
              onChange={(e) => setAreaMode(e.currentTarget.checked)}
            />
            <span>Area graph</span>
          </label>
          <label hidden={!query}>
            <span>Similarity threshold</span>
            <input
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
                  setSimThreshold(parsed);
                }
              }}
            />
          </label>
        </div>
      </section>

      <SentimentSection
        areaMode={areaMode}
        query={query}
        simThreshold={simThreshold}
        onChangeSimThreshold={setSimThreshold}
      />

      <div className="layout-row">
        <TopUsersSection query={query} simThreshold={simThreshold} />
        <PopularitySection query={query} simThreshold={simThreshold} />
      </div>

      <PageSwitcher />
    </div>
  );
};
