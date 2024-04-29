import { VFiniteNumber, VInteger, VStruct } from "@wzlin/valid";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import defined from "@xtjs/lib/defined";
import mapExists from "@xtjs/lib/mapExists";
import mapNonEmpty from "@xtjs/lib/mapNonEmpty";
import withoutUndefined from "@xtjs/lib/withoutUndefined";
import { useEffect, useMemo, useRef, useState } from "react";
import Plot from "react-plotly.js";
import { Ico } from "../component/Ico";
import { Loading } from "../component/Loading";
import { ApiGroupByOutput, ApiItemsOutput, apiCall } from "../util/api";
import { useMeasure } from "../util/dom";
import { usePromise } from "../util/fetch";
import { useHnItems } from "../util/item";
import "./Analysis.css";

export const AnalysisPage = () => {
  const $chartContainer = useRef<HTMLDivElement>(null);
  const chartContainerRect = useMeasure($chartContainer.current);

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

  const [query, setQuery] = useState("");
  const [simThreshold, setSimThreshold] = useState(0.8);
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
        timestamps: [...data.groups()].map(
          (d) => new Date(d * 7 * 24 * 60 * 60 * 1000),
        ),
        positives: [...data.column("sent_pos_thresh")],
        negatives: [...data.column("sent_neg_thresh")],
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
      return [...data.items()].map((i) =>
        new VStruct({
          id: new VInteger(1),
          sim: new VFiniteNumber(),
        }).parseRoot(i),
      );
    });
  }, [query, simThreshold]);

  const [queryRaw, setQueryRaw] = useState("");
  const [areaMode, setAreaMode] = useState(false);

  const chartData = useMemo(
    () =>
      mapExists(sentReq.data, ({ negatives, positives, timestamps }) => {
        const common = {
          x: timestamps,
          stackgroup: areaMode ? ("one" as const) : undefined,
          groupnorm: areaMode ? ("percent" as const) : undefined,
          type: "scatter" as const,
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
        size: 14,
      },
      margin: {
        b: 28,
        l: 42,
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
      },
    }),
    [chartContainerRect, areaMode, topPostsReq.data, items],
  );

  return (
    <div className="AnalysisPage">
      <form
        className="query-form"
        onSubmit={(e) => {
          e.preventDefault();
          setQuery(queryRaw.trim());
        }}
      >
        <input
          value={queryRaw}
          onChange={(e) => setQueryRaw(e.currentTarget.value)}
        />
        {sentReq.loading ? (
          <Loading size={24} />
        ) : (
          <button type="submit">
            <Ico i="query_stats" />
          </button>
        )}
      </form>

      <label>
        <input
          type="checkbox"
          checked={areaMode}
          onChange={(e) => setAreaMode(e.currentTarget.checked)}
        />
        <span>Area mode</span>
      </label>
      <label>
        <input
          type="range"
          min={0.8}
          max={1}
          step={0.01}
          value={simThreshold}
          onChange={(e) => setSimThreshold(e.currentTarget.valueAsNumber)}
        />
        <span>Similarity threshold</span>
      </label>

      <p>
        If any labelled post isn't relevant, press it to dismiss it and tune the
        similarity threshold.
      </p>
      <div ref={$chartContainer}>
        {chartData && (
          <Plot
            data={chartData}
            layout={chartLayout}
            onClickAnnotation={(a) => {
              const post = topPosts?.at(a.index);
              if (post) {
                setSimThreshold(post.sim + 0.01);
              }
            }}
          />
        )}
      </div>
    </div>
  );
};
