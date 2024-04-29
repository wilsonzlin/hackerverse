import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import { useState } from "react";
import { Line } from "react-chartjs-2";
import { Ico } from "../component/Ico";
import { Loading } from "../component/Loading";
import { ApiGroupByOutput, apiCall } from "../util/api";
import { usePromise } from "../util/fetch";
import "./Analysis.css";

const CHART_OPTIONS = {
  responsive: true,
  scales: {
    x: {
      type: "time",
    },
  },
} as const;

export const AnalysisPage = () => {
  const req = usePromise();

  const [queryRaw, setQueryRaw] = useState("");
  const [chartData, setChartData] = useState<{
    labels: Date[];
    datasets: Array<{
      label: string;
      data: number[];
      backgroundColor?: string;
      tension?: number;
    }>;
  }>();

  return (
    <div className="AnalysisPage">
      <form
        className="query-form"
        onSubmit={(e) => {
          e.preventDefault();
          const query = queryRaw.trim();
          if (!query) {
            req.clear();
            return;
          }
          req.set(async (signal) => {
            const res = await apiCall(signal, {
              dataset: "comments",
              queries: [query],
              sim_scale: { min: 0, max: 1 },
              thresholds: {
                sim: 0.7,
                sent_pos: 0.5,
                sent_neg: 0.5,
              },
              post_filter_clip: {
                sim_thresh: { min: 1.0, max: 1.0 },
              },
              outputs: [
                {
                  group_by: {
                    by: "ts_day",
                    cols: [
                      ["sent_pos", "sum"],
                      ["sent_neg", "sum"],
                    ],
                  },
                },
              ],
            });
            const data = assertInstanceOf(res[0], ApiGroupByOutput);
            setChartData({
              labels: [...data.groups()].map(
                (d) => new Date(d * 24 * 60 * 60 * 1000),
              ),
              datasets: [
                {
                  label: "Positive",
                  data: [...data.column("sent_pos")],
                  backgroundColor: "green",
                },
                {
                  label: "Negative",
                  data: [...data.column("sent_neg")],
                  backgroundColor: "red",
                },
              ],
            });
          });
        }}
      >
        <input
          value={queryRaw}
          onChange={(e) => setQueryRaw(e.currentTarget.value)}
        />
        {req.loading ? (
          <Loading size={24} />
        ) : (
          <button type="submit">
            <Ico i="query_stats" />
          </button>
        )}
      </form>

      {chartData && (
        <div>
          <p>{chartData.labels.length} datapoints</p>
          <Line data={chartData} options={CHART_OPTIONS} />
        </div>
      )}
    </div>
  );
};
