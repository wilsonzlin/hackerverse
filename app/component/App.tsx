import { Item, vItem } from "@wzlin/crawler-toolkit-hn";
import { VArray, VFiniteNumber, VInteger, VStruct } from "@wzlin/valid";
import assertInstanceOf from "@xtjs/lib/assertInstanceOf";
import mapExists from "@xtjs/lib/mapExists";
import { DateTime } from "luxon";
import { useEffect, useState } from "react";
import { useMeasure } from "../util/dom";
import { useRequest } from "../util/fetch";
import "./App.css";
import { Ico } from "./Ico";
import { Loading } from "./Loading";
import { PointMap } from "./PointMap";

export const App = () => {
  const [$root, setRootElem] = useState<HTMLDivElement | null>(null);
  const rootDim = useMeasure($root);

  const [query, setQuery] = useState<{
    query: string;
    weightSimilarity: number;
    weightScore: number;
    weightTimestamp: number;
    decayTimestamp: number;
  }>();
  const results = useRequest(
    "search",
    new VStruct({
      results: new VArray(
        new VStruct({
          id: new VInteger(1),
          score: new VFiniteNumber(),
        }),
      ),
    }),
  );
  useEffect(() => {
    if (!query) {
      results.clear();
    } else {
      const timeout = setTimeout(() => {
        results.go({
          ...query,
          limit: 10,
          dataset: "posts_bgem3",
        });
      }, 250);
      return () => clearTimeout(timeout);
    }
  }, [query]);

  const [items, setItems] = useState<Record<number, Item>>({});
  useEffect(() => {
    const ids = results.data?.results.map((r) => r.id) ?? [];
    const ac = new AbortController();
    (async () => {
      await Promise.all(
        ids.map(async (id) => {
          const res = await fetch(
            `https://hacker-news.firebaseio.com/v0/item/${id}.json`,
            {
              signal: ac.signal,
            },
          );
          const raw = await res.json();
          const item = vItem.parseRoot(raw);
          setItems((items) => ({ ...items, [id]: item }));
        }),
      );
    })();
    return () => ac.abort();
  }, [results.data]);

  return (
    <div ref={setRootElem} className="App">
      <PointMap height={rootDim?.height ?? 0} width={rootDim?.width ?? 0} />

      <form
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
          <button disabled={results.loading} type="submit">
            {results.loading ? <Loading size={24} /> : <Ico i="search" />}
          </button>
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
        {mapExists(results.error, (error) => (
          <p className="err">{error}</p>
        ))}

        {results.data?.results.map((r) => {
          const item = items[r.id];
          if (!item || !item.time || !item.by || !item.title) {
            return;
          }
          const hnUrl = `https://news.ycombinator.com/item?id=${item.id}`;
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
            <div key={r.id} className="result">
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
