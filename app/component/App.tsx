import { Item, vItem } from "@wzlin/crawler-toolkit-hn";
import { VArray, VFiniteNumber, VInteger, VStruct } from "@wzlin/valid";
import mapExists from "@xtjs/lib/mapExists";
import { useEffect, useState } from "react";
import { useMeasure } from "../util/dom";
import { useRequest } from "../util/fetch";
import "./App.css";
import { PointMap } from "./PointMap";

export const App = () => {
  const [$root, setRootElem] = useState<HTMLDivElement | null>(null);
  const rootDim = useMeasure($root);

  const [queryInput, setQueryInput] = useState("");
  const [query, setQuery] = useState("");
  const results = useRequest(
    "search",
    new VStruct({
      results: new VArray(
        new VStruct({
          id: new VInteger(1),
          distance: new VFiniteNumber(),
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
          query,
          limit: 10,
          dataset: "posts",
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
          setQuery(queryInput.trim());
        }}
      >
        <input
          placeholder="Search or ask"
          value={queryInput}
          onChange={(e) => setQueryInput(e.currentTarget.value)}
        />
        <button type="submit">Search</button>
      </form>

      <div className="results">
        {results.loading && <p>Loading...</p>}

        {mapExists(results.error, (error) => (
          <p className="err">{error}</p>
        ))}

        {results.data?.results.map((r) => (
          <div key={r.id}>
            <p>{r.id}</p>
            <p>{r.distance}</p>
            {mapExists(items[r.id], (item) => (
              <div>
                <h1 dangerouslySetInnerHTML={{ __html: item.title ?? "" }} />
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
};
