import { Item, vItem } from "@wzlin/crawler-toolkit-hn";
import { VArray, VFiniteNumber, VInteger, VStruct } from "@wzlin/valid";
import mapExists from "@xtjs/lib/mapExists";
import { useEffect, useState } from "react";
import { useRequest } from "../util/fetch";
import "./App.css";

export const App = () => {
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
          limit: 128,
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
    <div className="App">
      <form
        onSubmit={(e) => {
          e.preventDefault();
          setQuery(queryInput.trim());
        }}
      >
        <input
          value={queryInput}
          onChange={(e) => setQueryInput(e.currentTarget.value)}
        />
        <button type="submit">Search</button>
      </form>

      {results.loading && <p>Loading...</p>}

      {mapExists(results.error, (error) => (
        <p className="err">{error}</p>
      ))}

      <div>
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
