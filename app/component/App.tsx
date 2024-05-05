import { useEffect, useMemo, useState } from "react";
import { AnalysisPage } from "../page/Analysis";
import { CityPage } from "../page/City";
import { NotFoundPage } from "../page/NotFound";
import { SearchPage } from "../page/Search";
import { DEFAULT_EDGE, EdgeContext, findClosestEdge } from "../util/item";
import { router } from "../util/router";
import "./App.css";

export const App = () => {
  const [path, setPath] = useState(location.pathname);
  useEffect(() => {
    const handler = () => setPath(location.pathname);
    router.addListener(handler);
    return () => router.removeListener(handler);
  }, []);
  const Page = useMemo(
    () =>
      ({
        "/": SearchPage,
        "/city": CityPage,
        "/analysis": AnalysisPage,
      })[path] ?? NotFoundPage,
    [path],
  );

  const [edge, setEdge] = useState(DEFAULT_EDGE);
  useEffect(() => {
    const ac = new AbortController();
    (async () => {
      const edge = await findClosestEdge(ac.signal);
      console.log("Closest edge:", edge);
      setEdge(edge);
    })();
    return () => ac.abort();
  }, []);

  return (
    <EdgeContext.Provider value={edge}>
      <Page />
    </EdgeContext.Provider>
  );
};
