import { useEffect, useMemo, useState } from "react";
import { AnalysisPage } from "../page/Analysis";
import { NotFoundPage } from "../page/NotFound";
import { SearchPage } from "../page/Search";
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
        "/analysis": AnalysisPage,
      })[path] ?? NotFoundPage,
    [path],
  );

  return <Page />;
};
