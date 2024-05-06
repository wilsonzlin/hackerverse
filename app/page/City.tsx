import classNames from "@xtjs/lib/classNames";
import defined from "@xtjs/lib/defined";
import mapExists from "@xtjs/lib/mapExists";
import mapNonEmpty from "@xtjs/lib/mapNonEmpty";
import { useEffect, useMemo, useRef, useState } from "react";
import { Ico } from "../component/Ico";
import { Loading } from "../component/Loading";
import { PageSwitcher } from "../component/PageSwitcher";
import { Post } from "../component/Post";
import { RouteLink } from "../component/RouteLink";
import { postsApiCall, topUsersApiCall } from "../util/api";
import { useBrowserDimensions } from "../util/dom";
import { usePromise } from "../util/fetch";
import { useEdgePosts, useEdgeUrlMetas } from "../util/item";
import { router } from "../util/router";
import "./City.css";

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
      return await topUsersApiCall(signal, {
        limit: 20,
        query,
        simMinHundredths: Math.ceil(simThreshold * 100),
      });
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
                <td>{r.score.toFixed(2)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
};

export const CityPage = ({ params: [query] }: { params: string[] }) => {
  const simThreshold = 0.8;

  const $intersectionSentinel = useRef<HTMLDivElement>(null);
  const canLoadMore = useRef(false);
  useEffect(() => {
    if (!$intersectionSentinel.current) {
      return;
    }
    const observer = new IntersectionObserver(() => {
      if (!canLoadMore.current) {
        return;
      }
      canLoadMore.current = false;
      setLoadLimit((l) => l + 20);
    });
    observer.observe($intersectionSentinel.current);
    return () => observer.disconnect();
  }, []);

  const [queryRaw, setQueryRaw] = useState("");
  useEffect(() => setQueryRaw(query), [query]);

  const [orderBy, setOrderBy] = useState<"votes" | "ts">("votes");

  const onMobile = useBrowserDimensions().width <= 1024;
  const [showPanelOnMobile, setShowPanelOnMobile] = useState(false);
  const showPanel = showPanelOnMobile || !onMobile;

  const postsReq = usePromise<Array<{ id: number; sim: number }>>();
  const [loadLimit, setLoadLimit] = useState(20);
  const postsRaw = useMemo(
    () => postsReq.data?.slice(0, loadLimit) ?? [],
    [postsReq.data, loadLimit],
  );
  // Use edge post data as it has the normalized (not raw original) URL, required for `urlMetasReq`.
  const postMetas = useEdgePosts(postsRaw.map((i) => i.id) ?? []);
  const urlMetas = useEdgeUrlMetas(
    Object.values(postMetas)
      .map((p) => p.url)
      .filter((u) => u),
  );
  const posts = useMemo(
    () =>
      postsRaw
        .map(({ id, sim }) =>
          mapExists(postMetas[id], (item) =>
            mapNonEmpty(item.title ?? "", () => ({
              ...item,
              urlMeta: urlMetas[item.url],
              id,
              sim,
            })),
          ),
        )
        .filter(defined),
    [postsRaw, postMetas, urlMetas],
  );
  useEffect(() => {
    const postsLoaded = postsReq.data?.length;
    canLoadMore.current =
      postsLoaded != undefined && !!urlMetas && loadLimit < postsLoaded;
  }, [urlMetas]);

  const POSTS_REQ_LIMIT = 500;
  useEffect(() => {
    postsReq.set(async (signal) => {
      if (!query) {
        return;
      }
      return await postsApiCall(signal, {
        limit: POSTS_REQ_LIMIT,
        query,
        simMinHundredths: Math.ceil(simThreshold * 100),
        orderBy,
      });
    });
  }, [query, orderBy]);

  return (
    <div className={classNames("City", onMobile && "mobile")}>
      <div className="query-form-container">
        <form
          className="query-form"
          onSubmit={(e) => {
            e.preventDefault();
            router.change(`/c/${encodeURIComponent(queryRaw.trim())}`);
          }}
        >
          <button type="button" onClick={() => setShowPanelOnMobile(true)}>
            <Ico i={onMobile ? "menu" : "groups"} size={24} />
          </button>
          <input
            placeholder="Find your community"
            value={queryRaw}
            onChange={(e) => setQueryRaw(e.target.value)}
          />
        </form>
      </div>

      <PageSwitcher />

      <main>
        <div className="posts">
          <div className="controls">
            <p>
              {mapExists(
                postsReq.data?.length,
                (n) => `${n}${n == POSTS_REQ_LIMIT ? "+" : ""} posts`,
              )}
            </p>
            <select
              value={orderBy}
              onChange={(e) => setOrderBy(e.currentTarget.value as any)}
            >
              <option value="votes">Top</option>
              <option value="ts">New</option>
            </select>
          </div>
          {postsReq.loading && <Loading />}
          {postsReq.error && <p className="err">{postsReq.error}</p>}
          {posts?.map((p) => (
            <Post key={p.id} id={p.id} post={p} urlMeta={p.urlMeta} />
          ))}
          <div ref={$intersectionSentinel} className="intersection-sentinel" />
        </div>

        {showPanel && (
          <div className="panel">
            {onMobile && (
              <div className="header">
                <div className="text">
                  <h1>{query}</h1>
                  <p>Community details</p>
                </div>
                <Ico i="groups" size={32} />
              </div>
            )}
            <RouteLink
              className="link-to-analysis"
              href={`/a/${encodeURIComponent(query)}`}
            >
              <Ico i="open_in_new" size={20} />
              <span>Analyze popularity and sentiment</span>
            </RouteLink>
            <TopUsersSection query={query} simThreshold={simThreshold} />
            {onMobile && (
              <button
                className="close"
                onClick={() => setShowPanelOnMobile(false)}
              >
                Close
              </button>
            )}
          </div>
        )}
      </main>
    </div>
  );
};
