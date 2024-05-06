import { VObjectMap } from "@wzlin/valid";
import classNames from "@xtjs/lib/classNames";
import defined from "@xtjs/lib/defined";
import mapExists from "@xtjs/lib/mapExists";
import mapNonEmpty from "@xtjs/lib/mapNonEmpty";
import { DateTime } from "luxon";
import { useEffect, useMemo, useRef, useState } from "react";
import { UrlMeta, vUrlMeta } from "../../common/const";
import { Ico } from "../component/Ico";
import { Loading } from "../component/Loading";
import { PageSwitcher } from "../component/PageSwitcher";
import { RouteLink } from "../component/RouteLink";
import { apiCall, postsApiCall, topUsersApiCall } from "../util/api";
import { useBrowserDimensions } from "../util/dom";
import { usePromise } from "../util/fetch";
import { useEdgePosts } from "../util/item";
import { router } from "../util/router";
import "./City.css";

const Image = ({ className, src }: { className?: string; src: string }) => {
  const [loaded, setLoaded] = useState(false);
  return (
    <img
      className={className}
      data-loaded={loaded}
      loading="lazy"
      onLoad={() => setLoaded(true)}
      referrerPolicy="no-referrer"
      src={src}
    />
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
  const urlMetasReq = usePromise<Record<string, UrlMeta>>();
  const posts = useMemo(
    () =>
      postsRaw
        .map(({ id, sim }) =>
          mapExists(postMetas[id], (item) =>
            mapNonEmpty(item.title ?? "", () => ({
              ...item,
              urlMeta: urlMetasReq.data?.[item.url],
              id,
              sim,
            })),
          ),
        )
        .filter(defined),
    [postsRaw, postMetas, urlMetasReq.data],
  );
  useEffect(() => {
    const postsLoaded = postsReq.data?.length;
    canLoadMore.current =
      postsLoaded != undefined && !!urlMetasReq.data && loadLimit < postsLoaded;
  }, [urlMetasReq.data]);

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
  useEffect(() => {
    urlMetasReq.set(async (signal) => {
      const urls = Object.values(postMetas)
        .map((p) => p.url)
        .filter((u) => u);
      if (!urls?.length) {
        return;
      }
      return await apiCall(
        signal,
        "urlMetas",
        { urls },
        new VObjectMap(vUrlMeta),
      );
    });
  }, [postMetas]);

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
          {posts?.map((p) => {
            const url = p.url || `news.ycombinator.com/item?id=${p.id}`;
            const proto = p.proto || "https:";
            const domain = url.split("/")[0];
            const snippet = (
              p.urlMeta?.description || p.urlMeta?.snippet
            )?.trim();
            const imgUrl = p.urlMeta?.imageUrl;
            return (
              <a
                key={p.id}
                className="post"
                href={`${proto}//${url}`}
                rel="noopener noreferrer"
                target="_blank"
              >
                <div className="text">
                  <div className="header">
                    <Image
                      className="favicon"
                      src={`https://${domain}/favicon.ico`}
                    />
                    <div className="main">
                      <div className="site">{domain}</div>
                      <h2>{p.title}</h2>
                      <div className="sub">
                        {p.score} points by {p.author}{" "}
                        {DateTime.fromJSDate(p.ts).toRelative()}
                      </div>
                    </div>
                  </div>
                  {snippet && <p className="snippet">{snippet}</p>}
                </div>
                {imgUrl && <Image className="image" src={imgUrl} />}
              </a>
            );
          })}
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
