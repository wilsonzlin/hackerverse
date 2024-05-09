import { Item } from "@wzlin/crawler-toolkit-hn";
import classNames from "@xtjs/lib/classNames";
import defined from "@xtjs/lib/defined";
import mapExists from "@xtjs/lib/mapExists";
import mapNonEmpty from "@xtjs/lib/mapNonEmpty";
import { DateTime } from "luxon";
import {
  MutableRefObject,
  ReactNode,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Ico } from "../component/Ico";
import { Loading } from "../component/Loading";
import { PageSwitcher } from "../component/PageSwitcher";
import { Post } from "../component/Post";
import { RouteLink } from "../component/RouteLink";
import { itemsApiCall, topUsersApiCall } from "../util/api";
import { useBrowserDimensions } from "../util/dom";
import { usePromise } from "../util/fetch";
import {
  useEdgePosts,
  useEdgeUrlMetas,
  useHnCommentPosts,
  useHnItems,
} from "../util/item";
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

const InfiniteScroll = ({
  canLoadMore,
  onLoadMore,
  children,
  error,
  loading,
}: {
  canLoadMore: MutableRefObject<boolean>;
  onLoadMore: () => void;
  children: ReactNode | ReactNode[];
  error: string | undefined;
  loading: boolean;
}) => {
  const $intersectionSentinel = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!$intersectionSentinel.current) {
      return;
    }
    const observer = new IntersectionObserver(() => {
      if (!canLoadMore.current) {
        return;
      }
      canLoadMore.current = false;
      onLoadMore();
    });
    observer.observe($intersectionSentinel.current);
    return () => observer.disconnect();
  }, []);

  return (
    <div className="items">
      {loading && <Loading />}
      {error && <p className="err">{error}</p>}
      {children}
      <div ref={$intersectionSentinel} className="intersection-sentinel" />
    </div>
  );
};

const CommentNode = ({
  comment: c,
  parent,
  post,
}: {
  comment: Item;
  parent: Item | undefined;
  post: Item | undefined;
}) => {
  if (!c || !c.text || !c.time || !c.by) {
    return null;
  }

  return (
    <div className="comment">
      <p className="sup">
        By{" "}
        <a
          href={`https://news.ycombinator.com/user?id=${encodeURIComponent(c.by)}`}
          target="_blank"
          rel="noopener noreferrer"
        >
          {c.by}
        </a>{" "}
        <a
          href={`https://news.ycombinator.com/item?id=${c.id}`}
          target="_blank"
          rel="noopener noreferrer"
        >
          {DateTime.fromJSDate(c.time).toRelative()}
        </a>
        {post && post.title && (
          <>
            {" "}
            on{" "}
            <a
              href={`https://news.ycombinator.com/item?id=${post.id}`}
              target="_blank"
              rel="noopener noreferrer"
              dangerouslySetInnerHTML={{ __html: post.title }}
            />
          </>
        )}
      </p>

      {parent && (
        <CommentNode comment={parent} parent={undefined} post={undefined} />
      )}

      {/* TODO Sanitize, just in case. */}
      <p className="text" dangerouslySetInnerHTML={{ __html: c.text }} />
    </div>
  );
};

const Comments = ({
  query,
  simThreshold,
}: {
  query: string;
  simThreshold: number;
}) => {
  const canLoadMore = useRef(false);

  const commentsReq = usePromise<Array<{ id: number; sim: number }>>();
  const [loadLimit, setLoadLimit] = useState(20);
  const commentsRaw = useMemo(
    () => commentsReq.data?.slice(0, loadLimit) ?? [],
    [commentsReq.data, loadLimit],
  );
  const items = useHnItems(commentsRaw.flatMap((i) => i.id) ?? []);
  const parents = useHnItems(
    Object.values(items)
      .map((i) => i?.parent)
      .filter(defined) ?? [],
  );
  const kids = useHnItems(
    Object.values(items)
      .flatMap((i) => i?.kids)
      .filter(defined) ?? [],
  );
  const posts = useHnCommentPosts(
    Object.values(items)
      .map((i) => i?.id)
      .filter(defined) ?? [],
  );
  const roots = useMemo(() => {
    const ordered = commentsRaw.flatMap((c) => c.id);
    const roots = new Set(ordered);
    for (const c of Object.values(items).filter(defined)) {
      if (roots.has(c.parent!)) {
        roots.delete(c.id);
      }
    }
    for (const c of Object.values(kids).filter(defined)) {
      roots.delete(c.id);
    }
    return ordered.filter((id) => roots.has(id));
  }, [items, parents, kids]);
  useEffect(() => {
    const commentCount = commentsReq.data?.length;
    // Use "in" as value may be undefined.
    const allLoaded = commentsRaw.every((c) => c.id in items);
    canLoadMore.current =
      commentCount != undefined && allLoaded && loadLimit < commentCount;
  }, [items]);
  const COMMENTS_REQ_LIMIT = 500;
  useEffect(() => {
    commentsReq.set(async (signal) => {
      if (!query) {
        return;
      }
      return await itemsApiCall(signal, {
        dataset: "comment",
        limit: COMMENTS_REQ_LIMIT,
        query,
        simMinHundredths: Math.ceil(simThreshold * 100),
        // Comment scores aren't exposed by HN API.
        orderBy: "ts",
      });
    });
  }, [query]);

  return (
    <InfiniteScroll
      loading={commentsReq.loading}
      error={commentsReq.error}
      canLoadMore={canLoadMore}
      onLoadMore={() => setLoadLimit((l) => l + 20)}
    >
      {roots.map((id) =>
        mapExists(items[id], (c) => (
          <div key={c.id} className="comment-thread">
            <CommentNode
              comment={c}
              parent={parents[c.parent!]}
              post={posts[c.id]}
            />
            {c.kids?.map((kidId) =>
              mapExists(items[kidId], (k) => (
                <CommentNode
                  key={k.id}
                  comment={k}
                  parent={undefined}
                  post={undefined}
                />
              )),
            )}
          </div>
        )),
      )}
    </InfiniteScroll>
  );
};

const Posts = ({
  query,
  orderBy,
  simThreshold,
}: {
  query: string;
  orderBy: string;
  simThreshold: number;
}) => {
  const canLoadMore = useRef(false);

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
      return await itemsApiCall(signal, {
        dataset: "post",
        limit: POSTS_REQ_LIMIT,
        query,
        simMinHundredths: Math.ceil(simThreshold * 100),
        orderBy: orderBy == "new" ? "ts" : "votes",
      });
    });
  }, [query, orderBy]);

  return (
    <InfiniteScroll
      loading={postsReq.loading}
      error={postsReq.error}
      canLoadMore={canLoadMore}
      onLoadMore={() => setLoadLimit((l) => l + 20)}
    >
      {posts?.map((p) => (
        <Post key={p.id} id={p.id} post={p} urlMeta={p.urlMeta} />
      ))}
    </InfiniteScroll>
  );
};

export const CityPage = ({
  params: [query, mode = "posts", orderBy = "top"],
}: {
  params: string[];
}) => {
  const go = (q: string, m: string, o: string) =>
    router.change(
      `/c/${encodeURIComponent(q)}/${m}/${m == "comment" ? "new" : o}`,
    );

  const [simThreshold, setSimThreshold] = useState(0.8);

  const [queryRaw, setQueryRaw] = useState("");
  useEffect(() => setQueryRaw(query), [query]);

  const onMobile = useBrowserDimensions().width <= 1024;
  const [showPanelOnMobile, setShowPanelOnMobile] = useState(false);
  const showPanel = showPanelOnMobile || !onMobile;

  return (
    <div className={classNames("City", onMobile && "mobile")}>
      <div className="query-form-container">
        <form
          className="query-form"
          onSubmit={(e) => {
            e.preventDefault();
            const q = queryRaw.trim();
            if (q) {
              go(q, mode, orderBy);
            }
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
        <div className="items-container">
          <div className="controls">
            <select
              value={mode}
              onChange={(e) => go(query, e.currentTarget.value, orderBy)}
            >
              <option value="posts">Posts</option>
              <option value="comments">Comments</option>
            </select>

            <select
              value={orderBy}
              onChange={(e) => go(query, mode, e.currentTarget.value)}
            >
              {mode != "comments" && <option value="top">Top</option>}
              <option value="new">New</option>
            </select>
          </div>
          {mode == "posts" && (
            <Posts
              orderBy={orderBy}
              query={query}
              simThreshold={simThreshold}
            />
          )}
          {mode == "comments" && (
            <Comments query={query} simThreshold={simThreshold} />
          )}
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
