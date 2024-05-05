import { VObjectMap } from "@wzlin/valid";
import defined from "@xtjs/lib/defined";
import mapExists from "@xtjs/lib/mapExists";
import mapNonEmpty from "@xtjs/lib/mapNonEmpty";
import { useContext, useEffect, useState } from "react";
import { UrlMeta, vUrlMeta } from "../../common/const";
import { Ico } from "../component/Ico";
import { PageSwitcher } from "../component/PageSwitcher";
import { apiCall, topPostsApiCall } from "../util/api";
import { usePromise } from "../util/fetch";
import { EdgeContext, useEdgePosts } from "../util/item";
import "./City.css";

const Image = ({ className, src }: { className?: string; src: string }) => {
  const [loaded, setLoaded] = useState(false);
  const [errored, setErrored] = useState(false);
  return (
    // Don't remove from DOM if not loaded, as the browser may never load it if not present.
    // "load" event is still fired even if it failed to load, so we need to also listen on "error".
    <img
      className={className}
      data-loaded={loaded && !errored}
      src={src}
      referrerPolicy="no-referrer"
      onLoad={() => setLoaded(true)}
      onError={() => setErrored(true)}
    />
  );
};

export const CityPage = ({}: {}) => {
  const [queryRaw, setQueryRaw] = useState("");
  const [query, setQuery] = useState("");

  const edge = useContext(EdgeContext);
  const postsReq = usePromise<Array<{ id: number; sim: number }>>();
  // Use edge post data as it has the normalized (not raw original) URL, required for `urlMetasReq`.
  const postMetas = useEdgePosts(edge, postsReq.data?.map((i) => i.id) ?? []);
  const urlMetasReq = usePromise<Record<string, UrlMeta>>();
  const posts = postsReq.data
    ?.map(({ id, sim }) =>
      mapExists(postMetas[id], (item) =>
        mapNonEmpty(item.title ?? "", () => ({
          ...item,
          urlMeta: urlMetasReq.data?.[item.url],
          id,
          sim,
        })),
      ),
    )
    .filter(defined);

  useEffect(() => {
    postsReq.set(async (signal) => {
      if (!query) {
        return;
      }
      return await topPostsApiCall(signal, {
        limit: 20,
        query,
        simMinHundredths: 80,
      });
    });
  }, [query]);
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
    <div className="City">
      <form
        className="query-form"
        onSubmit={(e) => {
          e.preventDefault();
          setQuery(queryRaw.trim());
        }}
      >
        <Ico i="groups" size={32} />
        <input
          placeholder="Find your community"
          value={queryRaw}
          onChange={(e) => setQueryRaw(e.target.value)}
        />
      </form>
      <PageSwitcher />
      <main>
        <div className="posts">
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
                    <div className="right">
                      <div className="site">{domain}</div>
                      <h2>{p.title}</h2>
                    </div>
                  </div>
                  {snippet && <p className="snippet">{snippet}</p>}
                </div>
                {imgUrl && <Image className="image" src={imgUrl} />}
              </a>
            );
          })}
        </div>

        <div className="panel"></div>
      </main>
    </div>
  );
};
