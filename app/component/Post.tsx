import { DateTime } from "luxon";
import { EdgePost, EdgeUrlMeta } from "../util/item";
import { ImageOnLoad } from "./ImageOnLoad";
import "./Post.css";

export const Post = ({
  id,
  pointColor,
  post: p,
  urlMeta,
  hideImage,
}: {
  id: number;
  pointColor?: string;
  post: EdgePost;
  urlMeta?: EdgeUrlMeta;
  hideImage?: boolean;
}) => {
  const url = p.url || `news.ycombinator.com/item?id=${id}`;
  const proto = p.proto || "https:";
  const domain = url.split("/")[0];
  const snippet = (urlMeta?.description || urlMeta?.snippet)?.trim();
  const imgUrl = urlMeta?.image_url;
  return (
    <div className="Post">
      <div className="text">
        <div className="header">
          <ImageOnLoad
            className="favicon"
            src={`https://${domain}/favicon.ico`}
          />
          <div className="main">
            <div className="sup">
              {pointColor && (
                <div className="point" style={{ background: pointColor }} />
              )}
              <div className="site">{domain}</div>
            </div>
            <h2>
              <a
                href={`${proto}//${url}`}
                rel="noopener noreferrer"
                target="_blank"
              >
                {p.title}
              </a>
            </h2>
            <div className="sub">
              {p.score} points by{" "}
              <a
                href={`https://news.ycombinator.com/user?id=${p.author}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                {p.author}
              </a>{" "}
              <a
                href={`https://news.ycombinator.com/item?id=${id}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                {DateTime.fromJSDate(p.ts).toRelative()}
              </a>
            </div>
          </div>
        </div>
        {snippet && <p className="snippet">{snippet}</p>}
      </div>
      {imgUrl && !hideImage && <ImageOnLoad className="image" src={imgUrl} />}
    </div>
  );
};
