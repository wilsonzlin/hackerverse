import { DateTime } from "luxon";
import { UrlMeta } from "../../common/const";
import { EdgePost } from "../util/item";
import { ImageOnLoad } from "./ImageOnLoad";
import "./Post.css";

export const Post = ({
  id,
  pointColor,
  post: p,
  urlMeta,
}: {
  id: number;
  pointColor?: string;
  post: EdgePost;
  urlMeta?: UrlMeta;
}) => {
  const url = p.url || `news.ycombinator.com/item?id=${id}`;
  const proto = p.proto || "https:";
  const domain = url.split("/")[0];
  const snippet = (urlMeta?.description || urlMeta?.snippet)?.trim();
  const imgUrl = urlMeta?.imageUrl;
  return (
    <a
      className="Post"
      href={`${proto}//${url}`}
      rel="noopener noreferrer"
      target="_blank"
    >
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
            <h2>{p.title}</h2>
            <div className="sub">
              {p.score} points by {p.author}{" "}
              {DateTime.fromJSDate(p.ts).toRelative()}
            </div>
          </div>
        </div>
        {snippet && <p className="snippet">{snippet}</p>}
      </div>
      {imgUrl && <ImageOnLoad className="image" src={imgUrl} />}
    </a>
  );
};
