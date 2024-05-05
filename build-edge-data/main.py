from common.data import load_table
from common.data import load_umap
import msgpack


def load_umap_as_dict(dataset: str):
    print("Loading UMAP:", dataset)
    df = load_umap("toppost")
    df.set_index("id", inplace=True)
    return df.to_dict("index")


def load_posts():
    print("Loading posts")
    df = load_table("posts", columns=["id", "author", "score", "ts", "url"]).rename(
        columns={"author": "author_id", "url": "url_id"}
    )
    df["ts"] = df["ts"].astype("int64")
    df_users = load_table("users").rename(
        columns={"id": "author_id", "username": "author"}
    )
    df = df.merge(df_users, on="author_id", how="inner").drop(columns=["author_id"])
    df_urls = load_table(
        "urls", columns=["id", "url", "proto", "found_in_archive"]
    ).rename(columns={"id": "url_id"})
    df = df.merge(df_urls, on="url_id", how="left").drop(columns=["url_id"])
    df.loc[df["url"].isna(), "url"] = ""
    df.loc[df["proto"].isna(), "proto"] = ""
    df.loc[df["found_in_archive"].isna(), "found_in_archive"] = False
    df_titles = load_table("post_titles").rename(columns={"text": "title"})
    df = df.merge(df_titles, on="id", how="inner")
    df = df.set_index("id")
    return df.to_dict("index")


def load_map_data(dataset: str):
    print(f"Loading map data for {dataset}")
    with open(f"/hndr-data/map-{dataset}.msgpack", "rb") as f:
        data = msgpack.unpack(f)
    assert type(data) == dict
    return data


out = {
    "maps": {
        dataset: {
            "points": load_umap_as_dict(dataset),
            **load_map_data(dataset),
        }
        # Add "post", "comment" here if they are built in the future.
        for dataset in ["toppost"]
    },
    "posts": load_posts(),
}
print("Packing")
with open("/hndr-data/edge.msgpack", "wb") as f:
    msgpack.dump(out, f)
print("All done!")
