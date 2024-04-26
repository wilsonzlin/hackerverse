from common.data import load_mmap_matrix
from common.data import load_table
from common.emb_data import load_emb_table_ids
import msgpack
import numpy as np


def load_hnsw_umap():
    print("Loading hnsw UMAP")
    mat_id = load_emb_table_ids().total.to_numpy()
    count = mat_id.shape[0]
    mat_umap = load_mmap_matrix(
        "umap_hnsw_n50_d0.25_emb", (count, 2), np.float32
    ).tolist()
    out = {}
    for i, id_ in enumerate(mat_id.tolist()):
        out[id_] = {"x": mat_umap[i][0], "y": mat_umap[i][1]}
    return out


def load_hnsw_bgem3_umap():
    print("Loading hnsw-bgem3 UMAP")
    with open("/hndr-data/mat_post_embs_bgem3_dense_count.txt") as f:
        count = int(f.readline())
    mat_id = load_mmap_matrix("mat_post_embs_bgem3_dense_ids", (count,), np.uint32)
    mat_umap = load_mmap_matrix(
        "umap_hnsw-bgem3_n300_d0.25_emb", (count, 2), np.float32
    ).tolist()
    out = {}
    for i, id_ in enumerate(mat_id.tolist()):
        out[id_] = {"x": mat_umap[i][0], "y": mat_umap[i][1]}
    return out


def load_posts():
    print("Loading posts")
    df = load_table("posts", columns=["id", "author", "ts", "url"]).rename(
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


def load_map_data(name: str):
    print(f"Loading map data for {name}")
    data = msgpack.unpack(open(f"/hndr-data/map-{name}.msgpack", "rb"))
    assert type(data) == dict
    return data


out = {
    "maps": {
        "hnsw": {
            "points": load_hnsw_umap(),
            **load_map_data("hnsw"),
        },
        "hnsw-bgem3": {
            "points": load_hnsw_bgem3_umap(),
            **load_map_data("hnsw-bgem3"),
        },
    },
    "posts": load_posts(),
}
print("Packing")
with open("/hndr-data/edge.msgpack", "wb") as f:
    msgpack.dump(out, f)
print("All done!")
