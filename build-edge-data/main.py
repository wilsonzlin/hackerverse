from common.data import load_mmap_matrix
from common.data import load_table
from common.emb_data import load_emb_table_ids
from pandas import DataFrame
import msgpack
import numpy as np


def load_hnsw_data():
    mat_id = load_emb_table_ids().total.to_numpy()
    count = mat_id.shape[0]
    mat_umap = load_mmap_matrix("umap_hnsw_n50_d0.25_emb", (count, 2), np.float32)
    return DataFrame(
        {
            "id": mat_id,
            "x": mat_umap[:, 0],
            "y": mat_umap[:, 1],
        }
    )


def load_hnsw_bgem3_data():
    with open("/hndr-data/mat_post_embs_bgem3_dense_count.txt") as f:
        count = int(f.readline())
    mat_id = load_mmap_matrix("mat_post_embs_bgem3_dense_ids", (count,), np.uint32)
    mat_umap = load_mmap_matrix(
        "umap_hnsw-bgem3_n300_d0.25_emb", (count, 2), np.float32
    )
    return DataFrame(
        {
            "id": mat_id,
            "x": mat_umap[:, 0],
            "y": mat_umap[:, 1],
        }
    )


def join_post_metadata(df: DataFrame):
    df_posts = load_table("posts", columns=["id", "author", "score", "ts", "url"])
    df_posts["ts"] = df_posts["ts"].astype(np.int64)
    df = df.merge(df_posts, how="inner", on="id")
    df_post_titles = load_table("post_titles", columns=["id", "text"]).rename(
        columns={"text": "title"}
    )
    df = df.merge(df_post_titles, how="inner", on="id")
    return df


out = {
    "hnsw": {
        "data": join_post_metadata(load_hnsw_data()).to_dict("records"),
        "hnsw": msgpack.unpack(open("/hndr-data/map-hnsw.msgpack", "rb")),
    },
    "hnsw-bgem3": {
        "data": join_post_metadata(load_hnsw_bgem3_data()).to_dict("records"),
        "map": msgpack.unpack(open("/hndr-data/map-hnsw-bgem3.msgpack", "rb")),
    },
}
msgpack.pack(out, open("/hndr-data/edge.msgpack", "wb"))
