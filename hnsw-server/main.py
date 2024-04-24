from common.data import load_table
from fastapi import FastAPI
from FlagEmbedding import BGEM3FlagModel
from sentence_transformers import SentenceTransformer
import hnswlib
import numpy as np
import pandas as pd
import time

print("Loading model")
model_jinav2small = SentenceTransformer(
    "jinaai/jina-embeddings-v2-small-en",
    trust_remote_code=True,
)
model_bgem3 = BGEM3FlagModel(
    "BAAI/bge-m3",
    use_fp16=False,
    normalize_embeddings=True,
)

print("Loading tables")
df_posts = load_table("posts", columns=["id", "score", "ts"])
df_comments = load_table("comments", columns=["id", "score", "ts"])
df = pd.concat([df_posts, df_comments], ignore_index=True)
max_post_score = df["score"].max()
max_comment_score = df["score"].max()

print("Loading posts")
idx_posts = hnswlib.Index(space="ip", dim=512)
idx_posts.load_index("/hndr-data/hnsw_posts.index", allow_replace_deleted=True)
idx_posts.set_ef(128)

print("Loading posts (bgem3)")
idx_posts_bgem3 = hnswlib.Index(space="ip", dim=1024)
idx_posts_bgem3.load_index(
    "/hndr-data/hnsw_posts_bgem3.index", allow_replace_deleted=True
)
idx_posts_bgem3.set_ef(128)

print("Loading comments")
idx_comments = hnswlib.Index(space="ip", dim=512)
idx_comments.load_index("/hndr-data/hnsw_comments.index", allow_replace_deleted=True)
idx_comments.set_ef(128)

app = FastAPI()


@app.get("/")
def search(
    dataset: str,
    query: str,
    limit: int,
    w_sim: float,
    w_score: float,
    w_ts: float,
    decay_ts: float,
):
    if dataset == "posts":
        idx = idx_posts
        max_score = max_post_score
    elif dataset == "posts_bgem3":
        idx = idx_posts_bgem3
        max_score = max_post_score
    elif dataset == "comments":
        idx = idx_comments
        max_score = max_comment_score
    else:
        raise ValueError("Invalid dataset")
    if dataset == "posts_bgem3":
        emb = model_bgem3.encode([query])["dense_vecs"]
    else:
        emb = model_jinav2small.encode(
            [query], convert_to_numpy=True, normalize_embeddings=True
        )
    # `ids` and `dists` are matrices of shape (query_count, limit).
    ids, dists = idx.knn_query(emb, k=limit)
    res = pd.DataFrame({"id": ids[0], "sim": 1 - dists[0]})
    res = res.merge(df, on="id", how="inner")
    assert len(res) <= limit
    # All scores on HN are at least -1 (and no less), so we don't really need to calculate the range.
    res["score_norm"] = np.log(res["score"].clip(1)) / np.log(max_score)
    res["ts_day"] = res["ts"].astype("int64") / (60 * 60 * 24)
    res["ts_diff"] = (time.time() / 60 / 60 / 24) - res["ts_day"]
    res["ts_norm"] = np.exp(-decay_ts * res["ts_diff"])
    res["final_score"] = (
        w_sim * res["sim"] + w_score * res["score_norm"] + w_ts * res["ts_norm"]
    )
    res.sort_values("final_score", ascending=False, inplace=True)
    return [
        {"id": id, "score": score}
        for id, score in zip(res["id"].to_list(), res["final_score"].to_list())
    ]
