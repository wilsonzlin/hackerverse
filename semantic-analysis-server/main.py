from common.data import deserialize_emb_col
from common.data import load_table
from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import numpy as np


# Wrap in function to clean up and release memory at end.
def load_data():
    print("Loading data")

    df_posts = load_table("posts")
    df_post_embs = load_table("post_embs.arrow")
    df_posts = df_posts.merge(df_post_embs, on="id", how="inner")
    mat_post_embs = deserialize_emb_col(df_posts, "emb")
    mat_post_scores = df_posts["score"].values
    mat_post_days = df_posts["ts"].astype("int64").values // (60 * 60 * 24)
    print("Posts:", len(df_posts))

    df_comments = load_table("comments")
    df_comment_embs = load_table("comment_embs")
    df_comments = df_comments.merge(df_comment_embs, on="id", how="inner")
    df_comment_sentiments = load_table("comment_sentiments")
    df_comments = df_comments.merge(df_comment_sentiments, on="id", how="inner")
    mat_comment_embs = deserialize_emb_col(df_comments, "emb")
    mat_comment_scores = df_comments["score"].values
    mat_comment_days = df_comments["ts"].astype("int64").values // (60 * 60 * 24)
    mat_comment_sentiments = np.where(
        df_comments["negative"] > df_comments[["neutral", "positive"]].max(axis=1),
        -df_comments["negative"],
        np.where(
            df_comments["neutral"] > df_comments[["positive"]].max(axis=1),
            0,
            df_comments["positive"],
        ),
    )
    print("Comments:", len(df_comments))

    return (
        mat_post_embs,
        mat_post_scores,
        mat_post_days,
        mat_comment_embs,
        mat_comment_scores,
        mat_comment_days,
        mat_comment_sentiments,
    )


print("Loading model")
model = SentenceTransformer(
    "jinaai/jina-embeddings-v2-small-en",
    trust_remote_code=True,
)

(
    mat_post_embs,
    mat_post_scores,
    mat_post_days,
    mat_comment_embs,
    mat_comment_scores,
    mat_comment_days,
    mat_comment_sentiments,
) = load_data()

mat_post_scores_ln = np.log(mat_post_scores.clip(1).astype(np.float32))
mat_comment_scores_ln = np.log(mat_comment_scores.clip(1).astype(np.float32))
first_day = min(mat_post_days.min(), mat_comment_days.min())
mat_post_days_rel = mat_post_days - first_day
mat_comment_days_rel = mat_comment_days - first_day

app = FastAPI()


class AnalyseInput(BaseModel):
    query: str
    aggregate: str  # "score", "score_ln", "sentiment"
    dataset: str  # "posts", "comments"


@app.post("/")
def analyse(input: AnalyseInput):
    emb = model.encode(input.query, convert_to_numpy=True, normalize_embeddings=True)
    assert type(emb) == np.ndarray
    assert emb.shape == (512,)

    if input.dataset == "posts":
        mat_embs = mat_post_embs
        if input.aggregate == "score_ln":
            mat_weights = mat_post_scores_ln
        elif input.aggregate == "score":
            mat_weights = mat_post_scores
        else:
            raise ValueError("Invalid aggregate")
        mat_days_rel = mat_post_days_rel
    elif input.dataset == "comments":
        mat_embs = mat_comment_embs
        if input.aggregate == "score_ln":
            mat_weights = mat_comment_scores_ln
        elif input.aggregate == "score":
            mat_weights = mat_comment_scores
        elif input.aggregate == "sentiment":
            mat_weights = mat_comment_sentiments
        else:
            raise ValueError("Invalid aggregate")
        mat_days_rel = mat_comment_days_rel
    else:
        raise ValueError("Invalid dataset")
    dists = mat_embs @ emb
    weighted = dists * mat_weights
    by_day = np.bincount(mat_days_rel, weights=weighted)

    return {
        "first_day": first_day.item(),
        "values": by_day.tolist(),
    }
