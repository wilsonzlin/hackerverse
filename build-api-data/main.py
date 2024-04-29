from common.api_data import ApiDataset
from common.data import load_mmap_matrix
from common.data import load_table
from common.emb_data import load_comment_embs_table
from common.emb_data import load_post_embs_bgem3_table
from common.emb_data import load_post_embs_table
from common.emb_data import merge_posts_and_comments
import multiprocessing
import numpy as np
import numpy.typing as npt
import pandas as pd


def normalize_dataset(df: pd.DataFrame, mat_embs: np.ndarray):
    # This may be smaller than the original, if some rows have been filtered during inner joins.
    mat_embs_ordered = mat_embs[df.pop("emb_row").to_numpy()]

    score_min = df["score"].min()
    score_max = df["score"].max()
    # Call this "votes" to avoid confusion with the "score" that we assign.
    df.rename(columns={"score": "votes"}, inplace=True)
    # Add one to ensure no ln(0) which is undefined.
    df["votes_norm"] = np.log(df["votes"] - score_min + 1) / np.log(
        score_max - score_min + 1
    )
    df["ts"] = df["ts"].astype("int64")
    df["ts_day"] = df["ts"] / (60 * 60 * 24)
    df.set_index("id", inplace=True)

    meta = {
        "x_max": df["x"].max().item(),
        "x_min": df["x"].min().item(),
        "y_max": df["y"].max().item(),
        "y_min": df["y"].min().item(),
    }

    return df, mat_embs_ordered, meta


def load_umap(ids: npt.NDArray[np.uint32], name: str):
    mat = load_mmap_matrix(f"umap_{name}_emb", (ids.shape[0], 2), np.float32)
    return pd.DataFrame(
        {
            "id": ids,
            "x": mat[:, 0],
            "y": mat[:, 1],
        }
    )


def load_jinav2small_umap():
    df_posts, _ = load_post_embs_table()
    df_comments, _ = load_comment_embs_table()
    df = merge_posts_and_comments(posts=df_posts, comments=df_comments)
    mat_ids = df["id"].to_numpy()
    return load_umap(mat_ids, "hnsw_n50_d0.25")


def merge_comment_count(df: pd.DataFrame):
    df_comments = (
        load_table("comments", columns=["id", "post"])
        .groupby("post")
        .size()
        .reset_index(name="comment_count")
    )
    df = df.merge(df_comments, left_on="id", right_on="post", how="left")
    df["comment_count"] = df["comment_count"].fillna(0)
    return df


def build_posts_data():
    df = load_table("posts", columns=["id", "score", "ts"])
    df = merge_comment_count(df)
    df_embs, mat_emb = load_post_embs_table()
    df = df.merge(df_embs, on="id", how="inner")
    df = df.merge(load_jinav2small_umap(), on="id", how="inner")
    df, mat_emb, meta = normalize_dataset(df, mat_emb)
    print("Posts:", len(df))
    ApiDataset(
        name="posts",
        emb_mat=mat_emb,
        table=df,
        **meta,
    ).dump()


def build_posts_bgem3_data():
    df = load_table("posts", columns=["id", "score", "ts"])
    df = merge_comment_count(df)
    df_embs, mat_emb = load_post_embs_bgem3_table()
    df = df.merge(df_embs, on="id", how="inner")
    df_umap = load_umap(df_embs["id"].to_numpy(), "hnsw-bgem3_n300_d0.25")
    df = df.merge(df_umap, on="id", how="inner")
    df, mat_emb, meta = normalize_dataset(df, mat_emb)
    print("Posts bgem3:", len(df))
    ApiDataset(
        name="posts-bgem3",
        emb_mat=mat_emb,
        table=df,
        **meta,
    ).dump()


def load_comments_data():
    print("Loading comments")
    df = load_table("comments", columns=["id", "score", "ts"])
    print("Loading and merging comment embeddings")
    df_embs, mat_emb = load_comment_embs_table()
    df = df.merge(df_embs, on="id", how="inner")
    print("Loading and merging comment UMAP")
    df = df.merge(load_jinav2small_umap(), on="id", how="inner")
    print("Loading and merging comment sentiments")
    df_sent = load_table("comment_sentiments").rename(
        columns={
            "positive": "sent_pos",
            "neutral": "sent_neu",
            "negative": "sent_neg",
        }
    )
    df = df.merge(df_sent, on="id", how="inner")
    print("Calculating derived comment sentiment columns")
    df["sent"] = np.float32(0.0)
    df.loc[df["sent_neg"] > df[["sent_neu", "sent_pos"]].max(axis=1), "sent"] = -df[
        "sent_neg"
    ]
    df.loc[df["sent_pos"] > df["sent_neu"], "sent"] = df["sent_pos"]
    print("Normalizing comment table")
    df, mat_emb, meta = normalize_dataset(df, mat_emb)
    print("Comments:", len(df))
    ApiDataset(
        name="comments",
        emb_mat=mat_emb,
        table=df,
        **meta,
    ).dump()


if __name__ == "__main__":
    # Create processes for each function
    p1 = multiprocessing.Process(target=build_posts_data)
    p2 = multiprocessing.Process(target=build_posts_bgem3_data)
    p3 = multiprocessing.Process(target=load_comments_data)

    # Start each process
    p1.start()
    p2.start()
    p3.start()

    # Wait for all processes to finish
    p1.join()
    p2.join()
    p3.join()
    print("All done!")
