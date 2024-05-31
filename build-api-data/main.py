from common.data import ApiDataset
from common.data import load_embs_as_table
from common.data import load_table
from common.data import load_umap
import multiprocessing
import numpy as np
import pandas as pd


def normalize_dataset(df: pd.DataFrame, mat_embs: np.ndarray):
    # This may be smaller than the original, if some rows have been filtered during inner joins.
    mat_embs_ordered = mat_embs[df.pop("emb_row").to_numpy()]

    score_min = df["score"].min()
    score_max = df["score"].max()
    # Call this "votes" to avoid confusion with the "score" that we assign.
    df.rename(columns={"score": "votes"}, inplace=True)
    # Add one to ensure no ln(0) which is undefined.
    df["votes_norm"] = np.log((df["votes"] - score_min).clip(lower=1)) / np.log(
        score_max - score_min + 1
    )
    df["ts"] = df["ts"].astype("int64")
    df["ts_day"] = df["ts"] / (60 * 60 * 24)
    df.set_index("id", inplace=True)

    if "x" in df and "y" in df:
        meta = {
            "x_max": df["x"].max().item(),
            "x_min": df["x"].min().item(),
            "y_max": df["y"].max().item(),
            "y_min": df["y"].min().item(),
        }
    else:
        meta = {}

    return df, mat_embs_ordered, meta


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


def build_post_data():
    df = load_table("posts", columns=["id", "score", "ts"])
    df = merge_comment_count(df)
    df_embs, mat_emb = load_embs_as_table("post")
    df = df.merge(df_embs, on="id", how="inner")
    df, mat_emb, meta = normalize_dataset(df, mat_emb)
    print("Posts:", len(df))
    ApiDataset(
        name="post",
        emb_mat=mat_emb,
        table=df,
        **meta,
    ).dump()


def build_toppost_data():
    df = load_table("posts", columns=["id", "score", "ts"])
    df = merge_comment_count(df)
    df_embs, mat_emb = load_embs_as_table("toppost")
    df = df.merge(df_embs, on="id", how="inner")
    df = df.merge(load_umap("toppost"), on="id", how="inner")
    df, mat_emb, meta = normalize_dataset(df, mat_emb)
    print("Posts bgem3:", len(df))
    ApiDataset(
        name="toppost",
        emb_mat=mat_emb,
        table=df,
        **meta,
    ).dump()


def load_comment_data():
    print("Loading comments")
    df = load_table("comments", columns=["id", "author", "score", "ts"]).rename(
        columns={"author": "user_id"}
    )
    print("Loading and merging users")
    df_users = load_table("users").rename(columns={"id": "user_id", "username": "user"})
    df = df.merge(df_users, how="inner", on="user_id")
    print("Loading and merging comment embeddings")
    df_embs, mat_emb = load_embs_as_table("comment")
    df = df.merge(df_embs, on="id", how="inner")
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
        name="comment",
        emb_mat=mat_emb,
        table=df,
        **meta,
    ).dump()


if __name__ == "__main__":
    # Create processes for each function
    p1 = multiprocessing.Process(target=build_post_data)
    p2 = multiprocessing.Process(target=build_toppost_data)
    p3 = multiprocessing.Process(target=load_comment_data)

    # Start each process
    p1.start()
    p2.start()
    p3.start()

    # Wait for all processes to finish
    p1.join()
    p2.join()
    p3.join()
    print("All done!")
