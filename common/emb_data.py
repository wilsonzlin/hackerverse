from common.data import load_mmap_matrix
from common.data import load_table
from dataclasses import dataclass
from typing import TypeVar
import numpy as np
import numpy.typing as npt
import pandas as pd

# We go with 3 million (~75%) posts and an equivalent absolute amount (~10%) of comments.
# - We are limited in how many inputs we can put into UMAP, before it gets extremely expensive in memory and time to compute, so we must make some decision on what subset of the data we want.
# - Posts are good diverse anchors for primary content, whereas comments can wildly vary in context and "meta". Therefore, we use most posts but few comments.

SAMPLE_RANDOM_STATE = 42
SAMPLE_SIZE_POSTS = 3_000_000
SAMPLE_SIZE_COMMENTS = 3_000_000
SAMPLE_SIZE_TOTAL = SAMPLE_SIZE_POSTS + SAMPLE_SIZE_COMMENTS
# On the sample size of 6 million, the PCA explained variance with 150 dimensions is ~0.92, 128 is ~0.87.
PCA_COMPONENTS = 128


T = TypeVar("T", pd.DataFrame, pd.Series)


# Some of our functions require consistent determinstic outputs, which not only depends on the RNG seed, but also the order of stacking. This is why this function exists; always use this over `pd.concat` directly.
def merge_posts_and_comments(*, posts: T, comments: T) -> T:
    return pd.concat([posts, comments], ignore_index=True)


def load_count(pfx: str):
    with open(f"/hndr-data/{pfx}_count.txt") as f:
        return int(f.read())


def _load_embs_table(pfx: str, dim: int):
    count = load_count(pfx)
    mat_ids = load_mmap_matrix(f"{pfx}_ids", (count,), np.uint32)
    mat_embs = load_mmap_matrix(f"{pfx}_data", (count, dim), np.float32)
    return (
        pd.DataFrame(
            {
                "id": mat_ids,
                # We can't pass the (N, dim) matrix to DataFrame directly, it'll raise:
                # > ValueError: Per-column arrays must each be 1-dimensional
                # Splitting by row takes extremely long. Instead, we'll just store the corresponding row number.
                "emb_row": list(range(count)),
            }
        ),
        mat_embs,
    )


def load_post_embs_table():
    return _load_embs_table("mat_post_embs", 512)


def load_post_embs_bgem3_table():
    return _load_embs_table("mat_post_embs_bgem3_dense", 1024)


def load_comment_embs_table():
    return _load_embs_table("mat_comment_embs", 512)


# Load data built by the build-embs service.
def load_embs():
    count = load_count("embs")
    return load_mmap_matrix("embs", (count, 512), np.float32)


def load_embs_pca():
    count = load_count("embs")
    return load_mmap_matrix("pca_emb", (count, PCA_COMPONENTS), np.float32)


@dataclass
class LoadedEmbTableIds:
    posts: pd.Series
    comments: pd.Series
    total: pd.Series


# Loads the list of all IDs from the post and comment embedding tables, with the same consistent order each time. The consistency is important if we want to use derived data from the embedding tables without needing to store the associated IDs for each derived step/data for efficiency.
def load_emb_table_ids() -> LoadedEmbTableIds:
    df_posts = load_table("post_embs", columns=["id"])
    df_comments = load_table("comment_embs", columns=["id"])
    df_total = merge_posts_and_comments(posts=df_posts, comments=df_comments)
    return LoadedEmbTableIds(
        posts=df_posts["id"], comments=df_comments["id"], total=df_total["id"]
    )


# This will always give the same consistent output, which is important because we don't persist this alongside derived data.
def sample_emb_table_ids(d: LoadedEmbTableIds) -> pd.Series:
    posts = d.posts.sample(n=SAMPLE_SIZE_POSTS, random_state=SAMPLE_RANDOM_STATE)
    comments = d.comments.sample(
        n=SAMPLE_SIZE_COMMENTS, random_state=SAMPLE_RANDOM_STATE
    )
    return merge_posts_and_comments(posts=posts, comments=comments)


@dataclass
class LoadedEmbData:
    mat_emb: npt.NDArray[np.float32]
    sample_ids: pd.Series
    sample_rows_filter: npt.NDArray[np.bool_]
    total_count: int


# If `pca`, this will load the PCA matrix built by the pca service, which was trained on a subset sample but inferred across the entire dataset.
def load_emb_data_with_sampling(pca=False):
    loaded_table_ids = load_emb_table_ids()
    total_count = len(loaded_table_ids.total)

    sample_ids = sample_emb_table_ids(loaded_table_ids)

    # Boolean filter to select only sampled rows from the NumPy matrix.
    sample_rows_filter = loaded_table_ids.total.isin(sample_ids).values
    assert type(sample_rows_filter) == np.ndarray
    assert sample_rows_filter.dtype == np.bool_
    assert sample_rows_filter.shape == (total_count,)

    if pca:
        mat_emb = load_embs_pca()
    else:
        mat_emb = load_embs()

    return LoadedEmbData(
        mat_emb=mat_emb,
        sample_ids=sample_ids,
        sample_rows_filter=sample_rows_filter,
        total_count=total_count,
    )
