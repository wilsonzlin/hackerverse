from dataclasses import dataclass
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
import numpy as np
import numpy.typing as npt
import pandas as pd
import pyarrow.dataset as ds

# We go with 3 million (~75%) posts and an equivalent absolute amount (~10%) of comments.
# - We are limited in how many inputs we can put into UMAP, before it gets extremely expensive in memory and time to compute, so we must make some decision on what subset of the data we want.
# - Posts are good diverse anchors for primary content, whereas comments can wildly vary in context and "meta". Therefore, we use most posts but few comments.

SAMPLE_RANDOM_STATE = 42
SAMPLE_SIZE_POSTS = 3_000_000
SAMPLE_SIZE_COMMENTS = 3_000_000
SAMPLE_SIZE_TOTAL = SAMPLE_SIZE_POSTS + SAMPLE_SIZE_COMMENTS
# On the sample size of 6 million, the PCA explained variance with 150 dimensions is ~0.92, 128 is ~0.87.
PCA_COMPONENTS = 128


def load_table(basename: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    return (
        ds.dataset(f"/hndr-data/{basename}.arrow", format="ipc")
        .to_table(columns=columns)
        .to_pandas()
    )


T = TypeVar("T", pd.DataFrame, pd.Series)


# Some of our functions require consistent determinstic outputs, which not only depends on the RNG seed, but also the order of stacking. This is why this function exists; always use this over `pd.concat` directly.
def merge_posts_and_comments(*, posts: T, comments: T) -> T:
    return pd.concat([posts, comments], ignore_index=True)


def dump_mmap_matrix(out_basename: str, mat: np.ndarray):
    print("Exporting matrix:", out_basename)
    fp = np.memmap(
        f"/hndr-data/{out_basename}.mmap",
        dtype=mat.dtype,
        mode="w+",
        shape=mat.shape,
    )
    fp[:] = mat[:]
    fp.flush()


def load_mmap_matrix(basename: str, shape: Tuple[int, ...], dtype: np.dtype):
    print("Loading matrix:", basename)
    return np.memmap(
        f"/hndr-data/{basename}.mmap",
        dtype=dtype,
        mode="r",
        shape=shape,
    )


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


def load_emb_data_with_sampling():
    print("Loading data")
    df_posts = load_table("post_embs")
    print("Posts:", len(df_posts))

    df_comments = load_table("comment_embs")
    print("Comments:", len(df_comments))

    df_total = merge_posts_and_comments(posts=df_posts, comments=df_comments)
    total_count = len(df_total)
    print("Total:", total_count)
    mat_emb = np.stack(df_total["emb"].apply(np.frombuffer, dtype=np.float32))

    sample = sample_emb_table_ids(
        LoadedEmbTableIds(
            posts=df_posts["id"], comments=df_comments["id"], total=df_total["id"]
        )
    )

    # Boolean filter to select only sampled rows from the NumPy matrix.
    sample_rows_filter = df_total["id"].isin(sample).values
    assert type(sample_rows_filter) == np.ndarray
    assert sample_rows_filter.dtype == np.bool_
    assert sample_rows_filter.shape == (total_count,)

    return LoadedEmbData(
        mat_emb=mat_emb,
        sample_ids=sample,
        sample_rows_filter=sample_rows_filter,
        total_count=total_count,
    )


def load_emb_data_pca() -> LoadedEmbData:
    loaded_table_ids = load_emb_table_ids()
    total_count = len(loaded_table_ids.total)

    sample_ids = sample_emb_table_ids(loaded_table_ids)
    sample_rows_filter = loaded_table_ids.total.isin(sample_ids).values
    assert type(sample_rows_filter) == np.ndarray
    assert sample_rows_filter.dtype == np.bool_
    assert sample_rows_filter.shape == (total_count,)

    mat_emb = load_mmap_matrix("pca_emb", (total_count, PCA_COMPONENTS), np.float32)

    return LoadedEmbData(
        mat_emb=mat_emb,
        sample_ids=sample_ids,
        sample_rows_filter=sample_rows_filter,
        total_count=total_count,
    )
