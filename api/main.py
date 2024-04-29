from common.data import load_mmap_matrix
from common.data import load_table
from common.emb_data import load_comment_embs_table
from common.emb_data import load_post_embs_bgem3_table
from common.emb_data import load_post_embs_table
from common.emb_data import merge_posts_and_comments
from dataclasses import dataclass
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from FlagEmbedding import BGEM3FlagModel
from io import BytesIO
from PIL import Image
from pydantic import BaseModel
from scipy.ndimage import gaussian_filter
from sentence_transformers import SentenceTransformer
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
import hnswlib
import numpy as np
import numpy.typing as npt
import os
import pandas as pd
import struct
import time

DATASETS = os.getenv("HNDR_API_DATASETS").split(",")

print("Loading models")
model_bgem3 = BGEM3FlagModel("BAAI/bge-m3", use_fp16=False, normalize_embeddings=True)
model_jinav2small = SentenceTransformer(
    "jinaai/jina-embeddings-v2-small-en",
    trust_remote_code=True,
)


@dataclass
class Dataset:
    model: Union[BGEM3FlagModel, SentenceTransformer]
    table: pd.DataFrame
    # We store this separately from the DataFrame because we need it to be a continguous matrix, and .to_numpy() just creates a NumPy array of NumPy array objects.
    emb_mat: np.ndarray
    x_min: float
    x_max: float
    y_min: float
    y_max: float
    index: hnswlib.Index


def normalize_dataset(df: pd.DataFrame):
    score_min = df["score"].min()
    score_max = df["score"].max()
    # Call this "vote" to avoid confusion with the "score" that we assign.
    df.rename(columns={"score": "vote"}, inplace=True)
    # Add one to ensure no ln(0) which is undefined.
    df["vote_weight"] = np.log(df["vote"] - score_min + 1) / np.log(
        score_max - score_min + 1
    )
    df["ts"] = df["ts"].astype("int64")
    df["ts_day"] = df["ts"] / (60 * 60 * 24)
    df.set_index("id", inplace=True)
    return df


def load_hnsw_index(name: str, dim: int):
    print("Loading HNSW index", name)
    idx = hnswlib.Index(space="ip", dim=dim)
    idx.load_index(f"/hndr-data/hnsw_{name}.index", allow_replace_deleted=True)
    idx.set_ef(128)
    return idx


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
    df_posts = load_post_embs_table()
    df_comments = load_comment_embs_table()
    df = merge_posts_and_comments(posts=df_posts, comments=df_comments)
    mat_ids = df["id"].to_numpy()
    return load_umap(mat_ids, "hnsw_n50_d0.25")


def load_posts_data():
    df = load_table("posts", columns=["id", "score", "ts"])
    df = df.merge(load_post_embs_table(), on="id", how="inner")
    df = df.merge(load_jinav2small_umap(), on="id", how="inner")
    df = normalize_dataset(df)
    print("Posts:", len(df))
    return Dataset(
        emb_mat=np.vstack(df.pop("emb")),
        index=load_hnsw_index("posts", 512),
        model=model_jinav2small,
        table=df,
        x_max=df["x"].max(),
        x_min=df["x"].min(),
        y_max=df["y"].max(),
        y_min=df["y"].min(),
    )


def load_posts_bgem3_data():
    df = load_table("posts", columns=["id", "score", "ts"])
    df_embs = load_post_embs_bgem3_table()
    df = df.merge(df_embs, on="id", how="inner")
    df_umap = load_umap(df_embs["id"].to_numpy(), "hnsw-bgem3_n300_d0.25")
    df = df.merge(df_umap, on="id", how="inner")
    df = normalize_dataset(df)
    print("Posts bgem3:", len(df))
    return Dataset(
        emb_mat=np.vstack(df.pop("emb")),
        index=load_hnsw_index("posts_bgem3", 1024),
        model=model_bgem3,
        table=df,
        x_max=df["x"].max(),
        x_min=df["x"].min(),
        y_max=df["y"].max(),
        y_min=df["y"].min(),
    )


def load_comments_data():
    df = load_table("comments", columns=["id", "score", "ts"])
    df = df.merge(load_comment_embs_table(), on="id", how="inner")
    df = df.merge(load_jinav2small_umap(), on="id", how="inner")
    df = df.merge(load_table("comment_sentiments"), on="id", how="inner")
    df["sentiment_weight"] = np.where(
        df["negative"] > df[["neutral", "positive"]].max(axis=1),
        -df["negative"],
        np.where(
            df["neutral"] > df[["positive"]].max(axis=1),
            0,
            df["positive"],
        ),
    )
    df = normalize_dataset(df)
    print("Comments:", len(df))
    return Dataset(
        emb_mat=np.vstack(df.pop("emb")),
        index=load_hnsw_index("comments", 512),
        model=model_jinav2small,
        table=df,
        x_max=df["x"].max(),
        x_min=df["x"].min(),
        y_max=df["y"].max(),
        y_min=df["y"].min(),
    )


def load_data():
    print("Loading datasets:", DATASETS)
    loaders = {
        "posts": load_posts_data,
        "posts-bgem3": load_posts_bgem3_data,
        "comments": load_comments_data,
    }
    return {name: loaders[name]() for name in DATASETS}


def normalize_sim(raw: np.ndarray, clip: "Clip"):
    sim = raw.clip(min=clip.min, max=clip.max)
    return (sim - clip.min) / (clip.max - clip.min)


def pack_rows(df: pd.DataFrame, cols: List[str]):
    final_count = len(df)
    out = struct.pack("<I", final_count)
    for col in cols:
        out += df[col].to_numpy().tobytes()
    return out


datasets = load_data()
print("All data loaded!")


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Clip(BaseModel):
    min: Union[float, int]
    max: Union[float, int]


class HeatmapOutput(BaseModel):
    density: float
    color: Tuple[int, int, int]
    alpha_scale: float = 1.0
    sigma: int = 1
    upscale: int = 1  # Max 4.

    def calculate(self, d: Dataset, df: pd.DataFrame):
        # Make sure to use the range of the whole dataset, not just this subset.
        x_range = d.x_max - d.x_min
        y_range = d.y_max - d.y_min

        grid_width = int(x_range * self.density)
        grid_height = int(y_range * self.density)

        df = df.assign(
            grid_x=((df["x"] - d.x_min) * self.density)
            .clip(upper=grid_width - 1)
            .astype(int),
            grid_y=((df["y"] - d.y_min) * self.density)
            .clip(upper=grid_height - 1)
            .astype(int),
        )

        alpha_grid = np.zeros((grid_height, grid_width), dtype=np.float32)
        alpha_grid[df["grid_y"], df["grid_x"]] = df["final_score"]
        alpha_grid = alpha_grid.repeat(self.upscale, axis=0).repeat(
            self.upscale, axis=1
        )
        blur = gaussian_filter(alpha_grid, sigma=self.sigma)
        blur = (blur * self.alpha_scale).clip(min=0, max=1)

        img = np.full(
            (grid_height * self.upscale, grid_width * self.upscale, 4),
            (*self.color, 0),
            dtype=np.uint8,
        )
        img[:, :, 3] = (blur * 255).astype(np.uint8)

        webp_out = BytesIO()
        Image.fromarray(img, "RGBA").save(webp_out, format="webp")
        webp = webp_out.getvalue()
        return struct.pack("<I", len(webp)) + webp


class ItemsOutput(BaseModel):
    order_by: str = "final_score"
    order_asc: bool = False
    limit: Optional[int] = None

    def calculate(self, d: Dataset, df: pd.DataFrame):
        df = df.sort_values(self.order_by, ascending=self.order_asc)
        if self.limit is not None:
            df = df[: self.limit]
        df["final_score"] = df["final_score"].astype("float32")
        return pack_rows(df, ["id", "final_score"])


# To filter groups, filter the original column that is grouped by.
class GroupByOutput(BaseModel):
    # This will replace the ID column, which will instead represent the group.
    group_by: str
    # Each item belongs into the bucket `item[group_by] // group_bucket`.
    group_bucket: float = 1.0
    # mean, min, max, sum, count
    group_final_score_agg: str = "sum"

    def calculate(self, d: Dataset, df: pd.DataFrame):
        df = df.assign(
            group=(df[self.group_by] // (self.group_bucket or 1.0)).astype("int32")
        )
        df = df.groupby("group", as_index=False).agg(
            {"final_score": self.group_final_score_agg}
        )
        df = df[df["final_score"] > 0.0]
        df["final_score"] = df["final_score"].astype("float32")
        df = df.sort_values("group", ascending=True)
        return pack_rows(df, ["group", "final_score"])


class Output(BaseModel):
    # Exactly one of these must be set.
    group_by: Optional[GroupByOutput] = None
    heatmap: Optional[HeatmapOutput] = None
    items: Optional[ItemsOutput] = None

    def calculate(self, d: Dataset, df: pd.DataFrame):
        if self.group_by is not None:
            return self.group_by.calculate(d, df)
        if self.heatmap is not None:
            return self.heatmap.calculate(d, df)
        if self.items is not None:
            return self.items.calculate(d, df)
        assert False


class QueryInput(BaseModel):
    dataset: str
    queries: List[str]

    # Normalize similarity weights to this range.
    sim_scale: Clip
    # How to aggregate the query similarity values into one for each row/item.
    # Rows with zero agg. similiarity post-scaling will be filtered.
    sim_agg: str = "mean"  # mean, min, max.

    ts_weight_decay: float = 0.1

    # If provided, will first filter to this many ANN rows using the HNSW index.
    filter_hnsw: Optional[int] = None

    # Filter out rows where their column values are outside this range.
    filter_clip: Dict[str, Clip] = {}

    # How much to scale each component when calculating final score.
    # Keys must be the prefix of a "*_weight" column.
    # Values can be zero, which is the default implied when omitted.
    # WARNING: This means that if this is empty, all items will have a score of zero.
    weights: Dict[str, float]

    outputs: List[Output]


WEIGHT_COLS = ("sentiment", "sim", "ts", "vote")


@app.post("/")
def query(input: QueryInput):
    # Perform checks before expensive embedding encoding.
    d = datasets.get(input.dataset)
    if d is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    df = d.table
    if not (1 <= len(input.queries) <= 3):
        raise HTTPException(status_code=400, detail="Invalid query count")
    if not all(1 <= len(q) <= 128 for q in input.queries):
        raise HTTPException(status_code=400, detail="Invalid query length")
    if input.filter_hnsw is not None and not (1 <= input.filter_hnsw <= d.index.ef):
        raise HTTPException(status_code=400, detail="Invalid filter_hnsw")
    if not all(c in WEIGHT_COLS for c in input.weights.keys()):
        raise HTTPException(status_code=400, detail="Invalid weight")
    if input.sim_agg not in ("mean", "min", "max"):
        raise HTTPException(status_code=400, detail="Invalid sim_agg")

    model = d.model
    if type(model) == BGEM3FlagModel:
        q_mat = model.encode(input.queries)["dense_vecs"]
    elif type(model) == SentenceTransformer:
        q_mat = model.encode(
            input.queries, convert_to_numpy=True, normalize_embeddings=True
        )
    else:
        assert False
    assert type(q_mat) == np.ndarray
    assert q_mat.shape[0] == len(input.queries)

    if input.filter_hnsw is not None:
        # `ids` and `dists` are matrices of shape (query_count, limit).
        # TODO Support prefiltering using the `filter` callback.
        ids, dists = d.index.knn_query(q_mat, k=input.filter_hnsw)
        sims = normalize_sim(1 - dists, input.sim_scale)
        raw = pd.DataFrame(
            {
                "id": np.unique(ids).astype(np.uint32),
            }
        )
        for i in range(len(input.queries)):
            raw[f"sim{i}"] = 0.0
            raw.loc[raw["id"].isin(ids[i]), f"sim{i}"] = sims[i]
        cols = [f"sim{i}" for i in range(len(input.queries))]
        mat_sims = raw[cols].to_numpy()
        raw.drop(columns=cols, inplace=True)
        # This is why we index "id" in `d.table`.
        df = df.merge(raw, how="inner", on="id")
        for col, w in input.filter_clip.items():
            df = df[df[w.min <= df[col] <= w.max]]
    else:
        df = df.copy(deep=False)
        for col, w in input.filter_clip.items():
            df = df[df[w.min <= df[col] <= w.max]]
        mat_sims = normalize_sim(d.emb_mat @ q_mat.T, input.sim_scale)
    # Reset the index so we can select the `id` column again.
    df.reset_index(inplace=True)

    today = time.time() / (60 * 60 * 24)
    df["ts_weight"] = np.exp(-input.ts_weight_decay * (today - df["ts_day"]))

    assert mat_sims.shape == (len(df), len(input.queries))
    df["sim_weight"] = getattr(np, input.sim_agg)(mat_sims, axis=1)
    df = df[df["sim_weight"] > 0.0]
    df["final_score"] = 0
    for c, w in input.weights.items():
        df["final_score"] += df[f"{c}_weight"] * w
    df = df[df["final_score"] > 0.0]

    out = b""
    for o in input.outputs:
        out += o.calculate(d, df)
    return Response(out)
