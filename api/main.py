from common.api_data import ApiDataset
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from FlagEmbedding import BGEM3FlagModel
from io import BytesIO
from PIL import Image
from pydantic import BaseModel
from pydantic import Field
from scipy.ndimage import gaussian_filter
from sentence_transformers import SentenceTransformer
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
import hnswlib
import msgpack
import numpy as np
import os
import pandas as pd
import struct
import time

DATASETS = os.getenv("HNDR_API_DATASETS").split(",")
LOAD_HNSW = os.getenv("HNDR_API_LOAD_HNSW", "0") == "1"

print("Loading models")
model_bgem3 = BGEM3FlagModel("BAAI/bge-m3", use_fp16=False, normalize_embeddings=True)
model_jinav2small = SentenceTransformer(
    "jinaai/jina-embeddings-v2-small-en",
    trust_remote_code=True,
)


def load_hnsw_index(name: str, dim: int):
    print("Loading HNSW index:", name)
    idx = hnswlib.Index(space="ip", dim=dim)
    idx.load_index(f"/hndr-data/hnsw_{name}.index", allow_replace_deleted=True)
    idx.set_ef(128)
    return idx


def load_data():
    print("Loading datasets:", DATASETS)
    models = {
        "posts": model_jinav2small,
        "posts-bgem3": model_bgem3,
        "comments": model_jinav2small,
    }
    hnsw_loaders = {
        "posts": lambda: load_hnsw_index("posts", 512),
        "posts-bgem3": lambda: load_hnsw_index("posts_bgem3", 1024),
        "comments": lambda: load_hnsw_index("comments", 512),
    }
    return {
        name: (
            ApiDataset.load(name),
            models[name],
            hnsw_loaders[name]() if LOAD_HNSW else None,
        )
        for name in DATASETS
    }


def scale_series(raw: pd.Series, clip: "Clip"):
    sim = raw.clip(lower=clip.min, upper=clip.max)
    return (sim - clip.min) / (clip.max - clip.min)


def pack_rows(df: pd.DataFrame, cols: List[str]):
    final_count = len(df)
    out = struct.pack("<I", final_count)
    for col in cols:
        dt = df[col].dtype
        out += dt.kind.encode()
        if dt == object:
            # Probably strings. Anyway, use msgpack for simplicity (instead of inventing our own mechanism).
            raw = msgpack.packb(df[col].to_list())
            assert type(raw) == bytes
            out += struct.pack("<I", len(raw))
            out += raw
        else:
            out += struct.pack("B", df[col].dtype.itemsize)
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

    def calculate(self, d: ApiDataset, df: pd.DataFrame):
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
    cols: List[str] = ["id", "final_score"]
    order_by: str = "final_score"
    order_asc: bool = False
    limit: Optional[int] = None

    def calculate(self, d: ApiDataset, df: pd.DataFrame):
        df = df.sort_values(self.order_by, ascending=self.order_asc)
        if self.limit is not None:
            df = df[: self.limit]
        return pack_rows(df, self.cols)


# To filter groups, filter the original column that is grouped by.
class GroupByOutput(BaseModel):
    # This will replace the ID column, which will instead represent the group.
    by: str
    # If set, each item belongs into the bucket `item[by] // bucket` instead of `item[by]`. Note that this only works on numeric columns.
    bucket: Optional[float] = None
    # Mapping from column to aggregation method.
    # This is a list so that values are returned in a deterministic column order.
    cols: List[Tuple[str, str]] = [("final_score", "sum")]
    order_by: str = "group"
    order_asc: bool = True
    limit: Optional[int] = None

    def calculate(self, d: ApiDataset, df: pd.DataFrame):
        if self.bucket is not None:
            df = df.assign(group=(df[self.by] // self.bucket).astype("int32"))
        else:
            df = df.assign(group=df[self.by])
        df = df.groupby("group", as_index=False).agg(dict(self.cols))
        df = df.sort_values(self.order_by, ascending=self.order_asc)
        if self.limit is not None:
            df = df[: self.limit]
        return pack_rows(df, ["group"] + [c for c, _ in self.cols])


class Output(BaseModel):
    # Exactly one of these must be set.
    group_by: Optional[GroupByOutput] = None
    heatmap: Optional[HeatmapOutput] = None
    items: Optional[ItemsOutput] = None

    def calculate(self, d: ApiDataset, df: pd.DataFrame):
        if self.group_by is not None:
            return self.group_by.calculate(d, df)
        if self.heatmap is not None:
            return self.heatmap.calculate(d, df)
        if self.items is not None:
            return self.items.calculate(d, df)
        assert False


class QueryInput(BaseModel):
    dataset: str
    queries: List[str] = Field(default=[], max_length=3)

    # How to aggregate the query similarity values into one for each row/item.
    sim_agg: str = "mean"  # mean, min, max.

    ts_decay: float = 0.1

    # If provided, will first filter to this many ANN rows using the HNSW index.
    pre_filter_hnsw: Optional[int] = None

    # Filter out rows where their column values are outside this range.
    pre_filter_clip: Dict[str, Clip] = {}

    # Scale each column into a new column `{col}_scaled`.
    scales: Dict[str, Clip] = {}

    # Map from source column => threshold. Convert the column into 0 or 1, where it's 1 if the original column value is greater than or equal to the threshold. The resulting column will be named `{col}_thresh` and can be used as a weight.
    thresholds: Dict[str, float] = {}

    # How much to scale each column when calculating final score. If it's a str, it's a column name to use as a weight. If it's a float, it's the weight itself.
    # Values can be zero, which is the default implied when omitted.
    # WARNING: This means that if this is empty, all items will have a score of zero.
    weights: Dict[str, Union[str, float]] = {}

    # Filter out rows where their column values are outside this range, after calculating thresholds and final score (using weights).
    post_filter_clip: Dict[str, Clip] = {}

    outputs: List[Output]


@app.post("/")
def query(input: QueryInput):
    # Perform checks before expensive embedding encoding.
    d, model, hnsw_idx = datasets[input.dataset]
    df = d.table

    mat_sims = None
    if input.queries:
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

        if input.pre_filter_hnsw is not None:
            if hnsw_idx is None:
                raise HTTPException(status_code=400, detail="HNSW index not loaded")
            # `ids` and `dists` are matrices of shape (query_count, limit).
            # TODO Support prefiltering using the `filter` callback.
            ids, dists = hnsw_idx.knn_query(q_mat, k=input.pre_filter_hnsw)
            sims = 1 - dists
            raw = pd.DataFrame(
                {
                    "id": np.unique(ids).astype(np.uint32),
                }
            )
            for i in range(len(input.queries)):
                raw[f"sim{i}"] = np.float32(0.0)
                raw.loc[raw["id"].isin(ids[i]), f"sim{i}"] = sims[i]
            cols = [f"sim{i}" for i in range(len(input.queries))]
            mat_sims = raw[cols].to_numpy()
            raw.drop(columns=cols, inplace=True)
            # This is why we index "id" in `d.table`.
            df = df.merge(raw, how="inner", on="id")
            for col, w in input.pre_filter_clip.items():
                df = df[df[col].between(w.min, w.max)]
        else:
            # If there are pre-filters, do so before calculating similarity across all rows.
            for col, w in input.pre_filter_clip.items():
                df = df[df[col].between(w.min, w.max)]
            mat_sims = d.emb_mat @ q_mat.T

    # Reset the index so we can select the `id` column again.
    df = df.reset_index()

    today = time.time() / (60 * 60 * 24)
    df["ts_norm"] = np.exp(-input.ts_decay * (today - df["ts_day"]))

    if mat_sims is not None:
        assert mat_sims.shape == (len(df), len(input.queries))
        df["sim"] = getattr(np, input.sim_agg)(mat_sims, axis=1)

    for c, scale in input.scales.items():
        df[f"{c}_scaled"] = scale_series(df[c], scale)

    for c, t in input.thresholds.items():
        df[f"{c}_thresh"] = df[c] >= t

    df["final_score"] = np.float32(0.0)
    for c, w in input.weights.items():
        df["final_score"] += df[c] * (df[w] if type(w) == str else w)

    for col, w in input.post_filter_clip.items():
        df = df[df[col].between(w.min, w.max)]

    out = b""
    for o in input.outputs:
        out += o.calculate(d, df)
    return Response(out)
