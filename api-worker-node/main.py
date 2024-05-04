from common.data import load_ann
from common.data_gpu import ApiDatasetOnGpu
from common.data_gpu import DatasetEmbModelOnGpu
from common.heatmap import render_heatmap
from common.util import env
from cudf import DataFrame
from cudf import Series
from dataclasses import dataclass
from dataclasses import field
from dataclasses_json import dataclass_json
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
import base64
import cupy as cp
import msgpack
import os
import requests
import struct
import time
import traceback
import websocket

DATASETS = os.getenv("API_WORKER_NODE_DATASETS", "comment,post,toppost").split(",")
LOAD_ANN = os.getenv("API_WORKER_NODE_LOAD_ANN", "0") == "1"
TOKEN = env("API_WORKER_NODE_TOKEN")


def load_data():
    print("Loading datasets:", DATASETS)
    res = {
        name: (
            ApiDatasetOnGpu.load(name, to_gpu=True),
            DatasetEmbModelOnGpu(name),
            load_ann(name) if LOAD_ANN else None,
        )
        for name in DATASETS
    }
    print("Loaded datasets:", DATASETS)
    return res


def scale_series(raw: Series, clip: "Clip"):
    sim = raw.clip(lower=clip.min, upper=clip.max)
    return (sim - clip.min) / (clip.max - clip.min)


def assign_then_post_filter(
    df: DataFrame, filters: Dict[str, "Clip"], col: str, new: Series
):
    df = df.assign(**{col: new})
    clip = filters.pop(col, None)
    if clip is not None:
        df = df[df[col].between(clip.min, clip.max)]
    return df


def pack_rows(df: DataFrame, cols: Iterable[str]):
    final_count = len(df)
    out = struct.pack("<I", final_count)
    for col in cols:
        dt = df[col].dtype
        out += dt.kind.encode()
        if dt == object:
            # Probably strings. Anyway, use msgpack for simplicity (instead of inventing our own mechanism).
            # > TypeError: cuDF does not support conversion to host memory via the `tolist()` method. Consider using `.to_arrow().to_pylist()` to construct a Python list.
            raw = msgpack.packb(df[col].to_arrow().to_pylist())
            assert type(raw) == bytes
            out += struct.pack("<I", len(raw))
            out += raw
        else:
            out += struct.pack("B", df[col].dtype.itemsize)
            out += df[col].to_numpy().tobytes()
    return out


@dataclass_json
@dataclass
class Clip:
    min: float
    max: float


@dataclass_json
@dataclass
class HeatmapOutput:
    density: float
    color: Tuple[int, int, int]
    alpha_scale: float = 1.0
    sigma: int = 1
    upscale: int = 1  # Max 4.

    def calculate(self, d: ApiDatasetOnGpu, df: DataFrame):
        webp = render_heatmap(
            xs=df["x"].to_numpy(),
            ys=df["y"].to_numpy(),
            weights=df["final_score"].to_numpy(),
            # Make sure to use the range of the whole dataset, not just this subset.
            x_range=(d.x_min, d.x_max),
            y_range=(d.y_min, d.y_max),
            density=self.density,
            color=self.color,
            alpha_scale=self.alpha_scale,
            sigma=self.sigma,
            upscale=self.upscale,
        )
        return struct.pack("<I", len(webp)) + webp


@dataclass_json
@dataclass
class ItemsOutput:
    cols: Tuple[str, ...] = ("id", "final_score")
    order_by: str = "final_score"
    order_asc: bool = False
    limit: Optional[int] = None

    def calculate(self, d: ApiDatasetOnGpu, df: DataFrame):
        df = df.sort_values(self.order_by, ascending=self.order_asc)
        if self.limit is not None:
            df = df[: self.limit]
        return pack_rows(df, self.cols)


# To filter groups, filter the original column that is grouped by.
@dataclass_json
@dataclass
class GroupByOutput:
    # This will replace the ID column, which will instead represent the group.
    by: str
    # If set, each item belongs into the bucket `item[by] // bucket` instead of `item[by]`. Note that this only works on numeric columns.
    bucket: Optional[float] = None
    # Mapping from column to aggregation method.
    # This is a list so that values are returned in a deterministic column order.
    cols: Tuple[Tuple[str, str], ...] = (("final_score", "sum"),)
    order_by: str = "group"
    order_asc: bool = True
    limit: Optional[int] = None

    def calculate(self, d: ApiDatasetOnGpu, df: DataFrame):
        if self.bucket is not None:
            df = df.assign(group=(df[self.by] // self.bucket).astype("int32"))
        else:
            df = df.assign(group=df[self.by])
        df = df.groupby("group", as_index=False).agg(dict(self.cols))
        df = df.sort_values(self.order_by, ascending=self.order_asc)
        if self.limit is not None:
            df = df[: self.limit]
        return pack_rows(df, ["group"] + [c for c, _ in self.cols])


@dataclass_json
@dataclass
class Output:
    # Exactly one of these must be set.
    group_by: Optional[GroupByOutput] = None
    heatmap: Optional[HeatmapOutput] = None
    items: Optional[ItemsOutput] = None

    def calculate(self, d: ApiDatasetOnGpu, df: DataFrame):
        if self.group_by is not None:
            return self.group_by.calculate(d, df)
        if self.heatmap is not None:
            return self.heatmap.calculate(d, df)
        if self.items is not None:
            return self.items.calculate(d, df)
        assert False


# We don't support pre-filtering: it requires selecting arbitrary rows in the embedding matrix, which can literally be tens of gigabytes and is extremely slow. Most of the time, post filtering is better.
@dataclass_json
@dataclass
class QueryInput:
    dataset: str
    outputs: List[Output]
    queries: List[str] = field(default_factory=list)

    # How to aggregate the query similarity values into one for each row/item.
    sim_agg: str = "mean"  # mean, min, max.

    ts_decay: float = 0.1

    # If provided, will first filter to this many ANN rows using the ANN index.
    pre_filter_ann: Optional[int] = None

    # Scale each column into a new column `{col}_scaled`.
    scales: Dict[str, Clip] = field(default_factory=dict)

    # Map from source column => threshold. Convert the column into 0 or 1, where it's 1 if the original column value is greater than or equal to the threshold. The resulting column will be named `{col}_thresh` and can be used as a weight.
    thresholds: Dict[str, float] = field(default_factory=dict)

    # How much to scale each column when calculating final score. If it's a str, it's a column name to use as a weight. If it's a float, it's the weight itself.
    # Values can be zero, which is the default implied when omitted.
    # WARNING: This means that if this is empty, all items will have a score of zero.
    weights: Dict[str, Union[str, float]] = field(default_factory=dict)

    # Filter out rows where their column values are outside this range.
    post_filter_clip: Dict[str, Clip] = field(default_factory=dict)


def request_handler(input: QueryInput):
    # Perform checks before expensive embedding encoding.
    d, model, ann_idx = datasets[input.dataset]
    # As a precaution, do a shallow copy, in case we accidentally modify the original somewhere below.
    df = d.table.copy(deep=False)

    mat_sims = None
    if input.queries:
        # Our dataset embedding matrix is loaded as float16.
        q_mat = model.encode_f16(input.queries)
        assert type(q_mat) == cp.ndarray
        assert q_mat.shape[0] == len(input.queries)
        assert q_mat.dtype == cp.float16

        if input.pre_filter_ann is not None:
            if ann_idx is None:
                raise ValueError("ANN index not loaded")
            # `ids` and `dists` are matrices of shape (query_count, limit).
            ids, dists = ann_idx.query(q_mat, k=input.pre_filter_ann)
            sims = 1 - dists
            raw = DataFrame(
                {
                    "id": cp.unique(ids).astype(cp.uint32),
                }
            )
            for i in range(len(input.queries)):
                raw[f"sim{i}"] = cp.float32(0.0)
                raw.loc[raw["id"].isin(ids[i]), f"sim{i}"] = sims[i]
            cols = [f"sim{i}" for i in range(len(input.queries))]
            mat_sims = raw[cols].to_cupy()
            raw.drop(columns=cols, inplace=True)
            # This is why we index "id" in `d.table`.
            df = df.merge(raw, how="inner", on="id")
        else:
            mat_sims = d.emb_mat @ q_mat.T

    # Reset the index so we can select the `id` column again.
    df = df.reset_index()

    today = time.time() / (60 * 60 * 24)
    df = df.assign(ts_norm=cp.exp(-input.ts_decay * (today - df["ts_day"])))

    if mat_sims is not None:
        assert mat_sims.shape == (len(df), len(input.queries)), mat_sims.shape
        df = assign_then_post_filter(
            df,
            input.post_filter_clip,
            "sim",
            # cuDF doesn't support float16, a TypeError is raised.
            # https://github.com/rapidsai/cudf/issues/5770
            getattr(cp, input.sim_agg)(mat_sims, axis=1).astype(cp.float32),
        )

    for c, scale in input.scales.items():
        df = assign_then_post_filter(
            df,
            input.post_filter_clip,
            f"{c}_scaled",
            scale_series(df[c], scale),
        )

    for c, t in input.thresholds.items():
        df = assign_then_post_filter(
            df,
            input.post_filter_clip,
            f"{c}_thresh",
            df[c] >= t,
        )

    df["final_score"] = cp.float32(0.0)
    for c, w in input.weights.items():
        df["final_score"] += df[c] * (df[w] if type(w) == str else w)
    df = assign_then_post_filter(
        df,
        input.post_filter_clip,
        "final_score",
        df["final_score"],
    )

    # Process any remaining filters.
    for col, clip in input.post_filter_clip.items():
        df = df[df[col].between(clip.min, clip.max)]

    out = b""
    for o in input.outputs:
        out += o.calculate(d, df)
    return out


def on_error(ws, error):
    print("WS error:", error)


@dataclass_json
@dataclass
class BrokerMessage:
    id: int
    input: QueryInput


def on_message(ws, raw):
    msg = BrokerMessage.from_json(raw)

    try:
        res = {
            "output": request_handler(msg.input),
        }
    except Exception as err:
        typ = type(err).__name__
        trace = traceback.format_exc()
        print("Handler error:", typ, err, trace)
        res = {
            "error": {
                "type": typ,
                "message": str(err),
                "trace": trace,
            }
        }

    ws.send(
        msgpack.packb(
            {
                "id": msg.id,
                **res,
            }
        ),
        opcode=websocket.ABNF.OPCODE_BINARY,
    )


def on_open(ws):
    print("Opened connection")
    init_req = msgpack.packb({"token": TOKEN, "ip": public_ip})
    assert type(init_req) == bytes
    ws.send(init_req, opcode=websocket.ABNF.OPCODE_BINARY)


public_ip = requests.get("https://icanhazip.com").text.strip()
print("Public IP:", public_ip)

datasets = load_data()
print("All data loaded!")

websocket.setdefaulttimeout(30)
wsapp = websocket.WebSocketApp(
    "wss://api-worker-broker.hndr.wilsonl.in:6000",
    on_error=on_error,
    on_message=on_message,
    on_open=on_open,
)
print("Started listener")
with open("/tmp/cert.pem", "wb") as f:
    f.write(base64.standard_b64decode(env("API_WORKER_NODE_CERT_B64")))
wsapp.run_forever(
    reconnect=30,
    sslopt={
        "ca_certs": "/tmp/cert.pem",
    },
)
