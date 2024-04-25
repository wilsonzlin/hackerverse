from common.data import load_mmap_matrix
from common.data import load_table
from common.emb_data import load_emb_table_ids
from multiprocessing import Pool
from pandas import DataFrame
from typing import Dict
import math
import msgpack
import numpy as np
import os
import pandas as pd
import rtree
import struct

# "hnsw", "hnsw-bgem3", "hnsw-pca", "pynndescent-pca-sampling", "pynndescent-sampling"
MODE = os.getenv("MAP_POINT_SET")

INCLUDE_COMMENTS = False
# Each LOD level doubles the amount of information on screen.
# At LOD level 1, we want points to be at least 0.2 units apart.
# At LOD level 2, we want points to be at least 0.1 units apart.
# At LOD level 3, we want points to be at least 0.05 units apart.
# At LOD level 4, we want points to be at least 0.025 units apart.
# ...
# At the maximum LOD level, we show everything, regardless of distance.
BASE_LOD_AXIS_POINTS = 32


def load_data():
    if MODE == "hnsw-bgem3":
        with open("/hndr-data/mat_post_embs_bgem3_dense_count.txt") as f:
            count = int(f.readline())
        mat_id = load_mmap_matrix("mat_post_embs_bgem3_dense_ids", (count,), np.uint32)
        mat_umap = load_mmap_matrix(
            "umap_hnsw-bgem3_n300_d0.25_emb", (count, 2), np.float32
        )
    elif MODE == "hnsw":
        mat_id = load_emb_table_ids().total.to_numpy()
        count = mat_id.shape[0]
        mat_umap = load_mmap_matrix("umap_hnsw_n50_d0.25_emb", (count, 2), np.float32)
    else:
        raise NotImplementedError()

    df = DataFrame(
        {
            "id": mat_id,
            "x": mat_umap[:, 0],
            "y": mat_umap[:, 1],
        }
    )
    df_posts = load_table("posts", columns=["id", "score"])
    if INCLUDE_COMMENTS:
        df_comments = load_table("comments", columns=["id", "score"])
        df_scores = pd.concat([df_posts, df_comments], ignore_index=True)
    else:
        df_scores = df_posts
    return df.merge(df_scores, how="inner", on="id")


def calc_lod_levels(count: int) -> int:
    # With each increase in LOD level, the number of points and tiles on each axis doubles, so the number of tiles/points quadruples.
    # We want to aim for <20 KiB per tile, except for the last level, which can be <100 KiB.
    return max(1, math.ceil(math.log2(count / (BASE_LOD_AXIS_POINTS**2)) / 2))


def build_map_at_lod_level(lod_level: int):
    def lg(*msg):
        print(f"[lod={lod_level}]", *msg)

    df = load_data()
    x_min, x_max = df["x"].min(), df["x"].max()
    x_range = x_max - x_min
    y_min, y_max = df["y"].min(), df["y"].max()
    y_range = y_max - y_min
    count = len(df)
    lod_levels = calc_lod_levels(count)

    if lod_level == lod_levels - 1:
        lg("No filtering needed")
    else:
        lg("Sorting data")
        df = df.sort_values("score", ascending=False)

        mat = df[["x", "y"]].to_numpy()
        assert mat.shape == (count, 2) and mat.dtype == np.float32
        graph = rtree.index.Index()

        lg("Iterating points")
        for i in range(count):
            graph.insert(i, mat[i])
        filtered = []
        axis_point_count = BASE_LOD_AXIS_POINTS * (2**lod_level)
        for ix in range(axis_point_count):
            for iy in range(axis_point_count):
                x = x_min + x_range / (axis_point_count - 1) * ix
                y = y_min + y_range / (axis_point_count - 1) * iy
                for nearest in graph.nearest((x, y), 1):
                    filtered.append(nearest)
                    graph.delete(nearest, mat[nearest])

        df = df.iloc[filtered]
        count = len(df)
        assert count == len(filtered)
        lg("Filtered to", count, "points")

    axis_tile_count = 2**lod_level
    lg("Tiling to", axis_tile_count * axis_tile_count, "tiles")
    x_tile_width = x_range / axis_tile_count
    y_tile_width = y_range / axis_tile_count
    # The point that lies at x_max or y_max needs to be clipped to the last tile.
    df["tile_x"] = (
        ((df["x"] - x_min) // x_tile_width)
        .clip(upper=axis_tile_count - 1)
        .astype("uint32")
    )
    df["tile_y"] = (
        ((df["y"] - y_min) // y_tile_width)
        .clip(upper=axis_tile_count - 1)
        .astype("uint32")
    )

    out: Dict[str, bytes] = {}
    for _, tile_data in df.groupby(["tile_x", "tile_y"]):
        tile_x = tile_data["tile_x"].iloc[0]
        tile_y = tile_data["tile_y"].iloc[0]
        vec_id = tile_data["id"].to_numpy()
        vec_x = tile_data["x"].to_numpy()
        vec_y = tile_data["y"].to_numpy()
        vec_score = tile_data["score"].to_numpy()
        assert vec_id.shape == vec_x.shape == vec_y.shape == vec_score.shape
        assert vec_id.dtype == np.uint32
        assert vec_x.dtype == vec_y.dtype == np.float32
        assert vec_score.dtype == np.int16
        tile_count = vec_id.shape[0]
        out[f"{tile_x}-{tile_y}"] = (
            struct.pack("<I", tile_count)
            + vec_id.tobytes()
            + vec_x.tobytes()
            + vec_y.tobytes()
            + vec_score.tobytes()
        )

    lg("Done;", axis_tile_count * axis_tile_count, "tiles;", count, "points")
    return out


df = load_data()
x_min, x_max = df["x"].min(), df["x"].max()
y_min, y_max = df["y"].min(), df["y"].max()
score_min, score_max = df["score"].min(), df["score"].max()
count = len(df)
lod_levels = calc_lod_levels(count)
print("Total points:", count)
print("LOD levels:", lod_levels)
res = {}
res["meta"] = {
    "x_min": x_min.item(),
    "x_max": x_max.item(),
    "y_min": y_min.item(),
    "y_max": y_max.item(),
    "score_min": score_min.item(),
    "score_max": score_max.item(),
    "count": count,
    "lod_levels": lod_levels,
}
with Pool(lod_levels) as pool:
    res["tiles"] = pool.map(build_map_at_lod_level, range(lod_levels))
with open(f"/hndr-data/map-{MODE}.msgpack", "wb") as f:
    msgpack.dump(res, f)

print("All done!")
