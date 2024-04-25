from common.data import load_mmap_matrix
from common.data import load_table
from common.emb_data import load_emb_table_ids
from pandas import DataFrame
from typing import Dict
import math
import msgpack
import numpy as np
import os
import pandas as pd
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
    return max(1, math.ceil(math.log2(count / (BASE_LOD_AXIS_POINTS**2)) / 2) + 1)


print("Mode:", MODE)
df = load_data()
x_min, x_max = df["x"].min(), df["x"].max()
x_range = x_max - x_min
y_min, y_max = df["y"].min(), df["y"].max()
y_range = y_max - y_min
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
res["tiles"] = []

df["sampled"] = False

for lod_level in range(lod_levels):

    def lg(*msg):
        print(f"[lod={lod_level}]", *msg)

    # How we sample points:
    # - Split into equal sized grids, then choose the top post from each grid if not empty.
    #   - By sampling from a grid, instead of randomly picking or following a path, we ensure that there won't be a place that appears empty despite having points.
    #   - The top post is more interesting, and should have a "random" position. If we simply pick the nearest point to the centre/top-left/bottom-right/etc., the final set of points looks like an exact equidistant grid, which looks weird.
    #   - Since we want to target a specific amount of points, if there is still remaining capacity, sample uniformly randomly from the set of points, which should follow the background distribution of points.
    if lod_level == lod_levels - 1:
        lg("No filtering needed")
        df_subset = df
    else:
        axis_point_count = BASE_LOD_AXIS_POINTS * (2**lod_level)
        goal_point_count = axis_point_count**2
        x_grid_width = x_range / axis_point_count
        y_grid_width = y_range / axis_point_count
        lg("Splitting into", axis_point_count, "x", axis_point_count, "grid")
        df["rect_x"] = (
            ((df["x"] - x_min) // x_grid_width)
            .clip(upper=axis_point_count - 1)
            .astype("uint32")
        )
        df["rect_y"] = (
            ((df["y"] - y_min) // y_grid_width)
            .clip(upper=axis_point_count - 1)
            .astype("uint32")
        )
        lg("Sorting points")
        df_subset = (
            df.sort_values("score", ascending=False)
            .groupby(["rect_x", "rect_y"])
            .first()
            .reset_index(drop=True)
        )
        df_rem = df[~df["id"].isin(df_subset["id"])]
        df_rem_sampled_count = df_rem["sampled"].sum()
        extra = goal_point_count - len(df_subset)
        # Extend from previously sampled first. NOTE: This isn't simply the highest scoring in each grid, since we may have sampled extra uniform-randomly.
        # Check that sampled subset is nonzero as otherwise .sample() throws ValueError.
        if extra and df_rem_sampled_count:
            # `n` cannot be bigger than row count or else ValueError is thrown.
            df_extra = df_rem[df_rem["sampled"]].sample(
                n=min(df_rem_sampled_count, extra)
            )
            df_subset = pd.concat([df_subset, df_extra], ignore_index=True)
        df_rem = df[~df["id"].isin(df_subset["id"])]
        extra = goal_point_count - len(df_subset)
        if extra:
            df_extra = df[~df["id"].isin(df_subset["id"])].sample(n=extra)
            df_subset = pd.concat([df_subset, df_extra], ignore_index=True)
        lg("Sampled", len(df_subset), "with extra", extra)
        # Ensure next LOD levels always pick at least these points.
        df.loc[df["id"].isin(df_subset["id"]), "sampled"] = True

    axis_tile_count = 2**lod_level
    lg("Tiling to", axis_tile_count * axis_tile_count, "tiles")
    x_tile_width = x_range / axis_tile_count
    y_tile_width = y_range / axis_tile_count
    # The point that lies at x_max or y_max needs to be clipped to the last tile.
    df_subset["tile_x"] = (
        ((df_subset["x"] - x_min) // x_tile_width)
        .clip(upper=axis_tile_count - 1)
        .astype("uint32")
    )
    df_subset["tile_y"] = (
        ((df_subset["y"] - y_min) // y_tile_width)
        .clip(upper=axis_tile_count - 1)
        .astype("uint32")
    )

    out: Dict[str, bytes] = {}
    for _, tile_data in df_subset.groupby(["tile_x", "tile_y"]):
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

    lg("Done;", axis_tile_count * axis_tile_count, "tiles;", len(df_subset), "points")
    res["tiles"].append(out)

with open(f"/hndr-data/map-{MODE}.msgpack", "wb") as f:
    msgpack.dump(res, f)

print("All done!")
