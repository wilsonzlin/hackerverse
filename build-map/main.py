from common.data import load_mmap_matrix
from common.data import load_table
from common.emb_data import load_emb_table_ids
from multiprocessing import Pool
from pandas import DataFrame
import json
import math
import numpy as np
import os
import pandas as pd
import rtree
import struct

MODE = "hnsw-bgem3"  # "hnsw", "hnsw-bgem3", "hnsw-pca", "pynndescent-pca-sampling", "pynndescent-sampling"
base_dir = f"/hndr-data/map-{MODE}"
os.makedirs(base_dir, exist_ok=True)

INCLUDE_COMMENTS = False
# Each LOD level doubles the amount of information on screen.
# At LOD level 1, we want points to be at least 0.2 units apart.
# At LOD level 2, we want points to be at least 0.1 units apart.
# At LOD level 3, we want points to be at least 0.05 units apart.
# At LOD level 4, we want points to be at least 0.025 units apart.
# ...
# At the maximum LOD level, we show everything, regardless of distance.
BASE_LOD_APPROX_POINTS = 4096
BASE_DISTANCE = 0.2
BASE_AXIS_TILES = 8


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

    return DataFrame(
        {
            "id": mat_id,
            "x": mat_umap[:, 0],
            "y": mat_umap[:, 1],
        }
    )


def calc_lod_levels(count: int) -> int:
    return max(1, math.floor(math.log2(count / BASE_LOD_APPROX_POINTS)) - 1)


def build_map_at_lod_level(lod_level: int):
    def lg(*msg):
        print(f"[lod={lod_level}]", *msg)

    df = load_data()
    lod_levels = calc_lod_levels(len(df))

    df_posts = load_table("posts", columns=["id", "score"])
    if INCLUDE_COMMENTS:
        df_comments = load_table("comments", columns=["id", "score"])
        df_scores = pd.concat([df_posts, df_comments], ignore_index=True)
    else:
        df_scores = df_posts

    df = df.merge(df_scores, how="inner", on="id")
    count = len(df)

    if lod_level == lod_levels - 1:
        lg("No filtering needed")
        x_min, x_max = df["x"].min(), df["x"].max()
        y_min, y_max = df["y"].min(), df["y"].max()
        score_min, score_max = df["score"].min(), df["score"].max()
        with open(f"{base_dir}/meta.json", "w") as f:
            json.dump(
                {
                    "x_min": x_min.item(),
                    "x_max": x_max.item(),
                    "y_min": y_min.item(),
                    "y_max": y_max.item(),
                    "score_min": score_min.item(),
                    "score_max": score_max.item(),
                    "count": count,
                },
                f,
            )
    else:
        lg("Sorting data")
        df = df.sort_values("score", ascending=False)

        mat = df[["x", "y"]].to_numpy()
        assert mat.shape == (count, 2) and mat.dtype == np.float32
        graph = rtree.index.Index()

        min_dist = BASE_DISTANCE / (2**lod_level)
        lg("Iterating points")
        filtered = []
        for i in range(count):
            for nearest in graph.nearest(mat[i], 1):
                if np.linalg.norm(mat[i] - mat[nearest]) < min_dist:
                    break
            else:
                graph.insert(i, mat[i])
                filtered.append(i)

        df = df.iloc[filtered]
        count = len(df)
        assert count == len(filtered)
        lg("Filtered to", count, "points")

    axis_tile_count = BASE_AXIS_TILES * (2**lod_level)
    lg("Tiling to", axis_tile_count * axis_tile_count, "tiles")
    x_min, x_max = df["x"].min(), df["x"].max()
    y_min, y_max = df["y"].min(), df["y"].max()
    x_tile_width = (x_max - x_min) / axis_tile_count
    y_tile_width = (y_max - y_min) / axis_tile_count
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

    for _, tile_data in df.groupby(["tile_x", "tile_y"]):
        tile_x = tile_data["tile_x"].iloc[0]
        tile_y = tile_data["tile_y"].iloc[0]
        vec_id = tile_data["id"].to_numpy()
        vec_x = tile_data["x"].to_numpy()
        vec_y = tile_data["y"].to_numpy()
        vec_score = tile_data["score"].to_numpy()
        assert vec_id.shape == vec_x.shape == vec_y.shape == vec_score.shape
        tile_count = vec_id.shape[0]
        d = f"{base_dir}/z{lod_level}"
        os.makedirs(d, exist_ok=True)
        with open(f"{d}/{tile_x}-{tile_y}.bin", "wb") as f:
            f.write(struct.pack("<I", tile_count))
            f.write(vec_id.tobytes())
            f.write(vec_x.tobytes())
            f.write(vec_y.tobytes())
            f.write(vec_score.tobytes())

    vec_id = df["id"].to_numpy()
    vec_x = df["x"].to_numpy()
    vec_y = df["y"].to_numpy()
    vec_score = df["score"].to_numpy()
    assert vec_id.shape == vec_x.shape == vec_y.shape == vec_score.shape == (count,)
    assert vec_id.dtype == np.uint32
    assert vec_x.dtype == vec_y.dtype == np.float32
    assert vec_score.dtype == np.int16
    lg("Writing")
    with open(f"{base_dir}/z{lod_level}/all.bin", "wb") as f:
        f.write(struct.pack("<I", count))
        f.write(vec_id.tobytes())
        f.write(vec_x.tobytes())
        f.write(vec_y.tobytes())
        f.write(vec_score.tobytes())
    lg("Done;", axis_tile_count * axis_tile_count, "tiles;", count, "points")


total = len(load_data())
print("Total points:", total)
lod_levels = calc_lod_levels(total)
print("LOD levels:", lod_levels)
with Pool(lod_levels) as pool:
    pool.map(build_map_at_lod_level, range(lod_levels))

print("All done!")
