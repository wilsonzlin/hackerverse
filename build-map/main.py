from common.data import load_mmap_matrix
from common.data import load_table
from common.emb_data import load_emb_table_ids
from multiprocessing import Pool
from pandas import DataFrame
import json
import numpy as np
import os
import pandas as pd
import rtree
import struct

INCLUDE_COMMENTS = False
# Each LOD level doubles the amount of information on screen.
# At LOD level 1, we want points to be at least 0.2 units apart.
# At LOD level 2, we want points to be at least 0.1 units apart.
# At LOD level 3, we want points to be at least 0.05 units apart.
# At LOD level 4, we want points to be at least 0.025 units apart.
# ...
# At the maximum LOD level, we show everything, regardless of distance.
LOD_LEVELS = 7
BASE_DISTANCE = 0.2
BASE_AXIS_TILES = 8


def build_map_at_lod_level(lod_level: int):
    def lg(*msg):
        print(f"[lod={lod_level}]", *msg)

    lg("Loading data")
    df = DataFrame({"id": load_emb_table_ids().total})
    count = len(df)
    lg(count, "entries")
    lg("Loading UMAP coordinates")
    mat_umap = load_mmap_matrix("umap_n50_d0.25_emb", (count, 2), np.float32)
    df["x"] = mat_umap[:, 0]
    df["y"] = mat_umap[:, 1]

    lg("Loading scores")
    df_posts = load_table("posts", columns=["id", "score"])
    if INCLUDE_COMMENTS:
        df_comments = load_table("comments", columns=["id", "score"])
        df_scores = pd.concat([df_posts, df_comments], ignore_index=True)
    else:
        df_scores = df_posts

    lg("Merging scores")
    df = df.merge(df_scores, how="inner", on="id")
    count = len(df)
    lg("Reduced to", count, "entries")

    if lod_level == LOD_LEVELS - 1:
        lg("No filtering needed")
        x_min, x_max = df["x"].min(), df["x"].max()
        y_min, y_max = df["y"].min(), df["y"].max()
        with open("/hndr-data/map.json", "w") as f:
            json.dump(
                {
                    "x_min": x_min.item(),
                    "x_max": x_max.item(),
                    "y_min": y_min.item(),
                    "y_max": y_max.item(),
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
        d = f"/hndr-data/map/z{lod_level}"
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
    with open(f"/hndr-data/map_z{lod_level}.bin", "wb") as f:
        f.write(struct.pack("<I", count))
        f.write(vec_id.tobytes())
        f.write(vec_x.tobytes())
        f.write(vec_y.tobytes())
        f.write(vec_score.tobytes())
    lg("Done")


with Pool(LOD_LEVELS) as pool:
    pool.map(build_map_at_lod_level, range(LOD_LEVELS))

print("All done!")
