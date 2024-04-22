from common.data import load_mmap_matrix
from common.data import load_table
from common.emb_data import load_emb_table_ids
from multiprocessing import Pool
from pandas import DataFrame
import numpy as np
import pandas as pd
import rtree
import struct

INCLUDE_COMMENTS = False
# Each zoom level doubles the amount of information on screen.
# At zoom level 1, we want points to be at least 0.2 units apart.
# At zoom level 2, we want points to be at least 0.1 units apart.
# At zoom level 3, we want points to be at least 0.05 units apart.
# At zoom level 4, we want points to be at least 0.025 units apart.
# ...
# At the maximum zoom level, we show everything, regardless of distance.
ZOOM_LEVELS = 8
BASE_DISTANCE = 0.25
BASE_AXIS_TILES = 8


def build_map_at_zoom_level(zoom_level: int):
    def lg(*msg):
        print(f"[zoom={zoom_level}]", *msg)

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

    if zoom_level == ZOOM_LEVELS - 1:
        lg("No filtering needed")
    else:
        lg("Sorting data")
        df = df.sort_values("score", ascending=False)

        mat = df[["x", "y"]].to_numpy()
        assert mat.shape == (count, 2) and mat.dtype == np.float32
        graph = rtree.index.Index()

        min_dist = BASE_DISTANCE / (2**zoom_level)
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

    vec_id = df["id"].to_numpy()
    vec_x = df["x"].to_numpy()
    vec_y = df["y"].to_numpy()
    vec_score = df["score"].to_numpy()
    assert vec_id.shape == vec_x.shape == vec_y.shape == vec_score.shape == (count,)
    assert vec_id.dtype == np.uint32
    assert vec_x.dtype == vec_y.dtype == np.float32
    assert vec_score.dtype == np.int16
    lg("Writing")
    with open(f"/hndr-data/map_z{zoom_level}.bin", "wb") as f:
        f.write(struct.pack("<I", count))
        f.write(vec_id.tobytes())
        f.write(vec_x.tobytes())
        f.write(vec_y.tobytes())
        f.write(vec_score.tobytes())
    lg("Done")


with Pool(ZOOM_LEVELS) as pool:
    pool.map(build_map_at_zoom_level, range(ZOOM_LEVELS))

print("All done!")
