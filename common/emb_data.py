from common.data import load_mmap_matrix
import numpy as np
import pandas as pd


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


def load_ann_ids(name: str):
    pfx = f"/hndr-data/ann-{name}"
    with open(f"{pfx}-ids.mat", "rb") as f:
        return np.frombuffer(f.read(), dtype=np.uint32)


def load_umap(name: str):
    ids = load_ann_ids(name)
    mat = load_mmap_matrix(f"umap-{name}-emb", (ids.shape[0], 2), np.float32)
    return pd.DataFrame(
        {
            "id": ids,
            "x": mat[:, 0],
            "y": mat[:, 1],
        }
    )
