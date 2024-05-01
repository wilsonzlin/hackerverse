from common.data import load_mmap_matrix
from pynndescent import NNDescent
import numpy as np
import os
import pandas as pd
import pickle


def load_ids(name: str):
    pfx = f"/hndr-data/{name}"
    # To use memory map, get file size first, then divide by 4 (size of uint32) to get count.
    with open(f"{pfx}-ids.mat", "rb") as f:
        return np.frombuffer(f.read(), dtype=np.uint32)


def load_embs(name: str):
    mat_ids = load_ids(f"{name}-ids")
    count = mat_ids.shape[0]
    embs_raw_sz = os.path.getsize(f"/hndr-data/{name}-data.mat")
    mat_embs = load_mmap_matrix(
        f"{name}-data", (count, embs_raw_sz // 4 // count), np.float32
    )
    return mat_ids, mat_embs


def load_embs_as_table(name: str):
    mat_ids, mat_embs = load_embs(name)
    return (
        pd.DataFrame(
            {
                "id": mat_ids,
                # We can't pass the (N, dim) matrix to DataFrame directly, it'll raise:
                # > ValueError: Per-column arrays must each be 1-dimensional
                # Splitting by row takes extremely long. Instead, we'll just store the corresponding row number. This way, any operations on this table will still be able to reference the corresponding row in the embedding matrix.
                "emb_row": list(range(mat_ids.shape[0])),
            }
        ),
        mat_embs,
    )


def load_ann(name: str):
    with open(f"/hndr-data/ann-{name}.pickle", "rb") as f:
        ann = pickle.load(f)
    assert type(ann) == NNDescent
    return ann


def load_umap(name: str):
    ids = load_ids(f"ann-{name}")
    mat = load_mmap_matrix(f"umap-{name}-emb", (ids.shape[0], 2), np.float32)
    return pd.DataFrame(
        {
            "id": ids,
            "x": mat[:, 0],
            "y": mat[:, 1],
        }
    )
