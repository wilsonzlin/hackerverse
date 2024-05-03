from common.data import load_mmap_matrix
from FlagEmbedding import BGEM3FlagModel
from pynndescent import NNDescent
from sentence_transformers import SentenceTransformer
from typing import List
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
    pfx = f"{name}-embs"
    mat_ids = load_ids(pfx)
    count = mat_ids.shape[0]
    embs_raw_sz = os.path.getsize(f"/hndr-data/{pfx}-data.mat")
    mat_embs = load_mmap_matrix(
        f"{pfx}-data", (count, embs_raw_sz // 4 // count), np.float32
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


_emb_model_cache = {}


class DatasetEmbModel:
    def __init__(self, dataset: str):
        global _emb_model_cache
        if dataset == "toppost":
            k = "bgem3"
            if k not in _emb_model_cache:
                _emb_model_cache[k] = BGEM3FlagModel(
                    "BAAI/bge-m3", use_fp16=False, normalize_embeddings=True
                )
            self.model = _emb_model_cache[k]
        elif dataset in ("post", "comment"):
            k = "jinav2small"
            if k not in _emb_model_cache:
                _emb_model_cache[k] = SentenceTransformer(
                    "jinaai/jina-embeddings-v2-small-en", trust_remote_code=True
                )
            self.model = _emb_model_cache[k]
        else:
            raise ValueError(f"Invalid dataset: {dataset}")

    def encode(self, inputs: List[str]) -> np.ndarray:
        model = self.model
        if type(model) == BGEM3FlagModel:
            return model.encode(inputs)["dense_vecs"]
        if type(model) == SentenceTransformer:
            return model.encode(
                inputs, convert_to_numpy=True, normalize_embeddings=True
            )
        assert False
