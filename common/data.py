from dataclasses import dataclass
from FlagEmbedding import BGEM3FlagModel
from pynndescent import NNDescent
from sentence_transformers import SentenceTransformer
from typing import List
from typing import Optional
from typing import Tuple
import json
import numpy as np
import numpy.typing as npt
import os
import pandas as pd
import pickle
import pyarrow
import pyarrow.dataset as ds
import pyarrow.feather


def load_table(basename: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    return (
        ds.dataset(f"/hndr-data/{basename}.arrow", format="ipc")
        .to_table(columns=columns)
        .to_pandas()
    )


def dump_mmap_matrix(out_basename: str, mat: np.ndarray):
    fp = np.memmap(
        f"/hndr-data/{out_basename}.mat",
        dtype=mat.dtype,
        mode="w+",
        shape=mat.shape,
    )
    fp[:] = mat[:]
    fp.flush()


def load_mmap_matrix(basename: str, shape: Tuple[int, ...], dtype: npt.DTypeLike):
    return np.memmap(
        f"/hndr-data/{basename}.mat",
        dtype=dtype,
        mode="r",
        shape=shape,
    )


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
            res = model.encode(inputs, convert_to_numpy=True, normalize_embeddings=True)
            assert type(res) == np.ndarray
            return res
        assert False


@dataclass
class ApiDataset:
    name: str

    table: pd.DataFrame
    emb_mat: npt.NDArray[np.float32]
    # These do not exist for datasets without UMAP.
    x_min: Optional[float] = None
    x_max: Optional[float] = None
    y_min: Optional[float] = None
    y_max: Optional[float] = None

    def dump(self):
        pfx = f"/hndr-data/api-{self.name}"
        self.table.to_feather(f"{pfx}-table.feather")
        dump_mmap_matrix(f"api-{self.name}-emb", self.emb_mat)
        with open(f"{pfx}-meta.json", "w") as f:
            json.dump(
                {
                    "count": len(self.table),
                    "emb_dim": self.emb_mat.shape[1],
                    "x_min": self.x_min,
                    "x_max": self.x_max,
                    "y_min": self.y_min,
                    "y_max": self.y_max,
                },
                f,
            )

    @staticmethod
    def load(name: str):
        pfx = f"/hndr-data/api-{name}"
        with open(f"{pfx}-meta.json", "r") as f:
            meta = json.load(f)
        count = meta.pop("count")
        emb_dim = meta.pop("emb_dim")
        table = pyarrow.feather.read_feather(f"{pfx}-table.feather", memory_map=True)
        assert type(table) == pd.DataFrame
        emb_mat = load_mmap_matrix(f"api-{name}-emb", (count, emb_dim), np.float32)
        return ApiDataset(
            name=name,
            table=table,
            emb_mat=emb_mat,
            **meta,
        )
