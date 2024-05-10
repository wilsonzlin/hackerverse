from common.data import load_mmap_matrix
from dataclasses import dataclass
from FlagEmbedding import BGEM3FlagModel
from sentence_transformers import SentenceTransformer
from typing import List
from typing import Optional
from typing import Tuple
import cudf
import cupy as cp
import cupy.typing as cpt
import json
import numpy as np
import numpy.typing as npt
import torch

"""
This is in a separate file because cupy can only be imported if CUDA exists.
"""


# Our system or GPU memory may not be enough for anything other than 1x the matrix at once, so be careful with copies, buffers, and fragmented allocations. Examples:
# - Reading the entire raw bytes on disk and then converting to CuPy matrix will incur 2x the cost in system memory.
# - Allocating and then merging/stacking chunks requires temporarily 2x the amount of chunk used VRAM, and may fragment VRAM such that there won't be eventually enough room.
# - Loading as NumPy matrix and then converting to CuPy matrix requires a full copy in system memory first, even if the NumPy matrix is memory-mapped.
# - cupy.fromfile appears to first load entirely into system memory first.
def load_mmap_matrix_to_gpu(
    basename: str,
    shape: Tuple[int, ...],
    dtype: npt.DTypeLike,
    # Use this to downcast the matrix to a smaller dtype (e.g. fit more in VRAM).
    dest_dtype: Optional[cpt.DTypeLike] = None,
):
    gpu = cp.empty(shape, dest_dtype or dtype)
    print("GPU matrix allocated on device:", gpu.device)
    cpu = load_mmap_matrix(basename, shape, dtype)
    gpu_view = gpu.ravel()
    cpu_view = cpu.ravel()
    n = gpu_view.shape[0]
    BUFSIZE = 1024 * 1024 * 1024
    for start in range(0, n, BUFSIZE):
        end = min(n, start + BUFSIZE)
        gpu_view[start:end] = cp.asarray(cpu_view[start:end])
        print(f"Copied to GPU: {end / n * 100:.2f}%")
    return gpu


_emb_model_cache = {}


class DatasetEmbModelOnGpu:
    def __init__(self, dataset: str):
        global _emb_model_cache
        if dataset == "toppost":
            k = "bgem3"
            if k not in _emb_model_cache:
                _emb_model_cache[k] = BGEM3FlagModel(
                    "BAAI/bge-m3",
                    use_fp16=False,
                    normalize_embeddings=True,
                    device="cuda",
                )
            self.model = _emb_model_cache[k]
        elif dataset in ("post", "comment"):
            k = "jinav2small"
            if k not in _emb_model_cache:
                model = SentenceTransformer(
                    "jinaai/jina-embeddings-v2-small-en", trust_remote_code=True
                ).to("cuda")
                _emb_model_cache[k] = model
            self.model = _emb_model_cache[k]
        else:
            raise ValueError(f"Invalid dataset: {dataset}")

    # The output may be float16 or float32. To ensure float16, use the encode_f16() method.
    def encode(self, inputs: List[str]) -> cp.ndarray:
        model = self.model
        if type(model) == BGEM3FlagModel:
            # The FlagEmbedding library is hardcoded to convert GPU Tensor back to CPU NumPy matrix.
            return cp.asarray(model.encode(inputs)["dense_vecs"])
        if type(model) == SentenceTransformer:
            # The default is convert_to_numpy=True, so we must override with convert_to_tensor=True.
            tensor = model.encode(
                inputs, normalize_embeddings=True, convert_to_tensor=True
            )
            assert type(tensor) == torch.Tensor
            # https://docs.cupy.dev/en/stable/user_guide/interoperability.html#pytorch
            return cp.asarray(tensor)
        assert False

    def encode_f16(self, inputs: List[str]) -> cpt.NDArray[cp.float16]:
        return self.encode(inputs).astype(cp.float16)


@dataclass
class ApiDatasetOnGpu:
    name: str

    table: cudf.DataFrame
    emb_mat: cpt.NDArray[cp.float16]
    # These do not exist for datasets without UMAP.
    x_min: Optional[float] = None
    x_max: Optional[float] = None
    y_min: Optional[float] = None
    y_max: Optional[float] = None

    @staticmethod
    def load(name: str):
        pfx = f"/hndr-data/api-{name}"
        with open(f"{pfx}-meta.json", "r") as f:
            meta = json.load(f)
        count = meta.pop("count")
        emb_dim = meta.pop("emb_dim")
        table = cudf.read_feather(f"{pfx}-table.feather")
        assert type(table) == cudf.DataFrame
        emb_mat = load_mmap_matrix_to_gpu(
            f"api-{name}-emb", (count, emb_dim), np.float32, cp.float16
        )
        return ApiDatasetOnGpu(
            name=name,
            table=table,
            emb_mat=emb_mat,
            **meta,
        )
