from typing import List
from typing import Optional
from typing import Tuple
import numpy as np
import numpy.typing as npt
import pandas as pd
import pyarrow.dataset as ds


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


# Our system or GPU memory may not be enough for anything other than 1x the matrix at once, so be careful with copies, buffers, and fragmented allocations. Examples:
# - Reading the entire raw bytes on disk and then converting to CuPy matrix will incur 2x the cost in system memory.
# - Allocating and then merging/stacking chunks requires temporarily 2x the amount of chunk used VRAM, and may fragment VRAM such that there won't be eventually enough room.
# - Loading as NumPy matrix and then converting to CuPy matrix requires a full copy in system memory first, even if the NumPy matrix is memory-mapped.
def load_mmap_matrix_to_gpu(
    basename: str, shape: Tuple[int, ...], dtype: npt.DTypeLike
):
    # Conditionally import, as cupy requires CUDA to even install.
    import cupy as cp

    gpu = cp.empty(shape, dtype)
    cpu = load_mmap_matrix(basename, shape, dtype)
    gpu_view = gpu.ravel()
    cpu_view = cpu.ravel()
    n = gpu_view.shape[0]
    BUFSIZE = 1024 * 1024 * 1024
    for start in range(0, n, BUFSIZE):
        end = min(n, start + BUFSIZE)
        gpu_view[start:end] = cp.asarray(cpu_view[start:end])
    return gpu
