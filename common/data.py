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
        f"/hndr-data/{out_basename}.mmap",
        dtype=mat.dtype,
        mode="w+",
        shape=mat.shape,
    )
    fp[:] = mat[:]
    fp.flush()


def load_mmap_matrix(basename: str, shape: Tuple[int, ...], dtype: np.dtype):
    return np.memmap(
        f"/hndr-data/{basename}.mmap",
        dtype=dtype,
        mode="r",
        shape=shape,
    )


def deserialize_emb_col(df: pd.DataFrame, col_name: str) -> npt.NDArray[np.float32]:
    return np.stack(df[col_name].apply(np.frombuffer, dtype=np.float32))
