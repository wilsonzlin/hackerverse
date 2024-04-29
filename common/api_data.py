from common.data import dump_mmap_matrix
from common.data import load_mmap_matrix
from dataclasses import dataclass
import json
import numpy as np
import pandas as pd
import pyarrow
import pyarrow.feather


@dataclass
class ApiDataset:
    name: str

    table: pd.DataFrame
    emb_mat: np.ndarray
    x_min: float
    x_max: float
    y_min: float
    y_max: float

    def dump(self):
        pfx = f"/hndr-data/api_{self.name}"
        self.table.to_feather(f"{pfx}_table.feather")
        dump_mmap_matrix(f"api_{self.name}_emb", self.emb_mat)
        with open(f"{pfx}_meta.json", "w") as f:
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
        pfx = f"/hndr-data/api_{name}"
        with open(f"{pfx}_meta.json", "r") as f:
            meta = json.load(f)
        count = meta.pop("count")
        emb_dim = meta.pop("emb_dim")
        table = pyarrow.feather.read_feather(f"{pfx}_table.feather", memory_map=True)
        assert type(table) == pd.DataFrame
        emb_mat = load_mmap_matrix(f"api_{name}_emb", (count, emb_dim), np.float32)
        return ApiDataset(
            name=name,
            table=table,
            emb_mat=emb_mat,
            **meta,
        )
