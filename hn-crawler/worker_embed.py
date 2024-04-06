from dataclasses import dataclass
from FlagEmbedding import BGEM3FlagModel
from msgpipe import PyIpc
from typing import Dict, List
import numpy as np

# This will use the GPU automatically if it's available.
model = BGEM3FlagModel("/model", use_fp16=True, normalize_embeddings=True)


def convert_dict(d: Dict[str, np.ndarray]):
    return {
        k: v.item()
        for k, v in d.items()
    }


@dataclass
class EmbedReq:
    texts: List[str]


def embed_handler(x: EmbedReq):
    # Batch size of 16 tends to cause OOM on 24 GB NVIDIA RTX A5000.
    out = model.encode(x.texts, return_dense=True, return_sparse=True, batch_size=8)
    return {
        "embeddings": [
            {"dense": dense, "sparse": convert_dict(sparse)}
            # dense_vecs is a Numpy matrix, lexical_weights is a Python list of Python defaultdicts mapping string to np.float{16,32}.
            for dense, sparse in zip(out["dense_vecs"].tolist(), out["lexical_weights"])
        ]
    }


PyIpc().add_handler("embed", EmbedReq, embed_handler).begin_loop()
