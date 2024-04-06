from dataclasses import dataclass
from FlagEmbedding import BGEM3FlagModel
from msgpipe import PyIpc
from typing import List

# This will use the GPU automatically if it's available.
model = BGEM3FlagModel("/model", use_fp16=True, normalize_embeddings=True)


@dataclass
class EmbedReq:
    texts: List[str]


def embed_handler(x: EmbedReq):
    out = model.encode(x.texts, return_dense=True, return_sparse=True)
    return {
        "embeddings": [
            {"dense": dense, "sparse": sparse}
            # dense_vecs is a Numpy matrix, lexical_weights is a Python list of Python defaultdicts mapping string to float.
            for dense, sparse in zip(out["dense_vecs"].tolist(), out["lexical_weights"])
        ]
    }


PyIpc().add_handler("embed", EmbedReq, embed_handler).begin_loop()
