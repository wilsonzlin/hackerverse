from dataclasses import dataclass
from FlagEmbedding import BGEM3FlagModel
from msgpipe import PyIpc
from sentence_transformers import SentenceTransformer
from service_toolkit.panic import set_excepthook
from statsd import StatsClient
from typing import Dict
from typing import List
import numpy as np
import os
import time

set_excepthook()

MODE = os.getenv("HNDR_EMBEDDER_MODE")

statsd = StatsClient("localhost", 8125, prefix="embedder")

if MODE == "bgem3":
    # We intentionally use this model for a separate more-powerful more-accurate embeddings dataset, so don't use float16 even if loss is marginal.
    model = BGEM3FlagModel(
        "BAAI/bge-m3", use_fp16=False, normalize_embeddings=True, device="cuda"
    )
elif MODE == "jinav2small":
    model = SentenceTransformer(
        "jinaai/jina-embeddings-v2-small-en",
        trust_remote_code=True,
        device="cuda",
    )
else:
    raise ValueError(f"Unknown mode: {MODE}")


def convert_dict(d: Dict[str, np.ndarray]):
    return {k: v.item() for k, v in d.items()}


@dataclass
class EmbedReq:
    texts: List[str]


last_embed_time = time.time()


def embed_handler(x: EmbedReq):
    global last_embed_time
    embed_started = time.time()
    statsd.timing("idle_gpu_ms", (embed_started - last_embed_time) * 1000)
    if type(model) == BGEM3FlagModel:
        out = model.encode(x.texts, batch_size=1, return_dense=True, return_sparse=True)
        # dense_vecs is a NumPy matrix of shape (N, 1024); lexical_weights is a Python List[defaultdict[str, np.float32]].
        dense_vecs = out["dense_vecs"]
        lexical_weights = out["lexical_weights"]
        assert type(lexical_weights) == list
    elif type(model) == SentenceTransformer:
        out = model.encode(
            x.texts, batch_size=1, normalize_embeddings=True, convert_to_numpy=True
        )
        assert type(out) == np.ndarray
        dense_vecs = out
        lexical_weights = None
    else:
        assert False
    embed_ended = time.time()
    statsd.timing("embed_text_ms", (embed_ended - embed_started) * 1000)
    statsd.incr("embed_text_input_count", len(x.texts))
    statsd.incr("embed_text_char_count", sum(len(t) for t in x.texts))
    last_embed_time = embed_ended
    return {
        # Avoid expensive and pointless tolist() -> msgpack.encode -> msgpack.decode -> new Float32Array -> new Uint8Array.
        "embeddings_raw": dense_vecs.tobytes(),
        "lexical_weights": lexical_weights
        and [convert_dict(d) for d in lexical_weights],
    }


PyIpc().add_handler("embed", EmbedReq, embed_handler).begin_loop()
