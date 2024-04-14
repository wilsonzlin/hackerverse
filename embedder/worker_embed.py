from dataclasses import dataclass
from FlagEmbedding import BGEM3FlagModel
from msgpipe import PyIpc
from service_toolkit.panic import set_excepthook
from statsd import StatsClient
from typing import Dict
from typing import List
import numpy as np
import time

set_excepthook()

statsd = StatsClient("localhost", 8125, prefix="embedder")

# This will use the GPU automatically if it's available.
model = BGEM3FlagModel("/model", use_fp16=True, normalize_embeddings=True)


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
    # Batch size of 1 is fastest (according to bench.wilsonl.in) likely due to the long inputs, and least likely to cause OOM on 24 GB VRAM GPUs.
    out = model.encode(x.texts, return_dense=True, return_sparse=True, batch_size=1)
    embed_ended = time.time()
    statsd.timing("embed_text_ms", (embed_ended - embed_started) * 1000)
    statsd.incr("embed_text_input_count", len(x.texts))
    statsd.incr("embed_text_char_count", sum(len(t) for t in x.texts))
    last_embed_time = embed_ended
    return {
        "embeddings": [
            {"dense": dense, "sparse": convert_dict(sparse)}
            # dense_vecs is a Numpy matrix of shape (N, 1024); lexical_weights is a Python list of Python defaultdicts mapping string to np.float{16,32}.
            for dense, sparse in zip(out["dense_vecs"].tolist(), out["lexical_weights"])
        ]
    }


PyIpc().add_handler("embed", EmbedReq, embed_handler).begin_loop()
