from dataclasses import dataclass
from msgpipe import PyIpc
from sentence_transformers import SentenceTransformer
from service_toolkit.panic import set_excepthook
from statsd import StatsClient
from typing import List
import numpy as np
import time

set_excepthook()

statsd = StatsClient("localhost", 8125, prefix="embedder")

model = SentenceTransformer(
    "jinaai/jina-embeddings-v2-small-en",
    trust_remote_code=True,
    device="cuda",
)


@dataclass
class EmbedReq:
    texts: List[str]


last_embed_time = time.time()


def embed_handler(x: EmbedReq):
    global last_embed_time
    embed_started = time.time()
    statsd.timing("idle_gpu_ms", (embed_started - last_embed_time) * 1000)
    out = model.encode(
        x.texts, batch_size=1, normalize_embeddings=True, convert_to_numpy=True
    )
    assert type(out) == np.ndarray
    embed_ended = time.time()
    statsd.timing("embed_text_ms", (embed_ended - embed_started) * 1000)
    statsd.incr("embed_text_input_count", len(x.texts))
    statsd.incr("embed_text_char_count", sum(len(t) for t in x.texts))
    last_embed_time = embed_ended
    return {
        # Avoid expensive and pointless tolist() -> msgpack.encode -> msgpack.decode -> new Float32Array -> new Uint8Array.
        "embeddings_raw": out.tobytes(),
    }


PyIpc().add_handler("embed", EmbedReq, embed_handler).begin_loop()
