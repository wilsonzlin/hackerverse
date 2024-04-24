from huggingface_hub import snapshot_download
import os

MODE = os.getenv("HNDR_EMBEDDER_MODE")

if MODE == "bgem3":
    snapshot_download("BAAI/bge-m3")
elif MODE == "jinav2small":
    snapshot_download("jinaai/jina-embeddings-v2-small-en")
else:
    raise ValueError(f"Unknown mode: {MODE}")
