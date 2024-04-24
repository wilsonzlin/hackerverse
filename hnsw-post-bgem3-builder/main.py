from common.data import load_mmap_matrix
import hnswlib
import numpy as np

with open("/hndr-data/mat_post_embs_bgem3_dense_count.txt", "r") as f:
    count = int(f.read())
mat_emb = load_mmap_matrix(
    "mat_post_embs_bgem3_dense_data", (count, 1024), dtype=np.float32
)
mat_id = load_mmap_matrix("mat_post_embs_bgem3_dense_ids", (count,), dtype=np.uint32)
print("IDs:", mat_id.shape)
print("Embeddings:", mat_emb.shape)

idx = hnswlib.Index(space="ip", dim=1024)

print("Initializing index")
idx.init_index(
    max_elements=mat_id.shape[0],
    ef_construction=128,
    M=64,
    allow_replace_deleted=True,  # For future updating.
)

print("Adding items")
idx.add_items(mat_emb, mat_id)

print("Saving")
idx.save_index(f"/hndr-data/hnsw_posts_bgem3.index")

print("All done!")
