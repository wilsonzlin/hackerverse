from common.emb_data import load_embs
from pynndescent import NNDescent
import numpy as np
import pickle

DATASET = "toppost"

mat_id, mat_emb = load_embs(DATASET)
print("IDs:", mat_id.shape)
print("Embeddings:", mat_emb.shape)

print("Deduplicating embeddings")
# Deduplicate rows to prevent errors in NNDescent.
mat_emb, uniq_rows = np.unique(mat_emb, axis=0, return_index=True)
uniq_ids = mat_id[uniq_rows]
assert uniq_ids.dtype == np.uint32
print("After deduplicating:", mat_emb.shape, uniq_rows.shape, uniq_ids.shape)

print("Building index")
idx = NNDescent(
    mat_emb,
    n_neighbors=300,
    metric="cosine",
    verbose=True,
)

print("Saving")
with open(f"/hndr-data/ann-{DATASET}.pickle", "wb") as f:
    pickle.dump(idx, f, protocol=pickle.HIGHEST_PROTOCOL)

with open(f"/hndr-data/ann-{DATASET}-ids.mat", "wb") as f:
    f.write(uniq_ids.tobytes())

print("All done!")
