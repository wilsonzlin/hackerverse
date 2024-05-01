from common.emb_data import load_post_embs_bgem3_table
from pynndescent import NNDescent
import numpy as np
import pickle

df, mat_emb = load_post_embs_bgem3_table()
mat_id = df["id"].to_numpy()
print("IDs:", mat_id.shape)
print("Embeddings:", mat_emb.shape)

print("Deduplicating embeddings")
# Deduplicate rows to prevent errors in NNDescent.
mat_emb, uniq_rows = np.unique(mat_emb, axis=0, return_index=True)
uniq_ids = df["id"][uniq_rows].to_numpy()
print("After deduplicating:", mat_emb.shape, uniq_rows.shape, uniq_ids.shape)

print("Building index")
idx = NNDescent(
    mat_emb,
    n_neighbors=300,
    metric="cosine",
    verbose=True,
)

print("Saving")
with open("/hndr-data/ann-topposts.pickle", "wb") as f:
    pickle.dump(idx, f, protocol=pickle.HIGHEST_PROTOCOL)

with open("/hndr-data/ann-topposts-ids.mat", "wb") as f:
    f.write(uniq_ids.tobytes())

print("All done!")
