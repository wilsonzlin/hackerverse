from common.data import dump_mmap_matrix
from common.emb_data import load_ann_ids
from common.emb_data import load_post_embs_bgem3_table
import joblib
import numpy as np
import os
import pickle
import umap

DATASET = "topposts"
MIN_DIST = 0.25
N_NEIGHBORS = 300

out_name_pfx = f"umap-{DATASET}"

d, mat_emb = load_post_embs_bgem3_table()

with open("/hndr-data/ann-topposts.pickle", "rb") as f:
    ann = pickle.load(f)
# Copied from umap nearest_neighbors() function implementation.
knn_indices, knn_dists = ann.neighbor_graph
ann_ids = load_ann_ids(DATASET)
mat_emb = mat_emb[d["id"].isin(ann_ids)]
assert mat_emb.shape == (ann_ids.shape[0], 1024)

print("Training on", mat_emb.shape)
mapper = umap.UMAP(
    precomputed_knn=(knn_indices, knn_dists, ann),
    # Do not set a random state, it prevents parallelisation.
    n_components=2,
    metric="cosine",
    n_neighbors=N_NEIGHBORS,
    min_dist=MIN_DIST,
    low_memory=os.getenv("UMAP_LOW_MEMORY", "0") == "1",
    verbose=True,
)
mapper.fit(mat_emb)
# There's no need to run .transform() since the training data is the whole dataset already.
mat_umap = mapper.embedding_
# Save the UMAP model for later use.
with open(f"/hndr-data/{out_name_pfx}-model.joblib", "wb") as f:
    joblib.dump(mapper, f)

assert type(mat_umap) == np.ndarray
assert mat_umap.shape == (mat_emb.shape[0], 2)
dump_mmap_matrix(f"{out_name_pfx}-emb", mat_umap)

print("All done!")
