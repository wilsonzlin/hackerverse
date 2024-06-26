from common.data import dump_mmap_matrix
from common.emb_data import load_ann
from common.emb_data import load_embs
from common.emb_data import load_ids
from common.util import assert_exists
import joblib
import numpy as np
import os
import umap

DATASET = "toppost"
MIN_DIST = 0.25
N_NEIGHBORS = 300

out_name_pfx = f"umap-{DATASET}"

mat_id_orig, mat_emb = load_embs(DATASET)

ann = load_ann(DATASET)
# Copied from umap nearest_neighbors() function implementation.
knn_indices, knn_dists = assert_exists(ann.neighbor_graph)
ann_ids = load_ids(f"ann-{DATASET}")
mat_emb = mat_emb[np.isin(mat_id_orig, ann_ids)]

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
