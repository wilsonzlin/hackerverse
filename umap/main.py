from common.data import dump_mmap_matrix
from common.emb_data import load_emb_data_with_sampling
from common.emb_data import load_embs
import joblib
import numpy as np
import os
import umap


def env(name: str):
    val = os.getenv(name)
    if val is None:
        raise ValueError(f"Missing environment variable: {name}")
    return val


n_neighbors = int(env("N_NEIGHBORS"))
min_dist = float(env("MIN_DIST"))

MODE = "hnsw"  # "hnsw", "hnsw-sampling", "pynndescent-pca-sampling", "pynndescent-sampling"
LOG_PREFIX = (n_neighbors, min_dist)

out_name_pfx = f"umap_n{n_neighbors}_d{min_dist}"

if MODE == "hnsw":
    mat_emb = load_embs()
    mat_emb_train = mat_emb
else:
    d = load_emb_data_with_sampling(MODE == "pynndescent-pca-sampling")
    mat_emb = d.mat_emb
    mat_emb_train = d.mat_emb[d.sample_rows_filter]

with open(f"/hndr-data/umap_prep_knn_{MODE}.joblib", "rb") as f:
    knn = joblib.load(f)

print(LOG_PREFIX, "Training")
mapper = umap.UMAP(
    precomputed_knn=knn,
    # Do not set a random state, it prevents parallelisation.
    n_components=2,
    metric="cosine",
    n_neighbors=n_neighbors,
    min_dist=min_dist,
    low_memory=False,
    # The default spectral init fails after a very long time, and just falls back to random anyway. The error:
    # > UserWarning: Spectral initialisation failed! The eigenvector solver failed. This is likely due to too small an eigengap. Consider adding some noise or jitter to your data. Falling back to random initialisation!
    init="random",
)
mapper.fit(mat_emb_train)

# Save the UMAP model for later use.
with open(f"/hndr-data/{out_name_pfx}_model.joblib", "wb") as f:
    joblib.dump(mapper, f)

print(LOG_PREFIX, "Inferring")
mat_umap = mapper.transform(mat_emb)
assert type(mat_umap) == np.ndarray
assert mat_umap.shape == (mat_emb.shape[0], 2)
dump_mmap_matrix(f"{out_name_pfx}_emb", mat_umap)

print(LOG_PREFIX, "All done!")
