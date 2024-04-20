from common.emb_data import load_emb_data_pca
from common.emb_data import load_emb_data_with_sampling
import joblib
import os
import umap


def env(name: str):
    val = os.getenv(name)
    if val is None:
        raise ValueError(f"Missing environment variable: {name}")
    return val


n_neighbors = int(env("N_NEIGHBORS"))
min_dist = float(env("MIN_DIST"))

USE_PCA = True
LOG_PREFIX = (n_neighbors, min_dist)

out_name_pfx = f"/hndr-data/umap_n{n_neighbors}_d{min_dist}"

if USE_PCA:
    d = load_emb_data_pca()
else:
    d = load_emb_data_with_sampling()

with open("/hndr-data/umap_prep_knn_train.joblib", "rb") as f:
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
mapper.fit(d.mat_emb[d.sample_rows_filter])

# Save the UMAP model for later use.
with open(f"{out_name_pfx}_model.joblib", "wb") as f:
    joblib.dump(mapper, f)

print(LOG_PREFIX, "Inferring")
mat_umap = mapper.transform(d.mat_emb)
with open(f"{out_name_pfx}_emb.dat", "wb") as f:
    mat_umap.tofile(f)

print(LOG_PREFIX, "All done!")
