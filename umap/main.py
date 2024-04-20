import joblib
import numpy as np
import os
import umap

sample_size = int(os.getenv("SAMPLE_SIZE"))
n_neighbors = int(os.getenv("N_NEIGHBORS"))
min_dist = float(os.getenv("MIN_DIST"))

LOG_PREFIX = (sample_size, n_neighbors, min_dist)

out_name_pfx = f"/hndr-data/umap_{sample_size}_n{n_neighbors}_d{min_dist}"

total_count = int(open("/hndr-data/total_count.txt", "r").read())

fp_emb = np.memmap(
    "/hndr-data/mat_shuffled_emb.dat",
    dtype=np.float32,
    mode="r",
    shape=(total_count, 512),
)
mat_train_emb = fp_emb[0:sample_size]
assert mat_train_emb.shape == (sample_size, 512)
mat_rem_emb = fp_emb[sample_size:]
assert mat_rem_emb.shape == (total_count - sample_size, 512)

print(LOG_PREFIX, "Training")
mapper = umap.UMAP(
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
mapper.fit(mat_train_emb)

# Save the UMAP model for later use.
with open(f"{out_name_pfx}.joblib", "wb") as f:
    joblib.dump(mapper, f)

mat_train_umap = mapper.embedding_
assert mat_train_umap.shape == (sample_size, 2)
with open(f"{out_name_pfx}_train_umap.dat", "wb") as f:
    mat_train_umap.tofile(f)

print(LOG_PREFIX, "Inferring")
mat_rem_umap = mapper.transform(mat_rem_emb)
assert mat_rem_umap.shape == (total_count - sample_size, 2)
with open(f"{out_name_pfx}_rem_umap.dat", "wb") as f:
    mat_rem_umap.tofile(f)

print(LOG_PREFIX, "All done!")
