import numpy as np
import pyarrow.dataset as ds
import umap

print("Loading data")

f = ds.dataset(f"/hndr-data/posts_canon.arrow", format="ipc")
table = f.to_table()
df_canon = table.to_pandas()

f = ds.dataset(f"/hndr-data/posts.arrow", format="ipc")
table = f.to_table(columns=["id", "emb_dense_title"])
df = table.to_pandas()
print("Posts:", len(df))
assert len(df) == len(df_canon)

df = df[df["emb_dense_title"].notnull()]
print("After filtering NULL titles:", len(df))

df = df[df["id"] == df_canon["canon_id"]]
print("After filtering non-canon:", len(df))

mat_emb = [np.frombuffer(buf, dtype=np.float32) for buf in df.pop("emb_dense_title")]

print("UMAPping")
mapper = umap.UMAP(
    # Do not set a random state, it prevents parallelisation.
    n_components=2,
    metric="cosine",
    n_neighbors=300,
    min_dist=0.25,
    low_memory=False,
    # The default spectral init fails after a very long time, and just falls back to random anyway. The error:
    # > UserWarning: Spectral initialisation failed! The eigenvector solver failed. This is likely due to too small an eigengap. Consider adding some noise or jitter to your data. Falling back to random initialisation!
    init="random",
).fit(mat_emb)
del mat_emb  # Free memory.
mat_umap = mapper.embedding_
assert mat_umap.shape == (len(df), 2)
df["x"] = mat_umap[:, 0].tolist()
df["y"] = mat_umap[:, 1].tolist()

print("Saving")
df.to_feather("/hndr-data/umap.arrow")

print("All done!")
