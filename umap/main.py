import joblib
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import umap

print("Loading data")

df_posts = (
    ds.dataset(f"/hndr-data/post_embs.arrow", format="ipc").to_table().to_pandas()
)
print("Posts:", len(df_posts))
df_comments = (
    ds.dataset(f"/hndr-data/comment_embs.arrow", format="ipc").to_table().to_pandas()
)
print("Comments:", len(df_comments))
df = pd.concat([df_posts, df_comments], ignore_index=True)
print("Total:", len(df))

mat_emb = [np.frombuffer(buf, dtype=np.float32) for buf in df.pop("emb")]

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
)
mapper.fit(mat_emb)
del mat_emb  # Free memory.
# Save the UMAP model for later use.
with open("/hndr-data/umap.joblib", "wb") as f:
    joblib.dump(mapper, f)
mat_umap = mapper.embedding_
assert mat_umap.shape == (len(df), 2)
df["x"] = mat_umap[:, 0].tolist()
df["y"] = mat_umap[:, 1].tolist()

print("Saving")
df.to_feather("/hndr-data/umap.arrow")

print("All done!")
