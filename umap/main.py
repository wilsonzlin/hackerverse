from sklearn.cluster import MiniBatchKMeans
import numpy as np
import pyarrow.dataset as ds
import umap

print("Loading data")
f = ds.dataset(f"/hndr-data/posts.arrow", format="ipc")
table = f.to_table(columns=["id", "emb_dense_title"])
df = table.to_pandas()
df = df[df["emb_dense_title"].notnull()]
mat_emb = [np.frombuffer(buf, dtype=np.float32) for buf in df.pop("emb_dense_title")]

print("UMAPping")
mapper = umap.UMAP(n_components=2).fit(mat_emb)
del mat_emb  # Free memory.
mat_umap = mapper.embedding_
assert mat_umap.shape == (len(df), 2)
df["x"] = mat_umap[:, 0].tolist()
df["y"] = mat_umap[:, 1].tolist()

for zoom_level in range(6):
    k = 16384 * (2**zoom_level)
    print("K-clustering", k)
    # fit_predict just returns `.fit(X).labels_` (check the source code).
    km = MiniBatchKMeans(
        init="k-means++",
        n_clusters=k,
    ).fit(mat_umap)
    # One element per input row, representing the ID of the cluster that input row is in, where a cluster ID is an integer in the range [0, k).
    df[f"k{k}_cluster_id"] = km.labels_.tolist()

print("Saving")
df.to_feather("/hndr-data/umap.arrow")

print("All done!")
