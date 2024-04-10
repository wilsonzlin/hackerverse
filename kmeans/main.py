from sklearn.cluster import MiniBatchKMeans
import numpy as np
import pyarrow.dataset as ds

print("Loading data")
f = ds.dataset(f"/hndr-data/umap.arrow", format="ipc")
table = f.to_table(columns=["id", "x", "y"])
df = table.to_pandas()
mat = np.array(zip(df.pop("x"), df.pop("y")), dtype=np.float32)

for zoom_level in range(6):
    k = 16384 * (2**zoom_level)
    print("K-clustering", k)
    # fit_predict just returns `.fit(X).labels_` (check the source code).
    km = MiniBatchKMeans(
        init="k-means++",
        n_clusters=k,
    ).fit(mat)
    # One element per input row, representing the ID of the cluster that input row is in, where a cluster ID is an integer in the range [0, k).
    df[f"k{k}_cluster_id"] = km.labels_.tolist()

print("Saving")
df.to_feather("/hndr-data/kmeans.arrow")

print("All done!")
