from sklearn.cluster import MiniBatchKMeans
import multiprocessing as mp
import numpy as np
import os
import pandas as pd
import pyarrow.dataset as ds

nt = int(os.getenv("OPENBLAS_NUM_THREADS", mp.cpu_count()))
if nt > 64:
    # > OpenBLAS warning: precompiled NUM_THREADS exceeded, adding auxiliary array for thread metadata.
    # > To avoid this warning, please rebuild your copy of OpenBLAS with a larger NUM_THREADS setting or set the environment variable OPENBLAS_NUM_THREADS to 64 or lower.
    raise ValueError(
        "OpenBLAS does not support more than 64 threads, will result in a crash"
    )


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
mat = np.array(
    [np.frombuffer(buf, dtype=np.float32) for buf in df.pop("emb")], dtype=np.float32
)
print("Matrix:", mat.shape)
assert mat.shape == (len(df), 512)

for k in (16, 32, 64, 128, 256, 512, 768, 1024, 1280, 1536, 1792, 2048, 2560):
    print("K-clustering", k)
    # fit_predict just returns `.fit(X).labels_` (check the source code).
    km = MiniBatchKMeans(
        init="k-means++",
        n_clusters=k,
        reassignment_ratio=0.1,
        max_iter=300,
    ).fit(mat)
    # One element per input row, representing the ID of the cluster that input row is in, where a cluster ID is an integer in the range [0, k).
    df[f"k{k}_cluster_id"] = km.labels_.tolist()

print("Saving")
df.to_feather("/hndr-data/kmeans.arrow")

print("All done!")
