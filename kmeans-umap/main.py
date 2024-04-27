from common.data import load_mmap_matrix
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import silhouette_score
import json
import multiprocessing as mp
import numpy as np
import os
import pandas as pd

nt = int(os.getenv("OPENBLAS_NUM_THREADS", mp.cpu_count()))
if nt > 64:
    # > OpenBLAS warning: precompiled NUM_THREADS exceeded, adding auxiliary array for thread metadata.
    # > To avoid this warning, please rebuild your copy of OpenBLAS with a larger NUM_THREADS setting or set the environment variable OPENBLAS_NUM_THREADS to 64 or lower.
    raise ValueError(
        "OpenBLAS does not support more than 64 threads, will result in a crash"
    )


with open("/hndr-data/mat_post_embs_bgem3_dense_count.txt") as f:
    count = int(f.read())
mat_id = load_mmap_matrix("mat_post_embs_bgem3_dense_ids", (count,), np.uint32)
mat_umap = load_mmap_matrix("umap_hnsw-bgem3_n300_d0.25_emb", (count, 2), np.float32)

df = pd.DataFrame({"id": mat_id})

MAX_KS = {16, 32, 64, 128, 256, 512, 768, 1024, 1280, 1536, 1792, 2048, 2560, 4096}

optimal_k = None
optimal_ks = {}
for k in range(2, max(MAX_KS) + 1):
    # fit_predict just returns `.fit(X).labels_` (check the source code).
    km = MiniBatchKMeans(
        init="k-means++",
        n_clusters=k,
        reassignment_ratio=0.1,
        max_iter=300,
    ).fit(mat_umap)
    s = silhouette_score(mat_umap, km.labels_)
    if optimal_k is None or optimal_k["s"] < s:
        optimal_k = {
            "s": s,
            "k": k,
            # One element per input row, representing the ID of the cluster that input row is in, where a cluster ID is an integer in the range [0, k).
            "cluster_ids": km.labels_,
        }
    if k in MAX_KS:
        optimal_ks[k] = optimal_k
        print(f"Optimal k up to {k}:", optimal_k)
        df[f"k{k}_cluster_id"] = optimal_k["cluster_ids"]

print("Saving")
df.to_feather("/hndr-data/kmeans-umap-hnsw-bgem3.arrow")
with open("/hndr-data/kmeans-umap-hnsw-bgem3.json", "w") as f:
    json.dump(
        {
            k: {
                "silhouette_score": optimal_k["s"],
                "k": optimal_k["k"],
            }
            for k, optimal_k in optimal_ks.items()
        },
        f,
    )

print("All done!")
