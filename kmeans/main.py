from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import silhouette_score
import json
import multiprocessing as mp
import os
import time

from common.emb_data import load_post_embs_bgem3_table

nt = int(os.getenv("OPENBLAS_NUM_THREADS", mp.cpu_count()))
if nt > 64:
    # > OpenBLAS warning: precompiled NUM_THREADS exceeded, adding auxiliary array for thread metadata.
    # > To avoid this warning, please rebuild your copy of OpenBLAS with a larger NUM_THREADS setting or set the environment variable OPENBLAS_NUM_THREADS to 64 or lower.
    raise ValueError(
        "OpenBLAS does not support more than 64 threads, will result in a crash"
    )
if nt != 1:
    print("Warning: OPENBLAS_NUM_THREADS is not 1, which may cause performance issues due to multiprocessing")

K_MIN = int(os.getenv("K_MIN", "2"))
K_MAX = int(os.getenv("K_MAX", "5000"))

os.makedirs("/hndr-data/kmeans_hnsw_bgem3", exist_ok=True)

def calc_kmeans(k):
    print("Loading data")
    df, mat_emb = load_post_embs_bgem3_table()
    df = df.drop(columns=["emb_row"])
    assert mat_emb.shape == (len(df), 1024)

    print("K-clustering", k)
    started = time.time()
    # fit_predict just returns `.fit(X).labels_` (check the source code).
    km = MiniBatchKMeans(
        init="k-means++",
        n_clusters=k,
        reassignment_ratio=0.1,
        max_iter=300,
    ).fit(mat_emb)
    elapsed = time.time() - started
    print(f"K-clustering {k} done in {elapsed:.2f} seconds")

    print("Calculating silhouette score for", k)
    started = time.time()
    s = silhouette_score(mat_emb, km.labels_)
    elapsed = time.time() - started
    print(f"Silhouette score for {k} is {s}, calculated in {elapsed:.2f} seconds")

    # One element per input row, representing the ID of the cluster that input row is in, where a cluster ID is an integer in the range [0, k).
    df[f"k{k}_cluster"] = km.labels_

    df.to_feather(f"/hndr-data/kmeans_hnsw_bgem3/k{k}_cluster.arrow")
    with open(f"/hndr-data/kmeans_hnsw_bgem3/k{k}.json", "w") as f:
      json.dump({
        "silhouette_score": s,
        "k": k,
      }, f)
    print("Saved", k)

with mp.Pool() as p:
    p.map(calc_kmeans, range(K_MIN, K_MAX))
print("All done!")
