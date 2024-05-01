from common.emb_data import load_embs
from sklearn.cluster import MiniBatchKMeans
import json
import multiprocessing as mp
import os
import pandas as pd
import time

nt = int(os.getenv("OPENBLAS_NUM_THREADS", mp.cpu_count()))
if nt > 64:
    # > OpenBLAS warning: precompiled NUM_THREADS exceeded, adding auxiliary array for thread metadata.
    # > To avoid this warning, please rebuild your copy of OpenBLAS with a larger NUM_THREADS setting or set the environment variable OPENBLAS_NUM_THREADS to 64 or lower.
    raise ValueError(
        "OpenBLAS does not support more than 64 threads, will result in a crash"
    )

DATASET = "toppost"
K_MIN = int(os.getenv("K_MIN", "2"))
K_MAX = int(os.getenv("K_MAX", "5000"))

d = f"/hndr-data/kmeans-{DATASET}"
os.makedirs(d, exist_ok=True)


def calc_kmeans(k: int):
    f_json = f"{d}/k{k}.json"
    if os.path.isfile(f_json):
        print("Skipping", k, "as it already exists")
        return

    print("Loading data")
    mat_id, mat_emb = load_embs(DATASET)

    print("K-clustering", k)
    started = time.time()
    # fit_predict just returns `.fit(X).labels_` (check the source code).
    km = MiniBatchKMeans(
        init="k-means++",
        n_clusters=k,
        # https://stackoverflow.com/a/23527049
        reassignment_ratio=0,
        max_iter=300,
    ).fit(mat_emb)
    elapsed = time.time() - started
    print(f"K-clustering {k} done in {elapsed:.2f} seconds")

    # We can't use silhouette score, since that requires O(n^2) computations and memory for pairwise distances. It's too slow and expensive. We'll just use the inertia value. Even if we precompute ourselves using the dot product, we run out of memory (~500K ^ 2 is huge). https://datascience.stackexchange.com/a/36074

    # One element per input row, representing the ID of the cluster that input row is in, where a cluster ID is an integer in the range [0, k).
    df = pd.DataFrame({"id": mat_id, f"k{k}_cluster": km.labels_})

    df.to_feather(f"{d}/k{k}_cluster.arrow")
    with open(f"{f_json}.tmp", "w") as f:
        json.dump(
            {
                "cluster_centers": km.cluster_centers_.tolist(),
                "inertia": km.inertia_,
                "iters": km.n_iter_,
                "k": k,
                "steps": km.n_steps_,
                "train_time_sec": elapsed,
            },
            f,
        )
    os.rename(f"{f_json}.tmp", f_json)
    print("Saved", k)


with mp.Pool() as p:
    p.map(calc_kmeans, range(K_MIN, K_MAX))
print("All done!")
