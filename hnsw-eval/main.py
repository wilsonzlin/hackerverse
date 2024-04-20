from common.emb_data import load_embs
from math import ceil
from multiprocessing import Pool
import hnswlib
import numpy as np
import os
import time

# We want a 100% accurate baseline to compare to, which we compute using the dot product.
# This requires N * N computations and memory, which grows quadratically and is extreme if we use the full dataset.
# Therefore, we must sample the dataset to a manageable size.
SAMPLE_SIZE = 200_000
BASELINE_WORKERS = os.cpu_count()
CHUNK_SIZE = ceil(SAMPLE_SIZE / BASELINE_WORKERS)


def load_embs_sample():
    np.random.seed(42)
    mat_emb = load_embs()
    mat_emb = mat_emb[np.random.choice(mat_emb.shape[0], SAMPLE_SIZE, replace=False)]
    return mat_emb


def calc_baseline_chunk(start: int):
    log_pfx = f"[{start}]"
    print(log_pfx, "Loading embeddings")
    mat_emb = load_embs_sample()
    np.random.seed(42)
    end = min(start + CHUNK_SIZE, SAMPLE_SIZE)
    log_pfx = f"[{start}-{end}]"

    assert mat_emb.shape == (SAMPLE_SIZE, 512), mat_emb.shape

    print(log_pfx, "Computing dot product")
    chunk = 1 - (mat_emb[start:end] @ mat_emb.T)
    print(log_pfx, "Sorting")
    top_k = np.argsort(chunk, axis=1)[:, :320]  # Slice to save memory.
    print(log_pfx, "Done")
    return top_k


# Parallelize argsort.
print("Calculating baseline using", BASELINE_WORKERS, "workers")
baseline_started = time.time()
with Pool(BASELINE_WORKERS) as pool:
    baseline_chunks = pool.map(calc_baseline_chunk, range(0, SAMPLE_SIZE, CHUNK_SIZE))
baseline_top_k_indices = np.concatenate(baseline_chunks)
print(
    "Baseline",
    baseline_top_k_indices.shape,
    "took",
    time.time() - baseline_started,
    "seconds",
)


def recall_score(true_indices: np.ndarray, predicted_indices: np.ndarray):
    assert true_indices.shape == predicted_indices.shape, (
        true_indices.shape,
        predicted_indices.shape,
    )
    recall_scores = [
        len(np.intersect1d(ti, pi)) / len(ti)
        for ti, pi in zip(true_indices, predicted_indices)
    ]
    return np.mean(recall_scores)


mat_emb = load_embs_sample()

for M in (2, 8, 24, 40, 64, 96, 128):
    for ef in (10, 20, 40, 80, 160, 320):
        log_pfx = f"[M={M}, ef={ef}]"
        started = time.time()

        idx = hnswlib.Index(space="ip", dim=512)
        print(log_pfx, "Initializing index")
        idx.init_index(
            max_elements=SAMPLE_SIZE,
            ef_construction=ef,
            M=M,
        )

        add_started = time.time()
        idx.add_items(mat_emb, np.arange(SAMPLE_SIZE, dtype=np.uint64))
        print(
            log_pfx,
            "Adding",
            mat_emb.shape,
            "items took",
            time.time() - add_started,
            "seconds",
        )

        idx.set_ef(ef)
        query_started = time.time()
        # This can fail entirely:
        # > RuntimeError: Cannot return the results in a contiguous 2D array. Probably ef or M is too small
        try:
            nns, dists = idx.knn_query(mat_emb, k=ef)
        except RuntimeError:
            print(log_pfx, "Failed to query, skipping (assume -inf recall)")
            continue
        print(
            log_pfx,
            "Querying for",
            ef,
            "neighbors took",
            time.time() - query_started,
            "seconds",
        )

        assert type(nns) == np.ndarray
        assert nns.shape == (SAMPLE_SIZE, ef), nns.shape
        assert nns.dtype == np.uint64, nns.dtype
        assert type(dists) == np.ndarray
        assert dists.shape == (SAMPLE_SIZE, ef), dists.shape
        assert dists.dtype == np.float32, dists.dtype

        expected = baseline_top_k_indices[:, :ef]
        recall = recall_score(expected, nns)
        print(log_pfx, "Recall:", recall)


print("All done!")
