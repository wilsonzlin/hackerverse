from common.emb_data import load_emb_data_with_sampling
from common.emb_data import load_emb_table_ids
from common.emb_data import load_embs
import hnswlib
import joblib
import numpy as np
import umap

MODE = "hnsw"  # "hnsw", "hnsw-sampling", "pynndescent-pca-sampling", "pynndescent-sampling"
# WARNING: Over 100 without PCA takes too long with PyNNDescent (up to several days, even on 96-core machines) and a lot of memory (several hundred gigabytes, possibly terabytes).
UMAP_N_NEIGHBORS_MAX = 300


def calc_knn():
    if MODE.startswith("hnsw"):
        if MODE == "hnsw":
            mat_id = load_emb_table_ids().total.values
            mat_emb = load_embs()
        else:
            d = load_emb_data_with_sampling()
            mat_id = d.sample_ids.values
            mat_emb = d.mat_emb[d.sample_rows_filter]
        count = mat_id.shape[0]
        assert mat_emb.shape == (count, 512), mat_emb.shape
        print("Loaded", count, "embeddings", mat_emb.shape)
        idx = hnswlib.Index(space="ip", dim=512)
        idx.init_index(
            max_elements=count,
            ef_construction=UMAP_N_NEIGHBORS_MAX,
            M=48,
        )
        print("Indexing", mat_emb.shape, "embeddings")
        idx.add_items(mat_emb, mat_id)
        print("Querying")
        # knn_query will return itself as the first result, so we don't need to specially add it.
        nns, dists = idx.knn_query(mat_emb, k=UMAP_N_NEIGHBORS_MAX)
        print("Got results", nns.shape, dists.shape)
        assert type(nns) == np.ndarray
        assert nns.shape == (count, UMAP_N_NEIGHBORS_MAX)
        assert nns.dtype == np.uint64
        assert type(dists) == np.ndarray
        assert dists.shape == (count, UMAP_N_NEIGHBORS_MAX)
        assert dists.dtype == np.float64
        # Save some storage space by converting to smaller types.
        knn = (nns.astype(np.uint32), dists.astype(np.float32))
    else:
        d = load_emb_data_with_sampling(MODE == "pynndescent-pca-sampling")

        print("Calculating KNN", UMAP_N_NEIGHBORS_MAX)
        # This uses PyNNDescent internally.
        knn = umap.umap_.nearest_neighbors(
            d.mat_emb[d.sample_rows_filter],
            angular=False,
            low_memory=False,
            metric_kwds=None,
            metric="cosine",
            n_neighbors=UMAP_N_NEIGHBORS_MAX,
            # Don't use random_state to use parallelism.
            random_state=None,
            verbose=True,
        )

    print("Saving KNN")
    with open(f"/hndr-data/umap_prep_knn_{MODE}.joblib", "wb") as f:
        joblib.dump(knn, f)


calc_knn()
print("All done!")
