from common.emb_data import load_emb_data_pca
from common.emb_data import load_emb_data_with_sampling
import joblib
import umap

# Over 100 takes too long (up to several days, even on 96-core machines) and a lot of memory (several hundred gigabytes, possibly terabytes).
UMAP_N_NEIGHBORS_MAX = 100
USE_PCA = True


def calc_knn():
    if USE_PCA:
        d = load_emb_data_pca()
    else:
        d = load_emb_data_with_sampling()

    print("Calculating KNN", UMAP_N_NEIGHBORS_MAX)
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
    with open("/hndr-data/umap_prep_knn_train.joblib", "wb") as f:
        joblib.dump(knn, f)


calc_knn()
print("All done!")
