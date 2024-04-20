from common.emb_data import dump_mmap_matrix
from common.emb_data import load_emb_data_with_sampling
from common.emb_data import PCA_COMPONENTS
from sklearn.decomposition import PCA
import joblib


def calc_pca():
    d = load_emb_data_with_sampling()
    mat_emb_train = d.mat_emb[d.sample_rows_filter]

    pca = PCA(n_components=PCA_COMPONENTS)
    print("Fitting PCA", PCA_COMPONENTS, "over training data", mat_emb_train.shape)
    pca.fit(mat_emb_train)
    print("Explained variance:", pca.explained_variance_ratio_.sum())
    print("Saving PCA")
    with open("/hndr-data/pca_model.joblib", "wb") as f:
        joblib.dump(pca, f)

    print("Transforming all embeddings using PCA", d.mat_emb.shape)
    mat_emb_pca = pca.transform(d.mat_emb)
    assert mat_emb_pca.shape == (d.total_count, PCA_COMPONENTS)

    dump_mmap_matrix("pca_emb", mat_emb_pca)


calc_pca()
print("All done!")
