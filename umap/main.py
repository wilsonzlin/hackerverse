from sklearn.model_selection import train_test_split
import joblib
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import umap

print("Loading data")

df_posts = (
    ds.dataset(f"/hndr-data/post_embs.arrow", format="ipc").to_table().to_pandas()
)
print("Posts:", len(df_posts))
df_comments = (
    ds.dataset(f"/hndr-data/comment_embs.arrow", format="ipc").to_table().to_pandas()
)
print("Comments:", len(df_comments))
df_total = pd.concat([df_posts, df_comments], ignore_index=True)
total_count = len(df_total)
print("Total:", total_count)


def process_data(*, sample_size: int, n_neighbors: int, min_dist: float):
    out_name_pfx = f"/hndr-data/umap_{sample_size}_n{n_neighbors}_d{min_dist}"

    df_train, df_rem = train_test_split(
        df_total, train_size=sample_size, random_state=42
    )
    assert len(df_train) == sample_size
    print("Sample size:", len(df_train) / total_count * 100, "%")

    mat_train_emb = np.stack(df_train.pop("emb").apply(np.frombuffer, dtype=np.float32))
    assert mat_train_emb.shape == (sample_size, 512)

    mat_rem_emb = np.stack(df_rem.pop("emb").apply(np.frombuffer, dtype=np.float32))
    assert mat_rem_emb.shape == (total_count - sample_size, 512)

    print("Training", sample_size, n_neighbors, min_dist)
    mapper = umap.UMAP(
        # Do not set a random state, it prevents parallelisation.
        n_components=2,
        metric="cosine",
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        low_memory=False,
        # The default spectral init fails after a very long time, and just falls back to random anyway. The error:
        # > UserWarning: Spectral initialisation failed! The eigenvector solver failed. This is likely due to too small an eigengap. Consider adding some noise or jitter to your data. Falling back to random initialisation!
        init="random",
    )
    mapper.fit(mat_train_emb)

    # Save the UMAP model for later use.
    with open(f"{out_name_pfx}.joblib", "wb") as f:
        joblib.dump(mapper, f)

    mat_train_umap = mapper.embedding_
    assert mat_train_umap.shape == (sample_size, 2)
    df_train["x"] = mat_train_umap[:, 0].tolist()
    df_train["y"] = mat_train_umap[:, 1].tolist()
    df_train["pred"] = False

    print("Inferring")
    mat_rem_umap = mapper.transform(mat_rem_emb)
    assert mat_rem_umap.shape == (total_count - sample_size, 2)
    df_rem["x"] = mat_rem_umap[:, 0].tolist()
    df_rem["y"] = mat_rem_umap[:, 1].tolist()
    df_rem["pred"] = True

    df = pd.concat([df_train, df_rem], ignore_index=True)
    assert len(df) == total_count

    print("Saving")
    df.to_feather(f"{out_name_pfx}.arrow")


for subsample in (100_000, 500_000, 1_000_000, 4_000_000):
    for n_neighbors in (20, 50, 100, 300):
        for min_dist in (0.1, 0.25, 0.5, 0.85):
            process_data(
                sample_size=subsample, n_neighbors=n_neighbors, min_dist=min_dist
            )
print("All done!")
