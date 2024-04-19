import hnswlib
import numpy as np
import pyarrow.dataset as ds

print("Loading data")

df_posts = (
    ds.dataset(f"/hndr-data/post_embs.arrow", format="ipc").to_table().to_pandas()
)
print("Posts:", len(df_posts))
df_comments = (
    ds.dataset(f"/hndr-data/comment_embs.arrow", format="ipc").to_table().to_pandas()
)
print("Comments:", len(df_comments))

for name, df in [("posts", df_posts), ("comments", df_comments)]:
    print("Processing", name)

    emb_mat = np.stack(df["emb"].apply(np.frombuffer, dtype=np.float32))
    print("Embedding matrix:", emb_mat.shape)
    # This results in a 1-dimensional (NOT (N, 1)-shaped) NumPy array, which is what we want.
    id_mat = df["id"].values
    print("ID matrix:", id_mat.shape)

    idx = hnswlib.Index(space="ip", dim=512)

    print("Initializing index")
    idx.init_index(
        max_elements=id_mat.shape[0],
        ef_construction=128,
        M=64,
        allow_replace_deleted=True,  # For future updating.
    )

    print("Adding items")
    idx.add_items(emb_mat, id_mat, replace_deleted=True)

    print("Saving", name)
    idx.save_index(f"/hndr-data/hnsw_{name}.index")

print("All done!")
