from common.data import deserialize_emb_col
from common.data import load_table
import hnswlib

print("Loading data")

df_posts = load_table("post_embs")
print("Posts:", len(df_posts))
df_comments = load_table("comment_embs")
print("Comments:", len(df_comments))

for name, df in [("posts", df_posts), ("comments", df_comments)]:
    print("Processing", name)

    emb_mat = deserialize_emb_col(df, "emb")
    print("Embedding matrix:", emb_mat.shape)
    # This results in a 1-dimensional (NOT (N, 1)-shaped) NumPy array, which is what we want.
    id_mat = df["id"].values
    print("ID matrix:", id_mat.shape)

    idx = hnswlib.Index(space="ip", dim=512)

    print("Initializing index")
    # As proven by hnsw-eval/main.py, M=48 is good for ef > 100. A high ef is also practical for big K queries.
    idx.init_index(
        max_elements=id_mat.shape[0],
        ef_construction=128,
        M=48,
        allow_replace_deleted=True,  # For future updating.
    )

    print("Adding items")
    idx.add_items(emb_mat, id_mat, replace_deleted=True)

    print("Saving", name)
    idx.save_index(f"/hndr-data/hnsw_{name}.index")

print("All done!")
