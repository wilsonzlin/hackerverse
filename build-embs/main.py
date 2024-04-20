from common.data import deserialize_emb_col
from common.data import dump_mmap_matrix
from common.data import load_table
from common.emb_data import merge_posts_and_comments

# This takes a long time (reading from disk and deserializing) and is done repeatedly, so we do it once here and then export it.
# As a bonus, we can mmap the exported matrix which saves memory, esp. if used by parallel processes. It also removes any loading time.
# The row order is the same as the original tables and the same as `load_emb_table_ids`, so you can efficiently load the IDs only safely without worrying about ordering.

print("Loading data")
df_posts = load_table("post_embs")
df_comments = load_table("comment_embs")
df = merge_posts_and_comments(posts=df_posts, comments=df_comments)
count = len(df)
print("Deserializing", count, "embeddings")
mat_emb = deserialize_emb_col(df, "emb")
dump_mmap_matrix("embs", mat_emb)
with open("/hndr-data/embs_count.txt", "w") as f:
    f.write(str(count))
print("All done!")
