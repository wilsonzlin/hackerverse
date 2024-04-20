import pyarrow.dataset as ds
import pandas as pd
import numpy as np

print("Loading data")

df_posts = (
    ds.dataset(f"/hndr-data/post_embs.arrow", format="ipc").to_table().to_pandas()
)
print("Posts:", len(df_posts))
df_comments = (
    ds.dataset(f"/hndr-data/comment_embs.arrow", format="ipc").to_table().to_pandas()
)
print("Comments:", len(df_comments))
df = pd.concat([df_posts, df_comments], ignore_index=True)
total_count = len(df)
print("Total:", total_count)

mat_emb = np.stack(df["emb"].apply(np.frombuffer, dtype=np.float32))
assert mat_emb.shape == (total_count, 512)
mat_id = df["id"].values
assert mat_id.shape == (total_count,)

p = np.random.permutation(total_count)
mat_emb = mat_emb[p]
mat_id = mat_id[p]

with open("total_count.txt", "w") as f:
    f.write(str(total_count))

fp_emb = np.memmap('mat_shuffled_emb.dat', dtype=np.float32, mode='w+', shape=mat_emb.shape)
fp_emb[:] = mat_emb[:]
fp_emb.flush()

fp_id = np.memmap('mat_shuffled_id.dat', dtype=np.int64, mode='w+', shape=mat_id.shape)
fp_id[:] = mat_id[:]
fp_id.flush()
