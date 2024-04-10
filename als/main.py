from scipy.sparse import csr_matrix
import implicit
import numpy as np
import pickle
import pyarrow.dataset as ds

df_canon = (
    ds.dataset(f"/hndr-data/posts_canon.arrow", format="ipc").to_table().to_pandas()
)
df = ds.dataset(f"/hndr-data/interactions.arrow", format="ipc").to_table().to_pandas()
print("Interactions:", len(df))

df = df[df.groupby("user")["user"].transform("count") > 3]
print("After filtering low count users:", len(df))

# Not all posts are in posts_cannon e.g. dead, deleted. Therefore, do an inner, not left, join.
df = df.merge(df_canon, how="inner", left_on="post", right_on="id")

# From the docs for [AlternatingLeastSquares.fit](https://benfred.github.io/implicit/api/models/cpu/als.html#implicit.cpu.als.AlternatingLeastSquares.fit):
# > Matrix of confidences for the liked items. This matrix should be a csr_matrix where the rows of the matrix are the users, the columns are the items liked that user, and the value is the confidence that the user liked the item.
# csr_matrix docs: https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html.
print("Creating matrix")
mat = csr_matrix((np.ones(df.shape[0]), (df["user"], df["canon_id"])))

print("Fitting")
model = implicit.als.AlternatingLeastSquares(
    factors=128,
    iterations=20,
)
model.fit(mat)

print("Saving")
with open("/hndr-data/als.pkl", "wb") as f:
    pickle.dump(model, f)
