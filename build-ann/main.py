from common.emb_data import load_post_embs_bgem3_table
from pynndescent import NNDescent
import pickle

df, mat_emb = load_post_embs_bgem3_table()
mat_id = df["id"].to_numpy()
print("IDs:", mat_id.shape)
print("Embeddings:", mat_emb.shape)


print("Building index")
idx = NNDescent(
    mat_emb,
    n_neighbors=100,
    metric="cosine",
    verbose=True,
)

print("Saving")
with open("/hndr-data/ann-topposts.pickle", "wb") as f:
    pickle.dump(idx, f, protocol=pickle.HIGHEST_PROTOCOL)

print("All done!")
