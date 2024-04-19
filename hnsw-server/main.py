from fastapi import FastAPI
from sentence_transformers import SentenceTransformer
import hnswlib

print("Loading model")
model = SentenceTransformer(
    "jinaai/jina-embeddings-v2-small-en",
    trust_remote_code=True,
)

print("Loading posts")
idx_posts = hnswlib.Index(space="ip", dim=512)
idx_posts.load_index("/hndr-data/hnsw_posts.index", allow_replace_deleted=True)
idx_posts.set_ef(128)

print("Loading comments")
idx_comments = hnswlib.Index(space="ip", dim=512)
idx_comments.load_index("/hndr-data/hnsw_comments.index", allow_replace_deleted=True)
idx_comments.set_ef(128)

app = FastAPI()


@app.get("/")
def search(dataset: str, query: str, limit: int):
    if dataset == "posts":
        idx = idx_posts
    elif dataset == "comments":
        idx = idx_comments
    else:
        raise ValueError("Invalid dataset")
    emb = model.encode([query], convert_to_numpy=True, normalize_embeddings=True)
    ids, dists = idx.knn_query(emb, k=limit)
    return [
        {"id": id, "distance": dist} for id, dist in zip(ids.tolist(), dists.tolist())
    ]
