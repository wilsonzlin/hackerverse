from common.data import load_mmap_matrix
from common.emb_data import load_emb_table_ids
import msgpack
import numpy as np


def load_hnsw_umap():
    mat_id = load_emb_table_ids().total.to_numpy()
    count = mat_id.shape[0]
    mat_umap = load_mmap_matrix(
        "umap_hnsw_n50_d0.25_emb", (count, 2), np.float32
    ).tolist()
    out = {}
    for i, id_ in enumerate(mat_id.tolist()):
        out[id_] = {"x": mat_umap[i][0], "y": mat_umap[i][1]}
    return out


def load_hnsw_bgem3_umap():
    with open("/hndr-data/mat_post_embs_bgem3_dense_count.txt") as f:
        count = int(f.readline())
    mat_id = load_mmap_matrix("mat_post_embs_bgem3_dense_ids", (count,), np.uint32)
    mat_umap = load_mmap_matrix(
        "umap_hnsw-bgem3_n300_d0.25_emb", (count, 2), np.float32
    ).tolist()
    out = {}
    for i, id_ in enumerate(mat_id.tolist()):
        out[id_] = {"x": mat_umap[i][0], "y": mat_umap[i][1]}
    return out


out = {
    "hnsw": {
        "umap": load_hnsw_umap(),
        "map": msgpack.unpack(open("/hndr-data/map-hnsw.msgpack", "rb")),
    },
    "hnsw-bgem3": {
        "umap": load_hnsw_bgem3_umap(),
        "map": msgpack.unpack(open("/hndr-data/map-hnsw-bgem3.msgpack", "rb")),
    },
}
msgpack.pack(out, open("/hndr-data/edge.msgpack", "wb"))
