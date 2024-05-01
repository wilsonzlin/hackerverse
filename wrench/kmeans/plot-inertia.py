from multiprocessing import Pool
from typing import Optional
from typing import Tuple
import json
import matplotlib.pyplot as plt
import os
import re
import sys

max_k = int(sys.argv[1])

d = "/hndr-data/kmeans_hnsw_bgem3"
ks = []
for f in os.listdir(d):
    m = re.search(r"^k([0-9]+)\.json$", f)
    if not m:
        continue
    k = int(m.group(1))
    if k <= max_k:
        ks.append(k)


def load_inertia(k: int) -> Optional[Tuple[int, float]]:
    with open(f"{d}/k{k}.json") as f:
        try:
            meta = json.load(f)
        except:
            return None
    return (k, meta["inertia"])


with Pool() as p:
    pairs = p.map(load_inertia, ks)
pairs = [p for p in pairs if p is not None]
pairs.sort(key=lambda p: p[0])
x, y = zip(*pairs)

plt.figure()
plt.plot(x, y, marker="o", markersize=2)
plt.title("k-means")
plt.xlabel("k")
plt.ylabel("inertia")
plt.grid(True)
plt.savefig(f"kmeans-inertia-max{max_k}.webp", dpi=600)
