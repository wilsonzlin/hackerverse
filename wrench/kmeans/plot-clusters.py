from common.data import load_table
from common.emb_data import load_bgem3_umap
import matplotlib.pyplot as plt
import os
import sys

k = int(sys.argv[1])
plot_labels = os.getenv("LABELS", "0") == "1"

print("Loading clusters")
df = load_table(f"kmeans_hnsw_bgem3/k{k}_cluster")
print("Loading and merging UMAP")
df = df.merge(load_bgem3_umap(), on="id")

plt.figure()
print("Plotting points")
plt.scatter(df["x"], df["y"], c=df[f"k{k}_cluster"], cmap="hsv", s=1)
if plot_labels:
    print("Plotting labels")
    for _, row in df.iterrows():
        plt.annotate(str(int(row[f"k{k}_cluster"])), (row["x"], row["y"]), fontsize=1)
print("Saving")
plt.title(f"k={k}")
plt.grid(False)
plt.savefig(f"kmeans-clusters-{k}.webp", dpi=600)
