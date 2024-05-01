from common.data import load_table
import json
import sys

k = int(sys.argv[1])


def get_site(url: str):
    return url.split("/")[0]


print("Loading clusters")
df = load_table(f"kmeans_hnsw_bgem3/k{k}_cluster")
print("Loading and merging posts")
df = df.merge(
    load_table("posts", columns=["id", "url"]).rename(
        columns={
            "url": "url_id",
        }
    ),
    on="id",
)
print("Loading and merging URLs")
df = df.merge(
    load_table("urls", columns=["id", "url"]).rename(
        columns={
            "id": "url_id",
        }
    ),
    on="url_id",
)
df["site"] = df["url"].apply(get_site)
print("Loading and merging titles")
df = df.merge(load_table("post_titles"), on="id")

print("Grouping by cluster")
out = []
for _, group in df.groupby(f"k{k}_cluster"):
    group = group.sample(n=100, random_state=42)
    out.append(
        {
            "cluster": int(group[f"k{k}_cluster"].iloc[0]),
            "titles": (group["text"] + " (" + group["site"] + ")").tolist(),
        }
    )
print("Saving")
with open(f"cluster-titles-{k}.json", "w") as f:
    json.dump(out, f, indent=2)
