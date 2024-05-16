from common.data import DatasetEmbModel
from common.data import load_embs
from common.data import load_table
from common.util import env
from db_rpc_client_py import DbRpcClient
from fastapi import FastAPI
from fastapi import Form
from fastapi import Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pandas import DataFrame
from typing import Annotated
import msgpack
import os
import uvicorn

DIR = os.path.dirname(os.path.realpath(__file__))

db = DbRpcClient(
    endpoint="https://db-rpc.posh.wilsonl.in",
    api_key=env("DB_RPC_API_KEY"),
).database("hndr")


def get_kv(k: str):
    rows = db.query("select v from kv where k = ?", [k])
    return rows[0]["v"] if rows else None


print("Loading model")
model = DatasetEmbModel("post")
print("Loading posts")
data = load_table("posts").rename(columns={"url": "url_id"})
print("Loading post titles")
data = data.merge(
    load_table("post_titles").rename(columns={"text": "title"}),
    on="id",
    how="inner",
)
print("Loading post URLs")
data = data.merge(
    load_table("urls").rename(columns={"id": "url_id"}),
    on="url_id",
    how="inner",
)
print("Loading embeddings")
mat_ids, mat_embs = load_embs("post")
print("All data loaded")

templates = Jinja2Templates(directory=f"{DIR}/templates")

app = FastAPI()

app.mount("/static", StaticFiles(directory=f"{DIR}/static"), name="static")


@app.get("/")
def get_home(request: Request):
    return templates.TemplateResponse(request=request, name="home.html")


@app.get("/c")
def get_community(request: Request, community: str, sim_threshold: float):
    df = data.copy(deep=False)
    mat_q = model.encode(community)
    mat_sims = mat_embs @ mat_q
    df_sim = DataFrame(
        {
            "id": mat_ids,
            "sim": mat_sims,
        }
    )
    df = df.merge(df_sim, on="id", how="inner")
    df = df[df["sim"] >= sim_threshold]
    df = df.sort_values("sim", ascending=True)
    df = df[:100]
    return templates.TemplateResponse(
        request=request,
        name="community.html",
        context={
            "community": community,
            "sim_threshold": sim_threshold,
            "posts": df.to_dict("records"),
        },
    )


@app.get("/post/{post_id}")
def get_post(
    request: Request,
    post_id: int,
):
    emb_input_raw = get_kv(f"post/{post_id}/emb_input").decode("utf-8")
    url_id = db.query("select url from post where id = ?", [post_id])[0]["url"]
    text = get_kv(f"url/{url_id}/text").decode("utf-8")
    meta = msgpack.loads(get_kv(f"url/{url_id}/meta"))
    # Use `.get(key) or ""` instead of `.get(key, "")` as the key may exist but value is None.
    emb_input = (
        emb_input_raw.replace("<<<REPLACE_WITH_PAGE_TITLE>>>", meta.get("title") or "")
        .replace("<<<REPLACE_WITH_PAGE_DESCRIPTION>>>", meta.get("description") or "")
        .replace("<<<REPLACE_WITH_PAGE_TEXT>>>", text)
    )
    return templates.TemplateResponse(
        request=request,
        name="post.html",
        context={
            "emb_input": emb_input,
        },
    )


@app.post("/c/examples")
def set_community_example(
    request: Request,
    community: Annotated[str, Form()],
    item: Annotated[int, Form()],
    sim: Annotated[float, Form()],
    positive: Annotated[bool, Form()] = False,
):
    db.exec(
        """
        insert into community_example (community, item, positive, sim)
        values (?, ?, ?, ?)
        on duplicate key update
          positive = values(positive),
          sim = values(sim)
        """,
        [community, item, positive, sim],
    )
    return templates.TemplateResponse(request=request, name="autoclose.html")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(env("ADMIN_PORT")))
