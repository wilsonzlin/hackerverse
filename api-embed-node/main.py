from dataclasses import dataclass
from FlagEmbedding import BGEM3FlagModel
import base64
import msgpack
import numpy as np
import os
import requests
import websocket


def env(name: str):
    val = os.getenv(name)
    if val is None:
        raise ValueError(f"Missing environment variable: {name}")
    return val


TOKEN = env("API_EMBED_NODE_TOKEN")

public_ip = requests.get("https://icanhazip.com").text.strip()
print("Public IP:", public_ip)

model = BGEM3FlagModel("BAAI/bge-m3", use_fp16=False, normalize_embeddings=True)
print("Model loaded")


@dataclass
class Req:
    id: int
    text: str


def on_error(ws, error):
    print("WS error:", error)


def on_message(ws, raw):
    msg = Req(**msgpack.unpackb(raw))
    out = model.encode(msg.text, return_sparse=True, return_dense=True)
    emb_dense = out["dense_vecs"]
    assert type(emb_dense) == np.ndarray
    emb_sparse = out["lexical_weights"]
    ws.send(
        msgpack.packb(
            {"id": msg.id, "emb_dense": emb_dense.tobytes(), "emb_sparse": emb_sparse}
        )
    )


def on_open(ws):
    print("Opened connection")
    init_req = msgpack.packb({"token": TOKEN, "ip": public_ip})
    assert type(init_req) == bytes
    ws.send(init_req, opcode=websocket.ABNF.OPCODE_BINARY)


websocket.setdefaulttimeout(30)
wsapp = websocket.WebSocketApp(
    "wss://api-embed-node-broker.hndr.wilsonl.in:6000",
    on_error=on_error,
    on_message=on_message,
    on_open=on_open,
)
print("Started listener")
wsapp.run_forever(
    reconnect=30,
    sslopt={
        "ca_certs": base64.standard_b64decode(env("API_EMBED_NODE_CERT_B64")),
    },
)
