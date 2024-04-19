from dataclasses import dataclass
from msgpipe import PyIpc
from scipy.special import softmax
from service_toolkit.panic import set_excepthook
from statsd import StatsClient
from transformers import AutoConfig
from transformers import AutoModelForSequenceClassification
from transformers import AutoTokenizer
from typing import List
import os
import time
import torch

set_excepthook()

statsd = StatsClient("localhost", 8125, prefix=os.getenv("MAIN"))

MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"

tokenizer = AutoTokenizer.from_pretrained(MODEL)
config = AutoConfig.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL).to("cuda")

# Comments can vary in length dramatically, so padding will cause a quadratic jump in VRAM requirements if just one comment is very long.
# Also, many GPUs have less than 16 GB VRAM.
# Also, after a certain batch size, the performance does not increase.
# Therefore, tune batch size well.
total_vram_bytes = torch.cuda.get_device_properties(0).total_memory
total_vram_gib = total_vram_bytes / (1024**3)
BATCH_SIZE = round(0.75 * total_vram_gib)
print("VRAM:", total_vram_gib, "GiB")
print("Batch size:", BATCH_SIZE)


@dataclass
class ModelReq:
    texts: List[str]


def model_handler(x: ModelReq):
    started = time.time()
    res = []
    for i in range(0, len(x.texts), BATCH_SIZE):
        batch = x.texts[i : i + BATCH_SIZE]
        encoded_input = tokenizer(
            batch, return_tensors="pt", padding=True, truncation=True, max_length=512
        ).to("cuda")
        output = model(**encoded_input)
        scores = output.logits.detach().cpu().numpy()
        for j in range(len(batch)):
            res.append(
                {
                    config.id2label[k]: score
                    # Make sure to apply softmax to each row, not to an entire batch matrix.
                    for k, score in enumerate(softmax(scores[j]).tolist())
                }
            )
    statsd.timing("model_ms", (time.time() - started) * 1000)
    statsd.incr("model_input_count", len(x.texts))
    statsd.incr("model_char_count", sum(len(t) for t in x.texts))
    return {
        "scores": res,
    }


PyIpc().add_handler("model", ModelReq, model_handler).begin_loop()
