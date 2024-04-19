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

set_excepthook()

statsd = StatsClient("localhost", 8125, prefix=os.getenv("MAIN"))

MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"

tokenizer = AutoTokenizer.from_pretrained(MODEL)
config = AutoConfig.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL).to("cuda")


@dataclass
class ModelReq:
    texts: List[str]


def model_handler(x: ModelReq):
    started = time.time()
    res = []
    # Comments can vary in length dramatically, so padding will cause a quadratic jump in VRAM requirements if just one comment is very long.
    # Also, many GPUs have less than 16 GB VRAM.
    # Also, after a certain batch size, the performance does not increase.
    # Therefore, tune batch size well.
    BATCH_SIZE = 8
    for i in range(0, len(x.texts), BATCH_SIZE):
        batch = x.texts[i : i + BATCH_SIZE]
        # Set padding=True even though we only have one input, in case we change the batch size in the future.
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
