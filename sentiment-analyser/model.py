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
    encoded_input = tokenizer(
        x.texts, return_tensors="pt", padding=True, truncation=True
    )
    output = model(**encoded_input)
    scores = output.logits.detach().numpy()
    scores = [
        {
            config.id2label[i]: score
            # Make sure to apply softmax to each row, not to the entire matrix.
            for i, score in enumerate(softmax(row).tolist())
        }
        for row in scores
    ]
    statsd.timing("model_ms", (time.time() - started) * 1000)
    statsd.incr("model_input_count", len(x.texts))
    statsd.incr("model_char_count", sum(len(t) for t in x.texts))
    return {
        "scores": scores,
    }


PyIpc().add_handler("model", ModelReq, model_handler).begin_loop()
