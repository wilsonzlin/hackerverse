from typing import Optional
from typing import TypeVar
from typing import Union
import os

# pyserde will reject an integer for a float field.
Number = Union[float, int]


def env(name: str):
    val = os.getenv(name)
    if val is None:
        raise ValueError(f"Missing environment variable: {name}")
    return val


T = TypeVar("T")


def assert_exists(val: Optional[T]) -> T:
    assert val is not None
    return val
