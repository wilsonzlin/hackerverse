from typing import Optional
from typing import TypeVar
import os


def env(name: str):
    val = os.getenv(name)
    if val is None:
        raise ValueError(f"Missing environment variable: {name}")
    return val


T = TypeVar("T")


def assert_exists(val: Optional[T]) -> T:
    assert val is not None
    return val
