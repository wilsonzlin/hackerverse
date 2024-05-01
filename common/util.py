from typing import Optional
import os


def env(name: str):
    val = os.getenv(name)
    if val is None:
        raise ValueError(f"Missing environment variable: {name}")
    return val


def assert_exists[T](val: Optional[T]) -> T:
    assert val is not None
    return val
