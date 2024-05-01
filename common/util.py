import os


def env(name: str):
    val = os.getenv(name)
    if val is None:
        raise ValueError(f"Missing environment variable: {name}")
    return val
