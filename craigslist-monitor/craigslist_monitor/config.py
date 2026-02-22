"""Load config from TOML."""
from __future__ import annotations

from pathlib import Path

if __import__("sys").version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def load_config(path: str | Path = "config.toml") -> dict:
    """Load config.toml from path. Returns dict with craigslist, telegram, monitor sections."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}. Copy config.example.toml to config.toml and edit.")
    with open(p, "rb") as f:
        return tomllib.load(f)
