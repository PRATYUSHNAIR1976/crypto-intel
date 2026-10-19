"""Immutable raw landing zone: every provider response, gzipped, exactly as received.

Layout: raw/{provider}/{dataset}/{market}/{symbol}/{interval|all}/{YYYY-MM-DD}/{run_id}.json.gz
"""

from __future__ import annotations

import gzip
import json
from dataclasses import asdict
from pathlib import Path

from cryptointel.domain.models import utc_now
from cryptointel.providers.base import RawPage


class RawStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def write(self, *, run_id: str, provider: str, dataset: str, market: str, symbol: str,
              interval: str | None, pages: list[RawPage]) -> Path:
        day = utc_now().strftime("%Y-%m-%d")
        path = self.root / provider / dataset / market / symbol / (interval or "all") / day / f"{run_id}.json.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        document = {
            "run_id": run_id, "provider": provider, "dataset": dataset, "market": market,
            "symbol": symbol, "interval": interval, "pages": [asdict(p) for p in pages],
        }
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            json.dump(document, fh, default=str)
        return path

    @staticmethod
    def read(path: Path) -> dict:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
