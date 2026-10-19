"""Structured logging (stdlib only).

Fields passed via ``extra={...}`` become top-level keys, e.g.
``logger.info("ingestion finished", extra={"event": "ingest_finished", "run_id": ...})``.

Formats:
  json  — one JSON object per line (default; machine-readable)
  text  — human-readable line followed by key=value pairs
Logs go to stderr so CLI tables on stdout stay clean.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}


def _fields(record: logging.LogRecord) -> dict:
    return {k: v for k, v in record.__dict__.items() if k not in _RESERVED and not k.startswith("_")}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            **_fields(record),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        pairs = " ".join(f"{k}={v}" for k, v in _fields(record).items())
        line = f"{ts} {record.levelname:<7} {record.name}: {record.getMessage()}" + (f" | {pairs}" if pairs else "")
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else KeyValueFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
