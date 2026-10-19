"""Loads data-quality rule configuration (packaged defaults + optional user override)."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from functools import lru_cache
from pathlib import Path

DEFAULT_RULES_PATH = Path(__file__).with_name("dq_rules.toml")


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class RuleConfig:
    enabled: bool
    severity: Severity


@dataclass(frozen=True)
class DQConfig:
    rules: dict[str, RuleConfig]
    max_gap_intervals: int
    extreme_funding_rate: float
    future_tolerance: timedelta
    max_abs_return: dict[str, float]

    def rule(self, name: str) -> RuleConfig:
        return self.rules[name]

    def enabled(self, name: str) -> bool:
        return self.rules[name].enabled


def _merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for key, value in override.items():
        out[key] = _merge(out[key], value) if isinstance(value, dict) and isinstance(out.get(key), dict) else value
    return out


def parse_dq_config(raw: dict, known_checks: set[str] | None = None) -> DQConfig:
    checks = raw.get("checks", {})
    known = known_checks if known_checks is not None else set(checks)
    unknown = set(checks) - known
    if unknown:
        raise ValueError(f"Unknown data-quality checks in config: {sorted(unknown)}")
    rules = {}
    for name, spec in checks.items():
        try:
            severity = Severity(spec.get("severity", "warning"))
        except ValueError as exc:
            raise ValueError(f"Check {name!r}: severity must be one of {[s.value for s in Severity]}") from exc
        rules[name] = RuleConfig(enabled=bool(spec.get("enabled", True)), severity=severity)
    params = raw.get("params", {})
    return DQConfig(
        rules=rules,
        max_gap_intervals=int(params.get("max_gap_intervals", 1)),
        extreme_funding_rate=float(params.get("extreme_funding_rate", 0.03)),
        future_tolerance=timedelta(seconds=float(params.get("future_tolerance_seconds", 60))),
        max_abs_return={str(k): float(v) for k, v in params.get("max_abs_return", {}).items()},
    )


def load_dq_config(path: Path | None = None) -> DQConfig:
    """Packaged defaults, deep-merged with the TOML file at ``path`` if given."""
    defaults = tomllib.loads(DEFAULT_RULES_PATH.read_text())
    raw = defaults if path is None else _merge(defaults, tomllib.loads(Path(path).read_text()))
    return parse_dq_config(raw, known_checks=set(defaults["checks"]))


@lru_cache
def default_dq_config() -> DQConfig:
    return load_dq_config()
