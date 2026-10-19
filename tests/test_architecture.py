"""Engineering rule: the analytics/strategy side must never know where data came from.

Packages listed in SOURCE_AGNOSTIC may consume market data only through
``cryptointel.domain.ports.MarketDataReader``. They must not import providers,
storage, ingestion, HTTP or database clients. Packages that don't exist yet are
covered automatically as soon as they are created.
"""

from __future__ import annotations

import ast
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "src" / "cryptointel"

SOURCE_AGNOSTIC = ["domain", "features", "regime", "strategy", "scanner", "risk"]
FORBIDDEN_PREFIXES = (
    "cryptointel.providers", "cryptointel.storage", "cryptointel.ingestion",
    "httpx", "requests", "duckdb", "sqlite3", "pyarrow",
)


def _imports(path: Path, package_root: Path) -> list[str]:
    module_parts = list(path.relative_to(package_root.parent).with_suffix("").parts)
    names: list[str] = []
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Import):
            names += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # resolve relative imports against the module's package
                base = module_parts[: -node.level]
                names.append(".".join(base + ([node.module] if node.module else [])))
            elif node.module:
                names.append(node.module)
    return names


def find_violations(package_root: Path, packages: list[str]) -> list[str]:
    violations = []
    for package in packages:
        for path in sorted((package_root / package).rglob("*.py")):
            for name in _imports(path, package_root):
                if name.startswith(FORBIDDEN_PREFIXES):
                    violations.append(f"{path.relative_to(package_root)} imports {name}")
    return violations


def test_source_agnostic_packages_do_not_import_data_sources():
    assert find_violations(PACKAGE_ROOT, SOURCE_AGNOSTIC) == []


def test_checker_catches_absolute_and_relative_violations(tmp_path):
    pkg = tmp_path / "cryptointel"
    (pkg / "strategy").mkdir(parents=True)
    (pkg / "strategy" / "bad_abs.py").write_text("from cryptointel.providers.binance import BinanceProvider\n")
    (pkg / "strategy" / "bad_rel.py").write_text("from ..storage import query\n")
    (pkg / "strategy" / "bad_lib.py").write_text("import duckdb\n")
    (pkg / "strategy" / "ok.py").write_text("from cryptointel.domain.ports import MarketDataReader\nimport pandas\n")
    found = find_violations(pkg, ["strategy"])
    assert len(found) == 3 and not any("ok.py" in v for v in found)
