"""Credentials must never appear in source code, logs, reprs or git."""

import re
from pathlib import Path

from cryptointel.config import Settings

ROOT = Path(__file__).resolve().parents[1]

# Assignments of long literal strings to names that look like credentials.
SECRET_ASSIGNMENT = re.compile(
    r"""(api[_-]?key|secret|token|password|passphrase)\w*\s*[:=]\s*(SecretStr\()?["'][A-Za-z0-9_\-]{12,}["']""",
    re.IGNORECASE,
)


def test_no_hardcoded_credentials_in_source():
    offenders = []
    for path in [*ROOT.glob("src/**/*.py"), ROOT / ".env.example", ROOT / "pyproject.toml"]:
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if SECRET_ASSIGNMENT.search(line):
                offenders.append(f"{path.relative_to(ROOT)}:{n}")
    assert offenders == []


def test_env_example_has_no_values_for_keys():
    for line in (ROOT / ".env.example").read_text().splitlines():
        if "API_KEY" in line and not line.lstrip().startswith("#"):
            assert line.split("=", 1)[1].strip() == ""


def test_secret_settings_are_masked(monkeypatch):
    monkeypatch.setenv("CIP_COINGECKO_API_KEY", "super-secret-demo-key")
    s = Settings(_env_file=None)
    assert s.coingecko_api_key.get_secret_value() == "super-secret-demo-key"
    for rendering in (repr(s), str(s), s.model_dump_json()):
        assert "super-secret-demo-key" not in rendering


def test_env_file_is_git_ignored():
    ignored = (ROOT / ".gitignore").read_text().splitlines()
    assert ".env" in ignored
