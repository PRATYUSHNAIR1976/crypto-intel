from datetime import timedelta

import pytest

from cryptointel.validation.rules import Severity, load_dq_config
from cryptointel.validation.validator import CHECKS


def test_defaults_cover_exactly_the_implemented_checks():
    cfg = load_dq_config()
    assert set(cfg.rules) == set(CHECKS)
    assert cfg.rule("null_values").severity is Severity.ERROR
    assert cfg.rule("extreme_price_jump").severity is Severity.WARNING
    assert cfg.max_abs_return == {"15m": 0.10, "1h": 0.15, "4h": 0.25, "1d": 0.40}
    assert cfg.future_tolerance == timedelta(seconds=60) and cfg.max_gap_intervals == 1


def test_override_merges_only_what_it_sets(tmp_path):
    path = tmp_path / "r.toml"
    path.write_text('[checks.zero_volume]\nseverity = "error"\n[params.max_abs_return]\n1h = 0.05\n')
    cfg = load_dq_config(path)
    assert cfg.rule("zero_volume").severity is Severity.ERROR and cfg.rule("zero_volume").enabled
    assert cfg.max_abs_return["1h"] == 0.05 and cfg.max_abs_return["1d"] == 0.40
    assert cfg.rule("null_values").severity is Severity.ERROR


def test_unknown_check_name_is_rejected(tmp_path):
    path = tmp_path / "r.toml"
    path.write_text("[checks.nul_values]\nenabled = false\n")  # typo must not silently disable nothing
    with pytest.raises(ValueError, match="nul_values"):
        load_dq_config(path)


def test_invalid_severity_is_rejected(tmp_path):
    path = tmp_path / "r.toml"
    path.write_text('[checks.null_values]\nseverity = "fatal"\n')
    with pytest.raises(ValueError, match="severity"):
        load_dq_config(path)


def test_settings_point_at_custom_rules(tmp_path, monkeypatch):
    from cryptointel.config import Settings

    monkeypatch.setenv("CIP_DQ_RULES_PATH", str(tmp_path / "r.toml"))
    assert Settings(_env_file=None).dq_rules_path == tmp_path / "r.toml"
