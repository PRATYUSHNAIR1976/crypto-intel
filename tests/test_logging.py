import json
import logging

from cryptointel.logging_config import JsonFormatter, KeyValueFormatter, configure_logging


def _record(msg="ingestion finished", **extra) -> logging.LogRecord:
    record = logging.LogRecord("cryptointel.test", logging.INFO, __file__, 1, msg, (), None)
    record.__dict__.update(extra)
    return record


def test_json_formatter_emits_one_object_with_extra_fields():
    line = JsonFormatter().format(_record(event="ingest_finished", run_id="abc", rows_written=42))
    payload = json.loads(line)
    assert payload["msg"] == "ingestion finished" and payload["level"] == "INFO"
    assert payload["event"] == "ingest_finished" and payload["run_id"] == "abc" and payload["rows_written"] == 42
    assert payload["ts"].endswith("+00:00")
    assert "args" not in payload and "levelno" not in payload  # stdlib internals are not leaked


def test_json_formatter_includes_exception():
    try:
        raise ValueError("boom")
    except ValueError:
        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", (), __import__("sys").exc_info())
    assert "ValueError: boom" in json.loads(JsonFormatter().format(record))["exc"]


def test_text_formatter_appends_key_values():
    line = KeyValueFormatter().format(_record(event="retry", attempt=2))
    assert "ingestion finished | event=retry attempt=2" in line


def test_configure_logging_replaces_handlers(capsys):
    configure_logging("INFO", "json")
    logging.getLogger("cryptointel.x").info("hello", extra={"event": "e1"})
    assert json.loads(capsys.readouterr().err.strip())["event"] == "e1"
    assert len(logging.getLogger().handlers) == 1


def test_pipeline_logs_structured_finish_event(tmp_path, caplog):
    from tests.test_pipeline import JOB, FakeProvider, build
    from tests.conftest import ohlcv_frame

    pipe, _, _ = build(tmp_path, FakeProvider(ohlcv_frame(24)))
    with caplog.at_level(logging.INFO, logger="cryptointel.ingestion.pipeline"):
        report = pipe.run(JOB)
    finished = [r for r in caplog.records if getattr(r, "event", None) == "ingest_finished"]
    assert len(finished) == 1
    rec = finished[0]
    assert (rec.run_id, rec.symbol, rec.status, rec.rows_written) == (report.run_id, "BTC-USDT", "success", 24)
