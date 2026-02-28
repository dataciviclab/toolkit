from __future__ import annotations

import logging

from toolkit.core.logging import bind_logger, safe_console_text


def test_bind_logger_includes_context(caplog):
    name = "tests.logging_context"
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = True

    with caplog.at_level(logging.INFO, logger=name):
        base = bind_logger(logger, dataset="ds", year=2030, run_id="run-1234")
        layer_logger = bind_logger(base, layer="clean")

        layer_logger.info("hello context")

    matching = [
        rec
        for rec in caplog.records
        if rec.name == name and "hello context" in rec.getMessage()
    ]
    assert matching
    record = matching[-1]
    message = record.getMessage()
    assert record.name == name
    assert "dataset=ds" in message
    assert "run_id=run-1234" in message
    assert "layer=clean" in message
    assert getattr(record, "dataset") == "ds"
    assert getattr(record, "year") == 2030
    assert getattr(record, "run_id") == "run-1234"
    assert getattr(record, "layer") == "clean"


def test_safe_console_text_falls_back_for_non_utf8_encoding():
    text = "GEN-SQL → output.sql"

    assert safe_console_text(text, encoding="cp1252") == "GEN-SQL -> output.sql"
