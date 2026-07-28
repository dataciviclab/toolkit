"""Tests for timed_query and query timing instrumentation.

Covers:
- timed_query context manager: timing accuracy, error handling, logging
- profile_relation uses timed_query internally
- RunContext.add_query_timing persists timings
- Clean _run_sql registers timings when run_ctx is provided
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from lab_connectors.duckdb import safe_connect

from toolkit.core.layer_profile import profile_relation, timed_query
from toolkit.core.run_context import RunContext

pytestmark = pytest.mark.contract


# ---------------------------------------------------------------------------
# timed_query unit tests
# ---------------------------------------------------------------------------


def test_timed_query_returns_result_and_elapsed() -> None:
    with safe_connect() as con:
        con.execute("CREATE TABLE __test_tq AS SELECT 42 AS x UNION ALL SELECT 99 AS x")
        result, elapsed = timed_query(con, "SELECT SUM(x) FROM __test_tq", "test:sum")
        assert result is not None
        assert result.fetchone()[0] == 141
        assert elapsed > 0


def test_timed_query_elapsed_grows_with_slower_query() -> None:
    with safe_connect() as con:
        con.execute("CREATE TABLE __test_tq_slow AS SELECT * FROM range(10000)")
        _, fast_elapsed = timed_query(con, "SELECT COUNT(*) FROM __test_tq_slow", "test:fast")
        _, slow_elapsed = timed_query(
            con,
            "SELECT COUNT(*) FROM __test_tq_slow CROSS JOIN __test_tq_slow",
            "test:slow",
        )
        assert slow_elapsed >= fast_elapsed


def test_timed_query_logs_at_info_level() -> None:
    """Verify timed_query produces INFO log. Uses module logger directly."""
    import logging

    # Capture via a dedicated handler attached to the module logger
    from toolkit.core import layer_profile as lp_mod

    logger = logging.getLogger(lp_mod.__name__)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        from io import StringIO

        buf = StringIO()
        handler.setStream(buf)
        with safe_connect() as con:
            con.execute("CREATE TABLE __test_tq_log AS SELECT 1 AS a")
            timed_query(con, "SELECT COUNT(*) FROM __test_tq_log", "test:logme")
        output = buf.getvalue()
        assert "test:logme" in output and "completed" in output
    finally:
        logger.removeHandler(handler)


def test_timed_query_skips_log_when_log_result_false() -> None:
    """Verify log_result=False suppresses the INFO log."""
    import logging
    from toolkit.core import layer_profile as lp_mod

    logger = logging.getLogger(lp_mod.__name__)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        from io import StringIO

        buf = StringIO()
        handler.setStream(buf)
        with safe_connect() as con:
            con.execute("CREATE TABLE __test_tq_nolog AS SELECT 1 AS a")
            timed_query(con, "SELECT COUNT(*) FROM __test_tq_nolog", "test:nolog", log_result=False)
        output = buf.getvalue()
        assert "test:nolog" not in output
    finally:
        logger.removeHandler(handler)


def test_timed_query_re_raises_on_error() -> None:
    with safe_connect() as con:
        with pytest.raises(duckdb.CatalogException, match="does not exist"):
            timed_query(con, "SELECT * FROM nonexistent_table", "test:error")


def test_timed_query_logs_warning_on_failure() -> None:
    """Verify timed_query logs a warning on failure."""
    import logging
    from toolkit.core import layer_profile as lp_mod

    logger = logging.getLogger(lp_mod.__name__)
    handler = logging.StreamHandler()
    handler.setLevel(logging.WARNING)
    logger.setLevel(logging.WARNING)
    logger.addHandler(handler)
    try:
        from io import StringIO

        buf = StringIO()
        handler.setStream(buf)
        with safe_connect() as con:
            with pytest.raises(duckdb.CatalogException):
                timed_query(con, "SELECT * FROM __nonexistent_42", "test:fail_warn")
        assert "FAILED" in buf.getvalue()
    finally:
        logger.removeHandler(handler)


# ---------------------------------------------------------------------------
# profile_relation timing (uses timed_query internally)
# ---------------------------------------------------------------------------


def test_profile_relation_timing_is_logged() -> None:
    """Verify profile_relation emits timing breakdown at DEBUG level."""
    import logging
    from toolkit.core import layer_profile as lp_mod

    logger = logging.getLogger(lp_mod.__name__)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        from io import StringIO

        buf = StringIO()
        handler.setStream(buf)
        with safe_connect() as con:
            con.execute("CREATE TABLE __test_pr AS SELECT a, a * 2 AS b FROM range(100) t(a)")
            profile = profile_relation(con, "__test_pr")
            assert profile["row_count"] == 100
            assert len(profile["columns"]) == 2
        assert "profile_relation(__test_pr)" in buf.getvalue()
    finally:
        logger.removeHandler(handler)


# ---------------------------------------------------------------------------
# RunContext.add_query_timing
# ---------------------------------------------------------------------------


def test_run_context_query_timings_persisted(tmp_path: Path) -> None:
    ctx = RunContext("test_qt", 2030, root=str(tmp_path))
    ctx.add_query_timing("clean", label="clean:transform", duration_seconds=1.234)
    ctx.add_query_timing("clean", label="clean:export", duration_seconds=0.567)
    ctx.add_query_timing("mart", label="mart:CREATE TABLE agg1", duration_seconds=3.456)

    stored = json.loads(ctx.path.read_text(encoding="utf-8"))
    clean_timings = stored["layers"]["clean"]["query_timings"]
    mart_timings = stored["layers"]["mart"]["query_timings"]

    assert len(clean_timings) == 2
    assert clean_timings[0]["label"] == "clean:transform"
    assert clean_timings[0]["duration_seconds"] == 1.234
    assert clean_timings[1]["label"] == "clean:export"
    assert clean_timings[1]["duration_seconds"] == 0.567

    assert len(mart_timings) == 1
    assert mart_timings[0]["label"] == "mart:CREATE TABLE agg1"
    assert mart_timings[0]["duration_seconds"] == 3.456


def test_run_context_query_timings_empty_by_default(tmp_path: Path) -> None:
    ctx = RunContext("test_qt_empty", 2030, root=str(tmp_path))
    stored = json.loads(ctx.path.read_text(encoding="utf-8"))
    for layer in ("raw", "clean", "mart"):
        assert stored["layers"][layer]["query_timings"] == []


def test_run_context_query_timings_round_trip_thru_run_record(tmp_path: Path) -> None:
    from toolkit.core.run_records import read_run_record, get_run_dir

    ctx = RunContext("qt_roundtrip", 2030, root=str(tmp_path))
    ctx.add_query_timing("clean", label="clean:read", duration_seconds=0.123)
    ctx.add_query_timing("mart", label="mart:export:t1", duration_seconds=1.456)

    record = read_run_record(get_run_dir(tmp_path, "qt_roundtrip", 2030), ctx.run_id)
    assert record["layers"]["clean"]["query_timings"][0]["label"] == "clean:read"
    assert record["layers"]["mart"]["query_timings"][0]["label"] == "mart:export:t1"


# ---------------------------------------------------------------------------
# Integration: profile_parquet_files (calls profile_relation → timed_query)
# ---------------------------------------------------------------------------


def test_profile_parquet_files_works_with_timing(tmp_path: Path) -> None:
    """Verify profile_parquet_files creates a parquet and profiles it."""
    parquet_path = tmp_path / "test.parquet"
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    df.to_parquet(parquet_path)

    from toolkit.core.layer_profile import profile_parquet_files

    profile = profile_parquet_files([parquet_path])
    assert profile["row_count"] == 3
    assert len(profile["columns"]) == 2
