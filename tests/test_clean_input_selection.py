from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb
import pytest

from toolkit.clean.input_selection import list_raw_candidates, select_inputs
from toolkit.clean.run import run_clean
from toolkit.core.manifest import write_raw_manifest


class _NoopLogger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


def _write_csv(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _write_clean_sql(tmp_path: Path, sql: str = "SELECT 1 AS x") -> Path:
    sql_path = tmp_path / "clean.sql"
    sql_path.write_text(sql, encoding="utf-8")
    return sql_path


def _run_clean_capture_inputs(
    monkeypatch,
    tmp_path: Path,
    clean_cfg: dict[str, object],
    *,
    logger=None,
) -> dict[str, object]:
    seen: dict[str, object] = {}

    def _fake_run_sql(input_files, sql_query, output_path, read_cfg=None, **_kwargs):
        seen["input_files"] = input_files
        seen["sql_query"] = sql_query
        seen["output_path"] = output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"PAR1")
        return ("strict", {"delim": ";", "decimal": ",", "encoding": "utf-8"})

    monkeypatch.setattr("toolkit.clean.run._run_sql", _fake_run_sql)
    run_clean("demo", 2024, str(tmp_path), clean_cfg, logger or _NoopLogger())
    return seen


# Candidate discovery and selection

def test_list_input_files_accepts_csv_gz_and_excludes_php(tmp_path: Path):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)

    gz_file = raw_dir / "data.csv.gz"
    gz_file.write_bytes(b"fake-gzip-content")
    php_file = raw_dir / "data.csv.php"
    php_file.write_text("<?php echo 'x';", encoding="utf-8")

    files = list_raw_candidates(str(tmp_path), "demo", 2024)

    assert gz_file in files
    assert php_file not in files


def test_select_inputs_explicit_requires_include(tmp_path: Path) -> None:
    candidate = _write_csv(tmp_path / "input.csv", "a\n1\n")

    with pytest.raises(ValueError, match="requires clean.read.include"):
        select_inputs([candidate], mode="explicit")


def test_select_inputs_latest_uses_newest_mtime(tmp_path: Path) -> None:
    older = _write_csv(tmp_path / "older.csv", "a\n1\n")
    newer = _write_csv(tmp_path / "newer.csv", "a\n2\n")

    os.utime(older, (100, 100))
    os.utime(newer, (200, 200))
    assert select_inputs([older, newer], mode="latest") == [newer]


def test_select_inputs_all_returns_all_in_name_order(tmp_path: Path) -> None:
    b_file = _write_csv(tmp_path / "b.csv", "a\n1\n")
    a_file = _write_csv(tmp_path / "a.csv", "a\n2\n")

    selected = select_inputs([b_file, a_file], mode="all")

    assert selected == [a_file, b_file]


# Canonical config behavior

def test_run_clean_accepts_csv_gz_inputs(tmp_path: Path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    gz_file = raw_dir / "data.csv.gz"
    gz_file.write_bytes(b"fake-gzip-content")

    sql_path = _write_clean_sql(tmp_path)
    seen = _run_clean_capture_inputs(
        monkeypatch,
        tmp_path,
        {"sql": str(sql_path), "read": {}},
    )

    assert seen["input_files"] == [gz_file]


def test_run_clean_include_pattern_restricts_to_matching_input(tmp_path: Path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    target_file = raw_dir / "dettaglio_comunale.csv"
    target_file.write_text("a\n1\n", encoding="utf-8")
    other_file = raw_dir / "other.csv"
    other_file.write_text("a\n2\n", encoding="utf-8")

    sql_path = _write_clean_sql(tmp_path)
    seen = _run_clean_capture_inputs(
        monkeypatch,
        tmp_path,
        {"sql": str(sql_path), "read": {"include": ["dettaglio_*.csv"]}},
    )

    assert seen["input_files"] == [target_file]


def test_run_clean_uses_manifest_primary(tmp_path: Path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    selected_file = raw_dir / "preferred.csv"
    selected_file.write_text("a\n1\n", encoding="utf-8")
    other_file = raw_dir / "other.csv"
    other_file.write_text("a\n2\n", encoding="utf-8")
    write_raw_manifest(
        raw_dir,
        {
            "dataset": "demo",
            "year": 2024,
            "run_id": "run-1",
            "created_at": "2026-02-28T00:00:00+00:00",
            "sources": [{"name": "source_1", "output_file": "preferred.csv"}],
            "primary_output_file": "preferred.csv",
        },
    )

    sql_path = _write_clean_sql(tmp_path)
    seen = _run_clean_capture_inputs(
        monkeypatch,
        tmp_path,
        {"sql": str(sql_path), "read": {}},
    )

    assert seen["input_files"] == [selected_file]


def test_run_clean_rejects_php_only_inputs_with_clear_error(tmp_path: Path):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "data.csv.php").write_text("<?php echo 'x';", encoding="utf-8")

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT 1 AS x", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="No usable RAW files found"):
        run_clean(
            "demo",
            2024,
            str(tmp_path),
            {"sql": str(sql_path), "read": {}},
            _NoopLogger(),
        )


def test_run_clean_dirty_csv_needs_auto_detect_false(tmp_path: Path):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    dirty_csv = raw_dir / "dirty.csv"
    dirty_csv.write_text("a;b\n1;2;3\n4;5\n", encoding="utf-8")

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT a, b FROM raw_input", encoding="utf-8")

    run_clean(
        "demo",
        2024,
        str(tmp_path),
        {
            "sql": str(sql_path),
            "read": {
                "header": True,
                "delim": ";",
                "encoding": "utf-8",
            },
        },
        _NoopLogger(),
    )

    out = tmp_path / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    assert out.exists()

    con = duckdb.connect(":memory:")
    assert int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0]) > 0
    con.close()


def test_run_clean_dirty_csv_strict_mode_fails(tmp_path: Path):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    dirty_csv = raw_dir / "dirty.csv"
    dirty_csv.write_text("a;b\n1;2;3\n4;5\n", encoding="utf-8")

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT a, b FROM raw_input", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="Failed to read CLEAN CSV input.*clean.read.columns.*clean.read.source",
    ):
        run_clean(
            "demo",
            2024,
            str(tmp_path),
            {
                "sql": str(sql_path),
                "read_mode": "strict",
                "read": {"header": True, "delim": ";", "encoding": "utf-8"},
            },
            _NoopLogger(),
        )


def test_run_clean_csv_error_message_includes_columns_hint(tmp_path: Path):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    bad_csv = raw_dir / "bad.csv"
    bad_csv.write_text("a;b\n1;2;3\n4;5\n", encoding="utf-8")

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT a, b FROM raw_input", encoding="utf-8")

    with pytest.raises(ValueError, match="clean.read.columns.*clean.read.source"):
        run_clean(
            "demo",
            2024,
            str(tmp_path),
            {
                "sql": str(sql_path),
                "read_mode": "strict",
                "read": {"header": True, "delim": ";", "encoding": "utf-8"},
            },
            _NoopLogger(),
        )


def test_run_clean_accepts_decimal_read_option(tmp_path: Path):
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_file = raw_dir / "data.csv"
    csv_file.write_text("a;b\n1,5;2,5\n", encoding="utf-8")

    sql_path = tmp_path / "clean.sql"
    sql_path.write_text("SELECT a, b FROM raw_input", encoding="utf-8")

    run_clean(
        "demo",
        2024,
        str(tmp_path),
        {
            "sql": str(sql_path),
            "read": {
                "header": True,
                "delim": ";",
                "encoding": "utf-8",
                "decimal": ",",
                "auto_detect": False,
            },
        },
        _NoopLogger(),
    )

    out = tmp_path / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
    assert out.exists()


# Legacy behavior kept for compatibility

def test_run_clean_legacy_mode_warns_and_keeps_largest_selection(tmp_path: Path, monkeypatch, caplog) -> None:
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    _write_csv(raw_dir / "small.csv", "a\n1\n")
    large_file = _write_csv(raw_dir / "large.csv", "a\n" + ("1\n" * 20))

    sql_path = _write_clean_sql(tmp_path)
    logger_name = "tests.clean_input_selection.legacy_mode"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARNING)
    logger.propagate = True

    with caplog.at_level(logging.WARNING, logger=logger_name):
        seen = _run_clean_capture_inputs(
            monkeypatch,
            tmp_path,
            {"sql": str(sql_path), "read": {}},
            logger=logger,
        )

    assert seen["input_files"] == [large_file]
    assert "defaulting to largest file (legacy)" in caplog.text


def test_clean_manifest_missing_falls_back_and_warns(tmp_path: Path, monkeypatch, caplog) -> None:
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    _write_csv(raw_dir / "small.csv", "a\n1\n")
    large_file = _write_csv(raw_dir / "large.csv", "a\n" + ("1\n" * 20))

    sql_path = _write_clean_sql(tmp_path)
    logger_name = "tests.clean_input_selection.manifest_missing"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARNING)
    logger.propagate = True

    with caplog.at_level(logging.WARNING, logger=logger_name):
        seen = _run_clean_capture_inputs(
            monkeypatch,
            tmp_path,
            {"sql": str(sql_path), "read": {}},
            logger=logger,
        )

    assert seen["input_files"] == [large_file]
    assert "manifest missing, using legacy selection" in caplog.text


def test_clean_manifest_points_missing_file_falls_back_and_warns(tmp_path: Path, monkeypatch, caplog) -> None:
    raw_dir = tmp_path / "data" / "raw" / "demo" / "2024"
    _write_csv(raw_dir / "small.csv", "a\n1\n")
    large_file = _write_csv(raw_dir / "large.csv", "a\n" + ("1\n" * 20))
    write_raw_manifest(
        raw_dir,
        {
            "dataset": "demo",
            "year": 2024,
            "run_id": "run-1",
            "created_at": "2026-02-28T00:00:00+00:00",
            "sources": [{"name": "source_1", "output_file": "missing.csv"}],
            "primary_output_file": "missing.csv",
        },
    )

    sql_path = _write_clean_sql(tmp_path)
    logger_name = "tests.clean_input_selection.missing_primary"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARNING)
    logger.propagate = True

    with caplog.at_level(logging.WARNING, logger=logger_name):
        seen = _run_clean_capture_inputs(
            monkeypatch,
            tmp_path,
            {"sql": str(sql_path), "read": {}},
            logger=logger,
        )

    assert seen["input_files"] == [large_file]
    assert "primary_output_file is missing or invalid: missing.csv" in caplog.text
