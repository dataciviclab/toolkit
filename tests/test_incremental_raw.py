"""Test del fetch incrementale append-only (raw.sources[].incremental).

Protegge: il contratto del blocco ``incremental``, la risoluzione del
watermark, la clausola ``{since}``, l'append fedele al baseline e la
sostituzione del placeholder in ``_format_args``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from toolkit.core.config import RawSourceConfig
from toolkit.core.exceptions import DownloadError
from toolkit.core.literals import render_sparql_literal
from toolkit.raw._fetch_utils import _format_args
from toolkit.raw.incremental import (
    append_delta,
    build_since_clause,
    read_watermark,
    validate_incremental,
)

pytestmark = pytest.mark.pure_unit


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


# ── validate_incremental ────────────────────────────────────────────────


def test_validate_incremental_ok():
    validate_incremental({"key_expr": "?data", "key_column": "data"}, "raw.sources[0]")


def test_validate_incremental_unknown_key():
    with pytest.raises(ValueError, match="unknown key"):
        validate_incremental({"key_expr": "?d", "key_column": "d", "boh": 1}, "raw.sources[0]")


def test_validate_incremental_requires_key_expr_and_column():
    with pytest.raises(ValueError, match="key_expr"):
        validate_incremental({"key_column": "data"}, "raw.sources[0]")
    with pytest.raises(ValueError, match="key_column"):
        validate_incremental({"key_expr": "?data"}, "raw.sources[0]")


def test_validate_incremental_bad_literal_mode():
    with pytest.raises(ValueError, match="key_literal"):
        validate_incremental(
            {"key_expr": "?d", "key_column": "d", "key_literal": "wat"}, "raw.sources[0]"
        )


# ── read_watermark ──────────────────────────────────────────────────────


def test_read_watermark_missing_file_is_none(tmp_path: Path):
    assert read_watermark(tmp_path / "nope.csv", "data") is None


def test_read_watermark_lexicographic_for_dates(tmp_path: Path):
    p = _write(
        tmp_path / "b.csv",
        "data,voto\n2026-01-02,si\n2026-01-10,no\n2026-01-03,si\n",
    )
    assert read_watermark(p, "data") == "2026-01-10"


def test_read_watermark_numeric_uses_numeric_max(tmp_path: Path):
    """Max stringa mentirebbe ('9' > '10'): il watermark deve essere numerico."""
    p = _write(tmp_path / "b.csv", "n,x\n9,a\n10,b\n")
    assert read_watermark(p, "n") == "10"


def test_read_watermark_missing_column_raises(tmp_path: Path):
    p = _write(tmp_path / "b.csv", "data,voto\n2026-01-02,si\n")
    with pytest.raises(DownloadError, match="colonna 'nope' assente"):
        read_watermark(p, "nope")


def test_read_watermark_all_empty_column_is_none(tmp_path: Path):
    p = _write(tmp_path / "b.csv", "data,voto\n,si\n,no\n")
    assert read_watermark(p, "data") is None


# ── build_since_clause ──────────────────────────────────────────────────


def test_build_since_clause_with_baseline(tmp_path: Path):
    p = _write(tmp_path / "b.csv", "data,voto\n2026-01-02,si\n2026-01-10,no\n")
    clause = build_since_clause(
        p, {"key_expr": "?data", "key_column": "data", "key_literal": "string"}
    )
    assert clause == 'FILTER(?data > "2026-01-10")'


def test_build_since_clause_without_baseline_is_empty(tmp_path: Path):
    clause = build_since_clause(tmp_path / "nope.csv", {"key_expr": "?data", "key_column": "data"})
    assert clause == ""


# ── append_delta ────────────────────────────────────────────────────────


def test_append_delta_preserves_baseline_bytes(tmp_path: Path):
    baseline = _write(tmp_path / "b.csv", "data,voto\n2026-01-02,si\n")
    delta = b"data,voto\n2026-01-03,no\n2026-01-04,si\n"
    out = append_delta(baseline, delta)
    assert out == b"data,voto\n2026-01-02,si\n2026-01-03,no\n2026-01-04,si\n"


def test_append_delta_empty_delta_keeps_baseline(tmp_path: Path):
    baseline = _write(tmp_path / "b.csv", "data,voto\n2026-01-02,si\n")
    assert append_delta(baseline, b"data,voto\n") == baseline.read_bytes()


def test_append_delta_without_baseline_returns_delta(tmp_path: Path):
    delta = b"data,voto\n2026-01-03,no\n"
    assert append_delta(tmp_path / "nope.csv", delta) == delta


def test_append_delta_handles_missing_trailing_newline(tmp_path: Path):
    baseline = _write(tmp_path / "b.csv", "data,voto\n2026-01-02,si")
    out = append_delta(baseline, b"data,voto\n2026-01-03,no\n")
    assert out == b"data,voto\n2026-01-02,si\n2026-01-03,no\n"


# ── _format_args {since} ────────────────────────────────────────────────


def test_format_args_substitutes_since():
    args = {"query": "SELECT ?d WHERE { ?x ?p ?d . {since} }"}
    result = _format_args(args, 2026, extra={"since": 'FILTER(?d > "2026-01-01")'})
    assert result["query"] == 'SELECT ?d WHERE { ?x ?p ?d . FILTER(?d > "2026-01-01") }'


def test_format_args_since_empty_removes_placeholder():
    args = {"query": "SELECT ?d WHERE { ?x ?p ?d . {since} }"}
    result = _format_args(args, 2026, extra={"since": ""})
    assert result["query"] == "SELECT ?d WHERE { ?x ?p ?d .  }"


def test_format_args_without_extra_keeps_sparql_braces():
    """Regression #186: senza placeholder noti le graffe SPARQL restano intatte."""
    q = "SELECT ?s WHERE { ?s ?p ?o }"
    assert _format_args({"query": q}, 2026)["query"] == q


# ── contratto config ────────────────────────────────────────────────────


def test_raw_source_config_round_trip_incremental():
    src = RawSourceConfig.from_dict(
        {
            "name": "voti",
            "type": "sparql",
            "args": {"endpoint": "https://x", "query": "SELECT {since}"},
            "incremental": {"key_expr": "?data", "key_column": "data", "key_literal": "string"},
        }
    )
    assert src.incremental == {
        "key_expr": "?data",
        "key_column": "data",
        "key_literal": "string",
    }
    assert src.to_dict()["incremental"] == src.incremental


def test_raw_source_config_absent_incremental_is_none():
    src = RawSourceConfig.from_dict({"name": "x", "type": "http_file"})
    assert src.incremental is None
    assert "incremental" not in src.to_dict()


def test_render_sparql_literal_modes():
    assert render_sparql_literal("https://a/b") == "<https://a/b>"
    assert render_sparql_literal("2026-01-01", "string") == '"2026-01-01"'
    assert render_sparql_literal("42", "number") == "42"
    assert render_sparql_literal("2026-01-01") == '"2026-01-01"'
