from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from toolkit.cli.sql_dry_run import (
    _build_clean_preview,
    _create_placeholder_raw_input,
    _create_placeholder_raw_input_with_columns,
    _dedupe_preserve_order,
    _extract_missing_binder_column,
    _normalize_sql,
    _placeholder_columns,
    _validate_mart_sql,
    validate_sql_dry_run,
)


# --- Helpers ---


class _FakeConfig:
    """Minimal config-like object matching the shape returned by load_config."""

    def __init__(
        self,
        tmp_path: Path,
        *,
        dataset: str = "demo_ds",
        years: list[int] | None = None,
        clean_sql: str | None = None,
        clean_read: dict | None = None,
        mart_tables: list[dict] | None = None,
        support: list[dict] | None = None,
    ):
        self.base_dir = tmp_path
        self.dataset = dataset
        self.years = years or [2022]
        self.root = tmp_path / "out"
        self.root.mkdir(parents=True, exist_ok=True)

        # Create clean.sql if requested
        self._clean_sql_path: Path | None = None
        if clean_sql:
            sql_dir = tmp_path / "sql"
            sql_dir.mkdir(parents=True, exist_ok=True)
            self._clean_sql_path = sql_dir / "clean.sql"
            self._clean_sql_path.write_text(clean_sql, encoding="utf-8")

        clean_read_cfg = clean_read or {}
        self.clean = {
            "sql": str(self._clean_sql_path) if self._clean_sql_path else None,
            "read": clean_read_cfg,
        }
        if not clean_sql:
            self.clean["sql"] = None

        self.mart = {"tables": mart_tables or []}
        self.support = support or []


# --- Unit tests for utility functions ---


class TestDedupePreserveOrder:
    def test_removes_duplicates(self):
        assert _dedupe_preserve_order(["a", "b", "a", "c", "b"]) == ["a", "b", "c"]

    def test_skips_empty(self):
        assert _dedupe_preserve_order(["", "a", "", "b"]) == ["a", "b"]

    def test_preserves_order(self):
        assert _dedupe_preserve_order(["z", "a", "z", "m"]) == ["z", "a", "m"]


class TestNormalizeSql:
    def test_strips_whitespace(self):
        assert _normalize_sql("  select 1  ") == "select 1"

    def test_removes_trailing_semicolon(self):
        assert _normalize_sql("select 1;") == "select 1"

    def test_removes_semicolon_then_space(self):
        assert _normalize_sql("select 1;  ") == "select 1"


class TestExtractMissingBinderColumn:
    def test_matches_duckdb_error(self):
        exc = Exception('Referenced column "my_col" not found in FROM clause')
        assert _extract_missing_binder_column(exc) == "my_col"

    def test_returns_none_for_unrelated_error(self):
        exc = Exception("syntax error at or near SELECT")
        assert _extract_missing_binder_column(exc) is None


class TestPlaceholderColumns:
    def test_uses_read_columns(self):
        clean_cfg = {"read": {"columns": {"col_a": "VARCHAR", "col_b": "INT"}}}
        cols = _placeholder_columns(clean_cfg, "")
        assert cols == ["col_a", "col_b"]

    def test_falls_back_to_quoted_identifiers(self):
        clean_cfg = {"read": {}}
        sql = 'select "foo", "bar" from raw_input'
        cols = _placeholder_columns(clean_cfg, sql)
        assert cols == ["foo", "bar"]

    def test_combines_read_columns_and_identifiers(self):
        clean_cfg = {"read": {"columns": {"a": "VARCHAR"}}}
        sql = 'select a, "b" from raw_input'
        cols = _placeholder_columns(clean_cfg, sql)
        assert cols == ["a", "b"]

    def test_dedupes(self):
        clean_cfg = {"read": {"columns": {"a": "VARCHAR"}}}
        sql = 'select "a", "b", "a" from raw_input'
        cols = _placeholder_columns(clean_cfg, sql)
        assert cols == ["a", "b"]


class TestCreatePlaceholderRawInput:
    def test_creates_view_with_columns(self):
        con = duckdb.connect(":memory:")
        try:
            _create_placeholder_raw_input_with_columns(con, ["x", "y"])
            result = con.execute("DESCRIBE raw_input").fetchall()
            names = [row[0] for row in result]
            assert names == ["x", "y"]
        finally:
            con.close()

    def test_creates_fallback_placeholder(self):
        con = duckdb.connect(":memory:")
        try:
            _create_placeholder_raw_input_with_columns(con, [])
            result = con.execute("DESCRIBE raw_input").fetchall()
            names = [row[0] for row in result]
            assert names == ["__dry_run_placeholder"]
        finally:
            con.close()

    def test_infers_columns_from_sql(self):
        con = duckdb.connect(":memory:")
        try:
            clean_cfg = {"read": {}}
            sql = 'select "val" from raw_input'
            _create_placeholder_raw_input(con, clean_cfg, sql)
            result = con.execute("DESCRIBE raw_input").fetchall()
            names = [row[0] for row in result]
            assert names == ["val"]
        finally:
            con.close()


# --- Integration tests for _build_clean_preview ---


class TestBuildCleanPreview:
    def test_simple_sql_passes_immediately(self, tmp_path: Path):
        cfg = _FakeConfig(tmp_path, clean_sql="select 1 as value")
        con = duckdb.connect(":memory:")
        try:
            _build_clean_preview(cfg, year=2022, con=con)
            # Table should exist after successful build
            con.execute("SELECT * FROM __dry_run_clean_preview")
        finally:
            con.close()

    def test_sql_with_unquoted_column_infers_incrementally(self, tmp_path: Path):
        """Clean SQL uses unquoted column name not in read.columns."""
        cfg = _FakeConfig(tmp_path, clean_sql="select x from raw_input")
        con = duckdb.connect(":memory:")
        try:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")
        finally:
            con.close()

    def test_sql_with_read_columns(self, tmp_path: Path):
        cfg = _FakeConfig(
            tmp_path,
            clean_sql='select "amount" from raw_input',
            clean_read={"columns": {"amount": "DOUBLE"}},
        )
        con = duckdb.connect(":memory:")
        try:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")
        finally:
            con.close()

    def test_sql_with_multiple_columns_and_casts(self, tmp_path: Path):
        sql = (
            'select TRY_CAST("id" AS BIGINT) AS id, '
            'TRY_CAST("name" AS VARCHAR) AS name '
            "from raw_input"
        )
        cfg = _FakeConfig(
            tmp_path,
            clean_sql=sql,
            clean_read={"columns": {"id": "VARCHAR", "name": "VARCHAR"}},
        )
        con = duckdb.connect(":memory:")
        try:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")
        finally:
            con.close()

    def test_non_binder_error_raises_clean_dry_run_failure(self, tmp_path: Path):
        """Non-binder SQL errors should surface as CLEAN SQL dry-run failures."""
        cfg = _FakeConfig(tmp_path, clean_sql="select from raw_input")
        con = duckdb.connect(":memory:")
        try:
            with pytest.raises(ValueError, match="CLEAN SQL dry-run failed"):
                _build_clean_preview(cfg, year=2022, con=con)
        finally:
            con.close()

    def test_missing_column_error_includes_path(self, tmp_path: Path):
        """SQL syntax error should include file path in the message."""
        cfg = _FakeConfig(tmp_path, clean_sql="select from raw_input")
        con = duckdb.connect(":memory:")
        try:
            with pytest.raises(ValueError) as exc_info:
                _build_clean_preview(cfg, year=2022, con=con)
        finally:
            con.close()
        msg = str(exc_info.value)
        assert "CLEAN SQL dry-run failed" in msg
        assert "clean.sql" in msg


# --- Integration tests for _validate_mart_sql ---


class TestValidateMartSql:
    def test_simple_mart_sql_passes(self, tmp_path: Path):
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text("select * from clean_input", encoding="utf-8")

        cfg = _FakeConfig(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        con = duckdb.connect(":memory:")
        try:
            # Build clean preview first (required by mart validation)
            _build_clean_preview(cfg, year=2022, con=con)
            _validate_mart_sql(cfg, year=2022, con=con)
        finally:
            con.close()

    def test_mart_sql_with_missing_column_fails(self, tmp_path: Path):
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text(
            "select nonexistent_col from clean_input", encoding="utf-8"
        )

        cfg = _FakeConfig(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        con = duckdb.connect(":memory:")
        try:
            _build_clean_preview(cfg, year=2022, con=con)
            with pytest.raises(ValueError, match="MART SQL dry-run failed"):
                _validate_mart_sql(cfg, year=2022, con=con)
        finally:
            con.close()

    def test_mart_sql_with_template_placeholder_resolved(self, tmp_path: Path):
        """Mart SQL with {year} placeholder should be resolved by template rendering."""
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text(
            "select * from clean_input where anno = {year}", encoding="utf-8"
        )

        cfg = _FakeConfig(
            tmp_path,
            clean_sql="select 1 as anno",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        con = duckdb.connect(":memory:")
        try:
            _build_clean_preview(cfg, year=2022, con=con)
            _validate_mart_sql(cfg, year=2022, con=con)
        finally:
            con.close()

    def test_mart_sql_with_unresolved_placeholder_fails(self, tmp_path: Path):
        """Mart SQL with unresolved placeholder should fail."""
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text(
            "select * from clean_input where col = {unknown_placeholder}", encoding="utf-8"
        )

        cfg = _FakeConfig(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        con = duckdb.connect(":memory:")
        try:
            _build_clean_preview(cfg, year=2022, con=con)
            with pytest.raises(ValueError, match="unresolved"):
                _validate_mart_sql(cfg, year=2022, con=con)
        finally:
            con.close()


# --- Top-level validate_sql_dry_run ---
# Support dataset paths remain covered indirectly in test_run_dry_run.py.


class TestValidateSqlDryRun:
    def test_clean_and_mart_pass(self, tmp_path: Path):
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "out.sql").write_text("select * from clean_input", encoding="utf-8")

        cfg = _FakeConfig(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "out", "sql": "sql/mart/out.sql"}],
        )
        # Should not raise
        validate_sql_dry_run(cfg, year=2022, layers=["clean", "mart"])

    def test_mart_only_pass(self, tmp_path: Path):
        """Without clean.sql, mart validation skips clean preview."""
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "out.sql").write_text("select 1 as value", encoding="utf-8")

        cfg = _FakeConfig(
            tmp_path,
            clean_sql=None,
            mart_tables=[{"name": "out", "sql": "sql/mart/out.sql"}],
        )
        validate_sql_dry_run(cfg, year=2022, layers=["mart"])

    def test_no_matching_layers_returns_early(self, tmp_path: Path):
        """If layers don't include clean or mart, function returns without doing anything."""
        cfg = _FakeConfig(tmp_path)
        # Should not raise even though no SQL files exist
        validate_sql_dry_run(cfg, year=2022, layers=["cross_year"])

    def test_fails_on_clean_sql_error(self, tmp_path: Path):
        cfg = _FakeConfig(tmp_path, clean_sql="select from raw_input")
        with pytest.raises(ValueError, match="CLEAN SQL dry-run failed"):
            validate_sql_dry_run(cfg, year=2022, layers=["clean"])

    def test_fails_on_mart_sql_error(self, tmp_path: Path):
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "out.sql").write_text("select bad_col from clean_input", encoding="utf-8")

        cfg = _FakeConfig(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "out", "sql": "sql/mart/out.sql"}],
        )
        with pytest.raises(ValueError, match="MART SQL dry-run failed"):
            validate_sql_dry_run(cfg, year=2022, layers=["clean", "mart"])
