"""Test: unita' di toolkit/core/multi_year_source.py — logica pura + DuckDB.

Contratto:
  - collect_multi_year_files: risolve file parquet multi-anno da filesystem
  - bind_multi_year_view:   crea viste DuckDB da file multi-anno
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from toolkit.core.multi_year_source import (
    bind_multi_year_view,
    collect_multi_year_files,
)

pytestmark = pytest.mark.pure_unit


# ── collect_multi_year_files ──────────────────────────────────────────────


class TestCollectMultiYearFiles:
    """Test per collect_multi_year_files()."""

    def _make_clean_years(self, root: Path, dataset: str, years: list[int]) -> None:
        """Create clean parquet files for given years."""
        for y in years:
            d = root / "data" / "clean" / dataset / str(y)
            d.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(":memory:")
            con.execute(f"CREATE TABLE t AS SELECT {y} AS anno, 'x' AS val")
            con.execute(f"COPY t TO '{d / f'{dataset}_{y}_clean.parquet'}' (FORMAT PARQUET)")
            con.close()

    def test_clean_layer_single_year(self, tmp_path: Path) -> None:
        """Un anno -> lista con un file."""
        self._make_clean_years(tmp_path, "demo", [2023])
        files = collect_multi_year_files(str(tmp_path), "demo", years=[2023])
        assert len(files) == 1
        assert files[0].suffix == ".parquet"

    def test_clean_layer_multiple_years(self, tmp_path: Path) -> None:
        """Multipli anni -> file ordinati per anno."""
        self._make_clean_years(tmp_path, "demo", [2022, 2023, 2024])
        files = collect_multi_year_files(str(tmp_path), "demo", years=[2022, 2023, 2024])
        assert len(files) == 3
        assert all(f.suffix == ".parquet" for f in files)

    def test_clean_layer_missing_dir(self, tmp_path: Path) -> None:
        """Directory anno mancante -> FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="CLEAN dir not found"):
            collect_multi_year_files(str(tmp_path), "demo", years=[2023])

    def test_clean_layer_missing_parquet(self, tmp_path: Path) -> None:
        """Directory esiste ma senza parquet -> FileNotFoundError."""
        d = tmp_path / "data" / "clean" / "demo" / "2023"
        d.mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="No CLEAN parquet"):
            collect_multi_year_files(str(tmp_path), "demo", years=[2023])

    def test_empty_years_raises(self, tmp_path: Path) -> None:
        """Anni vuoti -> ValueError."""
        with pytest.raises(ValueError, match="years list must not be empty"):
            collect_multi_year_files(str(tmp_path), "demo", years=[])

    def test_mart_layer_single_year(self, tmp_path: Path) -> None:
        """Layer mart con source_table -> singolo file."""
        d = tmp_path / "data" / "mart" / "demo" / "2023"
        d.mkdir(parents=True)
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE t AS SELECT 1 AS anno")
        con.execute(f"COPY t TO '{d / 'my_table.parquet'}' (FORMAT PARQUET)")
        con.close()
        files = collect_multi_year_files(
            str(tmp_path), "demo", years=[2023], source_layer="mart", source_table="my_table"
        )
        assert len(files) == 1

    def test_mart_layer_missing_source_table(self, tmp_path: Path) -> None:
        """Layer mart senza source_table -> ValueError."""
        with pytest.raises(ValueError, match="source_table is required"):
            collect_multi_year_files(str(tmp_path), "demo", years=[2023], source_layer="mart")

    def test_mart_layer_missing_file(self, tmp_path: Path) -> None:
        """Layer mart con file mancante -> FileNotFoundError."""
        d = tmp_path / "data" / "mart" / "demo" / "2023"
        d.mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="MART parquet not found"):
            collect_multi_year_files(
                str(tmp_path), "demo", years=[2023], source_layer="mart", source_table="missing"
            )

    def test_unsupported_layer_raises(self, tmp_path: Path) -> None:
        """Layer non supportato -> ValueError."""
        with pytest.raises(ValueError, match="Unsupported source_layer: raw"):
            collect_multi_year_files(str(tmp_path), "demo", years=[2023], source_layer="raw")


# ── bind_multi_year_view ──────────────────────────────────────────────────


class TestBindMultiYearView:
    """Test per bind_multi_year_view()."""

    def _make_parquet(self, path: Path, rows: list[tuple[int, str]]) -> None:
        """Create a parquet file with columns (anno, val) from rows."""
        con = duckdb.connect(":memory:")
        for i, (anno, val) in enumerate(rows):
            sql = f"CREATE {'OR REPLACE' if i else ''} TABLE t AS SELECT {anno} AS anno, '{val}' AS val"
            con.execute(sql)
        con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT PARQUET)")
        con.close()

    def test_single_file_creates_views(self, tmp_path: Path) -> None:
        """Singolo file -> source_input, clean_input, clean create."""
        p = tmp_path / "data.parquet"
        self._make_parquet(p, [(2023, "a")])

        con = duckdb.connect(":memory:")
        bind_multi_year_view(con, [p])
        rows = con.execute("SELECT * FROM clean").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 2023
        con.close()

    def test_multiple_files_creates_union(self, tmp_path: Path) -> None:
        """Multipli file -> union via read_parquet([...])."""
        p1 = tmp_path / "2022.parquet"
        p2 = tmp_path / "2023.parquet"
        self._make_parquet(p1, [(2022, "a")])
        self._make_parquet(p2, [(2023, "b")])

        con = duckdb.connect(":memory:")
        bind_multi_year_view(con, [p1, p2])
        rows = con.execute("SELECT * FROM clean ORDER BY 1").fetchall()
        assert len(rows) == 2
        assert rows[0][0] == 2022
        assert rows[1][0] == 2023
        con.close()

    def test_mart_layer_extra_views(self, tmp_path: Path) -> None:
        """Layer=mart -> anche mart_input, mart, mart_all_years."""
        p = tmp_path / "data.parquet"
        self._make_parquet(p, [(2023, "a")])

        con = duckdb.connect(":memory:")
        bind_multi_year_view(con, [p], source_layer="mart")
        rows = con.execute("SELECT * FROM mart").fetchall()
        assert len(rows) == 1
        rows = con.execute("SELECT * FROM mart_all_years").fetchall()
        assert len(rows) == 1
        con.close()
