"""Smoke test: flusso end-to-end completo con CLI locale.

Usa fixture CSV/ZIP locali, nessuna dipendenza di rete.
Copre: flusso standard, ZIP extractor, {year} template, multi-year MART.
"""

from __future__ import annotations

import json
import shutil
import textwrap
import zipfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app

pytestmark = [pytest.mark.contract, pytest.mark.core]

FIXTURES_DIR = Path(__file__).parent / "fixtures"
RUNNER = CliRunner()


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")


def _invoke(args: list[str]) -> pytest.CapturedResult:
    result = RUNNER.invoke(app, args, catch_exceptions=False)
    return result


def _assert_parquet_has_rows(path: Path) -> int:
    assert path.exists(), f"Parquet non trovato: {path}"
    from lab_connectors.duckdb import safe_connect

    with safe_connect() as con:
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
        assert count > 0, f"Parquet vuoto: {path}"
        return int(count)


def _setup_project(tmp_path: Path, dataset: str = "smoke_test", year: int = 2024) -> Path:
    """Crea dataset.yml + SQL + CSV fixture per test e2e standard."""
    (tmp_path / "data").mkdir(parents=True)
    shutil.copy(FIXTURES_DIR / "it_small.csv", tmp_path / "data" / "it_small.csv")
    _write_text(
        tmp_path / "sql" / "clean.sql",
        """SELECT comune, CAST(anno AS INTEGER) AS anno, CAST(valore AS DOUBLE) AS valore FROM raw_input""",
    )
    _write_text(
        tmp_path / "sql" / "mart.sql",
        """SELECT anno, SUM(valore) AS totale FROM clean_input GROUP BY anno""",
    )
    _write_text(
        tmp_path / "dataset.yml",
        f"""
        schema_version: 1
        root: out
        dataset:
          name: "{dataset}"
          years: [{year}]
        raw:
          output_policy: overwrite
          sources:
            - name: csv_it
              type: local_file
              primary: "true"
              args:
                path: data/it_small.csv
                filename: "{dataset}_{year}.csv"
        clean:
          sql: sql/clean.sql
          read_mode: strict
          read:
            source: config_only
            header: true
            delim: ";"
            decimal: ","
            mode: explicit
            include: "{dataset}_{year}.csv"
          required_columns: comune
          validate:
            not_null: valore
        mart:
          tables:
            - name: mart_totali
              sql: sql/mart.sql
          required_tables: mart_totali
          validate:
            table_rules:
              mart_totali:
                required_columns: [anno, totale]
        """,
    )
    return tmp_path / "dataset.yml"


class TestEndToEndFlow:
    """Flusso completo: run init → run full → validate → status."""

    @pytest.mark.smoke
    def test_init_then_full_then_validate(self, tmp_path: Path):
        config_path = _setup_project(tmp_path)
        dataset = "smoke_test"
        year = 2024

        # --- FASE 1: run init (scaffold + raw) ---
        result = _invoke(["run", "init", "--config", str(config_path)])
        assert result.exit_code == 0, f"run init fallito: {result.output}"

        out = tmp_path / "out"
        raw_dir = out / "data" / "raw" / dataset / str(year)
        assert (raw_dir / "metadata.json").exists()
        assert (raw_dir / "_profile" / "raw_profile.json").exists()

        # --- FASE 2: run full (clean + mart + validate + review) ---
        result = _invoke(["run", "full", "--config", str(config_path)])
        assert result.exit_code == 0, f"run full fallito: {result.output}"

        clean_dir = out / "data" / "clean" / dataset / str(year)
        mart_dir = out / "data" / "mart" / dataset / str(year)

        clean_parquet = clean_dir / f"{dataset}_{year}_clean.parquet"
        mart_parquet = mart_dir / "mart_totali.parquet"

        _assert_parquet_has_rows(clean_parquet)
        _assert_parquet_has_rows(mart_parquet)

        assert (clean_dir / "_validate" / "clean_validation.json").exists()
        assert (mart_dir / "_validate" / "mart_validation.json").exists()

        # --- FASE 3: validate --json ---
        result = _invoke(
            [
                "validate",
                "all",
                "--config",
                str(config_path),
                "--json",
            ]
        )
        assert result.exit_code == 0, f"validate fallito: {result.output}"
        val_results = json.loads(result.output)
        assert len(val_results) == 3  # raw + clean + mart
        for r in val_results:
            assert r["passed"] is True, f"{r['layer']} validation fallita: {r}"

        # --- FASE 4: inspect summary --json ---
        result = _invoke(["inspect", "summary", "--config", str(config_path), "--json"])
        assert result.exit_code == 0, f"status fallito: {result.output}"
        status_data = json.loads(result.output)
        assert status_data["dataset"] == dataset
        assert status_data["record"]["status"] == "SUCCESS"


# ---------------------------------------------------------------------------
# Scenario: ZIP extractor
# ---------------------------------------------------------------------------


def test_zip_extractor(tmp_path: Path):
    """Pipeline con fonte raw in ZIP + extractor unzip_first_csv."""
    project = tmp_path / "zip_project"
    zip_path = project / "data" / "source_payload.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.write(FIXTURES_DIR / "zip_small.csv", arcname="zip_payload.csv")

    _write_text(
        project / "sql" / "clean.sql",
        """SELECT capoluogo, CAST(punteggio AS INTEGER) AS punteggio FROM raw_input""",
    )
    _write_text(
        project / "sql" / "mart_scores.sql",
        """SELECT COUNT(*) AS righe, SUM(punteggio) AS totale FROM clean_input""",
    )
    _write_text(
        project / "dataset.yml",
        """
        schema_version: 1
        root: out
        dataset:
          name: tiny_zip
          years: [2025]
        raw:
          output_policy: overwrite
          sources:
            - name: zipped
              type: local_file
              args:
                path: data/source_payload.zip
              extractor:
                type: unzip_first_csv
        clean:
          sql: sql/clean.sql
          read:
            source: config_only
            header: true
            mode: explicit
            include: zip_payload.csv
          required_columns: [capoluogo, punteggio]
          validate:
            not_null: [capoluogo, punteggio]
        mart:
          tables:
            - name: mart_scores
              sql: sql/mart_scores.sql
          required_tables: [mart_scores]
          validate:
            table_rules:
              mart_scores:
                required_columns: [righe, totale]
        """,
    )

    result = _invoke(["run", "all", "--config", str(project / "dataset.yml")])
    assert result.exit_code == 0, result.output

    out = project / "out"
    raw_meta = json.loads(
        (out / "data" / "raw" / "tiny_zip" / "2025" / "metadata.json").read_text()
    )
    assert raw_meta["primary_output_file"] == "zip_payload.csv"
    _assert_parquet_has_rows(out / "data" / "mart" / "tiny_zip" / "2025" / "mart_scores.parquet")


# ---------------------------------------------------------------------------
# Scenario: {year} template in local_file path
# ---------------------------------------------------------------------------


def test_year_template_in_path(tmp_path: Path):
    """local_file con {year} nel path raw risolto correttamente."""
    project = tmp_path / "tpl_project"
    (project / "data").mkdir(parents=True)
    shutil.copy(FIXTURES_DIR / "it_small.csv", project / "data" / "it_small_2024.csv")

    _write_text(
        project / "sql" / "clean.sql",
        """SELECT comune, CAST(anno AS INTEGER) AS anno, CAST(valore AS DOUBLE) AS valore FROM raw_input""",
    )
    _write_text(
        project / "sql" / "mart.sql",
        """SELECT anno, SUM(valore) AS totale FROM clean_input GROUP BY anno""",
    )
    _write_text(
        project / "dataset.yml",
        """
        schema_version: 1
        root: out
        dataset:
          name: tiny_tpl
          years: [2024]
        raw:
          output_policy: overwrite
          sources:
            - name: csv_it
              type: local_file
              primary: true
              args:
                path: data/it_small_{year}.csv
                filename: tiny_it_{year}.csv
        clean:
          sql: sql/clean.sql
          read_mode: strict
          read:
            source: config_only
            header: true
            delim: ";"
            decimal: ","
            mode: explicit
            include: tiny_it_2024.csv
          required_columns: comune
          validate:
            not_null: valore
        mart:
          tables:
            - name: mart_totali
              sql: sql/mart.sql
          required_tables: mart_totali
          validate:
            table_rules:
              mart_totali:
                required_columns: [anno, totale]
        """,
    )

    result = _invoke(["run", "all", "--config", str(project / "dataset.yml")])
    assert result.exit_code == 0, result.output

    out = project / "out"
    raw_meta = json.loads(
        (out / "data" / "raw" / "tiny_tpl" / "2024" / "metadata.json").read_text()
    )
    assert raw_meta["primary_output_file"] == "tiny_it_2024.csv"
    _assert_parquet_has_rows(out / "data" / "mart" / "tiny_tpl" / "2024" / "mart_totali.parquet")


# ---------------------------------------------------------------------------
# Scenario: multi-year MART (ex cross_year)
# ---------------------------------------------------------------------------


def test_multi_year_mart(tmp_path: Path):
    """Tabelle MART con years esplicito producono output aggregato a livello dataset."""
    project = tmp_path / "my_project"
    (project / "data").mkdir(parents=True)
    # Due file CSV, uno per anno
    shutil.copy(FIXTURES_DIR / "it_small.csv", project / "data" / "it_small_2024.csv")
    shutil.copy(FIXTURES_DIR / "it_small.csv", project / "data" / "it_small_2025.csv")

    _write_text(
        project / "sql" / "clean.sql",
        """SELECT comune, CAST(anno AS INTEGER) AS anno, CAST(valore AS DOUBLE) AS valore FROM raw_input""",
    )
    _write_text(
        project / "sql" / "clean_union.sql",
        """SELECT * FROM clean_input""",
    )
    _write_text(
        project / "dataset.yml",
        """
        schema_version: 1
        root: out
        dataset:
          name: tiny_my
          years: [2024, 2025]
        raw:
          output_policy: overwrite
          sources:
            - name: csv_it
              type: local_file
              primary: true
              args:
                path: data/it_small_{year}.csv
                filename: tiny_my_{year}.csv
        clean:
          sql: sql/clean.sql
          read_mode: strict
          read:
            source: config_only
            header: true
            delim: ";"
            decimal: ","
          required_columns: comune
          validate:
            not_null: valore
        mart:
          tables:
            - name: clean_union
              sql: sql/clean_union.sql
              years: [2024, 2025]
        """,
    )

    # run all per-year + run mart esplicito fa scattare il multi-year automatico
    result = _invoke(["run", "all", "--config", str(project / "dataset.yml")])
    assert result.exit_code == 0, result.output
    result = _invoke(["run", "mart", "--config", str(project / "dataset.yml")])
    assert result.exit_code == 0, result.output

    out = project / "out"
    mart_dir = out / "data" / "mart" / "tiny_my"
    _assert_parquet_has_rows(mart_dir / "clean_union.parquet")
    assert (mart_dir / "metadata.json").exists()

    meta = json.loads((mart_dir / "metadata.json").read_text(encoding="utf-8"))
    assert meta["layer"] == "mart_multi_year"
    tables = meta.get("tables") or []
    assert any(t.get("name") == "clean_union" for t in tables)
