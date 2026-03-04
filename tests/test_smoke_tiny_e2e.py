from __future__ import annotations

import json
import shutil
import textwrap
import zipfile
from pathlib import Path

import duckdb

from toolkit.cli.cmd_run import run_year
from toolkit.core.config import load_config
from toolkit.core.logging import get_logger


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")


def _project_logger():
    return get_logger(name="toolkit-smoke-tests", level="WARNING")


def _assert_run_success(run_record: Path) -> None:
    payload = json.loads(run_record.read_text(encoding="utf-8"))
    assert payload["status"] == "SUCCESS"
    for layer in ("raw", "clean", "mart"):
        assert payload["layers"][layer]["status"] == "SUCCESS"
        assert payload["validations"][layer]["passed"] is True


def _assert_common_outputs(root: Path, dataset: str, year: int, mart_tables: list[str]) -> None:
    raw_dir = root / "data" / "raw" / dataset / str(year)
    clean_dir = root / "data" / "clean" / dataset / str(year)
    mart_dir = root / "data" / "mart" / dataset / str(year)

    clean_parquet = clean_dir / f"{dataset}_{year}_clean.parquet"

    assert (raw_dir / "metadata.json").exists()
    assert (raw_dir / "manifest.json").exists()
    assert (raw_dir / "raw_validation.json").exists()
    assert clean_parquet.exists()
    assert (clean_dir / "metadata.json").exists()
    assert (clean_dir / "manifest.json").exists()
    assert (clean_dir / "_validate" / "clean_validation.json").exists()
    assert (mart_dir / "metadata.json").exists()
    assert (mart_dir / "manifest.json").exists()
    assert (mart_dir / "_validate" / "mart_validation.json").exists()

    for table in mart_tables:
        assert (mart_dir / f"{table}.parquet").exists()

    clean_validation = json.loads((clean_dir / "_validate" / "clean_validation.json").read_text(encoding="utf-8"))
    mart_validation = json.loads((mart_dir / "_validate" / "mart_validation.json").read_text(encoding="utf-8"))
    assert clean_validation["ok"] is True
    assert mart_validation["ok"] is True

    clean_manifest = json.loads((clean_dir / "manifest.json").read_text(encoding="utf-8"))
    mart_manifest = json.loads((mart_dir / "manifest.json").read_text(encoding="utf-8"))
    assert clean_manifest["summary"]["ok"] is True
    assert mart_manifest["summary"]["ok"] is True

    con = duckdb.connect(":memory:")
    assert int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{clean_parquet.as_posix()}')").fetchone()[0]) > 0
    for table in mart_tables:
        parquet = mart_dir / f"{table}.parquet"
        assert int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet.as_posix()}')").fetchone()[0]) > 0
    con.close()


def _write_csv_it_project(project_dir: Path) -> Path:
    (project_dir / "data").mkdir(parents=True, exist_ok=True)
    shutil.copy(FIXTURES_DIR / "it_small.csv", project_dir / "data" / "it_small.csv")

    _write_text(
        project_dir / "sql" / "clean.sql",
        """
        SELECT
          comune,
          CAST(anno AS INTEGER) AS anno,
          CAST(valore AS DOUBLE) AS valore
        FROM raw_input
        """,
    )
    _write_text(
        project_dir / "sql" / "mart_totali.sql",
        """
        SELECT
          anno,
          SUM(valore) AS totale
        FROM clean_input
        GROUP BY anno
        """,
    )
    _write_text(
        project_dir / "dataset.yml",
        """
        schema_version: 1
        root: out
        dataset:
          name: tiny_csv_it
          years: [2024]
        raw:
          output_policy: overwrite
          sources:
            - name: csv_it
              type: local_file
              primary: "true"
              args:
                path: data/it_small.csv
                filename: tiny_it_2024.csv
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
            ranges:
              valore:
                min: 0
                max: 100
        mart:
          tables:
            - name: mart_totali
              sql: sql/mart_totali.sql
          required_tables: mart_totali
          validate:
            table_rules:
              mart_totali:
                required_columns: [anno, totale]
                ranges:
                  totale:
                    min: 0
                    max: 100
        validation:
          fail_on_error: "true"
        output:
          artifacts: standard
          legacy_aliases: "false"
        """,
    )
    return project_dir / "dataset.yml"


def _write_zip_project(project_dir: Path) -> Path:
    source_csv = FIXTURES_DIR / "zip_small.csv"
    zip_path = project_dir / "data" / "source_payload.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.write(source_csv, arcname="zip_payload.csv")

    _write_text(
        project_dir / "sql" / "clean.sql",
        """
        SELECT
          capoluogo,
          CAST(punteggio AS INTEGER) AS punteggio
        FROM raw_input
        """,
    )
    _write_text(
        project_dir / "sql" / "mart_scores.sql",
        """
        SELECT
          COUNT(*) AS righe,
          SUM(punteggio) AS totale
        FROM clean_input
        """,
    )
    _write_text(
        project_dir / "dataset.yml",
        """
        schema_version: 1
        root: out
        dataset:
          name: tiny_zip_local
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
            ranges:
              punteggio:
                min: 0
                max: 10
        mart:
          tables:
            - name: mart_scores
              sql: sql/mart_scores.sql
          required_tables: [mart_scores]
          validate:
            table_rules:
              mart_scores:
                required_columns: [righe, totale]
                ranges:
                  totale:
                    min: 0
                    max: 20
        """,
    )
    return project_dir / "dataset.yml"


def test_smoke_e2e_csv_it_semicolon_decimal_comma(tmp_path: Path) -> None:
    project_dir = tmp_path / "csv_it_project"
    dataset_yml = _write_csv_it_project(project_dir)

    cfg = load_config(dataset_yml)
    year = cfg.years[0]
    context = run_year(cfg, year, step="all", logger=_project_logger())

    _assert_run_success(context.path)
    _assert_common_outputs(Path(cfg.root), cfg.dataset, year, ["mart_totali"])


def test_smoke_e2e_local_zip_extractor(tmp_path: Path) -> None:
    project_dir = tmp_path / "zip_project"
    dataset_yml = _write_zip_project(project_dir)

    cfg = load_config(dataset_yml)
    year = cfg.years[0]
    context = run_year(cfg, year, step="all", logger=_project_logger())

    _assert_run_success(context.path)
    _assert_common_outputs(Path(cfg.root), cfg.dataset, year, ["mart_scores"])

    raw_dir = Path(cfg.root) / "data" / "raw" / cfg.dataset / str(year)
    raw_manifest = json.loads((raw_dir / "manifest.json").read_text(encoding="utf-8"))
    assert raw_manifest["primary_output_file"] == "zip_payload.csv"


def test_smoke_e2e_local_file_path_year_template(tmp_path: Path) -> None:
    project_dir = tmp_path / "templated_local_project"
    data_dir = project_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(FIXTURES_DIR / "it_small.csv", data_dir / "it_small_2024.csv")

    _write_text(
        project_dir / "sql" / "clean.sql",
        """
        SELECT
          comune,
          CAST(anno AS INTEGER) AS anno,
          CAST(valore AS DOUBLE) AS valore
        FROM raw_input
        """,
    )
    _write_text(
        project_dir / "sql" / "mart_totali.sql",
        """
        SELECT
          anno,
          SUM(valore) AS totale
        FROM clean_input
        GROUP BY anno
        """,
    )
    _write_text(
        project_dir / "dataset.yml",
        """
        schema_version: 1
        root: out
        dataset:
          name: tiny_csv_it_templated
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
              sql: sql/mart_totali.sql
          required_tables: mart_totali
          validate:
            table_rules:
              mart_totali:
                required_columns: [anno, totale]
        """,
    )

    cfg = load_config(project_dir / "dataset.yml")
    year = cfg.years[0]
    context = run_year(cfg, year, step="all", logger=_project_logger())

    _assert_run_success(context.path)
    _assert_common_outputs(Path(cfg.root), cfg.dataset, year, ["mart_totali"])

    raw_dir = Path(cfg.root) / "data" / "raw" / cfg.dataset / str(year)
    raw_manifest = json.loads((raw_dir / "manifest.json").read_text(encoding="utf-8"))
    assert raw_manifest["primary_output_file"] == "tiny_it_2024.csv"
