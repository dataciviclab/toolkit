"""Test di metadata approfonditi su project-example.

Il golden path CLI generico è coperto da test_smoke_templates_golden_path.py
(3 template parametrizzati). Questo file verifica la struttura INTERNA dei
metadata prodotta dal toolkit sul dataset canonico project-example,
usando la Python API (run_raw/run_clean/run_mart) per accesso diretto
ai layer e controllo dei contratti di metadata.

Non duplica gli assert generici (path, esistenza, sostituibilità) che
sono già coperti dal test parametrizzato.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from tests.helpers import NoopLogger
from tests.helpers_assert_paths import (
    assert_clean_parquet,
    assert_mart_parquet,
    assert_metadata_file,
    assert_raw_dir,
    assert_validation_file,
)

from toolkit.clean.run import run_clean
from toolkit.cli.cmd_validate import validate as validate_cmd
from toolkit.core.config import load_config
from toolkit.mart.run import run_mart
from toolkit.raw.run import run_raw

pytestmark = pytest.mark.smoke


def test_project_example_deep_metadata(project_example: Path, monkeypatch) -> None:
    """Esegue la pipeline su project-example e verifica metadata in profondità."""
    dst = project_example
    monkeypatch.chdir(dst)

    cfg = load_config(dst / "dataset.yml")
    year = cfg.years[0]
    logger = NoopLogger()
    root = Path(cfg.root)

    # Esecuzione pipeline via Python API (non CLI — accesso diretto ai layer)
    run_raw(
        cfg.dataset,
        year,
        cfg.root,
        cfg.raw,
        logger,
        base_dir=cfg.base_dir,
        output_cfg=cfg.output,
        clean_cfg=cfg.clean,
    )
    run_clean(
        cfg.dataset, year, cfg.root, cfg.clean, logger, base_dir=cfg.base_dir, output_cfg=cfg.output
    )
    run_mart(
        cfg.dataset,
        year,
        cfg.root,
        cfg.mart,
        logger,
        base_dir=cfg.base_dir,
        clean_cfg=cfg.clean,
        output_cfg=cfg.output,
    )
    validate_cmd(step="clean", config=str(dst / "dataset.yml"))
    validate_cmd(step="mart", config=str(dst / "dataset.yml"))

    # ── Contratti comuni a tutti i metadata ─────────────────────────
    for layer, meta in [
        ("raw", assert_metadata_file(root, cfg.dataset, "raw", year)),
        ("clean", assert_metadata_file(root, cfg.dataset, "clean", year)),
        ("mart", assert_metadata_file(root, cfg.dataset, "mart", year)),
    ]:
        assert meta["metadata_schema_version"] == 1, f"{layer}: schema_version"
        assert "toolkit_version" in meta, f"{layer}: toolkit_version"
        assert "config_hash" in meta, f"{layer}: config_hash"
        assert isinstance(meta["config_hash"], str) and meta["config_hash"], (
            f"{layer}: config_hash vuoto"
        )
        assert "inputs" in meta, f"{layer}: inputs"
        assert isinstance(meta["inputs"], list) and meta["inputs"], f"{layer}: inputs vuoto"
        assert "outputs" in meta, f"{layer}: outputs"
        assert isinstance(meta["outputs"], list) and meta["outputs"], f"{layer}: outputs vuoto"
        assert {"file", "sha256", "bytes"} <= set(meta["outputs"][0].keys()), (
            f"{layer}: formato output"
        )
        assert {"file", "sha256", "bytes"} <= set(meta["inputs"][0].keys()), (
            f"{layer}: formato input"
        )
        assert "summary" in meta, f"{layer}: summary"
        assert meta["summary"]["ok"] is True, f"{layer}: summary.ok"
        assert isinstance(meta["summary"]["errors_count"], int), f"{layer}: errors_count"
        assert isinstance(meta["summary"]["warnings_count"], int), f"{layer}: warnings_count"

    # ── RAW metadata ────────────────────────────────────────────────
    raw_dir = assert_raw_dir(root, cfg.dataset, year)
    assert_validation_file(root, cfg.dataset, "raw", year)
    raw_meta = assert_metadata_file(root, cfg.dataset, "raw", year)

    assert raw_meta["validation"] == "raw_validation.json"
    assert raw_meta["primary_output_file"] == raw_meta["outputs"][0]["file"]
    assert (raw_dir / raw_meta["primary_output_file"]).exists()
    assert raw_meta["sources"]
    assert raw_meta["profile_hints"]["encoding_suggested"] == "utf-8"
    assert raw_meta["profile_hints"]["delim_suggested"] == ";"
    assert raw_meta["profile_hints"]["columns_preview"][0] == "Regione"
    assert any("Provincia" in column for column in raw_meta["profile_hints"]["columns_preview"])
    assert raw_meta["profile_hints"]["file_used"] == raw_meta["primary_output_file"]

    # ── CLEAN metadata ──────────────────────────────────────────────
    assert_validation_file(root, cfg.dataset, "clean", year)
    clean_meta = assert_metadata_file(root, cfg.dataset, "clean", year)

    assert clean_meta["input_files"] == [Path(raw_meta["primary_output_file"]).name]
    assert clean_meta["read_source_used"] in {"strict", "robust", "parquet"}
    assert isinstance(clean_meta["read_params_used"], dict)
    assert isinstance(clean_meta["read_params_source"], list)
    assert clean_meta["read_params_source"]
    assert clean_meta["sql"] == "sql/clean.sql"
    assert clean_meta["sql_rendered"] == ("data/clean/project_example/2022/_run/clean_rendered.sql")
    assert clean_meta["output_profile"]["row_count"] > 0
    assert any(item["name"] == "regione" for item in clean_meta["output_profile"]["columns"])
    assert "debug" not in clean_meta

    clean_parquet = assert_clean_parquet(root, cfg.dataset, year)

    # ── MART metadata ───────────────────────────────────────────────
    assert_validation_file(root, cfg.dataset, "mart", year)
    mart_meta = assert_metadata_file(root, cfg.dataset, "mart", year)

    assert mart_meta["output_paths"] == [
        "data/mart/project_example/2022/rd_by_regione.parquet",
        "data/mart/project_example/2022/rd_by_provincia.parquet",
    ]
    assert (
        mart_meta["clean_input_profile"]["row_count"] == clean_meta["output_profile"]["row_count"]
    )
    assert set(mart_meta["table_profiles"].keys()) == {"rd_by_regione", "rd_by_provincia"}
    assert len(mart_meta["transition_profiles"]) == 2
    for item in mart_meta["transition_profiles"]:
        assert item["from"] == "clean"
        assert item["to"] == "mart"
        assert "target_name" in item, "transition target_name mancante"
        assert isinstance(item["source_row_count"], int)
        assert isinstance(item["target_row_count"], int)
        assert isinstance(item["added_columns"], list)
        assert isinstance(item["removed_columns"], list)
        assert isinstance(item["type_changes"], list)
    assert {item["target_name"] for item in mart_meta["transition_profiles"]} == {
        "rd_by_regione",
        "rd_by_provincia",
    }
    assert mart_meta["tables"] == [
        {
            "name": "rd_by_regione",
            "sql": "sql/mart/mart_regione_anno.sql",
            "sql_rendered": ("data/mart/project_example/2022/_run/01_rd_by_regione_rendered.sql"),
            "output": "data/mart/project_example/2022/rd_by_regione.parquet",
        },
        {
            "name": "rd_by_provincia",
            "sql": "sql/mart/mart_provincia_anno.sql",
            "sql_rendered": ("data/mart/project_example/2022/_run/02_rd_by_provincia_rendered.sql"),
            "output": "data/mart/project_example/2022/rd_by_provincia.parquet",
        },
    ]
    assert "debug" not in mart_meta

    for table in ("rd_by_regione", "rd_by_provincia"):
        assert_mart_parquet(root, cfg.dataset, year, table)

    # ── Integrità dati ──────────────────────────────────────────────
    con = duckdb.connect(":memory:")
    for label, parquet_path in [
        ("clean", clean_parquet),
        ("mart_regione", assert_mart_parquet(root, cfg.dataset, year, "rd_by_regione")),
        ("mart_provincia", assert_mart_parquet(root, cfg.dataset, year, "rd_by_provincia")),
    ]:
        count = int(
            con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_path.as_posix()}')"
            ).fetchone()[0]
        )
        assert count > 0, f"{label}: parquet vuoto"
    con.close()
