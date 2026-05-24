"""Test: hierarchy levels come generatore runtime di tabelle aggregate."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb
import pytest
import yaml

from toolkit.core.config import load_config
from toolkit.mart.run import _run_hierarchy_levels

_null_logger = logging.getLogger("test_null")
_null_logger.addHandler(logging.NullHandler())

pytestmark = [pytest.mark.smoke, pytest.mark.core]


def _setup_clean_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create clean_input view with mixed column types for hierarchy testing."""
    con.execute(
        "CREATE OR REPLACE VIEW clean_input AS SELECT * FROM (VALUES "
        "  ('Roma', 'Lazio', 100, 50.5, 'A'), "
        "  ('Milano', 'Lombardia', 200, 75.2, 'B'), "
        "  ('Napoli', 'Campania', 150, 60.0, 'A'), "
        "  ('Roma', 'Lazio', 120, 55.0, 'A'), "
        "  ('Milano', 'Lombardia', 180, 70.0, 'B') "
        ") AS t(comune, regione, valore, costo, categoria)"
    )
    con.execute("CREATE OR REPLACE VIEW clean AS SELECT * FROM clean_input")


def _describe_cols(con, table: str) -> list[tuple[str, str]]:
    """Return (name, type) pairs from DESCRIBE for introspection."""
    return con.execute(f"DESCRIBE {table}").fetchall()


def test_hierarchy_levels_generate_aggregation(tmp_path: Path) -> None:
    """Hierarchy levels generate correct aggregation SQL."""
    con = duckdb.connect()
    _setup_clean_view(con)

    # Introspect column naming (DuckDB may upper-case VALUE columns)
    raw_cols = _describe_cols(con, "clean_input")
    col_map = {c[0].lower(): c[0] for c in raw_cols}
    # Find the actual names DuckDB chose for valore and costo
    v_col = col_map.get("valore", "valore")
    c_col = col_map.get("costo", "costo")

    mart_cfg = {
        "hierarchy": {
            "axis": "territoriale",
            "levels": [
                {"level": "comune", "table": "mart_comune", "grain": ["comune", "regione"]},
                {"level": "regione", "table": "mart_regione", "grain": ["regione"]},
            ],
        }
    }

    mart_dir = tmp_path / "mart"
    mart_dir.mkdir()

    written, executed, total_rows = _run_hierarchy_levels(
        con, mart_cfg, "test", 2024, mart_dir, logger=_null_logger,
    )

    # Both levels should produce parquet files
    assert len(written) == 2, f"expected 2 parquet files, got {len(written)}"
    assert (mart_dir / "mart_comune.parquet").exists()
    assert (mart_dir / "mart_regione.parquet").exists()

    # Both levels should be in executed records
    assert len(executed) == 2
    level_names = [e["level"] for e in executed]
    assert "comune" in level_names
    assert "regione" in level_names

    # Verify aggregations are correct by querying parquet files
    comune_rows = con.execute(
        f"SELECT comune, regione, \"{v_col}\", \"{c_col}\" FROM read_parquet('" +
        str(mart_dir / "mart_comune.parquet") + "') ORDER BY comune"
    ).fetchall()
    # Napoli, Milano, Roma → 3 gruppi
    assert len(comune_rows) == 3, f"expected 3 aggregated rows, got {len(comune_rows)}"
    # Roma: valore=100+120=220, costo=50.5+55.0=105.5
    roma = [r for r in comune_rows if r[0] == "Roma"][0]
    assert roma[2] == 220, f"expected SUM(valore)=220 for Roma, got {roma[2]}"
    assert abs(float(roma[3]) - 105.5) < 0.01, f"expected SUM(costo)=105.5 for Roma, got {roma[3]}"
    # Milano: valore=200+180=380, costo=75.2+70.0=145.2
    milano = [r for r in comune_rows if r[0] == "Milano"][0]
    assert milano[2] == 380, f"expected SUM(valore)=380 for Milano, got {milano[2]}"

    regione_rows = con.execute(
        f"SELECT regione, \"{v_col}\", \"{c_col}\" FROM read_parquet('" +
        str(mart_dir / "mart_regione.parquet") + "') ORDER BY regione"
    ).fetchall()
    assert len(regione_rows) == 3, f"expected 3 aggregated rows, got {len(regione_rows)}"
    for r in regione_rows:
        if r[0] == "Lombardia":
            assert r[1] == 380  # SUM(valore)
        elif r[0] == "Lazio":
            assert r[1] == 220  # SUM(valore)
        elif r[0] == "Campania":
            assert r[1] == 150  # SUM(valore)

    assert total_rows == 6  # 3 comune rows + 3 regione rows
    con.close()


def test_hierarchy_source_table_override(tmp_path: Path) -> None:
    """Hierarchy level can aggregate from a different source table."""
    con = duckdb.connect()
    _setup_clean_view(con)

    # Introspect column names
    raw_cols = _describe_cols(con, "clean_input")
    col_map = {c[0].lower(): c[0] for c in raw_cols}
    v_col = col_map.get("valore", "valore")

    # Create a custom mart table to aggregate from
    con.execute("CREATE OR REPLACE TABLE mart_base AS SELECT * FROM clean_input")

    mart_cfg = {
        "hierarchy": {
            "axis": "territoriale",
            "levels": [
                {
                    "level": "regione",
                    "table": "mart_regione",
                    "grain": ["regione"],
                    "source_table": "mart_base",
                },
            ],
        }
    }

    mart_dir = tmp_path / "mart2"
    mart_dir.mkdir()

    written, executed, total_rows = _run_hierarchy_levels(
        con, mart_cfg, "test", 2024, mart_dir, logger=_null_logger,
    )

    assert len(written) == 1
    assert (mart_dir / "mart_regione.parquet").exists()

    regione_rows = con.execute(
        f"SELECT regione, \"{v_col}\" FROM read_parquet('" +
        str(mart_dir / "mart_regione.parquet") + "') ORDER BY regione"
    ).fetchall()
    assert len(regione_rows) == 3, f"expected 3 rows, got {len(regione_rows)}"
    for r in regione_rows:
        if r[0] == "Lombardia":
            assert r[1] == 380  # SUM(valore)
    con.close()


def test_hierarchy_no_metric_columns_count_fallback(tmp_path: Path) -> None:
    """Without numeric columns, hierarchy falls back to COUNT(*)."""
    con = duckdb.connect()
    con.execute(
        "CREATE OR REPLACE VIEW clean_input AS SELECT * FROM (VALUES "
        "  ('Roma', 'Lazio', 'X'), "
        "  ('Milano', 'Lombardia', 'Y'), "
        "  ('Napoli', 'Campania', 'Z') "
        ") AS t(comune, regione, codice)"
    )

    mart_cfg = {
        "hierarchy": {
            "axis": "territoriale",
            "levels": [
                {"level": "comune", "table": "mart_comune", "grain": ["comune", "regione"]},
            ],
        }
    }

    mart_dir = tmp_path / "mart3"
    mart_dir.mkdir()

    written, executed, total_rows = _run_hierarchy_levels(
        con, mart_cfg, "test", 2024, mart_dir, logger=_null_logger,
    )

    assert len(written) == 1
    rows = con.execute(
        "SELECT comune, record_count FROM read_parquet('" +
        str(mart_dir / "mart_comune.parquet") + "') ORDER BY comune"
    ).fetchall()
    assert len(rows) == 3  # one row per comune
    # Each comune has exactly 1 record
    for r in rows:
        assert r[1] == 1, f"expected COUNT(*)=1 for {r[0]}"
    con.close()


def test_hierarchy_empty_config(tmp_path: Path) -> None:
    """Empty or missing hierarchy returns empty results."""
    con = duckdb.connect()
    _setup_clean_view(con)
    mart_dir = tmp_path / "empty"
    mart_dir.mkdir()

    w, e, r = _run_hierarchy_levels(con, {}, "test", 2024, mart_dir, logger=_null_logger)
    assert w == []
    assert e == []
    assert r == 0

    w2, e2, r2 = _run_hierarchy_levels(
        con, {"hierarchy": {"axis": "x", "levels": []}}, "test", 2024, mart_dir, logger=_null_logger
    )
    assert w2 == []
    assert e2 == []
    assert r2 == 0
    con.close()


def test_hierarchy_missing_source_raises(tmp_path: Path) -> None:
    """Missing source table raises a clear error."""
    con = duckdb.connect()
    mart_dir = tmp_path / "missing"
    mart_dir.mkdir()

    mart_cfg = {
        "hierarchy": {
            "axis": "territoriale",
            "levels": [
                {"level": "comune", "table": "mart_comune", "grain": ["comune"]},
            ],
        }
    }

    with pytest.raises(ValueError, match="source table.*not found"):
        _run_hierarchy_levels(con, mart_cfg, "test", 2024, mart_dir, logger=_null_logger)
    con.close()


@pytest.mark.smoke
def test_hierarchy_integration_via_run(project_example: Path) -> None:
    """End-to-end: hierarchy levels generated correctly through full pipeline."""
    config_path = project_example / "dataset.yml"

    # Add hierarchy to existing project_example config
    config_text = config_path.read_text(encoding="utf-8")
    config_data = yaml.safe_load(config_text)
    config_data.setdefault("mart", {})["hierarchy"] = {
        "axis": "territoriale",
        "levels": [
            {
                "level": "provincia",
                "table": "h_provincia",
                "grain": ["provincia", "regione"],
            },
            {
                "level": "regione",
                "table": "h_regione",
                "grain": ["regione"],
            },
        ],
    }
    config_path.write_text(
        yaml.dump(config_data, default_flow_style=False, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    from toolkit.cli.cmd_run import run as run_cmd
    run_cmd(step="all", config=str(config_path))

    # Hierarchy parquet files should exist alongside normal mart tables
    mart_dir_2022 = project_example / "_smoke_out" / "data" / "mart" / "project_example" / "2022"
    assert (mart_dir_2022 / "h_provincia.parquet").exists(), "hierarchy provincia parquet missing"
    assert (mart_dir_2022 / "h_regione.parquet").exists(), "hierarchy regione parquet missing"

    # Normal mart tables still work
    assert (mart_dir_2022 / "rd_by_regione.parquet").exists()

    # Hierarchy data should be aggregated
    con = duckdb.connect()
    prov_rows = con.execute(
        f"SELECT * FROM read_parquet('{mart_dir_2022 / 'h_provincia.parquet'}')"
    ).fetchall()
    assert len(prov_rows) > 0, "h_provincia should have data"
    # Fetch column names via duckdb col description
    prov_desc = con.execute(
        f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{mart_dir_2022 / 'h_provincia.parquet'}'))"
    ).fetchall()
    col_names = [r[0].lower() for r in prov_desc]
    assert "provincia" in col_names, f"provincia not in columns: {col_names}"
    con.close()


def test_source_id_propagated_to_toolkit_config(tmp_path: Path) -> None:
    """source_id from dataset.yml is propagated to ToolkitConfig."""
    yml = tmp_path / "dataset.yml"
    yml.write_text(
        "\n".join([
            "dataset:",
            "  name: test_sid",
            "  years: [2024]",
            "  source_id: my_test_source",
            "raw: {}",
            "clean: {}",
            "mart: {}",
        ]),
        encoding="utf-8",
    )
    cfg = load_config(yml)
    assert cfg.source_id == "my_test_source"

    # Without source_id
    yml2 = tmp_path / "dataset_no_sid.yml"
    yml2.write_text(
        "\n".join([
            "dataset:",
            "  name: test_no_sid",
            "  years: [2024]",
            "raw: {}",
            "clean: {}",
            "mart: {}",
        ]),
        encoding="utf-8",
    )
    cfg2 = load_config(yml2)
    assert cfg2.source_id is None


def test_source_id_in_metadata_after_run(project_example: Path) -> None:
    """source_id appears in layer metadata after pipeline run."""
    config_path = project_example / "dataset.yml"
    config_text = config_path.read_text(encoding="utf-8")
    config_data = yaml.safe_load(config_text)
    config_data["dataset"]["source_id"] = "ispra_linked_data"
    config_path.write_text(
        yaml.dump(config_data, default_flow_style=False, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    from toolkit.cli.cmd_run import run as run_cmd
    run_cmd(step="all", config=str(config_path))

    # Check clean metadata
    clean_dir = project_example / "_smoke_out" / "data" / "clean" / "project_example" / "2022"
    clean_meta = json.loads((clean_dir / "metadata.json").read_text(encoding="utf-8"))
    assert clean_meta.get("source_id") == "ispra_linked_data", f"got {clean_meta.get('source_id')}"

    # Check mart metadata
    mart_dir = project_example / "_smoke_out" / "data" / "mart" / "project_example" / "2022"
    mart_meta = json.loads((mart_dir / "metadata.json").read_text(encoding="utf-8"))
    assert mart_meta.get("source_id") == "ispra_linked_data", f"got {mart_meta.get('source_id')}"
