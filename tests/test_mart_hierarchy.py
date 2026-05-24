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
    con.execute(
        "CREATE OR REPLACE VIEW clean_input AS SELECT * FROM (VALUES "
        "  ('Roma', 'Lazio', 100, 50.5), "
        "  ('Milano', 'Lombardia', 200, 75.2), "
        "  ('Napoli', 'Campania', 150, 60.0), "
        "  ('Roma', 'Lazio', 120, 55.0), "
        "  ('Milano', 'Lombardia', 180, 70.0) "
        ") AS t(comune, regione, valore, costo)"
    )
    con.execute("CREATE OR REPLACE VIEW clean AS SELECT * FROM clean_input")


def test_hierarchy_generates_aggregation(tmp_path: Path) -> None:
    """Two-level hierarchy + source_table override."""
    con = duckdb.connect()
    _setup_clean_view(con)
    mart_dir = tmp_path / "mart"
    mart_dir.mkdir()

    # Two hierarchy levels aggregate from clean_input
    written, executed, total_rows = _run_hierarchy_levels(
        con, {
            "hierarchy": {
                "axis": "territoriale",
                "levels": [
                    {"level": "comune", "table": "h_comune", "grain": ["comune", "regione"]},
                    {"level": "regione", "table": "h_regione", "grain": ["regione"]},
                ],
            }
        },
        "test", 2024, mart_dir, logger=_null_logger,
    )
    assert len(written) == 2
    assert (mart_dir / "h_comune.parquet").exists()
    assert (mart_dir / "h_regione.parquet").exists()

    # Verify SUM values: Roma=100+120=220, Milano=200+180=380
    rows = con.execute(
        f"SELECT comune, valore FROM read_parquet('{mart_dir / 'h_comune.parquet'}') ORDER BY comune"
    ).fetchall()
    assert rows[0] == ("Milano", 380)
    assert rows[2] == ("Roma", 220)
    assert total_rows == 6  # 3 comune + 3 regione

    # Second pass: source_table override aggregates from a mart table
    con.execute("CREATE TABLE mart_base AS SELECT * FROM clean_input")
    w2, _, _ = _run_hierarchy_levels(
        con, {
            "hierarchy": {
                "axis": "x", "levels": [
                    {"level": "s", "table": "h_sub", "grain": ["regione"], "source_table": "mart_base"},
                ]
            }
        },
        "test", 2024, mart_dir, logger=_null_logger,
    )
    assert len(w2) == 1
    rows2 = con.execute(
        f"SELECT regione, valore FROM read_parquet('{mart_dir / 'h_sub.parquet'}') ORDER BY regione"
    ).fetchall()
    assert rows2[2] == ("Lombardia", 380)  # SUM(valore) from mart_base
    con.close()


def test_hierarchy_count_fallback(tmp_path: Path) -> None:
    """Without numeric columns, hierarchy uses COUNT(*)."""
    con = duckdb.connect()
    con.execute(
        "CREATE VIEW clean_input AS SELECT * FROM (VALUES ('Roma','Lazio','X'),('Milano','Lombardia','Y')) "
        "AS t(comune, regione, codice)"
    )
    mart_dir = tmp_path / "m"
    mart_dir.mkdir()
    written, _, _ = _run_hierarchy_levels(
        con, {"hierarchy": {"axis": "x", "levels": [{"level": "c", "table": "h_c", "grain": ["comune"]}]}},
        "test", 2024, mart_dir, logger=_null_logger,
    )
    rows = con.execute(
        f"SELECT record_count FROM read_parquet('{mart_dir / 'h_c.parquet'}')"
    ).fetchall()
    assert rows == [(1,), (1,)]  # COUNT(*) per comune
    con.close()


def test_hierarchy_edge_cases(tmp_path: Path) -> None:
    """Empty config returns empty; numeric grain not treated as metric."""
    con = duckdb.connect()
    _setup_clean_view(con)
    d = tmp_path / "d"
    d.mkdir()

    # Empty hierarchy → empty result
    w, e, r = _run_hierarchy_levels(con, {}, "t", 2024, d, logger=_null_logger)
    assert (w, e, r) == ([], [], 0)

    # Numeric grain column (valore=INTEGER) must NOT appear as SUM metric
    # Grain column must be excluded from metric_cols
    w2, e2, _ = _run_hierarchy_levels(
        con, {"hierarchy": {"axis": "x", "levels": [{"level": "x", "table": "h_test", "grain": ["valore"]}]}},
        "t", 2024, d, logger=_null_logger,
    )
    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{d / 'h_test.parquet'}')").fetchall()
    col_names = [c[0].lower() for c in cols]
    assert "valore" in col_names
    # There should NOT be a SUM(valore) AS valore_1 or similar extra metric
    metric_count = sum(1 for c in col_names if c.startswith("valore"))
    assert metric_count == 1, f"valore appears {metric_count} times (grain leaking as metric): {col_names}"
    con.close()

    # Missing source → ValueError (separate connection without clean_input)
    con2 = duckdb.connect()
    d2 = tmp_path / "d2"
    d2.mkdir()
    with pytest.raises(ValueError, match="source table.*not found"):
        _run_hierarchy_levels(
            con2, {"hierarchy": {"axis": "x", "levels": [{"level": "x", "table": "x", "grain": ["comune"]}]}},
            "t", 2024, d2, logger=_null_logger,
        )
    con2.close()


@pytest.mark.smoke
def test_hierarchy_integration_via_run(project_example: Path) -> None:
    """End-to-end: hierarchy + source_id in metadata."""
    config_path = project_example / "dataset.yml"
    config_text = config_path.read_text(encoding="utf-8")
    config_data = yaml.safe_load(config_text)
    config_data["dataset"]["source_id"] = "ispra_linked_data"
    config_data.setdefault("mart", {})["hierarchy"] = {
        "axis": "territoriale",
        "levels": [
            {"level": "provincia", "table": "h_provincia", "grain": ["provincia", "regione"]},
            {"level": "regione", "table": "h_regione", "grain": ["regione"]},
        ],
    }
    config_path.write_text(yaml.dump(config_data, default_flow_style=False, allow_unicode=True, sort_keys=False), encoding="utf-8")

    from toolkit.cli.cmd_run import run as run_cmd
    run_cmd(step="all", config=str(config_path))

    mart_dir = project_example / "_smoke_out" / "data" / "mart" / "project_example" / "2022"
    assert (mart_dir / "h_provincia.parquet").exists()
    assert (mart_dir / "h_regione.parquet").exists()

    # source_id in metadata
    for layer in ("clean", "mart"):
        meta = json.loads((project_example / "_smoke_out" / "data" / layer / "project_example" / "2022" / "metadata.json").read_text())
        assert meta.get("source_id") == "ispra_linked_data", f"{layer} missing source_id"


def test_source_id_propagated(tmp_path: Path) -> None:
    """source_id from dataset.yml reaches ToolkitConfig."""
    yml = tmp_path / "d.yml"
    yml.write_text("dataset:\n  name: t\n  years: [2024]\n  source_id: src1\nraw: {}\nclean: {}\nmart: {}\n")
    assert load_config(yml).source_id == "src1"

    yml2 = tmp_path / "d2.yml"
    yml2.write_text("dataset:\n  name: t\n  years: [2024]\nraw: {}\nclean: {}\nmart: {}\n")
    assert load_config(yml2).source_id is None
