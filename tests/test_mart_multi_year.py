"""Test: multi-year MART tables (assorbe ex cross_year)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from toolkit.cli.cmd_run import run as run_cmd

pytestmark = [pytest.mark.core]


def test_mart_multi_year_on_project_example(project_example: Path) -> None:
    """Run multi-year mart table on project_example with 2 years."""
    config_path = project_example / "dataset.yml"
    my_sql_dir = project_example / "sql" / "multi_year"
    my_sql_dir.mkdir(parents=True, exist_ok=True)
    (my_sql_dir / "clean_union.sql").write_text(
        "\n".join(
            [
                "select",
                "  count(*) as rows_total,",
                "  count(distinct anno) as anni_distinti",
                "from clean_input",
            ]
        ),
        encoding="utf-8",
    )

    config_text = config_path.read_text(encoding="utf-8")
    config_data = yaml.safe_load(config_text)
    config_data["dataset"]["years"] = [2022, 2023]
    # Add multi-year table to existing mart.tables
    config_data.setdefault("mart", {}).setdefault("tables", []).append(
        {
            "name": "clean_union",
            "sql": "sql/multi_year/clean_union.sql",
            "years": [2022, 2023],
        }
    )
    config_path.write_text(
        yaml.dump(config_data, default_flow_style=False, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    # Run all years + multi-year mart
    run_cmd(step="all", config=str(config_path))

    # Multi-year output goes to data/mart/{dataset}/{name}.parquet (dataset-level)
    mart_dir = project_example / "_smoke_out" / "data" / "mart" / "project_example"
    assert (mart_dir / "clean_union.parquet").exists(), "multi-year parquet should exist"

    # Single-year mart files still work (per-year tables unchanged)
    assert (mart_dir / "2022" / "rd_by_regione.parquet").exists()
    assert (mart_dir / "2022" / "rd_by_provincia.parquet").exists()
    assert (mart_dir / "2023" / "rd_by_regione.parquet").exists()
    assert (mart_dir / "2023" / "rd_by_provincia.parquet").exists()

    metadata = json.loads((mart_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata.get("layer") == "mart_multi_year", f"got {metadata.get('layer')}"
    tables = metadata.get("tables") or []
    assert any(t.get("name") == "clean_union" for t in tables), "clean_union missing from metadata"
    assert any(t.get("years") == [2022, 2023] for t in tables), "years missing from metadata"
