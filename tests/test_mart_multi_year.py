"""Test: multi-year MART tables (assorbe ex cross_year)."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from toolkit.cli.cmd_run import run as run_cmd

pytestmark = [pytest.mark.contract, pytest.mark.core]


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


def test_mart_only_multi_year(project_example: Path) -> None:
    """Solo tabelle multi-year: il run per-anno non deve fallire (issue #445).

    Regressione: prima del fix, un candidate con TUTTE le tabelle mart
    dichiarate ``years`` falliva la validazione per-anno (Missing required
    MART tables) e il passaggio multi-year non partiva mai.
    """
    config_path = project_example / "dataset.yml"
    sql_dir = project_example / "sql" / "multi_year"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (sql_dir / "solo_multi.sql").write_text(
        "\n".join(
            [
                "select",
                "  anno,",
                "  count(*) as righe",
                "from clean_input",
                "group by anno",
            ]
        ),
        encoding="utf-8",
    )

    config_text = config_path.read_text(encoding="utf-8")
    config_data = yaml.safe_load(config_text)
    config_data["dataset"]["years"] = [2022, 2023]
    # Rimuove tutte le tabelle per-anno esistenti: solo tabelle multi-year
    config_data["mart"] = {
        "tables": [
            {
                "name": "solo_multi",
                "sql": "sql/multi_year/solo_multi.sql",
                "years": [2022, 2023],
            }
        ],
        "required_tables": ["solo_multi"],
        "validate": {
            "table_rules": {
                "solo_multi": {
                    "required_columns": ["anno", "righe"],
                    "primary_key": ["anno"],
                    "min_rows": 1,
                }
            }
        },
    }
    config_path.write_text(
        yaml.dump(config_data, default_flow_style=False, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    # Run all years + multi-year mart: deve passare (prima falliva)
    run_cmd(step="all", config=str(config_path))

    # Output a livello dataset
    mart_dir = project_example / "_smoke_out" / "data" / "mart" / "project_example"
    assert (mart_dir / "solo_multi.parquet").exists(), "multi-year parquet should exist"

    # La validazione multi-year deve essere applicata (issue #445 gap 2):
    # il metadata registra l'esito della validazione delle tabelle multi-year.
    metadata = json.loads((mart_dir / "metadata.json").read_text(encoding="utf-8"))
    validation = metadata.get("validation") or {}
    assert validation.get("passed") is True, (
        f"multi-year validation failed: {validation.get('errors')}"
    )
    assert validation.get("errors_count") == 0, (
        f"multi-year validation errors: {validation.get('errors')}"
    )


def test_mart_output_paths_multi_year_resolve_to_dataset_level(tmp_path: Path) -> None:
    """Le tabelle multi-year risolvono a livello dataset, non per-anno (issue #445).

    Regressione: il path resolver elencava TUTTI gli output mart nel dir
    per-anno, quindi readiness/summary segnalavano mart_outputs_missing
    per le tabelle multi-year (scritte a data/mart/{dataset}/{name}.parquet).
    """
    from toolkit.core.config import load_config
    from toolkit.domain.path_resolver import payload_for_year

    root = tmp_path / "out"
    (root / "data" / "raw" / "demo_ds" / "2022").mkdir(parents=True)
    cfg_path = tmp_path / "dataset.yml"
    cfg_path.write_text(
        "\n".join(
            [
                f'root: "{root.as_posix()}"',
                "dataset:",
                '  name: "demo_ds"',
                "  years: [2022]",
                "raw:",
                "  sources:",
                "    - type: local_file",
                "      args:",
                '        path: "."',
                '        filename: "dummy.csv"',
                "mart:",
                "  tables:",
                '    - name: "mart_per_anno"',
                '      sql: "sql/mart_per_anno.sql"',
                '    - name: "mart_multi"',
                '      sql: "sql/mart_multi.sql"',
                "      years: [2022]",
            ]
        ),
        encoding="utf-8",
    )

    cfg = load_config(str(cfg_path), strict_config=False)
    payload = payload_for_year(cfg, 2022)
    outputs = payload["paths"]["mart"]["outputs"]

    # mart_per_anno -> nel dir per-anno
    assert any(o.endswith("data/mart/demo_ds/2022/mart_per_anno.parquet") for o in outputs), (
        f"per-year mart should be in year dir: {outputs}"
    )
    # mart_multi -> a livello dataset
    assert any(o.endswith("data/mart/demo_ds/mart_multi.parquet") for o in outputs), (
        f"multi-year mart should be at dataset level: {outputs}"
    )


def test_mart_only_multi_year_validation_failure(project_example: Path) -> None:
    """Validazione multi-year fallita: table_rules violata blocca il run (issue #445).

    Regressione: il ramo di errore di _validate_multi_year_tables non era
    coperto — la validazione multi-year applica le table_rules ma il
    fallimento (validation_passed=False) deve far fallire il run quando
    fail_on_error è attivo.
    """
    config_path = project_example / "dataset.yml"
    sql_dir = project_example / "sql" / "multi_year"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (sql_dir / "solo_multi_viol.sql").write_text(
        "\n".join(
            [
                "select",
                "  anno,",
                "  count(*) as righe",
                "from clean_input",
                "group by anno",
            ]
        ),
        encoding="utf-8",
    )

    config_text = config_path.read_text(encoding="utf-8")
    config_data = yaml.safe_load(config_text)
    config_data["dataset"]["years"] = [2022, 2023]
    config_data["mart"] = {
        "tables": [
            {
                "name": "solo_multi_viol",
                "sql": "sql/multi_year/solo_multi_viol.sql",
                "years": [2022, 2023],
            }
        ],
        "required_tables": ["solo_multi_viol"],
        "validate": {
            # required_columns include una colonna che la query non produce:
            # la validazione multi-year deve fallire.
            "table_rules": {
                "solo_multi_viol": {
                    "required_columns": ["anno", "colonna_inesistente"],
                    "primary_key": ["anno"],
                    "min_rows": 1,
                }
            }
        },
    }
    config_path.write_text(
        yaml.dump(config_data, default_flow_style=False, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    # fail_on_error attivo (default): il run deve fallire con la validazione
    # multi-year segnalata come errore. Il logger rich spezza le righe lunghe
    # (ancora cmd_run.py:NNNN / run.py:NNNN in mezzo): tollerare con regex.
    from typer.testing import CliRunner
    from toolkit.cli.app import app

    runner = CliRunner()
    result = runner.invoke(app, ["run", "--config", str(config_path)])
    assert result.exit_code != 0, "run should fail when multi-year validation fails"
    normalized = re.sub(r"\s+", " ", result.output)
    assert re.search(r"MART multi-year validation failed", normalized), normalized

    # Il metadata registra l'esito della validazione fallita.
    mart_dir = project_example / "_smoke_out" / "data" / "mart" / "project_example"
    metadata = json.loads((mart_dir / "metadata.json").read_text(encoding="utf-8"))
    validation = metadata.get("validation") or {}
    assert validation.get("passed") is False, "multi-year validation should have failed"
    assert len(validation.get("errors") or []) > 0, "errors should be recorded"
