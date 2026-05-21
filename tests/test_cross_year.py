from __future__ import annotations

import json
from pathlib import Path

from toolkit.cli.cmd_run import run as run_cmd


def test_cli_run_cross_year_on_project_example(project_example: Path) -> None:
    config_path = project_example / "dataset.yml"
    cross_sql_dir = project_example / "sql" / "cross"
    cross_sql_dir.mkdir(parents=True, exist_ok=True)
    (cross_sql_dir / "clean_union.sql").write_text(
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
    config_text = config_text.replace("years: [2022]", "years: [2022, 2023]")
    config_text += (
        "\n"
        "cross_year:\n"
        "  tables:\n"
        '    - name: "clean_union"\n'
        '      sql: "sql/cross/clean_union.sql"\n'
        '      source_layer: "clean"\n'
    )
    config_path.write_text(config_text, encoding="utf-8")

    run_cmd(step="all", config=str(config_path))
    run_cmd(step="cross_year", config=str(config_path))

    cross_dir = project_example / "_smoke_out" / "data" / "cross" / "project_example"
    assert (cross_dir / "clean_union.parquet").exists()
    assert (cross_dir / "metadata.json").exists()
    assert (cross_dir / "manifest.json").exists()
    assert (cross_dir / "_validate" / "cross_validation.json").exists()

    manifest = json.loads((cross_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["validation"] == "_validate/cross_validation.json"
    assert manifest["summary"]["ok"] is True
