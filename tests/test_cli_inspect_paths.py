from __future__ import annotations

import json
from pathlib import Path

import pytest

from toolkit.cli.app import app

pytestmark = pytest.mark.contract


def test_inspect_paths_reports_dataset_repo_layout_from_other_cwd(
    project_example: Path, runner, chdir_tmp: Path
) -> None:
    config_path = project_example / "dataset.yml"

    run_result = runner.invoke(
        app,
        [
            "run",
            "--config",
            str(config_path),
        ],
    )
    assert run_result.exit_code == 0, run_result.output

    result = runner.invoke(
        app,
        [
            "inspect",
            "--config",
            str(config_path),
            "--year",
            "2022",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert isinstance(data, dict)
    assert data.get("dataset") == "project_example"
    assert data.get("year") == 2022


def test_inspect_paths_json_is_notebook_friendly(
    project_example: Path, runner, chdir_tmp: Path
) -> None:
    """``inspect --json`` produce output parsabile."""
    config_path = project_example / "dataset.yml"

    result = runner.invoke(
        app,
        [
            "inspect",
            "--json",
            "--config",
            str(config_path),
            "--year",
            "2022",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dataset"] == "project_example"
    assert payload["year"] == 2022
    assert "layers" in payload


def test_inspect_paths_json_reports_resolved_support_outputs(tmp_path: Path, runner) -> None:
    support_root = tmp_path / "support_out"
    support_config = tmp_path / "support_dataset.yml"
    support_config.write_text(
        "\n".join(
            [
                f'root: "{support_root.as_posix()}"',
                "dataset:",
                '  name: "support_ds"',
                "  years: [2024]",
                "raw: {}",
                "clean: {}",
                "mart:",
                "  tables:",
                '    - name: "support_table"',
                '      sql: "sql/support.sql"',
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "demo_ds"',
                "  years: [2022]",
                "raw: {}",
                "clean: {}",
                "mart: {}",
                "support:",
                '  - name: "scuole"',
                f'    config: "{support_config.as_posix()}"',
                "    years: [2024]",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "inspect",
            "--json",
            "--config",
            str(config_path),
            "--year",
            "2022",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert isinstance(payload, dict)
    assert "layers" in payload


def test_inspect_paths_json_exposes_layer_profiles(tmp_path: Path, runner) -> None:
    config_path = tmp_path / "dataset.yml"
    root_dir = tmp_path / "out"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{root_dir.as_posix()}"',
                "dataset:",
                '  name: "demo_ds"',
                "  years: [2022]",
                "raw: {}",
                "clean:",
                '  sql: "sql/clean.sql"',
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart/mart_example.sql"',
            ]
        ),
        encoding="utf-8",
    )

    clean_dir = root_dir / "data" / "clean" / "demo_ds" / "2022"
    mart_dir = root_dir / "data" / "mart" / "demo_ds" / "2022"
    clean_dir.mkdir(parents=True, exist_ok=True)
    mart_dir.mkdir(parents=True, exist_ok=True)

    (clean_dir / "metadata.json").write_text(
        json.dumps(
            {
                "output_profile": {
                    "row_count": 39506,
                    "columns": [
                        {"name": "comune", "type": "VARCHAR"},
                        {"name": "reddito", "type": "DOUBLE"},
                    ],
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (mart_dir / "metadata.json").write_text(
        json.dumps(
            {
                "clean_input_profile": {
                    "row_count": 39506,
                    "columns": [
                        {"name": "comune", "type": "VARCHAR"},
                        {"name": "reddito", "type": "DOUBLE"},
                    ],
                },
                "table_profiles": {
                    "mart_example": {
                        "row_count": 7904,
                        "columns": [
                            {"name": "comune", "type": "VARCHAR"},
                            {"name": "totale", "type": "DOUBLE"},
                        ],
                    }
                },
                "transition_profiles": [
                    {
                        "target_name": "mart_example",
                        "source_row_count": 39506,
                        "target_row_count": 7904,
                        "added_columns": ["totale"],
                        "removed_columns": ["reddito"],
                        "type_changes": [{"column": "comune", "from": "VARCHAR", "to": "TEXT"}],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "inspect",
            "--json",
            "--config",
            str(config_path),
            "--year",
            "2022",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert isinstance(payload, dict)
    assert "layers" in payload
