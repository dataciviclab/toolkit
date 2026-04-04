from __future__ import annotations

import json
import shutil
from pathlib import Path

from typer.testing import CliRunner

from toolkit.cli.app import app


def test_inspect_paths_reports_dataset_repo_layout_from_other_cwd(
    tmp_path: Path, monkeypatch
) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    shutil.rmtree(dst / "_smoke_out", ignore_errors=True)
    config_path = dst / "dataset.yml"

    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    run_result = runner.invoke(app, ["run", "all", "--config", str(config_path), "--strict-config"])
    assert run_result.exit_code == 0, run_result.output

    result = runner.invoke(
        app,
        ["inspect", "paths", "--config", str(config_path), "--year", "2022", "--strict-config"],
    )

    assert result.exit_code == 0, result.output
    assert f"config_path: {config_path}" in result.output
    assert f"root: {dst / '_smoke_out'}" in result.output
    assert (
        f"raw_dir: {dst / '_smoke_out' / 'data' / 'raw' / 'project_example' / '2022'}"
        in result.output
    )
    assert (
        f"raw_manifest: {dst / '_smoke_out' / 'data' / 'raw' / 'project_example' / '2022' / 'manifest.json'}"
        in result.output
    )
    assert (
        f"clean_output: {dst / '_smoke_out' / 'data' / 'clean' / 'project_example' / '2022' / 'project_example_2022_clean.parquet'}"
        in result.output
    )
    assert (
        f"clean_validation: {dst / '_smoke_out' / 'data' / 'clean' / 'project_example' / '2022' / '_validate' / 'clean_validation.json'}"
        in result.output
    )
    assert (
        f"mart_manifest: {dst / '_smoke_out' / 'data' / 'mart' / 'project_example' / '2022' / 'manifest.json'}"
        in result.output
    )
    assert "raw_hints:" in result.output
    assert "primary_output_file:" in result.output
    assert "suggested_read_exists: True" in result.output
    assert "latest_run_status: SUCCESS" in result.output


def test_inspect_paths_json_is_notebook_friendly(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    shutil.rmtree(dst / "_smoke_out", ignore_errors=True)
    config_path = dst / "dataset.yml"

    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(
        app,
        [
            "inspect",
            "paths",
            "--config",
            str(config_path),
            "--year",
            "2022",
            "--json",
            "--strict-config",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["dataset"] == "project_example"
    assert payload["year"] == 2022
    assert payload["config_path"] == str(config_path)
    assert payload["paths"]["clean"]["output"].endswith("project_example_2022_clean.parquet")
    assert payload["paths"]["clean"]["validation"].endswith("clean_validation.json")
    assert payload["paths"]["raw"]["manifest"].endswith("manifest.json")
    assert payload["paths"]["mart"]["outputs"]
    assert payload["paths"]["mart"]["metadata"].endswith("metadata.json")
    assert payload["raw_hints"]["primary_output_file"] is None
    assert payload["raw_hints"]["suggested_read_exists"] is False
    assert payload["raw_hints"]["suggested_read_path"].endswith("suggested_read.yml")
    assert payload["latest_run"] is None


def test_inspect_paths_json_reports_resolved_support_outputs(tmp_path: Path) -> None:
    runner = CliRunner()

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
            "paths",
            "--config",
            str(config_path),
            "--year",
            "2022",
            "--json",
            "--strict-config",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["paths"]["support"]
    support_payload = payload["paths"]["support"][0]
    assert support_payload["name"] == "scuole"
    assert support_payload["dataset"] == "support_ds"
    assert support_payload["years"] == [2024]
    assert support_payload["outputs"] == [
        str(support_root / "data" / "mart" / "support_ds" / "2024" / "support_table.parquet")
    ]
    assert support_payload["mart"].endswith("support_table.parquet")


def test_inspect_paths_json_exposes_layer_profiles(tmp_path: Path) -> None:
    runner = CliRunner()

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
            "paths",
            "--config",
            str(config_path),
            "--year",
            "2022",
            "--json",
            "--strict-config",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["layer_profiles"]["clean_output"]["row_count"] == 39506
    assert payload["layer_profiles"]["mart_clean_input"]["columns_preview"][0]["name"] == "comune"
    assert payload["layer_profiles"]["mart_tables"][0]["name"] == "mart_example"
    assert payload["layer_profiles"]["clean_to_mart"][0]["target_name"] == "mart_example"
    assert payload["layer_profiles"]["clean_to_mart"][0]["type_change_count"] == 1
