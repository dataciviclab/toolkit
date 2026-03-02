from __future__ import annotations

import json
import shutil
from pathlib import Path

from typer.testing import CliRunner

from toolkit.cli.app import app


def test_inspect_paths_reports_dataset_repo_layout_from_other_cwd(tmp_path: Path, monkeypatch) -> None:
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
    assert f"raw_dir: {dst / '_smoke_out' / 'data' / 'raw' / 'project_example' / '2022'}" in result.output
    assert f"raw_manifest: {dst / '_smoke_out' / 'data' / 'raw' / 'project_example' / '2022' / 'manifest.json'}" in result.output
    assert f"clean_output: {dst / '_smoke_out' / 'data' / 'clean' / 'project_example' / '2022' / 'project_example_2022_clean.parquet'}" in result.output
    assert f"clean_validation: {dst / '_smoke_out' / 'data' / 'clean' / 'project_example' / '2022' / '_validate' / 'clean_validation.json'}" in result.output
    assert f"mart_manifest: {dst / '_smoke_out' / 'data' / 'mart' / 'project_example' / '2022' / 'manifest.json'}" in result.output
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
        ["inspect", "paths", "--config", str(config_path), "--year", "2022", "--json", "--strict-config"],
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
    assert payload["latest_run"] is None
