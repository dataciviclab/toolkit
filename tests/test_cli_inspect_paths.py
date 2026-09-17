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
