from __future__ import annotations

import shutil
from pathlib import Path

from typer.testing import CliRunner

from toolkit.cli.app import app


def _copy_project_example_multi_year(dst: Path) -> Path:
    src = Path("project-example")
    shutil.copytree(src, dst)
    shutil.rmtree(dst / "_smoke_out", ignore_errors=True)

    config_path = dst / "dataset.yml"
    config_text = config_path.read_text(encoding="utf-8")
    config_text = config_text.replace('  years: [2022]\n', '  years: [2022, 2023]\n')
    config_path.write_text(config_text, encoding="utf-8")
    return config_path


def test_cli_run_all_supports_years_filter(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example_multi_year(project_dir)

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "all", "--config", str(config_path), "--years", "2023", "--strict-config"],
    )

    assert result.exit_code == 0, result.output

    root = project_dir / "_smoke_out"
    raw_2022_dir = root / "data" / "raw" / "project_example" / "2022"
    raw_2023_dir = root / "data" / "raw" / "project_example" / "2023"
    mart_2022_dir = root / "data" / "mart" / "project_example" / "2022"
    mart_2023_dir = root / "data" / "mart" / "project_example" / "2023"

    assert not raw_2022_dir.exists()
    assert raw_2023_dir.exists()
    assert not mart_2022_dir.exists()
    assert mart_2023_dir.exists()


def test_cli_validate_all_supports_years_filter(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example_multi_year(project_dir)

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    run_result = runner.invoke(
        app,
        ["run", "all", "--config", str(config_path), "--years", "2023", "--strict-config"],
    )
    assert run_result.exit_code == 0, run_result.output

    validate_result = runner.invoke(
        app,
        ["validate", "all", "--config", str(config_path), "--years", "2023", "--strict-config"],
    )
    assert validate_result.exit_code == 0, validate_result.output


def test_cli_years_filter_rejects_unconfigured_year(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "project-example"
    config_path = _copy_project_example_multi_year(project_dir)

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "all", "--config", str(config_path), "--years", "2024", "--strict-config"],
    )

    assert result.exit_code != 0
    assert result.exception is not None
    assert "Year(s) not configured in dataset.yml: 2024" in str(result.exception)
