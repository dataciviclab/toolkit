from __future__ import annotations

from pathlib import Path

import pytest

from toolkit.cli.app import app
from toolkit.cli.cmd_run import run as run_cmd


def _make_multi_year(project_example: Path) -> Path:
    """Aggiunge un secondo anno e SQL cross al project-example."""
    config_path = project_example / "dataset.yml"
    config_text = config_path.read_text(encoding="utf-8")
    config_text = config_text.replace("  years: [2022]\n", "  years: [2022, 2023]\n")
    config_path.write_text(config_text, encoding="utf-8")
    return config_path


@pytest.mark.contract
def test_cli_run_all_supports_years_filter(project_example: Path, runner, chdir_tmp: Path) -> None:
    config_path = _make_multi_year(project_example)
    result = runner.invoke(
        app,
        [
            "run",
            "all",
            "--config",
            str(config_path),
            "--years",
            "2023",
        ],
    )
    assert result.exit_code == 0, result.output

    root = project_example / "_smoke_out"
    assert not (root / "data" / "raw" / "project_example" / "2022").exists()
    assert (root / "data" / "raw" / "project_example" / "2023").exists()
    assert not (root / "data" / "mart" / "project_example" / "2022").exists()
    assert (root / "data" / "mart" / "project_example" / "2023").exists()


@pytest.mark.contract
def test_cli_validate_all_supports_years_filter(
    project_example: Path, runner, chdir_tmp: Path
) -> None:
    config_path = _make_multi_year(project_example)
    run_result = runner.invoke(
        app,
        [
            "run",
            "all",
            "--config",
            str(config_path),
            "--years",
            "2023",
        ],
    )
    assert run_result.exit_code == 0, run_result.output

    validate_result = runner.invoke(
        app,
        [
            "validate",
            "all",
            "--config",
            str(config_path),
            "--years",
            "2023",
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output


@pytest.mark.contract
def test_cli_years_filter_rejects_unconfigured_year(
    project_example: Path, runner, chdir_tmp: Path
) -> None:
    config_path = _make_multi_year(project_example)
    result = runner.invoke(
        app,
        [
            "run",
            "all",
            "--config",
            str(config_path),
            "--years",
            "2024",
        ],
    )
    assert result.exit_code != 0
    assert result.exception is not None
    assert "Year(s) not configured in dataset.yml: 2024" in str(result.exception)


@pytest.mark.contract
def test_cli_run_all_with_year_single_filter(
    project_example: Path, runner, chdir_tmp: Path
) -> None:
    """--year (singular) should work identically to --years for single year."""
    config_path = _make_multi_year(project_example)
    result = runner.invoke(
        app,
        [
            "run",
            "all",
            "--config",
            str(config_path),
            "--year",
            "2023",
        ],
    )
    assert result.exit_code == 0, result.output

    root = project_example / "_smoke_out"
    assert not (root / "data" / "raw" / "project_example" / "2022").exists()
    assert (root / "data" / "raw" / "project_example" / "2023").exists()


@pytest.mark.contract
def test_cli_validate_with_year_single_filter(
    project_example: Path, runner, chdir_tmp: Path
) -> None:
    """--year (singular) should work for validate command."""
    config_path = _make_multi_year(project_example)

    run_result = runner.invoke(
        app,
        [
            "run",
            "all",
            "--config",
            str(config_path),
            "--year",
            "2023",
        ],
    )
    assert run_result.exit_code == 0, run_result.output

    validate_result = runner.invoke(
        app,
        [
            "validate",
            "all",
            "--config",
            str(config_path),
            "--year",
            "2023",
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output


@pytest.mark.policy
def test_cli_year_and_years_mutual_exclusion(
    project_example: Path, runner, chdir_tmp: Path
) -> None:
    """Using both --year and --years must be rejected."""
    config_path = _make_multi_year(project_example)
    result = runner.invoke(
        app,
        ["run", "raw", "--config", str(config_path), "--year", "2023", "--years", "2022"],
    )
    assert result.exit_code != 0
    assert result.exception is not None
    assert "Use either --year or --years, not both" in str(result.exception)


@pytest.mark.contract
def test_cli_run_all_without_years_keeps_direct_python_invocation_compat(
    project_example: Path, chdir_tmp: Path
) -> None:
    config_path = _make_multi_year(project_example)
    run_cmd(step="all", config=str(config_path))

    root = project_example / "_smoke_out"
    assert (root / "data" / "raw" / "project_example" / "2022").exists()
    assert (root / "data" / "raw" / "project_example" / "2023").exists()
