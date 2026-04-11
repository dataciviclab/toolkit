from __future__ import annotations

import shutil
from pathlib import Path

from typer.testing import CliRunner

from toolkit.cli.app import app
from toolkit.cli.cmd_profile import render_profile_md


def test_render_profile_md_includes_expected_sections() -> None:
    profile = {
        "dataset": "demo_ds",
        "year": 2024,
        "file_used": "demo.csv",
        "encoding_suggested": "utf-8",
        "header_line": "col1;col2",
        "columns_raw": ["col1", "col2"],
        "missingness_top": [{"column": "col2", "missing_pct": 12.5}],
        "warnings": ["warning uno"],
        "mapping_suggestions": {
            "col1_clean": {"type": "text"},
            "col2_clean": {"type": "number", "parse": {"kind": "int"}},
        },
        "robust_read_suggested": True,
    }

    md = render_profile_md(profile)

    assert "# RAW Profile - demo_ds (2024)" in md
    assert "## Suggested read options" in md
    assert "## Header (first line)" in md
    assert "## Missingness (top)" in md
    assert "## Mapping suggestions (first 15)" in md
    assert "## Warnings" in md
    assert "`col2`: 12.5%" in md


def test_cli_profile_raw_happy_path(tmp_path: Path, monkeypatch) -> None:
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    shutil.rmtree(dst / "_smoke_out", ignore_errors=True)
    config_path = dst / "dataset.yml"

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    run_result = runner.invoke(
        app,
        ["run", "raw", "--config", str(config_path), "--strict-config"],
    )
    assert run_result.exit_code == 0, run_result.output

    profile_result = runner.invoke(
        app,
        ["profile", "raw", "--config", str(config_path), "--strict-config"],
    )
    assert profile_result.exit_code == 0, profile_result.output
    assert "PROFILE RAW ->" in profile_result.output

    profile_dir = (
        dst
        / "_smoke_out"
        / "data"
        / "raw"
        / "project_example"
        / "2022"
        / "_profile"
    )
    assert (profile_dir / "raw_profile.json").exists()
