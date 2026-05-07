from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app
from toolkit.core.io import write_json_atomic


def test_cli_profile_raw_happy_path(tmp_path: Path, monkeypatch) -> None:
    src = Path(__file__).resolve().parents[1] / "project-example"
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


def test_write_json_atomic_handles_nan(tmp_path: Path) -> None:
    """write_json_atomic should not raise on NaN/inf float values (pandas NaT edge case)."""
    p = tmp_path / "out.json"
    data = {
        "col1": float("nan"),
        "col2": float("inf"),
        "col3": float("-inf"),
        "col4": 3.14,
        "normal": 42,
    }
    write_json_atomic(p, data)
    loaded = json.loads(p.read_text())
    assert loaded["normal"] == 42
    # NaN/inf serialized as strings survive the round-trip
    assert loaded["col1"] == "nan"
    assert loaded["col2"] == "inf"
    assert loaded["col3"] == "-inf"
    # Normal finite values are preserved as JSON numbers
    assert loaded["col4"] == 3.14


def test_write_json_atomic_raises_for_unknown_types(tmp_path: Path) -> None:
    """write_json_atomic should raise for types it cannot handle."""
    p = tmp_path / "out.json"
    data = {"col1": set([1, 2, 3])}
    with pytest.raises(TypeError):
        write_json_atomic(p, data)
