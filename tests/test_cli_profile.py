from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app
from toolkit.core.io import write_json_atomic


def _setup_project(tmp_path: Path) -> tuple[Path, CliRunner]:
    src = Path(__file__).resolve().parents[1] / "project-example"
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    shutil.rmtree(dst / "_smoke_out", ignore_errors=True)
    config_path = dst / "dataset.yml"

    runner = CliRunner()
    run_result = runner.invoke(
        app,
        ["run", "raw", "--config", str(config_path), "--strict-config"],
    )
    assert run_result.exit_code == 0, run_result.output

    return config_path, runner


def _assert_profile_written(dst: Path) -> None:
    profile_dir = (
        dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022" / "_profile"
    )
    assert (profile_dir / "raw_profile.json").exists()


def test_cli_profile_raw_happy_path(tmp_path: Path, monkeypatch) -> None:
    config_path, runner = _setup_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    profile_result = runner.invoke(
        app,
        ["profile", "raw", "--config", str(config_path), "--strict-config"],
    )
    assert profile_result.exit_code == 0, profile_result.output
    assert "PROFILE RAW ->" in profile_result.output
    _assert_profile_written(tmp_path / "project-example")


def test_inspect_profile_happy_path(tmp_path: Path, monkeypatch) -> None:
    config_path, runner = _setup_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    profile_result = runner.invoke(
        app,
        ["inspect", "profile", "--config", str(config_path), "--strict-config"],
    )
    assert profile_result.exit_code == 0, profile_result.output
    assert "PROFILE RAW ->" in profile_result.output
    _assert_profile_written(tmp_path / "project-example")


def test_inspect_profile_single_year(tmp_path: Path, monkeypatch) -> None:
    config_path, runner = _setup_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    profile_result = runner.invoke(
        app,
        ["inspect", "profile", "--config", str(config_path), "--year", "2022"],
    )
    assert profile_result.exit_code == 0, profile_result.output
    assert "PROFILE RAW ->" in profile_result.output
    _assert_profile_written(tmp_path / "project-example")


SIMPLE_CSV = "a,b,c\n1,2,3\n4,5,6\n"


def test_inspect_profile_csv_path_text_output(tmp_path: Path, monkeypatch) -> None:
    """--csv-path stampa encoding/delim/colonne in output testo."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(SIMPLE_CSV, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    result = runner.invoke(app, ["inspect", "profile", "--csv-path", str(csv_file)])
    assert result.exit_code == 0, result.output
    assert "Encoding:" in result.output
    assert "Delim:" in result.output
    assert "Colonne:" in result.output
    assert "a" in result.output
    assert "b" in result.output


def test_inspect_profile_csv_path_json_output(tmp_path: Path, monkeypatch) -> None:
    """--csv-path --json produce JSON parsabile con struttura attesa."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(SIMPLE_CSV, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    runner = CliRunner()
    result = runner.invoke(
        app, ["inspect", "profile", "--csv-path", str(csv_file), "--json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["column_count"] == 3
    assert payload["encoding_suggested"] == "utf-8"
    assert len(payload["columns"]) == 3
    assert len(payload["preview"]) == 2  # 2 data rows


def test_inspect_profile_csv_path_file_not_found(tmp_path: Path, monkeypatch) -> None:
    """--csv-path con file inesistente deve fallire con errore leggibile."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app, ["inspect", "profile", "--csv-path", str(tmp_path / "missing.csv")]
    )
    assert result.exit_code != 0
    # L'eccezione può finire in result.output o result.exception
    err_text = result.output or str(result.exception or "")
    assert "CSV non trovato" in err_text or "non trovato" in err_text


def test_inspect_profile_csv_path_requires_either_flag(tmp_path: Path, monkeypatch) -> None:
    """Senza --config e senza --csv-path, deve dare errore."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(app, ["inspect", "profile"])
    assert result.exit_code != 0
    assert "Serve --config o --csv-path" in result.output


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
