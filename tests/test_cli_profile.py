from __future__ import annotations

import json
from pathlib import Path

import pytest

from toolkit.cli.app import app
from toolkit.core.io import write_json_atomic

pytestmark = pytest.mark.contract


def _run_raw(project_example: Path, runner) -> Path:
    """Run RAW layer on project-example, return config path."""
    config_path = project_example / "dataset.yml"
    run_result = runner.invoke(
        app,
        [
            "run",
            "raw",
            "--config",
            str(config_path),
        ],
    )
    assert run_result.exit_code == 0, run_result.output
    return config_path


def _assert_profile_written(project_example: Path) -> None:
    profile_dir = (
        project_example / "_smoke_out" / "data" / "raw" / "project_example" / "2022" / "_profile"
    )
    assert (profile_dir / "raw_profile.json").exists()


def test_cli_profile_raw_happy_path(project_example: Path, runner, chdir_tmp: Path) -> None:
    config_path = _run_raw(project_example, runner)

    profile_result = runner.invoke(
        app,
        [
            "inspect",
            "profile",
            "--config",
            str(config_path),
        ],
    )
    assert profile_result.exit_code == 0, profile_result.output
    assert "PROFILE RAW ->" in profile_result.output
    _assert_profile_written(project_example)


def test_inspect_profile_happy_path(project_example: Path, runner, chdir_tmp: Path) -> None:
    config_path = _run_raw(project_example, runner)

    profile_result = runner.invoke(
        app,
        [
            "inspect",
            "profile",
            "--config",
            str(config_path),
        ],
    )
    assert profile_result.exit_code == 0, profile_result.output
    assert "PROFILE RAW ->" in profile_result.output
    _assert_profile_written(project_example)


def test_inspect_profile_single_year(project_example: Path, runner, chdir_tmp: Path) -> None:
    config_path = _run_raw(project_example, runner)

    profile_result = runner.invoke(
        app,
        ["inspect", "profile", "--config", str(config_path), "--year", "2022"],
    )
    assert profile_result.exit_code == 0, profile_result.output
    assert "PROFILE RAW ->" in profile_result.output
    _assert_profile_written(project_example)


SIMPLE_CSV = "a,b,c\n1,2,3\n4,5,6\n"


def test_inspect_profile_csv_path_text_output(tmp_path: Path, runner, chdir_tmp: Path) -> None:
    """--csv-path stampa encoding/delim/colonne in output testo."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(SIMPLE_CSV, encoding="utf-8")

    result = runner.invoke(app, ["inspect", "profile", "--csv-path", str(csv_file)])
    assert result.exit_code == 0, result.output
    assert "Encoding:" in result.output
    assert "Delim:" in result.output
    assert "Colonne:" in result.output
    assert "a" in result.output
    assert "b" in result.output


def test_inspect_profile_csv_path_json_output(tmp_path: Path, runner, chdir_tmp: Path) -> None:
    """--csv-path --json produce JSON parsabile con struttura attesa."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(SIMPLE_CSV, encoding="utf-8")

    result = runner.invoke(app, ["inspect", "profile", "--csv-path", str(csv_file), "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["column_count"] == 3
    assert payload["encoding_suggested"] == "utf-8"
    assert len(payload["columns"]) == 3
    assert len(payload["preview"]) == 2  # 2 data rows


def test_inspect_profile_csv_path_file_not_found(tmp_path: Path, runner, chdir_tmp: Path) -> None:
    """--csv-path con file inesistente deve fallire con errore leggibile."""
    result = runner.invoke(app, ["inspect", "profile", "--csv-path", str(tmp_path / "missing.csv")])
    assert result.exit_code != 0
    err_text = result.output or str(result.exception or "")
    assert "CSV non trovato" in err_text or "non trovato" in err_text


def test_inspect_profile_csv_path_requires_either_flag(
    tmp_path: Path, runner, chdir_tmp: Path
) -> None:
    """Senza --config e senza --csv-path, deve dare errore."""
    result = runner.invoke(app, ["inspect", "profile"])
    assert result.exit_code != 0, f"Expected failure, got:\n{result.output}"
    output_clean = result.output.strip()
    assert "config" in output_clean.lower() and "csv" in output_clean.lower()


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
    assert loaded["col1"] == "nan"
    assert loaded["col2"] == "inf"
    assert loaded["col3"] == "-inf"
    assert loaded["col4"] == 3.14


def test_write_json_atomic_raises_for_unknown_types(tmp_path: Path) -> None:
    """write_json_atomic should raise for types it cannot handle."""
    p = tmp_path / "out.json"
    data = {"col1": set([1, 2, 3])}
    with pytest.raises(TypeError):
        write_json_atomic(p, data)


@pytest.fixture
def _mock_preview_url(monkeypatch):
    """Fixture che sostituisce preview_url con un mock che cattura known_skip."""
    from types import SimpleNamespace
    import toolkit.profile.preview as preview_mod

    captured: dict = {}

    def _mock(url, *, known_encoding=None, known_delim=None, known_decimal=None, known_skip=None):
        captured["known_skip"] = known_skip
        return SimpleNamespace(
            url=url,
            status="success",
            reachable=True,
            http_status=200,
            file_size=100,
            resource_format="CSV",
            encoding_suggested="utf-8",
            delim_suggested=",",
            decimal_suggested=None,
            skip_suggested=0,
            columns=["a", "b"],
            col_types={},
            preview_row_count=10,
            robust_read_suggested=False,
            mapping_suggestions={},
            granularity="comune",
            year_min=None,
            year_max=None,
            quality_score=None,
            quality_structural_score=None,
            quality_semantic_score=None,
            quality_combined_score=None,
            quality_sampled=None,
            quality_verdict=None,
            quality_flags=None,
            quality_ontologies=None,
            quality_note=None,
        )

    monkeypatch.setattr(preview_mod, "preview_url", _mock)
    return captured
