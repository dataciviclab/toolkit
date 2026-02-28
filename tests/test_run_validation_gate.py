from __future__ import annotations

import json
from pathlib import Path

import pytest

from toolkit.cli import cmd_run


def _write_config(path: Path, *, fail_on_error: bool) -> None:
    sql_dir = path.parent / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (path.parent / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")
    path.write_text(
        "\n".join(
            [
                f'root: "{(path.parent / "out").as_posix()}"',
                "dataset:",
                '  name: "test_dataset"',
                "  years: [2022]",
                "raw: {}",
                "clean:",
                '  sql: "sql/clean.sql"',
                "mart:",
                "  tables:",
                '    - name: "mart_example"',
                '      sql: "sql/mart/mart_example.sql"',
                "validation:",
                f'  fail_on_error: {"true" if fail_on_error else "false"}',
            ]
        ),
        encoding="utf-8",
    )


def _read_run_record(root: Path) -> dict[str, object]:
    runs_dir = root / "data" / "_runs" / "test_dataset" / "2022"
    records = list(runs_dir.glob("*.json"))
    assert len(records) == 1
    return json.loads(records[0].read_text(encoding="utf-8"))


def _ok_summary() -> dict[str, object]:
    return {
        "passed": True,
        "errors_count": 0,
        "warnings_count": 0,
        "checks": [],
    }


def _failed_summary() -> dict[str, object]:
    return {
        "passed": False,
        "errors_count": 1,
        "warnings_count": 0,
        "checks": [{"name": "errors", "status": "failed", "details": "errors=1"}],
    }


def test_run_stops_after_failed_validation_when_fail_on_error_true(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "dataset.yml"
    _write_config(config_path, fail_on_error=True)

    calls = {"mart": 0}

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: calls.__setitem__("mart", calls["mart"] + 1))
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: _ok_summary())
    monkeypatch.setattr(cmd_run, "run_clean_validation", lambda *args, **kwargs: _failed_summary())
    monkeypatch.setattr(cmd_run, "run_mart_validation", lambda *args, **kwargs: _ok_summary())

    with pytest.raises(cmd_run.ValidationGateError):
        cmd_run.run(step="all", config=str(config_path))

    assert calls["mart"] == 0

    record = _read_run_record(tmp_path / "out")
    assert record["status"] == "FAILED"
    assert record["validations"]["clean"]["passed"] is False


def test_run_continues_after_failed_validation_when_fail_on_error_false(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "dataset.yml"
    _write_config(config_path, fail_on_error=False)

    calls = {"mart": 0}

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: None)
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: calls.__setitem__("mart", calls["mart"] + 1))
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: _ok_summary())
    monkeypatch.setattr(cmd_run, "run_clean_validation", lambda *args, **kwargs: _failed_summary())
    monkeypatch.setattr(cmd_run, "run_mart_validation", lambda *args, **kwargs: _ok_summary())

    cmd_run.run(step="all", config=str(config_path))

    assert calls["mart"] == 1

    record = _read_run_record(tmp_path / "out")
    assert record["status"] == "SUCCESS_WITH_WARNINGS"
    assert record["validations"]["clean"]["passed"] is False
