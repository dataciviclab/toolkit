from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from toolkit.cli import cmd_run
from toolkit.cli.app import app


def _write_config(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                f'root: "{(path.parent / "out").as_posix()}"',
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


def _write_old_run_record(path: Path, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dataset": "demo_ds",
                "year": 2022,
                "run_id": run_id,
                "started_at": "2026-02-28T09:00:00+00:00",
                "finished_at": "2026-02-28T09:05:00+00:00",
                "status": "FAILED",
                "layers": {
                    "raw": {"status": "SUCCESS", "started_at": "2026-02-28T09:00:00+00:00", "finished_at": "2026-02-28T09:01:00+00:00"},
                    "clean": {"status": "FAILED", "started_at": "2026-02-28T09:01:00+00:00", "finished_at": "2026-02-28T09:02:00+00:00"},
                    "mart": {"status": "PENDING", "started_at": None, "finished_at": None},
                },
                "validations": {"raw": {}, "clean": {}, "mart": {}},
                "resumed_from": None,
                "error": "clean failed",
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_non_portable_run_record(path: Path, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dataset": "demo_ds",
                "year": 2022,
                "run_id": run_id,
                "started_at": "2026-02-28T09:00:00+00:00",
                "finished_at": "2026-02-28T09:05:00+00:00",
                "status": "FAILED",
                "layers": {
                    "raw": {"status": "SUCCESS", "artifact_path": "/outside/root/file.csv"},
                    "clean": {"status": "FAILED", "started_at": "2026-02-28T09:01:00+00:00", "finished_at": "2026-02-28T09:02:00+00:00"},
                    "mart": {"status": "PENDING", "started_at": None, "finished_at": None},
                },
                "validations": {"raw": {}, "clean": {}, "mart": {}},
                "resumed_from": None,
                "error": "clean failed",
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_layer_artifacts(root: Path, dataset: str, year: int, layer: str) -> None:
    layer_dir = root / "data" / layer / dataset / str(year)
    layer_dir.mkdir(parents=True, exist_ok=True)

    (layer_dir / "metadata.json").write_text("{}", encoding="utf-8")
    if layer == "raw":
        payload = b"col\n1\n"
        (layer_dir / "raw.csv").write_bytes(payload)
        (layer_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "primary_output_file": "raw.csv",
                    "outputs": [{"file": "raw.csv", "bytes": len(payload), "sha256": "x"}],
                }
            ),
            encoding="utf-8",
        )
    elif layer == "clean":
        parquet = layer_dir / f"{dataset}_{year}_clean.parquet"
        parquet.write_bytes(b"PAR1")
        (layer_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "outputs": [{"file": parquet.name, "bytes": 4, "sha256": "x"}],
                }
            ),
            encoding="utf-8",
        )
    elif layer == "mart":
        parquet = layer_dir / "mart_example.parquet"
        parquet.write_bytes(b"PAR1")
        (layer_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "outputs": [{"file": parquet.name, "bytes": 4, "sha256": "x"}],
                }
            ),
            encoding="utf-8",
        )
    else:
        raise ValueError(layer)


def _write_success_with_warnings_run_record(path: Path, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dataset": "demo_ds",
                "year": 2022,
                "run_id": run_id,
                "started_at": "2026-02-28T09:00:00+00:00",
                "finished_at": "2026-02-28T09:05:00+00:00",
                "status": "SUCCESS_WITH_WARNINGS",
                "layers": {
                    "raw": {"status": "SUCCESS", "started_at": "2026-02-28T09:00:00+00:00", "finished_at": "2026-02-28T09:01:00+00:00"},
                    "clean": {"status": "SUCCESS", "started_at": "2026-02-28T09:01:00+00:00", "finished_at": "2026-02-28T09:02:00+00:00"},
                    "mart": {"status": "SUCCESS", "started_at": "2026-02-28T09:02:00+00:00", "finished_at": "2026-02-28T09:03:00+00:00"},
                },
                "validations": {
                    "raw": {"passed": True},
                    "clean": {"passed": False},
                    "mart": {"passed": True},
                },
                "resumed_from": None,
                "error": None,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_cli_resume_starts_from_first_non_success_layer(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "dataset.yml"
    _write_config(config_path)

    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    old_run_id = "old-run-id"
    runs_dir = tmp_path / "out" / "data" / "_runs" / "demo_ds" / "2022"
    _write_old_run_record(runs_dir / f"{old_run_id}.json", old_run_id)
    _write_layer_artifacts(tmp_path / "out", "demo_ds", 2022, "raw")

    calls = {"raw": 0, "clean": 0, "mart": 0}

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: calls.__setitem__("raw", calls["raw"] + 1))
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: calls.__setitem__("clean", calls["clean"] + 1))
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: calls.__setitem__("mart", calls["mart"] + 1))
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_clean_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_mart_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "resume",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--run-id",
            old_run_id,
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0
    assert calls["raw"] == 0
    assert calls["clean"] == 1
    assert calls["mart"] == 1

    records = sorted(runs_dir.glob("*.json"))
    assert len(records) == 2

    new_records = [p for p in records if p.stem != old_run_id]
    assert len(new_records) == 1
    new_record = json.loads(new_records[0].read_text(encoding="utf-8"))
    assert new_record["run_id"] != old_run_id
    assert new_record["resumed_from"] == old_run_id


def test_cli_resume_uses_config_root_when_cwd_differs(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "project" / "dataset.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    _write_config(config_path)

    sql_dir = config_path.parent / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (config_path.parent / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    old_run_id = "old-run-id"
    runs_dir = config_path.parent / "out" / "data" / "_runs" / "demo_ds" / "2022"
    _write_old_run_record(runs_dir / f"{old_run_id}.json", old_run_id)
    _write_layer_artifacts(config_path.parent / "out", "demo_ds", 2022, "raw")

    calls = {"raw": 0, "clean": 0, "mart": 0}

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: calls.__setitem__("raw", calls["raw"] + 1))
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: calls.__setitem__("clean", calls["clean"] + 1))
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: calls.__setitem__("mart", calls["mart"] + 1))
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_clean_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_mart_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "resume",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--run-id",
            old_run_id,
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0
    assert calls["raw"] == 0
    assert calls["clean"] == 1
    assert calls["mart"] == 1


def test_resume_finds_latest_run(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "project" / "dataset.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    _write_config(config_path)

    sql_dir = config_path.parent / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (config_path.parent / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    runs_dir = config_path.parent / "out" / "data" / "_runs" / "demo_ds" / "2022"
    _write_old_run_record(runs_dir / "old-run.json", "old-run")
    _write_old_run_record(runs_dir / "new-run.json", "new-run")
    _write_layer_artifacts(config_path.parent / "out", "demo_ds", 2022, "raw")

    old_payload = json.loads((runs_dir / "old-run.json").read_text(encoding="utf-8"))
    old_payload["started_at"] = "2026-02-28T08:00:00+00:00"
    (runs_dir / "old-run.json").write_text(json.dumps(old_payload, indent=2), encoding="utf-8")

    new_payload = json.loads((runs_dir / "new-run.json").read_text(encoding="utf-8"))
    new_payload["started_at"] = "2026-02-28T10:00:00+00:00"
    (runs_dir / "new-run.json").write_text(json.dumps(new_payload, indent=2), encoding="utf-8")

    calls = {"raw": 0, "clean": 0, "mart": 0}

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: calls.__setitem__("raw", calls["raw"] + 1))
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: calls.__setitem__("clean", calls["clean"] + 1))
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: calls.__setitem__("mart", calls["mart"] + 1))
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_clean_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_mart_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})

    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "resume",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--latest",
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0
    assert "Resumed from new-run starting at clean." in result.output
    assert calls["raw"] == 0
    assert calls["clean"] == 1
    assert calls["mart"] == 1


def test_cli_resume_denies_non_portable_record_without_compat(tmp_path: Path) -> None:
    config_path = tmp_path / "dataset.yml"
    _write_config(config_path)

    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    old_run_id = "old-run-id"
    runs_dir = tmp_path / "out" / "data" / "_runs" / "demo_ds" / "2022"
    _write_non_portable_run_record(runs_dir / f"{old_run_id}.json", old_run_id)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "resume",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--run-id",
            old_run_id,
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code != 0
    assert "Use --compat to resume anyway." in result.output


def test_cli_resume_falls_back_to_raw_when_raw_success_artifacts_are_missing(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "dataset.yml"
    _write_config(config_path)

    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    old_run_id = "old-run-id"
    runs_dir = tmp_path / "out" / "data" / "_runs" / "demo_ds" / "2022"
    _write_old_run_record(runs_dir / f"{old_run_id}.json", old_run_id)

    calls = {"raw": 0, "clean": 0, "mart": 0}

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: calls.__setitem__("raw", calls["raw"] + 1))
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: calls.__setitem__("clean", calls["clean"] + 1))
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: calls.__setitem__("mart", calls["mart"] + 1))
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_clean_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_mart_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "resume",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--run-id",
            old_run_id,
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0
    assert "Falling back to 'raw'" in result.output
    assert calls["raw"] == 1
    assert calls["clean"] == 1
    assert calls["mart"] == 1


def test_cli_resume_success_with_warnings_requires_from_layer_or_exits_cleanly(tmp_path: Path) -> None:
    config_path = tmp_path / "dataset.yml"
    _write_config(config_path)

    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    run_id = "warn-run"
    runs_dir = tmp_path / "out" / "data" / "_runs" / "demo_ds" / "2022"
    _write_success_with_warnings_run_record(runs_dir / f"{run_id}.json", run_id)
    _write_layer_artifacts(tmp_path / "out", "demo_ds", 2022, "raw")
    _write_layer_artifacts(tmp_path / "out", "demo_ds", 2022, "clean")
    _write_layer_artifacts(tmp_path / "out", "demo_ds", 2022, "mart")

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "resume",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--run-id",
            run_id,
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 2
    assert "Use --from-layer raw|clean|mart" in result.output


def test_cli_resume_success_with_warnings_allows_forced_from_layer(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "dataset.yml"
    _write_config(config_path)

    sql_dir = tmp_path / "sql" / "mart"
    sql_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
    (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")

    run_id = "warn-run"
    runs_dir = tmp_path / "out" / "data" / "_runs" / "demo_ds" / "2022"
    _write_success_with_warnings_run_record(runs_dir / f"{run_id}.json", run_id)
    _write_layer_artifacts(tmp_path / "out", "demo_ds", 2022, "raw")
    _write_layer_artifacts(tmp_path / "out", "demo_ds", 2022, "clean")
    _write_layer_artifacts(tmp_path / "out", "demo_ds", 2022, "mart")

    calls = {"raw": 0, "clean": 0, "mart": 0}

    monkeypatch.setattr(cmd_run, "run_raw", lambda *args, **kwargs: calls.__setitem__("raw", calls["raw"] + 1))
    monkeypatch.setattr(cmd_run, "run_clean", lambda *args, **kwargs: calls.__setitem__("clean", calls["clean"] + 1))
    monkeypatch.setattr(cmd_run, "run_mart", lambda *args, **kwargs: calls.__setitem__("mart", calls["mart"] + 1))
    monkeypatch.setattr(cmd_run, "run_raw_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_clean_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})
    monkeypatch.setattr(cmd_run, "run_mart_validation", lambda *args, **kwargs: {"passed": True, "errors_count": 0, "warnings_count": 0, "checks": []})

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "resume",
            "--dataset",
            "demo_ds",
            "--year",
            "2022",
            "--run-id",
            run_id,
            "--from-layer",
            "clean",
            "--config",
            str(config_path),
        ],
    )

    assert result.exit_code == 0
    assert calls["raw"] == 0
    assert calls["clean"] == 1
    assert calls["mart"] == 1
