from __future__ import annotations

import json
from pathlib import Path

from toolkit.core.run_context import RunContext, get_run_dir, read_run_record


def _read_context(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_run_context_persists_and_updates(tmp_path: Path) -> None:
    ctx = RunContext("test_ds", 2030, root=str(tmp_path))

    # initial file created
    stored = _read_context(ctx.path)
    assert stored["dataset"] == "test_ds"
    assert stored["year"] == 2030
    assert stored["status"] == "RUNNING"
    assert stored["layers"]["raw"]["status"] == "PENDING"
    assert stored["validations"]["raw"] == {}
    assert stored["finished_at"] is None

    ctx.start_layer("raw")
    ctx.complete_layer("raw")
    ctx.complete_run()

    stored = _read_context(ctx.path)
    assert stored["status"] == "SUCCESS"
    assert stored["finished_at"] is not None
    assert stored["layers"]["raw"]["status"] == "SUCCESS"


def test_read_run_record_migrates_absolute_paths_under_root_to_relative(tmp_path: Path) -> None:
    run_dir = get_run_dir(tmp_path, "demo_ds", 2022)
    run_dir.mkdir(parents=True, exist_ok=True)
    record_path = run_dir / "legacy.json"
    payload = {
        "dataset": "demo_ds",
        "year": 2022,
        "run_id": "legacy",
        "started_at": "2026-02-28T09:00:00+00:00",
        "finished_at": None,
        "status": "FAILED",
        "layers": {"raw": {"status": "SUCCESS", "artifact_path": str(tmp_path / "data" / "raw" / "demo_ds" / "2022" / "file.csv")}},
        "validations": {"raw": {}, "clean": {}, "mart": {}},
        "error": None,
    }
    record_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    record = read_run_record(run_dir, "legacy")

    assert record["layers"]["raw"]["artifact_path"] == "data/raw/demo_ds/2022/file.csv"
    assert record["_portability"]["portable"] is True
    assert record["_portability"]["warnings"] == []


def test_read_run_record_marks_absolute_paths_outside_root_as_non_portable(tmp_path: Path) -> None:
    run_dir = get_run_dir(tmp_path, "demo_ds", 2022)
    run_dir.mkdir(parents=True, exist_ok=True)
    record_path = run_dir / "legacy.json"
    payload = {
        "dataset": "demo_ds",
        "year": 2022,
        "run_id": "legacy",
        "started_at": "2026-02-28T09:00:00+00:00",
        "finished_at": None,
        "status": "FAILED",
        "layers": {"raw": {"status": "SUCCESS", "artifact_path": "/outside/root/file.csv"}},
        "validations": {"raw": {}, "clean": {}, "mart": {}},
        "error": None,
    }
    record_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    record = read_run_record(run_dir, "legacy")

    assert record["layers"]["raw"]["artifact_path"] == "/outside/root/file.csv"
    assert record["_portability"]["portable"] is False
    assert record["_portability"]["warnings"] == ["/outside/root/file.csv"]


def test_read_run_record_does_not_treat_error_message_as_path(tmp_path: Path) -> None:
    run_dir = get_run_dir(tmp_path, "demo_ds", 2022)
    run_dir.mkdir(parents=True, exist_ok=True)
    record_path = run_dir / "legacy.json"
    payload = {
        "dataset": "demo_ds",
        "year": 2022,
        "run_id": "legacy",
        "started_at": "2026-02-28T09:00:00+00:00",
        "finished_at": None,
        "status": "FAILED",
        "layers": {"raw": {"status": "SUCCESS"}},
        "validations": {"raw": {}, "clean": {}, "mart": {}},
        "error": "/diagnostic text that is not a filesystem path",
    }
    record_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    record = read_run_record(run_dir, "legacy")

    assert record["error"] == "/diagnostic text that is not a filesystem path"
    assert record["_portability"]["portable"] is True
    assert record["_portability"]["warnings"] == []
