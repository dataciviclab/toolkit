from __future__ import annotations

import json
from pathlib import Path

import time

from toolkit.core.run_context import RunContext, get_run_dir, read_run_record, write_run_record


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


def test_layer_metrics_default_to_null(tmp_path: Path) -> None:
    ctx = RunContext("ds", 2030, root=str(tmp_path))
    stored = _read_context(ctx.path)
    for layer in ("raw", "clean", "mart"):
        metrics = stored["layers"][layer]["metrics"]
        assert metrics["output_rows"] is None
        assert metrics["output_bytes"] is None
        assert metrics["tables_count"] is None


def test_set_layer_metrics_persists(tmp_path: Path) -> None:
    ctx = RunContext("ds", 2030, root=str(tmp_path))
    ctx.set_layer_metrics("clean", output_rows=1000, output_bytes=204800)
    stored = _read_context(ctx.path)
    m = stored["layers"]["clean"]["metrics"]
    assert m["output_rows"] == 1000
    assert m["output_bytes"] == 204800
    assert m["tables_count"] is None


def test_set_layer_metrics_mart_with_tables_count(tmp_path: Path) -> None:
    ctx = RunContext("ds", 2030, root=str(tmp_path))
    ctx.set_layer_metrics("mart", output_rows=5000, output_bytes=409600, tables_count=3)
    stored = _read_context(ctx.path)
    m = stored["layers"]["mart"]["metrics"]
    assert m["output_rows"] == 5000
    assert m["output_bytes"] == 409600
    assert m["tables_count"] == 3


def test_duration_seconds_computed_after_complete(tmp_path: Path) -> None:
    ctx = RunContext("ds", 2030, root=str(tmp_path))
    ctx.start_layer("raw")
    time.sleep(0.05)
    ctx.complete_layer("raw")
    ctx.complete_run()
    stored = _read_context(ctx.path)
    assert stored["layers"]["raw"]["duration_seconds"] is not None
    assert stored["layers"]["raw"]["duration_seconds"] >= 0
    assert stored["duration_seconds"] is not None
    assert stored["duration_seconds"] >= 0


def test_duration_seconds_null_while_running(tmp_path: Path) -> None:
    ctx = RunContext("ds", 2030, root=str(tmp_path))
    stored = _read_context(ctx.path)
    assert stored["duration_seconds"] is None
    assert stored["layers"]["raw"]["duration_seconds"] is None


def test_metrics_survive_json_round_trip(tmp_path: Path) -> None:
    ctx = RunContext("ds", 2030, root=str(tmp_path))
    ctx.set_layer_metrics("raw", output_bytes=8192)
    ctx.set_layer_metrics("clean", output_rows=500, output_bytes=16384)
    ctx.set_layer_metrics("mart", output_rows=200, output_bytes=4096, tables_count=2)
    record = read_run_record(get_run_dir(tmp_path, "ds", 2030), ctx.run_id)
    assert record["layers"]["raw"]["metrics"]["output_bytes"] == 8192
    assert record["layers"]["clean"]["metrics"]["output_rows"] == 500
    assert record["layers"]["mart"]["metrics"]["tables_count"] == 2


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


def test_write_run_record_retries_on_permission_error(tmp_path: Path, monkeypatch) -> None:
    run_dir = get_run_dir(tmp_path, "demo_ds", 2022)
    payload = {
        "dataset": "demo_ds",
        "year": 2022,
        "run_id": "retry_case",
        "started_at": "2026-02-28T09:00:00+00:00",
        "finished_at": None,
        "status": "RUNNING",
        "layers": {"raw": {"status": "PENDING"}, "clean": {"status": "PENDING"}, "mart": {"status": "PENDING"}},
        "validations": {"raw": {}, "clean": {}, "mart": {}},
        "error": None,
    }

    replace_calls = {"n": 0}
    original_replace = Path.replace

    def flaky_replace(self: Path, target: Path) -> Path:
        replace_calls["n"] += 1
        if replace_calls["n"] == 1 and self.name.endswith(".tmp"):
            raise PermissionError("[WinError 5] Access is denied")
        return original_replace(self, target)

    monkeypatch.setattr(Path, "replace", flaky_replace)

    written = write_run_record(run_dir, "retry_case", payload)

    assert written.exists()
    assert replace_calls["n"] >= 2
    stored = json.loads(written.read_text(encoding="utf-8"))
    assert stored["run_id"] == "retry_case"
