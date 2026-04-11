from __future__ import annotations

from pathlib import Path

from toolkit.cli import cmd_inspect, cmd_status, common


def test_cmd_status_read_json_returns_none_on_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "broken.json"
    path.write_text("{ invalid", encoding="utf-8")

    assert cmd_status._read_json(path) is None


def test_cmd_inspect_read_json_returns_none_on_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "missing.json"

    assert cmd_inspect._read_json(path) is None


def test_common_read_json_returns_payload_on_valid_json(tmp_path: Path) -> None:
    path = tmp_path / "ok.json"
    path.write_text('{"k": 1}', encoding="utf-8")

    assert common._read_json(path) == {"k": 1}
