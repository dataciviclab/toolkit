from __future__ import annotations

from pathlib import Path

import pytest

from toolkit.cli import cmd_inspect, common

pytestmark = pytest.mark.pure_unit


def test_cmd_status_read_json_returns_none_on_invalid_json(tmp_path: Path) -> None:
    from toolkit.core.io import read_json_or_none

    path = tmp_path / "broken.json"
    path.write_text("{ invalid", encoding="utf-8")

    assert read_json_or_none(path) is None


def test_cmd_inspect_read_json_returns_none_on_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "missing.json"

    assert cmd_inspect._read_json(path) is None


def test_common_read_json_returns_payload_on_valid_json(tmp_path: Path) -> None:
    path = tmp_path / "ok.json"
    path.write_text('{"k": 1}', encoding="utf-8")

    assert common._read_json(path) == {"k": 1}
