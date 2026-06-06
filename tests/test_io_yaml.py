"""Test per le funzioni YAML in toolkit.core.io.

Copre read_yaml, read_yaml_or_none, write_yaml, yaml_dumps.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from toolkit.core.io import (
    read_yaml,
    read_yaml_or_none,
    write_yaml,
    yaml_dumps,
)

pytestmark = pytest.mark.pure_unit


# ---------------------------------------------------------------------------
# read_yaml
# ---------------------------------------------------------------------------


def test_read_yaml_valid(tmp_path: Path) -> None:
    path = tmp_path / "valid.yml"
    path.write_text("key: value\nnested:\n  inner: 42\n", encoding="utf-8")
    data = read_yaml(path)
    assert data == {"key": "value", "nested": {"inner": 42}}


def test_read_yaml_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "empty.yml"
    path.write_text("", encoding="utf-8")
    assert read_yaml(path) is None


def test_read_yaml_invalid_syntax(tmp_path: Path) -> None:
    path = tmp_path / "broken.yml"
    path.write_text("{ invalid: yaml: :", encoding="utf-8")
    with pytest.raises(ValueError, match="YAML parse error"):
        read_yaml(path)


def test_read_yaml_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "nope.yml"
    with pytest.raises(FileNotFoundError):
        read_yaml(path)


# ---------------------------------------------------------------------------
# read_yaml_or_none
# ---------------------------------------------------------------------------


def test_read_yaml_or_none_valid(tmp_path: Path) -> None:
    path = tmp_path / "ok.yml"
    path.write_text("a: 1\nb: 2\n", encoding="utf-8")
    assert read_yaml_or_none(path) == {"a": 1, "b": 2}


def test_read_yaml_or_none_missing(tmp_path: Path) -> None:
    assert read_yaml_or_none(tmp_path / "missing.yml") is None


def test_read_yaml_or_none_invalid(tmp_path: Path) -> None:
    path = tmp_path / "broken.yml"
    path.write_text(": broken", encoding="utf-8")
    assert read_yaml_or_none(path) is None


# ---------------------------------------------------------------------------
# write_yaml
# ---------------------------------------------------------------------------


def test_write_yaml_roundtrip(tmp_path: Path) -> None:
    data = {"key": "value", "list": [1, 2, 3]}
    path = tmp_path / "out.yml"
    write_yaml(data, path)
    assert path.exists()
    reloaded = read_yaml(path)
    assert reloaded == data


def test_write_yaml_creates_parent_dir(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "nested" / "out.yml"
    write_yaml({"a": 1}, path)
    assert path.exists()
    assert read_yaml(path) == {"a": 1}


# ---------------------------------------------------------------------------
# yaml_dumps
# ---------------------------------------------------------------------------


def test_yaml_dumps_roundtrip() -> None:
    data = {"name": "test", "items": [10, 20]}
    dumped = yaml_dumps(data)
    assert isinstance(dumped, str)
    assert "name: test" in dumped
    assert "items:" in dumped
    # Il risultato deve essere riparsabile
    import yaml

    reloaded = yaml.safe_load(dumped)
    assert reloaded == data


def test_yaml_dumps_empty() -> None:
    assert isinstance(yaml_dumps({}), str)


def test_yaml_dumps_block_style() -> None:
    """Verifica che safe_dump usi block style (non flow style)."""
    data = {"a": {"b": "c"}}
    dumped = yaml_dumps(data)
    # Block style scrive su multiple righe, flow style su una riga
    assert "\n" in dumped.strip()
