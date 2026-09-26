from __future__ import annotations

import json

import pytest

from toolkit.cli.common import echo_json

pytestmark = pytest.mark.pure_unit


def test_echo_json_default(capsys):
    echo_json({"a": 1})
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed == {"a": 1}
    # indent=2 → multiline
    assert "\n" in out


def test_echo_json_compact(capsys):
    echo_json({"a": 1}, compact=True)
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed == {"a": 1}


def test_echo_json_nested(capsys):
    echo_json({"d": {"e": [1, 2]}})
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed == {"d": {"e": [1, 2]}}
