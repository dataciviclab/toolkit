from __future__ import annotations

import json
import logging
from unittest.mock import patch

import pytest

from toolkit.cli.common import echo_json, load_cfg_and_logger

pytestmark = pytest.mark.pure_unit


def test_echo_json_default(capsys):
    echo_json({"a": 1})
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed == {"a": 1}
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


@patch("toolkit.cli.common.load_config")
def test_load_cfg_verbose_and_quiet_raises(mock_load):
    with pytest.raises(ValueError, match="verbose and quiet cannot both be true"):
        load_cfg_and_logger(verbose=True, quiet=True)


@patch("toolkit.cli.common.load_config")
def test_load_cfg_verbose_sets_debug(mock_load):
    mock_load.return_value = type("Cfg", (), {"root": "/tmp"})()
    _, logger = load_cfg_and_logger(verbose=True)
    assert logger.level == logging.DEBUG


@patch("toolkit.cli.common.load_config")
def test_load_cfg_quiet_sets_warning(mock_load):
    mock_load.return_value = type("Cfg", (), {"root": "/tmp"})()
    _, logger = load_cfg_and_logger(quiet=True)
    assert logger.level == logging.WARNING
