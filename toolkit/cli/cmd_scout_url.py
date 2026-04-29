"""Backward-compat re-export — cmd_scout_url helpers moved to cmd_url_inspect.

DEPRECATED: import directly from cmd_url_inspect instead.
All symbols re-exported from cmd_url_inspect for backward compat with existing tests.
"""
from __future__ import annotations

import requests  # noqa: F401 — needed for monkeypatch compatibility in tests

from toolkit.cli.cmd_url_inspect import (
    _DEFAULT_TIMEOUT,
    _DEFAULT_USER_AGENT,
    _EXTENDED_EXTENSIONS,
    _MAX_PRINTED_LINKS,
    _candidate_links,
    _detect_ckan,
    _discover_ckan_resources,
    _extract_ckan_dataset_id,
    _generate_yaml_scaffold,
    _is_file_like,
    _is_html,
    probe_url,
)

__all__ = [
    "_EXTENDED_EXTENSIONS",
    "_MAX_PRINTED_LINKS",
    "_DEFAULT_TIMEOUT",
    "_DEFAULT_USER_AGENT",
    "_detect_ckan",
    "_discover_ckan_resources",
    "_extract_ckan_dataset_id",
    "_generate_yaml_scaffold",
    "_is_file_like",
    "_is_html",
    "probe_url",
]
