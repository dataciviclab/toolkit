"""Clean read configuration resolution.

Responsible for resolving clean.read contract from:
- explicit clean.read config
- suggested_read.yml hints from raw profiling
- merging and normalising into a unified read config

Does NOT handle runtime read execution (see duckdb_read.py).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from toolkit.core.csv_read import (
    READ_SELECTION_KEYS,
    READ_SOURCE_MODES,
    filter_suggested_format_keys,
    merge_read_cfg,
)


def _read_source_mode(clean_cfg: dict[str, Any], logger=None) -> tuple[str, dict[str, Any]]:
    raw_read_cfg = clean_cfg.get("read")
    read_source = clean_cfg.get("read_source")
    explicit_cfg: dict[str, Any] = {}

    if raw_read_cfg is None:
        pass
    elif isinstance(raw_read_cfg, dict):
        explicit_cfg = dict(raw_read_cfg)
        nested_source = explicit_cfg.pop("source", None)
        if nested_source is not None:
            read_source = nested_source
    else:
        raise ValueError("clean.read must be a mapping (dict)")

    normalized_source = str(read_source or "auto")
    if normalized_source not in READ_SOURCE_MODES:
        raise ValueError("clean.read source must be one of: auto, config_only")

    return normalized_source, explicit_cfg


def _split_read_cfg(explicit_cfg: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    selection_cfg = dict(explicit_cfg)
    relation_overrides = {
        key: value for key, value in explicit_cfg.items() if key not in READ_SELECTION_KEYS
    }
    return selection_cfg, relation_overrides


def load_suggested_read(raw_year_dir: Path) -> dict[str, Any] | None:
    suggested_path = raw_year_dir / "_profile" / "suggested_read.yml"
    if not suggested_path.exists():
        return None

    payload = yaml.safe_load(suggested_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None

    clean_cfg = payload.get("clean")
    if not isinstance(clean_cfg, dict):
        return None

    read_cfg = clean_cfg.get("read")
    if not isinstance(read_cfg, dict):
        return None

    return dict(read_cfg)


def filter_suggested_read(cfg: dict[str, Any] | None) -> dict[str, Any]:
    return filter_suggested_format_keys(cfg)


def resolve_clean_read_cfg(
    raw_year_dir: Path,
    clean_cfg: dict[str, Any],
    logger=None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    normalized_source, explicit_cfg = _read_source_mode(clean_cfg, logger)
    selection_cfg, relation_overrides = _split_read_cfg(explicit_cfg)

    suggested_cfg = load_suggested_read(raw_year_dir)
    filtered_suggested = filter_suggested_read(suggested_cfg)
    if normalized_source == "auto" and filtered_suggested and logger is not None:
        logger.info(
            "CLEAN read hints loaded from suggested_read.yml: %s",
            json.dumps(filtered_suggested, ensure_ascii=False, sort_keys=True),
        )

    merged_relation_cfg, params_source = merge_read_cfg(
        source=normalized_source,
        suggested=suggested_cfg,
        overrides=relation_overrides,
    )

    return selection_cfg, merged_relation_cfg, params_source
