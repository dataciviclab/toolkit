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

from toolkit.core.io import read_yaml
from toolkit.core.csv_read import (
    ALLOWED_READ_CSV_KEYS,
    READ_SELECTION_KEYS,
    READ_SOURCE_MODES,
    filter_suggested_format_keys,
    merge_read_cfg,
)
from toolkit.core.paths import RAW_PROFILE_DIR, RAW_SUGGESTED_READ


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
    suggested_path = raw_year_dir / RAW_PROFILE_DIR / RAW_SUGGESTED_READ
    if not suggested_path.exists():
        return None

    payload = read_yaml(suggested_path)
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


def apply_year_overrides(read_cfg: dict[str, Any], year: int) -> dict[str, Any]:
    """Return ``read_cfg`` with per-year overrides merged in for the given ``year``.

    Extracts ``overrides`` from the config (if present), looks up the year,
    and merges matching keys into the result.  The ``overrides`` key itself
    is removed — it is not a DuckDB ``read_csv`` parameter.

    Supported override keys:
    - Any ``ALLOWED_READ_CSV_KEYS`` param (``skip``, ``encoding``, etc.)
      → merged directly
    - ``columns_extra``   → merged into base ``columns`` (replaces/adds keys)
    - ``columns_prepend`` → merged into base ``columns``, placed FIRST

    This is the single shared resolution used by both the **clean runtime**
    (via ``resolve_clean_read_cfg``) and the **RAW profiling** (via
    ``profile_raw``) to guarantee profile and runtime never diverge.
    """
    if not read_cfg:
        return {}
    result = dict(read_cfg)
    raw_overrides = result.pop("overrides", None) or {}
    year_override = raw_overrides.get(year) or raw_overrides.get(str(year))
    if not year_override:
        return result

    year_cfg = dict(year_override)

    # Handle columns_extra / columns_prepend: merge into base columns
    extra = year_cfg.pop("columns_extra", None)
    prepend = year_cfg.pop("columns_prepend", None)
    if extra is not None or prepend is not None:
        base_columns = result.get("columns")
        if isinstance(base_columns, dict):
            merged = dict(base_columns)
            if isinstance(prepend, dict):
                merged = {**prepend, **merged}
            if isinstance(extra, dict):
                merged.update(extra)
            result["columns"] = merged

    # Remaining keys: filter to valid DuckDB params and merge
    filtered = {k: v for k, v in year_cfg.items() if k in ALLOWED_READ_CSV_KEYS}
    result.update(filtered)
    return result


def resolve_clean_read_cfg(
    raw_year_dir: Path,
    clean_cfg: dict[str, Any],
    logger=None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    normalized_source, explicit_cfg = _read_source_mode(clean_cfg, logger)

    # Extract per-year overrides BEFORE _split_read_cfg, so they NEVER
    # enter relation_overrides (overrides is NOT a DuckDB read_csv parameter)
    raw_overrides: dict[str | int, dict[str, Any]] = {}
    if "overrides" in explicit_cfg:
        raw_overrides = dict(explicit_cfg.pop("overrides", {}) or {})

    selection_cfg, relation_overrides = _split_read_cfg(explicit_cfg)
    # Belt and suspenders: ensure overrides doesn't leak
    relation_overrides.pop("overrides", None)

    # Validate each year's override dict against CleanReadConfig
    _validate_overrides(raw_overrides, logger)

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

    # Apply per-year override via the shared resolver (same as profile_raw uses)
    year = int(raw_year_dir.name)
    year_override_cfg = apply_year_overrides(
        {"overrides": dict(raw_overrides)} if raw_overrides else {},
        year,
    )
    if year_override_cfg:
        merged_relation_cfg.update(year_override_cfg)
        params_source.append(f"year_override_{year}")

    return selection_cfg, merged_relation_cfg, params_source


def _validate_overrides(
    raw_overrides: dict[str | int, dict[str, Any]],
    logger=None,
) -> None:
    """Validate per-year override configs against ``CleanReadConfig``.

    Raises ``ValueError`` with the offending year key if any override
    contains invalid or unknown fields (catches typos like ``delmi``).
    """
    if not raw_overrides:
        return
    from toolkit.core.config_models.clean import CleanReadConfig

    for year, cfg in raw_overrides.items():
        try:
            CleanReadConfig(**cfg)
        except Exception as exc:
            raise ValueError(f"clean.read.overrides.{year}: invalid config: {exc}") from exc
