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

    **Self-validating**: raw YAML values (``columns`` as list, booleans as
    ``"false"`` strings) are normalized through ``CleanReadConfig.model_validate``
    before merging.  Works identically whether called from the clean runtime
    (``resolve_clean_read_cfg``) or the RAW profiler (``run_raw``) —
    guarantees no divergence between profile and runtime.

    The ``overrides`` key is removed from the result — it is not a DuckDB
    ``read_csv`` parameter.
    """
    if not read_cfg:
        return {}

    from toolkit.core.config_models.clean import CleanReadConfig

    # Validate and normalize base config through CleanReadConfig.
    # This converts raw YAML: columns=list→dict, "false"→False, etc.
    validated = CleanReadConfig.model_validate(read_cfg).model_dump(exclude_unset=True)

    # Extract overrides (already validated as part of CleanReadConfig)
    raw_overrides = validated.pop("overrides", None) or {}
    year_override = raw_overrides.get(year) or raw_overrides.get(str(year))
    if not year_override:
        return validated

    # Merge override values — reject selection keys and unknown params
    invalid_selection = [k for k in year_override if k in READ_SELECTION_KEYS]
    if invalid_selection:
        raise ValueError(
            f"clean.read.overrides.{year}: selection keys not allowed in overrides: "
            f"{invalid_selection}. Use source-level configuration for per-year "
            f"file selection."
        )
    known = ALLOWED_READ_CSV_KEYS - READ_SELECTION_KEYS
    unknown = [k for k in year_override if k not in known]
    if unknown:
        raise ValueError(
            f"clean.read.overrides.{year}: unknown parameter(s): {unknown}. "
            f"Allowed: {sorted(known)}"
        )
    filtered = {k: v for k, v in year_override.items() if k in known}
    validated.update(filtered)

    # Re-validate merged result to normalize override values too
    validated = CleanReadConfig.model_validate(validated).model_dump(exclude_unset=True)
    return validated


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

    # Validate STRUCTURE of ALL override entries (not just the executed year).
    # Checks that every year's keys are known DuckDB params (parsing only,
    # no selection keys).  Dependency validation (e.g. align_by_header needs
    # normalize_rows_to_columns) happens later at merge time per-year.
    known_override_keys = ALLOWED_READ_CSV_KEYS - READ_SELECTION_KEYS
    for override_key, year_cfg in raw_overrides.items():
        if not isinstance(year_cfg, dict):
            raise ValueError(
                f"clean.read.overrides.{override_key}: must be a mapping (dict), "
                f"got {type(year_cfg).__name__}"
            )
        unknown = [k for k in year_cfg if k not in known_override_keys]
        if unknown:
            raise ValueError(
                f"clean.read.overrides.{override_key}: unknown parameter(s): "
                f"{unknown}. Allowed: {sorted(known_override_keys)}"
            )

    selection_cfg, relation_overrides = _split_read_cfg(explicit_cfg)
    # Belt and suspenders: ensure overrides doesn't leak
    relation_overrides.pop("overrides", None)

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

    # Apply per-year override with full base context (apply_year_overrides
    # needs the entire config to validate inter-dependent fields like
    # align_by_header + normalize_rows_to_columns)
    year = int(raw_year_dir.name)
    if raw_overrides and (year in raw_overrides or str(year) in raw_overrides):
        merged_relation_cfg = apply_year_overrides(
            {**merged_relation_cfg, "overrides": dict(raw_overrides)},
            year,
        )
        params_source.append(f"year_override_{year}")

    # Single normalisation pass after all merges — catches raw YAML values
    # (columns list, string booleans) from BOTH base and override in one shot.
    from toolkit.core.config_models.clean import CleanReadConfig

    merged_relation_cfg = CleanReadConfig.model_validate(merged_relation_cfg).model_dump(
        exclude_unset=True
    )

    return selection_cfg, merged_relation_cfg, params_source
