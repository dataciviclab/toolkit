"""Policy engine for dataset.yml contract validation.

Handles:
- Unknown key detection and rejection
- Deprecation notices (strict vs non-strict mode)
- Legacy payload normalization
- Top-level allowed keys whitelist
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from toolkit.core.config_models.path_normalization import _err
from toolkit.core.config_models.shared_models import _CONFIG_DEPRECATIONS


logger = logging.getLogger("toolkit.core.config")

_TOP_LEVEL_ALLOWED_KEYS = {
    "schema_version",
    "root",
    "dataset",
    "raw",
    "clean",
    "mart",
    "support",
    "cross_year",
    "config",
    "validation",
    "output",
}


def _declared_model_keys(model_cls: type[BaseModel]) -> set[str]:
    keys: set[str] = set()
    for field_name, field_info in model_cls.model_fields.items():
        keys.add(field_name)
        if field_info.alias:
            keys.add(str(field_info.alias))
    return keys


def _emit_deprecation_notice(
    key: str,
    *,
    strict_config: bool,
    path: Path,
) -> None:
    notice = _CONFIG_DEPRECATIONS[key]
    message = f"{notice.code} {notice.message}"
    logger.warning(message)
    if strict_config:
        raise _err(f"{notice.code} {notice.message}", path=path)


def _emit_unknown_keys_notice(
    key: str,
    extras: list[str],
    *,
    strict_config: bool,
    path: Path,
) -> None:
    notice = _CONFIG_DEPRECATIONS[key]
    formatted = ", ".join(sorted(extras))
    message = f"{notice.code} {notice.message}: {formatted}"
    logger.warning(message)
    if strict_config:
        raise _err(message, path=path)


def _normalize_legacy_payload(
    data: dict[str, Any],
    *,
    path: Path,
    strict_config: bool,
) -> dict[str, Any]:
    normalized = dict(data)

    raw = normalized.get("raw")
    if isinstance(raw, dict):
        normalized["raw"] = dict(raw)

    clean = normalized.get("clean")
    if isinstance(clean, dict):
        normalized["clean"] = dict(clean)

    mart = normalized.get("mart")
    if isinstance(mart, dict):
        normalized["mart"] = dict(mart)

    return normalized


def _warn_or_reject_unknown_keys(
    data: dict[str, Any],
    *,
    path: Path,
    strict_config: bool,
) -> dict[str, Any]:
    normalized = dict(data)

    top_level_extras = [key for key in normalized.keys() if key not in _TOP_LEVEL_ALLOWED_KEYS]
    if "bq" in top_level_extras:
        raise _err("bq is no longer supported; remove field", path=path)
    if top_level_extras:
        _emit_unknown_keys_notice(
            "unknown.top_level",
            top_level_extras,
            strict_config=strict_config,
            path=path,
        )
        if not strict_config:
            normalized = {k: v for k, v in normalized.items() if k in _TOP_LEVEL_ALLOWED_KEYS}

    from toolkit.core.config_models.raw import RawConfig
    from toolkit.core.config_models.clean import CleanConfig
    from toolkit.core.config_models.mart import MartConfig
    from toolkit.core.config_models.cross_year import CrossYearConfig

    for section_name, allowed_keys, notice_key in (
        ("raw", _declared_model_keys(RawConfig), "unknown.raw"),
        ("clean", _declared_model_keys(CleanConfig), "unknown.clean"),
        ("mart", _declared_model_keys(MartConfig), "unknown.mart"),
        ("cross_year", _declared_model_keys(CrossYearConfig), "unknown.cross_year"),
    ):
        section = normalized.get(section_name)
        if not isinstance(section, dict):
            continue
        extras = [k for k in section if k not in allowed_keys]
        # Unconditional rejections for legacy forms that are no longer supported.
        if section_name == "raw" and "source" in extras:
            raise _err("raw.source is no longer supported; use raw.sources", path=path)
        if section_name == "clean" and "sql_path" in extras:
            raise _err("clean.sql_path is no longer supported; use clean.sql", path=path)
        if section_name == "mart" and "sql_dir" in extras:
            raise _err("mart.sql_dir is no longer supported; use mart.tables[].sql", path=path)
        if extras:
            _emit_unknown_keys_notice(
                notice_key,
                extras,
                strict_config=strict_config,
                path=path,
            )

    return normalized
