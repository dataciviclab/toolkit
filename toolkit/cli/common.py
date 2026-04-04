from __future__ import annotations

import json
from pathlib import Path

from toolkit.core.config import load_config
from toolkit.core.logging import get_logger
from toolkit.core.paths import layer_year_dir


def load_cfg_and_logger(
    config_path: str,
    *,
    verbose: bool = False,
    quiet: bool = False,
    strict_config: bool = False,
):
    cfg = load_config(config_path, strict_config=strict_config)
    if verbose and quiet:
        raise ValueError("verbose and quiet cannot both be true")

    level: str | int = "INFO"
    if verbose:
        level = "DEBUG"
    elif quiet:
        level = "WARNING"

    logger = get_logger(level=level)
    return cfg, logger


def iter_years(cfg, year_arg: int | None = None) -> list[int]:
    if year_arg is None:
        return list(cfg.years)
    if year_arg not in cfg.years:
        raise ValueError(f"Year {year_arg} is not configured in dataset.yml")
    return [year_arg]


def iter_selected_years(
    cfg,
    *,
    year_arg: int | None = None,
    years_arg: str | None = None,
) -> list[int]:
    if year_arg is not None and years_arg is not None:
        raise ValueError("Use either --year or --years, not both")

    if years_arg is None:
        return iter_years(cfg, year_arg)

    requested: list[int] = []
    for raw_part in years_arg.split(","):
        part = raw_part.strip()
        if not part:
            raise ValueError("Invalid --years value: empty year entry")
        try:
            year = int(part)
        except ValueError as exc:
            raise ValueError(f"Invalid --years value: '{part}' is not an integer year") from exc
        if year not in requested:
            requested.append(year)

    if not requested:
        raise ValueError("Invalid --years value: no years provided")

    invalid = [year for year in requested if year not in cfg.years]
    if invalid:
        listed = ", ".join(str(year) for year in invalid)
        raise ValueError(f"Year(s) not configured in dataset.yml: {listed}")

    return requested


def _read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _profile_summary(profile: dict | None, *, max_columns: int = 6) -> dict | None:
    if not isinstance(profile, dict):
        return None

    columns = profile.get("columns") or []
    preview: list[dict[str, str | None]] = []
    for item in columns[:max_columns]:
        if not isinstance(item, dict):
            continue
        preview.append(
            {
                "name": item.get("name"),
                "type": item.get("type"),
            }
        )

    return {
        "row_count": profile.get("row_count"),
        "columns_count": len(columns) if isinstance(columns, list) else 0,
        "columns_preview": preview,
        "columns_truncated": max(
            0, (len(columns) if isinstance(columns, list) else 0) - len(preview)
        ),
    }


def _transition_summary(item: dict | None) -> dict | None:
    if not isinstance(item, dict):
        return None

    type_changes = item.get("type_changes") or []
    return {
        "target_name": item.get("target_name"),
        "source_row_count": item.get("source_row_count"),
        "target_row_count": item.get("target_row_count"),
        "added_columns": list(item.get("added_columns") or []),
        "removed_columns": list(item.get("removed_columns") or []),
        "type_change_count": len(type_changes) if isinstance(type_changes, list) else 0,
    }


def load_layer_profile_summaries(root: Path, dataset: str, year: int) -> dict[str, object] | None:
    clean_metadata = (
        _read_json(layer_year_dir(root, "clean", dataset, year) / "metadata.json") or {}
    )
    mart_metadata = _read_json(layer_year_dir(root, "mart", dataset, year) / "metadata.json") or {}

    clean_output = _profile_summary(clean_metadata.get("output_profile"))
    mart_clean_input = _profile_summary(mart_metadata.get("clean_input_profile"))

    mart_tables: list[dict[str, object]] = []
    for name, profile in (
        (mart_metadata.get("table_profiles") or {}).items()
        if isinstance(mart_metadata.get("table_profiles"), dict)
        else []
    ):
        summary = _profile_summary(profile)
        if summary is None:
            continue
        mart_tables.append({"name": name, **summary})

    transitions: list[dict[str, object]] = []
    raw_transitions = mart_metadata.get("transition_profiles") or []
    if isinstance(raw_transitions, list):
        for item in raw_transitions:
            summary = _transition_summary(item)
            if summary is not None:
                transitions.append(summary)

    has_any = any(
        [clean_output is not None, mart_clean_input is not None, mart_tables, transitions]
    )
    if not has_any:
        return None

    return {
        "clean_output": clean_output,
        "mart_clean_input": mart_clean_input,
        "mart_tables": mart_tables,
        "clean_to_mart": transitions,
    }


def format_profile_preview(summary: dict[str, object] | None) -> str:
    if not isinstance(summary, dict):
        return "rows=? columns=?"

    columns_preview = summary.get("columns_preview") or []
    rendered_columns: list[str] = []
    if isinstance(columns_preview, list):
        for item in columns_preview:
            if not isinstance(item, dict):
                continue
            rendered_columns.append(f"{item.get('name')}:{item.get('type')}")

    suffix = ""
    truncated = summary.get("columns_truncated")
    if isinstance(truncated, int) and truncated > 0:
        suffix = f" (+{truncated} more)"

    return (
        f"rows={summary.get('row_count')} "
        f"columns={summary.get('columns_count')} "
        f"preview={', '.join(rendered_columns) if rendered_columns else '-'}{suffix}"
    )
