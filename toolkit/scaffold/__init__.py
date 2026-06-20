from __future__ import annotations

import warnings
from typing import Any

from toolkit.scaffold.clean import generate_clean_sql
from toolkit.scaffold.full import (
    generate_full_scaffold,
    suggest_mart_sql,
    suggest_validation,
)
from toolkit.scaffold.sources import infer_ext, infer_filename, slugify


def suggest_clean_sql(columns: list[dict[str, Any]] | list[str], profile: dict[str, Any]) -> str:
    """Deprecato — usa generate_clean_sql() direttamente.

    Wrapper deprecato che costruisce un profilo sintetico e chiama
    generate_clean_sql(profile, dataset="candidate", year=2024).
    """
    warnings.warn(
        "suggest_clean_sql is deprecated — use generate_clean_sql(profile, dataset, year) directly",
        DeprecationWarning,
        stacklevel=2,
    )
    if columns and isinstance(columns[0], dict):
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(columns)]
    else:
        col_names = list(columns) if columns else []
    if not col_names:
        return "-- ATTENZIONE: profiling non ha rilevato colonne.\nSELECT 1 AS placeholder FROM raw_input\n"

    synthetic_profile: dict[str, Any] = dict(profile)
    existing_mapping = synthetic_profile.get("mapping_suggestions") or {}
    synthetic_profile["mapping_suggestions"] = dict(existing_mapping)

    for name in col_names:
        if name in existing_mapping:
            continue
        synthetic_profile["mapping_suggestions"][name] = {"type": "string"}

    return generate_clean_sql(synthetic_profile, "candidate", 2024)


__all__ = [
    "generate_clean_sql",
    "generate_full_scaffold",
    "infer_ext",
    "infer_filename",
    "slugify",
    "suggest_clean_sql",
    "suggest_mart_sql",
    "suggest_validation",
]
