"""Backward-compat shim — il codice è ora in ``lab_connectors.discovery.infer``.

Questo modulo re-exporta tutto da LC per mantenere compatibilità con i
consumer esistenti (SO, test, MCP). Il codice sorgente vive in LC.
"""

from __future__ import annotations

from lab_connectors.discovery.infer import (  # noqa: F401
    YEAR_COLUMN_HINTS,
    _expand_column_years,
    extract_years_from_columns,
    extract_years_from_filename,
    infer_granularity,
    infer_granularity_from_name_and_columns,
    infer_topics,
    infer_years,
    infer_years_from_columns,
    suggest_years,
)
