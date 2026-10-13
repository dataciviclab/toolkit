"""Backward-compat shim — il codice è ora in ``lab_connectors.discovery.links``.

Questo modulo re-exporta tutto da LC per mantenere compatibilità con i
consumer esistenti (SO, test, MCP). Il codice sorgente vive in LC.
"""

from __future__ import annotations

from lab_connectors.discovery.links import (  # noqa: F401
    DATA_EXTENSIONS,
    DataLink,
    LinkGroup,
    extract_candidate_links,
    extract_data_links,
    extract_filename,
    extract_prefix,
    extract_years,
    group_links,
    resolve_format,
)

# Alias backward-compat per i vecchi nomi privati
_extract_prefix = extract_prefix
_extract_years = extract_years
_resolve_format = resolve_format
_extract_filename = extract_filename
