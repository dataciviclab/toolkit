"""Schema diff — confronto segnali RAW tra anni.

Implementazione condivisa: sia CLI che MCP la chiamano.
MCP wrappa le eccezioni in ToolkitClientError.
"""

from __future__ import annotations

from typing import Any

from toolkit.core.config import load_config
from toolkit.domain.common import iter_years
from toolkit.domain.inspect_utils import _compare_schema_entries, _raw_schema_payload


def schema_diff_payload(config_path: str) -> dict[str, Any]:
    """Confronta i segnali di schema RAW tra gli anni configurati.

    Args:
        config_path: path al dataset.yml.

    Returns:
        Dict con entries per anno e comparazioni pairwise.
    """
    cfg = load_config(config_path)
    years = iter_years(cfg, None)
    entries = [_raw_schema_payload(cfg, selected_year) for selected_year in years]
    comparisons = _compare_schema_entries(entries)
    return {
        "dataset": cfg.dataset,
        "config_path": str(cfg.base_dir / "dataset.yml"),
        "years": [entry["year"] for entry in entries],
        "entries": entries,
        "comparisons": comparisons,
    }
