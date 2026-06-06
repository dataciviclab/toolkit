"""CLI-to-JSON adapter for the MCP toolkit client.

Provides:
- inspect_paths: resolve all paths for a dataset/year (direct call, no subprocess)
"""

from __future__ import annotations

from typing import Any, cast

from toolkit.cli.inspect._helpers import _payload_for_year
from toolkit.mcp.types import InspectPathsResult
from lab_connectors.mcp.errors import ErrorCode

from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import _safe_path
from toolkit.core.config import load_config


def inspect_paths(config_path: str, year: int | None = None) -> InspectPathsResult:
    """Risolve i path per un dataset/year.

    Se ``year`` è ``None`` e il dataset ha piú anni, usa l'anno più recente
    e lo segnala nel campo ``_year_resolution`` del risultato.
    """
    config = _safe_path(config_path)
    cfg = load_config(str(config), strict_config=False)

    years = list(cfg.years or [])
    _year_resolution: dict[str, Any] | None = None

    if year is None:
        if len(years) > 1:
            year = max(years)
            _year_resolution = {
                "note": f"Anno non specificato per dataset multi-year. Usato {year} come default.",
                "years_available": years,
            }
        elif years:
            year = years[0]

    if year is None:
        raise ToolkitClientError(
            "Nessun anno configurato nel dataset", code=ErrorCode.CONFIG_NOT_FOUND
        )

    result = cast(InspectPathsResult, _payload_for_year(cfg, year))
    if _year_resolution:
        result["_year_resolution"] = _year_resolution  # type: ignore[typeddict-unknown-key]
    return result
