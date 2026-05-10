"""CLI-to-JSON adapter for the MCP toolkit client.

Provides:
- inspect_paths: resolve all paths for a dataset/year (direct call, no subprocess)
"""

from __future__ import annotations

from typing import cast

from toolkit.cli.inspect._helpers import _payload_for_year
from toolkit.mcp.contracts import InspectPathsResult
from lab_connectors.mcp.errors import McpError, ErrorCode

from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import _safe_path
from toolkit.core.config import load_config


def inspect_paths(config_path: str, year: int | None = None) -> InspectPathsResult:
    """Risolve i path per un dataset/year.

    Richiede ``year`` per dataset multi-year. Se ``year`` è ``None`` e il dataset
    ha piú anni, alza ``ToolkitClientError`` con messaggio chiaro.
    """
    config = _safe_path(config_path)
    cfg = load_config(str(config), strict_config=False)

    if year is None:
        years = list(cfg.years or [])
        if len(years) > 1:
            raise ToolkitClientError(
                "year è obbligatorio per dataset multi-year. "
                f"Trovati {len(years)} anni: {years}. Usa --year per specificarne uno.",
                code=ErrorCode.INVALID_PARAMS,
            )
        year = years[0] if years else None

    if year is None:
        raise ToolkitClientError("Nessun anno configurato nel dataset", code=ErrorCode.CONFIG_NOT_FOUND)

    return cast(InspectPathsResult, _payload_for_year(cfg, year))
