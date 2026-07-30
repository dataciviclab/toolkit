"""Path safety and config loading for the MCP toolkit client.

Ora è un thin wrapper su ``toolkit.core.discovery`` — la logica di
risoluzione slug/path è centralizzata in ``resolve_config_path()``.

Provides:
- ``_safe_path``: resolve and validate a config path
- ``_load_cfg``: load a toolkit config with error translation
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from lab_connectors.mcp.errors import ErrorCode

from toolkit.core.config import load_config
from toolkit.core.discovery import resolve_config_path
from toolkit.core.paths import WORKSPACE_ROOT as WORKSPACE_ROOT  # noqa: F401 — re-export
from toolkit.mcp.errors import ToolkitClientError


TOOLKIT_PYTHON = Path(os.environ.get("DATACIVICLAB_TOOLKIT_PYTHON", sys.executable))


def _safe_path(config_path: str | Path) -> Path:
    """Risolve e valida un path a dataset.yml per tool MCP.

    Delega la risoluzione a ``resolve_config_path()`` traducendo
    ``FileNotFoundError`` in ``ToolkitClientError``.

    Args:
        config_path: Path o slug da risolvere.

    Returns:
        Path assoluto a dataset.yml.

    Raises:
        ToolkitClientError: CONFIG_NOT_FOUND se irrisolvibile.
    """
    try:
        return resolve_config_path(hint=config_path)
    except FileNotFoundError as exc:
        raise ToolkitClientError(str(exc), code=ErrorCode.CONFIG_NOT_FOUND) from exc


def _load_cfg(config_path: str | Path) -> tuple[Path, object]:
    """Carica config MCP con path safety + error translation."""
    config = _safe_path(str(config_path))
    try:
        cfg = load_config(str(config), strict_config=False)
    except Exception as exc:
        raise ToolkitClientError(
            f"Load config fallito per {config}: {exc}", code=ErrorCode.CONFIG_NOT_FOUND
        ) from exc
    return config, cfg
