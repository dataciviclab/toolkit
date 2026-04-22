"""Path safety and config loading for the MCP toolkit client.

Provides:
- _safe_path: resolve and validate a config path
- _load_cfg: load a toolkit config with error translation
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from toolkit.core.config import load_config
from toolkit.mcp.errors import ToolkitClientError


TOOLKIT_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = Path(
    os.environ.get("DATACIVICLAB_WORKSPACE", str(TOOLKIT_ROOT.parent))
).expanduser()
TOOLKIT_PYTHON = Path(os.environ.get("DATACIVICLAB_TOOLKIT_PYTHON", os.sys.executable))


def _safe_path(config_path: str) -> Path:
    path = Path(config_path).expanduser()
    if not path.is_absolute():
        path = (WORKSPACE_ROOT / path).resolve()
    if not path.exists():
        raise ToolkitClientError(f"Config non trovata: {path}")
    return path


def _load_cfg(config_path: str) -> tuple[Path, Any]:
    config = _safe_path(config_path)
    try:
        cfg = load_config(str(config), strict_config=False)
    except Exception as exc:
        raise ToolkitClientError(f"Load config fallito per {config}: {exc}") from exc
    return config, cfg
