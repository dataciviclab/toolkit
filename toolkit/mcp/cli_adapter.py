"""CLI-to-JSON adapter for the MCP toolkit client.

Provides:
- _toolkit_json: invoke toolkit CLI and parse JSON output
- inspect_paths: resolve all paths for a dataset/year via CLI
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any

from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import TOOLKIT_PYTHON, TOOLKIT_ROOT, _safe_path


def _toolkit_json(args: list[str]) -> dict[str, Any]:
    cmd = [str(TOOLKIT_PYTHON), "-m", "toolkit.cli.app", *args]
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(TOOLKIT_ROOT),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
            check=False,
        )
    except Exception as exc:
        raise ToolkitClientError(f"Esecuzione toolkit CLI fallita: {exc}") from exc

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        if stderr:
            detail = stderr
        elif stdout:
            detail = stdout
        else:
            detail = f"exit code {result.returncode}: {' '.join(cmd)}"
        raise ToolkitClientError(f"toolkit CLI fallita: {detail}")

    try:
        return json.loads(result.stdout)
    except Exception as exc:
        raise ToolkitClientError("toolkit CLI non ha restituito JSON valido") from exc


def inspect_paths(config_path: str, year: int | None = None) -> dict[str, Any]:
    config = _safe_path(config_path)
    args = ["inspect", "paths", "--config", str(config), "--json"]
    if year is not None:
        args.extend(["--year", str(year)])
    return _toolkit_json(args)
