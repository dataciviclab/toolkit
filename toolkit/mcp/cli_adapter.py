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

from toolkit.mcp.contracts import InspectPathsResult
from toolkit.mcp.errors import ToolkitClientError
from toolkit.mcp.path_safety import TOOLKIT_PYTHON, TOOLKIT_ROOT, _safe_path


def _toolkit_json(args: list[str]) -> Any:
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
            timeout=60,
        )
    except subprocess.TimeoutExpired as exc:
        raise ToolkitClientError(f"toolkit CLI timeout (>60s): {exc}") from exc
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


def inspect_paths(config_path: str, year: int | None = None) -> InspectPathsResult:
    """Risolve i path per un dataset/year.

    Richiede ``year`` per dataset multi-year. Se ``year`` è ``None`` e il dataset
    ha piú anni, alza ``ToolkitClientError`` invece di restituire una lista
    che farebbe crashare i consumer.
    """
    config = _safe_path(config_path)
    args = ["inspect", "paths", "--config", str(config), "--json"]
    if year is not None:
        args.extend(["--year", str(year)])
    result = _toolkit_json(args)
    if isinstance(result, list):
        raise ToolkitClientError(
            "year è obbligatorio per dataset multi-year. "
            f"Trovati {len(result)} anni. Usa --year per specificarne uno."
        )
    # Validazione contratto: verifica che le chiavi principali siano presenti.
    for key in ("dataset", "year", "paths"):
        if key not in result:
            raise ToolkitClientError(
                f"inspect paths: chiave '{key}' mancante nel risultato. "
                "Il contratto CLI/MCP potrebbe essere disallineato."
            )
    return result
