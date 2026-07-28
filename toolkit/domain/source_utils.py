"""Source resolution utilities.

Estratto da ``toolkit.cli.cmd_run`` per eliminare il ciclo di dipendenza
tra ``cmd_run`` e ``preflight_ops``.
"""

from __future__ import annotations

from typing import Any


def resolve_source(src, year: int) -> dict[str, Any]:
    """Normalizza una fonte raw.sources in dict con stype, name, args, url.

    Condiviso tra il probe (cmd_run) e preflight check per evitare duplicazione
    del parsing di source config (dict vs oggetto).
    """
    if isinstance(src, dict):
        stype = str(src.get("type", "http_file"))
        args: Any = src.get("args", {})
        name = str(src.get("name", stype))
    else:
        stype = str(getattr(src, "type", "http_file") or "http_file")
        args = getattr(src, "args", None) or {}
        name = str(getattr(src, "name", None) or stype)

    raw_url = (args.get("url") if isinstance(args, dict) else getattr(args, "url", "")) or ""
    return {
        "stype": stype,
        "args": args,
        "name": name,
        "url": str(raw_url).replace("{year}", str(year)),
    }
