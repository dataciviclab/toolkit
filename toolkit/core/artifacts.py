from __future__ import annotations

from typing import Any


def profile_required(cfg: Any) -> bool:
    """True quando il read source è ``"auto"`` e serve profiling raw."""
    clean_cfg = getattr(cfg, "clean", None) if not isinstance(cfg, dict) else cfg.get("clean")
    clean_cfg = clean_cfg or {}
    read_cfg = clean_cfg.get("read")

    if isinstance(read_cfg, dict):
        source = read_cfg.get("source", "auto")
    elif isinstance(read_cfg, str):
        source = read_cfg
    else:
        source = clean_cfg.get("read_source", "auto")

    return str(source or "auto").strip().lower() == "auto"


def should_write(layer: str, artifact_name: str, cfg: Any) -> bool:
    """Decide whether a given artifact should be produced.

    Most artifacts are always written. The only exception is
    ``suggested_read``, which is skipped when the read source
    is explicitly configured (non-``"auto"``).
    """
    if layer == "profile" and artifact_name == "suggested_read":
        return profile_required(cfg)
    return True
