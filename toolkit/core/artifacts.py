from __future__ import annotations

from typing import Any


def profile_required(cfg: Any) -> bool:
    """True quando il read source è ``"auto"`` e serve profiling raw."""
    if not cfg:
        return True  # default: assume profile needed
    if isinstance(cfg, dict):
        clean_cfg = cfg.get("clean") or {}
        read_cfg = clean_cfg.get("read")
        if isinstance(read_cfg, dict):
            source = read_cfg.get("source", "auto")
        elif isinstance(read_cfg, str):
            source = read_cfg
        else:
            source = clean_cfg.get("read_source", "auto")
    else:
        # ToolkitConfig object or duck-typed object with .clean
        clean_attr = cfg.clean
        if isinstance(clean_attr, dict):
            read_cfg = clean_attr.get("read")
            if isinstance(read_cfg, dict):
                source = read_cfg.get("source", "auto")
            elif isinstance(read_cfg, str):
                source = read_cfg
            else:
                source = clean_attr.get("read_source", "auto")
        else:
            read_cfg = clean_attr.read
            source = read_cfg.source if read_cfg else clean_attr.read_source
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
