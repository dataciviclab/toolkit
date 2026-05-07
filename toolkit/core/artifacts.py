from __future__ import annotations

from typing import Any


def resolve_artifact_policy(output_cfg: dict[str, Any] | None) -> str:
    """Backward-compat placeholder: accepts any value, always returns ``'standard'``."""
    return "standard"


def legacy_aliases_enabled(output_cfg: dict[str, Any] | None) -> bool:
    return bool((output_cfg or {}).get("legacy_aliases", False))


def profile_required(cfg: Any) -> bool:
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


def should_write(
    layer: str,
    artifact_name: str,
    policy: str,
    cfg: Any,
) -> bool:
    """Decide whether a given artifact should be produced.

    The ``policy`` parameter is accepted for backward compatibility but
    is no longer consulted — profiling and rendered-SQL artifacts are
    always written when applicable.
    """
    output_cfg = getattr(cfg, "output", None) if not isinstance(cfg, dict) else cfg.get("output")

    if layer == "profile":
        if artifact_name == "suggested_read":
            return profile_required(cfg)
        if artifact_name == "profile_alias":
            return legacy_aliases_enabled(output_cfg)

    return True
