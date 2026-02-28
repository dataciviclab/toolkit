from __future__ import annotations

from typing import Any


ARTIFACT_POLICY_MINIMAL = "minimal"
ARTIFACT_POLICY_STANDARD = "standard"
ARTIFACT_POLICY_DEBUG = "debug"
ARTIFACT_POLICIES = {
    ARTIFACT_POLICY_MINIMAL,
    ARTIFACT_POLICY_STANDARD,
    ARTIFACT_POLICY_DEBUG,
}


def resolve_artifact_policy(output_cfg: dict[str, Any] | None) -> str:
    policy = str((output_cfg or {}).get("artifacts", ARTIFACT_POLICY_STANDARD)).strip().lower()
    if policy not in ARTIFACT_POLICIES:
        allowed = ", ".join(sorted(ARTIFACT_POLICIES))
        raise ValueError(f"output.artifacts must be one of: {allowed}")
    return policy


def legacy_aliases_enabled(output_cfg: dict[str, Any] | None) -> bool:
    return bool((output_cfg or {}).get("legacy_aliases", True))


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
    if policy == ARTIFACT_POLICY_DEBUG:
        return True

    output_cfg = getattr(cfg, "output", None) if not isinstance(cfg, dict) else cfg.get("output")

    if layer == "profile":
        if artifact_name == "suggested_read":
            return profile_required(cfg)
        if artifact_name == "raw_profile":
            return policy != ARTIFACT_POLICY_MINIMAL
        if artifact_name == "profile_alias":
            return policy != ARTIFACT_POLICY_MINIMAL and legacy_aliases_enabled(output_cfg)
        if artifact_name in {"profile_md", "suggested_mapping"}:
            return False

    if layer in {"clean", "mart"} and artifact_name == "rendered_sql":
        return policy != ARTIFACT_POLICY_MINIMAL

    return True
