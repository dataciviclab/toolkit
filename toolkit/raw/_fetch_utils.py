"""Helper functions for RAW layer fetch orchestration.

Extracted from raw/run.py to reduce module size and clarify responsibilities.
All functions are private (underscore-prefixed) — not part of the public API.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from toolkit.core.registry import registry


# ---------------------------------------------------------------------------
# Argument formatting
# ---------------------------------------------------------------------------


def _format_args(args: dict, year: int) -> dict:
    formatted = {}
    for k, v in (args or {}).items():
        if isinstance(v, str) and "{year}" in v:
            # replace instead of str.format to avoid conflicts with SPARQL {} braces
            formatted[k] = v.replace("{year}", str(year))
        else:
            formatted[k] = v
    # Handle url_suffix_by_year: append per-year suffix to the formatted URL
    if "url" in formatted and "url_suffix_by_year" in (args or {}):
        suffix_map = args["url_suffix_by_year"]
        if isinstance(suffix_map, dict):
            suffix = suffix_map.get(year, "")
            if isinstance(suffix, str):
                formatted["url"] = formatted["url"] + suffix
    # Remove url_suffix_by_year from output — internal config, not for consumers
    formatted.pop("url_suffix_by_year", None)
    return formatted


# ---------------------------------------------------------------------------
# File extension inference
# ---------------------------------------------------------------------------


def _infer_ext(stype: str, formatted_args: dict, origin: str | None = None) -> str:
    if stype in {"sdmx", "sparql"}:
        return ".csv"

    if stype in {"http_file", "http_post_file", "ckan"}:
        url = origin or formatted_args.get("url", "")
        return _infer_from_url(url)

    if stype == "local_file":
        path_str = str(formatted_args.get("path", ""))
        return _infer_from_url(path_str)

    return ".bin"


def _infer_from_url(url: str) -> str:
    """Infer file extension from a URL or file path string."""
    parsed = urlparse(url)
    path = parsed.path or ""
    low_path = path.lower()

    # Some providers expose files behind php endpoints.
    # Prefer the meaningful extension and never keep ".php".
    if low_path.endswith(".csv.php"):
        return ".csv"
    if low_path.endswith(".zip.php"):
        return ".zip"

    suffix = Path(path).suffix.lower()
    if suffix and suffix != ".php":
        return suffix

    # fallback heuristics on full URL/query
    low = url.lower()
    if ".csv" in low or "csv" in low:
        return ".csv"
    if ".zip" in low or "zip" in low:
        return ".zip"

    return ".bin"


# ---------------------------------------------------------------------------
# Source fetching
# ---------------------------------------------------------------------------

_FETCH_DISPATCH: dict[str, Callable[..., tuple[bytes, str]]] = {}


def _register_fetch(stype: str):
    """Decorator to register a fetch handler for a source type."""

    def _wrap(fn):
        _FETCH_DISPATCH[stype] = fn
        return fn

    return _wrap


@_register_fetch("ckan")
def _fetch_ckan(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    src = registry.create(stype, **(client or {}))
    sample_bytes = formatted_args.get("sample_bytes")
    return src.fetch(
        formatted_args["portal_url"],
        str(formatted_args["resource_id"])
        if formatted_args.get("resource_id") is not None
        else None,
        str(formatted_args["dataset_id"]) if formatted_args.get("dataset_id") is not None else None,
        str(formatted_args["resource_name"])
        if formatted_args.get("resource_name") is not None
        else None,
        sample_bytes=sample_bytes,
    )


@_register_fetch("sdmx")
def _fetch_sdmx(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    src = registry.create(stype, **(client or {}))
    return src.fetch(
        str(formatted_args.get("agency") or "IT1"),
        str(formatted_args["flow"]),
        str(formatted_args["version"]),
        formatted_args.get("filters"),
    )


@_register_fetch("sparql")
def _fetch_sparql(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    src = registry.create(stype, **(client or {}))
    pages = int(formatted_args.get("pages", 1))
    step = int(formatted_args.get("step", 10000))
    return src.fetch(
        str(formatted_args["endpoint"]),
        str(formatted_args["query"]),
        str(formatted_args.get("accept_format", "csv")),
        pages=pages,
        step=step,
    )


@_register_fetch("http_file")
def _fetch_http_file(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    src = registry.create(stype, **(client or {}))
    sample_bytes = formatted_args.get("sample_bytes")
    payload = src.fetch(formatted_args["url"], sample_bytes=sample_bytes)
    return payload, formatted_args["url"]


@_register_fetch("http_post_file")
def _fetch_http_post_file(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    src = registry.create(stype, **(client or {}))
    post_data = formatted_args.get("post_data")
    sample_bytes = formatted_args.get("sample_bytes")
    payload = src.fetch(formatted_args["url"], data=post_data, sample_bytes=sample_bytes)
    return payload, formatted_args["url"]


@_register_fetch("local_file")
def _fetch_local_file(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    src = registry.create(stype, **(client or {}))
    payload = src.fetch(formatted_args["path"])
    return payload, formatted_args["path"]


def _fetch_fallback(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    src = registry.create(stype, **(client or {}))
    first_val = next(iter(formatted_args.values()))
    payload = src.fetch(first_val)
    return payload, str(first_val)


def _fetch_payload(stype: str, client: dict, formatted_args: dict) -> tuple[bytes, str]:
    """Dispatch fetch to the appropriate source-type handler."""
    handler = _FETCH_DISPATCH.get(stype, _fetch_fallback)
    return handler(stype, client, formatted_args)


# ---------------------------------------------------------------------------
# Output path helpers
# ---------------------------------------------------------------------------


def _next_available_path(out_dir: Path, fname: str) -> Path:
    candidate = out_dir / fname
    if not candidate.exists():
        return candidate

    stem = Path(fname).stem
    suffix = Path(fname).suffix
    i = 1
    while True:
        candidate = out_dir / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def _generate_run_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"


def _resolve_output_path(out_dir: Path, fname: str, policy: str) -> Path:
    candidate = out_dir / fname
    if policy == "overwrite":
        return candidate
    if policy == "versioned":
        return _next_available_path(out_dir, fname)
    raise ValueError("raw.output_policy must be one of: overwrite, versioned")


# ---------------------------------------------------------------------------
# Primary output selection
# ---------------------------------------------------------------------------


def _choose_primary_output(source_outputs: list[dict], logger) -> str:
    available = [entry for entry in source_outputs if entry.get("output_file")]
    if not available:
        raise RuntimeError(
            "RAW manifest cannot determine primary output file because no outputs were written."
        )

    primary_marked = [entry for entry in available if entry.get("primary")]
    if primary_marked:
        if len(primary_marked) > 1:
            logger.warning(
                "RAW manifest found multiple sources with primary: true; using the first one."
            )
        return str(primary_marked[0]["output_file"])

    if len(available) == 1:
        return str(available[0]["output_file"])

    logger.warning(
        "RAW manifest primary_output_file defaulting to the first source. "
        "Set raw.sources[].primary: true to choose explicitly."
    )
    return str(available[0]["output_file"])
