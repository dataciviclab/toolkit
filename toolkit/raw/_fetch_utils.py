"""Helper functions for RAW layer fetch orchestration.

Extracted from raw/run.py to reduce module size and clarify responsibilities.
All functions are private (underscore-prefixed) — not part of the public API.

Il dispatch fetch e' nel catalogo unico ``toolkit.core.source_types`` —
questo modulo contiene solo helper di formattazione e naming.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

# Re-export del dispatcher unico (contratto pubblico del runtime raw).
from toolkit.core.source_types import fetch_payload as _fetch_payload  # noqa: F401


# ---------------------------------------------------------------------------
# Argument formatting
# ---------------------------------------------------------------------------


def _format_args(args: dict, year: int) -> dict:
    formatted: dict[str, Any] = {}
    for k, v in (args or {}).items():
        if isinstance(v, Path):
            # _normalize_paths converte i path relativi in PosixPath: il
            # placeholder {year} va sostituito anche su valori Path (es.
            # raw.sources[].args.path per local_file).
            if "{year}" in str(v):
                formatted[k] = Path(str(v).replace("{year}", str(year)))
            else:
                formatted[k] = v
        elif isinstance(v, str) and "{year}" in v:
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
                # url può essere Path (local_file): normalizza a str per il suffix
                base_url = str(formatted["url"])
                formatted["url"] = base_url + suffix
    # Remove url_suffix_by_year from output — internal config, not for consumers
    formatted.pop("url_suffix_by_year", None)
    return formatted


# ---------------------------------------------------------------------------
# File extension inference
# ---------------------------------------------------------------------------


def _infer_ext(stype: str, formatted_args: dict, origin: str | None = None) -> str:
    from toolkit.core.source_types import SOURCE_TYPES

    spec = SOURCE_TYPES.get(stype)
    mode = spec.ext_mode if spec else "fixed"

    if mode == "fixed":
        return spec.fixed_ext if spec else ".bin"
    if mode == "from_url":
        url = origin or formatted_args.get("url", "")
        return _infer_from_url(url)
    if mode == "from_path":
        return _infer_from_url(str(formatted_args.get("path", "")))
    if mode == "from_origin":
        return _infer_from_url(origin) if origin else ".bin"
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
