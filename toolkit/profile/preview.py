"""Preview remoto di URL dati: HEAD → Range GET → sniff → profile → infer.

Centralizza la logica che oggi è divisa tra SO (``source_check_fetch.py``:
``_fetch_data_preview``, ``_profile_downloaded_*``, ``_download_preview_content``,
``_extract_year_values_from_sample``, ``_infer_granularity_from_columns``) e
toolkit (``sniff_source_file``, ``profile_with_read_cfg``, ``profile_excel``).

Una chiamata restituisce tutto quello che serve a SO per l'enrichment e a DI
per la valutazione pre-intake.

Usage::

    from toolkit.profile.preview import preview_url

    result = preview_url("https://example.com/dati.csv")
    print(result["columns"], result["granularity"], result["year_min"])
"""

from __future__ import annotations

import json
import logging
import math
import re
import tempfile
from pathlib import Path
from typing import Any

from lab_connectors.http import CircuitOpenError, HttpClient

from toolkit.profile.raw import (
    profile_excel,
    profile_with_read_cfg,
    sniff_source_file,
)
from toolkit.scout.http import probe_url_headers, resolve_preview_kind
from toolkit.scout.infer import infer_granularity

logger = logging.getLogger("toolkit.profile.preview")

# ── Costanti ──────────────────────────────────────────────────────────────────

_PREVIEW_FORMATS = frozenset({"csv", "tsv", "json", "xlsx", "xls"})
_RANGE_LIMIT: dict[str, int] = {
    "csv": 1024 * 1024,  # 1 MB
    "tsv": 1024 * 1024,
    "json": 1024 * 1024,
    "xlsx": 5 * 1024 * 1024,  # 5 MB
    "xls": 5 * 1024 * 1024,
}
_SAMPLE_SIZE: dict[str, int] = {
    "csv": 100 * 1024,
    "tsv": 100 * 1024,
    # JSON non viene campionato — la struttura deve essere integra per il parse.
    # La Range request limita già a _RANGE_LIMIT["json"] = 1MB.
}
_YEAR_COLUMN_HINTS = frozenset(
    {"anno", "year", "data", "date", "periodo", "period", "mese", "month"}
)
_YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20[012]\d)(?!\d)")


# ── Preview orchestrator ──────────────────────────────────────────────────────


def preview_url(
    url: str,
    client: HttpClient | None = None,
    *,
    known_encoding: str | None = None,
    known_delim: str | None = None,
    known_decimal: str | None = None,
    known_skip: int | None = None,
) -> dict[str, Any]:
    """Preview remoto di un URL dati.

    Esegue in sequenza:
    1. HEAD probe → reachability, content-type, content-length
    2. Risoluzione formato (CSV, JSON, XLSX, TSV)
    3. Range GET → download chunk preview
    4. Sniff (encoding, delim, decimal, skip) se formato testo
    5. DuckDB profile (colonne, tipi, sample, mapping)
    6. Infer granularità e anni da colonne/sample

    Args:
        url: URL del file dati remoto.
        client: HttpClient opzionale (con circuit breaker).
        known_encoding: Se già nota da inventory, salta sniff encoding.
        known_delim: Se già noto, salta sniff delim.
        known_decimal: Se già noto, salta sniff decimal.
        known_skip: Se già noto, salta sniff skip.

    Returns:
        dict con chiavi:
            - reachable, http_status, file_size, resource_format
            - encoding_suggested, delim_suggested, decimal_suggested, skip_suggested
            - columns, col_types, preview_row_count
            - mapping_suggestions, robust_read_suggested
            - granularity, year_min, year_max
            - enrich_method (sempre ``"csv_preview"`` in caso di successo)
    """
    result: dict[str, Any] = {
        "reachable": False,
        "http_status": None,
        "file_size": None,
        "resource_format": None,
        "encoding_suggested": None,
        "delim_suggested": None,
        "decimal_suggested": None,
        "skip_suggested": 0,
        "columns": None,
        "col_types": None,
        "preview_row_count": None,
        "mapping_suggestions": None,
        "robust_read_suggested": False,
        "granularity": "non_determinato",
        "year_min": None,
        "year_max": None,
        "enrich_method": None,
    }

    # ── 1. HEAD probe ────────────────────────────────────────────────────────
    try:
        probe = probe_url_headers(url, client=client)
    except (RuntimeError, CircuitOpenError) as exc:
        result["enrich_method"] = "probe_failed"
        result["reachable"] = False
        result["check_notes"] = str(exc)
        return result

    result["reachable"] = probe.get("status_code", 0) < 400
    result["http_status"] = probe.get("status_code")
    result["file_size"] = _parse_file_size(probe)

    # ── 2. Risoluzione formato ───────────────────────────────────────────────
    fmt = resolve_preview_kind(
        url,
        content_type=probe.get("content_type"),
        content_disposition=probe.get("content_disposition"),
    )
    if fmt is None:
        # Formato non supportato per preview
        # Tentiamo comunque un HEAD per registrare il content-type come formato
        ct = probe.get("content_type", "")
        if ct:
            fmt = ct.split(";")[0].strip()
        result["resource_format"] = fmt
        result["enrich_method"] = "unsupported_format"
        return result

    result["resource_format"] = fmt
    fmt_lower = fmt.lower()

    if fmt_lower not in _PREVIEW_FORMATS:
        result["enrich_method"] = "unsupported_format"
        return result

    # ── 3. Range GET download chunk ──────────────────────────────────────────
    content, file_size = _download_preview_chunk(url, fmt_lower, client)
    if content is None:
        result["enrich_method"] = "download_failed"
        return result

    # Aggiorna file_size se HEAD non l'aveva dato
    if file_size and not result.get("file_size"):
        result["file_size"] = file_size

    # ── 4. Sniff + profile per formato ───────────────────────────────────────
    with tempfile.NamedTemporaryFile(suffix=f".{fmt_lower}", delete=False) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)

    try:
        if fmt_lower in ("csv", "tsv"):
            _profile_csv(
                tmp_path, result, fmt_lower, known_encoding, known_delim, known_decimal, known_skip
            )
        elif fmt_lower in ("xlsx", "xls"):
            _profile_excel(tmp_path, result)
        elif fmt_lower == "json":
            _profile_json(content, result)
    finally:
        tmp_path.unlink(missing_ok=True)

    # ── 6. Infer granularità e anni ─────────────────────────────────────────
    columns = result.get("columns")
    if isinstance(columns, str):
        try:
            columns_list = json.loads(columns)
        except (json.JSONDecodeError, TypeError):
            columns_list = []
    elif isinstance(columns, list):
        columns_list = columns
    else:
        columns_list = []

    if columns_list:
        _infer_granularity_from_columns(columns_list, result)
        _infer_years_from_columns(columns_list, result)

    # Infer years from sample rows if still not determined
    if result["year_min"] is None and result.get("preview_row_count"):
        sample = result.get("_sample_rows", [])
        if sample:
            year_vals = _extract_year_values_from_sample(sample, columns_list)
            if year_vals:
                result["year_min"] = min(year_vals)
                result["year_max"] = max(year_vals)

    result["enrich_method"] = "csv_preview"
    return result


# ── Step interni ──────────────────────────────────────────────────────────────


def _parse_file_size(probe: dict) -> int | None:
    """Estrai file_size dal probe result."""
    # Tentativo da Content-Length via headers raw
    ct_len = probe.get("content_length")
    if ct_len:
        try:
            return int(ct_len)
        except (ValueError, TypeError):
            pass
    return None


def _download_preview_chunk(
    url: str, fmt: str, client: HttpClient | None = None
) -> tuple[bytes | None, int | None]:
    """Scarica un chunk preview del file remoto."""
    range_limit = _RANGE_LIMIT.get(fmt, 1024 * 1024)
    sample_size = _SAMPLE_SIZE.get(fmt)

    if client is None:
        client = HttpClient(timeout=(5, 10))

    fetch_result = client.get(url, headers={"Range": f"bytes=0-{range_limit - 1}"})
    if fetch_result is None or not fetch_result.is_ok or fetch_result.response is None:
        return None, None
    if fetch_result.response.status_code >= 400:
        return None, None

    content = fetch_result.response.content
    if sample_size is not None:
        content = content[:sample_size]
    elif len(content) > range_limit:
        return None, None  # troppo grande

    try:
        file_size = int(fetch_result.response.headers.get("Content-Length", "0"))
    except (ValueError, TypeError):
        file_size = 0
    if file_size <= 0:
        file_size = len(content)

    return content, file_size


def _profile_csv(
    tmp_path: Path,
    result: dict[str, Any],
    fmt: str,
    known_encoding: str | None,
    known_delim: str | None,
    known_decimal: str | None,
    known_skip: int | None,
) -> None:
    """Sniff + profile per CSV/TSV."""
    if known_encoding and known_delim:
        # Salta sniff — usa parametri noti dall'inventory
        sniff: dict[str, Any] = {
            "encoding_suggested": known_encoding,
            "delim_suggested": known_delim,
            "decimal_suggested": known_decimal,
            "skip_suggested": known_skip or 0,
            "header_line": None,
            "true_header_line": None,
            "warnings": [],
            "is_binary_file": None,
            "file_used": tmp_path.name,
        }
    else:
        sniff = sniff_source_file(tmp_path)

    result["encoding_suggested"] = sniff.get("encoding_suggested")
    result["delim_suggested"] = sniff.get("delim_suggested")
    result["decimal_suggested"] = sniff.get("decimal_suggested")
    result["skip_suggested"] = sniff.get("skip_suggested", 0)

    enc = sniff["encoding_suggested"]
    delim = sniff["delim_suggested"]
    dec = sniff["decimal_suggested"]
    skip = sniff["skip_suggested"]

    effective_read_cfg: dict[str, Any] = {}
    if delim:
        effective_read_cfg["delim"] = delim
    if enc:
        effective_read_cfg["encoding"] = enc
    if dec:
        effective_read_cfg["decimal"] = dec
    if skip:
        effective_read_cfg["skip"] = skip
    effective_read_cfg.setdefault("header", True)

    try:
        profile = profile_with_read_cfg(tmp_path, sniff, effective_read_cfg)
    except Exception as exc:
        logger.warning("DuckDB profile failed for %s: %s", tmp_path, exc)
        profile = {
            "columns_raw": [],
            "duckdb_types": [],
            "sample_rows": [],
            "mapping_suggestions": {},
            "robust_read_suggested": True,
            "warnings": [f"profile_failed: {exc}"],
        }

    columns_raw = profile.get("columns_raw", [])
    duckdb_types = profile.get("duckdb_types", [])
    sample_rows = profile.get("sample_rows", [])

    result["columns"] = columns_raw
    result["col_types"] = dict(zip(columns_raw, duckdb_types)) if columns_raw else {}
    result["preview_row_count"] = len(sample_rows) if sample_rows else None
    result["mapping_suggestions"] = profile.get("mapping_suggestions", {})
    result["robust_read_suggested"] = profile.get("robust_read_suggested", False)
    result["_sample_rows"] = sample_rows  # per infer anni


def _profile_excel(tmp_path: Path, result: dict[str, Any]) -> None:
    """Profile per Excel, con fallback CSV/TSV per file .xls mascherati."""
    try:
        profile = profile_excel(tmp_path)
    except Exception as exc:
        logger.warning("Excel profile failed: %s", exc)
        profile = {"columns_raw": [], "sample_rows": [], "robust_read_suggested": True}

    columns_raw = profile.get("columns_raw", [])

    # Fallback: .xls falso (es. TSV con estensione .xls) → prova come CSV
    if not columns_raw:
        try:
            sniff = sniff_source_file(tmp_path)
            enc = sniff.get("encoding_suggested") or "utf-8"
            delim = sniff.get("delim_suggested") or "\t"
            read_cfg = {
                "encoding": enc,
                "delim": delim,
                "header": True,
                "skip": sniff.get("skip_suggested", 0),
            }
            csv_profile = profile_with_read_cfg(tmp_path, sniff, read_cfg)
            columns_raw = csv_profile.get("columns_raw", [])
            sample_rows = csv_profile.get("sample_rows", [])
            result["columns"] = columns_raw
            result["col_types"] = (
                dict(zip(columns_raw, csv_profile.get("duckdb_types", []))) if columns_raw else {}
            )
            result["preview_row_count"] = len(sample_rows) if sample_rows else None
            result["robust_read_suggested"] = csv_profile.get("robust_read_suggested", False)
            result["_sample_rows"] = sample_rows
            return
        except Exception as exc:
            logger.warning("Excel CSV fallback failed: %s", exc)

    sample_rows = profile.get("sample_rows", [])
    result["columns"] = columns_raw
    result["col_types"] = {}
    result["preview_row_count"] = len(sample_rows) if sample_rows else None
    result["robust_read_suggested"] = profile.get("robust_read_suggested", False)
    result["_sample_rows"] = sample_rows


def _profile_json(content: bytes, result: dict[str, Any]) -> None:
    """Profile per JSON (inline, nessuna dipendenza DuckDB)."""
    try:
        data = json.loads(content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        result["enrich_method"] = "json_decode_failed"
        return

    if isinstance(data, dict):
        columns = list(data.keys())
        sample_rows = [data]
    elif isinstance(data, list):
        if data and isinstance(data[0], dict):
            columns = list(data[0].keys())
            sample_rows = data
        elif data:
            columns = []
            sample_rows = data[:50]
        else:
            columns = []
            sample_rows = []
    else:
        columns = []
        sample_rows = []

    result["columns"] = columns
    result["col_types"] = (
        {
            col: type(data[0][col]).__name__
            if data and isinstance(data, list) and data
            else type(data[col]).__name__
            if isinstance(data, dict)
            else "unknown"
            for col in columns
        }
        if columns
        else {}
    )
    result["preview_row_count"] = len(sample_rows) if isinstance(data, list) else None
    result["robust_read_suggested"] = False
    result["mapping_suggestions"] = {}
    result["_sample_rows"] = sample_rows


# ── Infer helpers ─────────────────────────────────────────────────────────────


def _infer_granularity_from_columns(columns: list[str], result: dict[str, Any]) -> None:
    """Inferisci granularità territoriale dai nomi colonna.

    Sostituisce underscore con spazi per garantire il match delle word
    boundary (es. ``denominazione_comune`` → matcha ``comune``).
    """
    combined = " ".join(c.lower().replace("_", " ") for c in columns)
    result["granularity"] = infer_granularity(combined)


def _infer_years_from_columns(columns: list[str], result: dict[str, Any]) -> None:
    """Inferisci anni da nomi colonna (anno, year, ...)."""
    year_vals: set[int] = set()
    for col in columns:
        m = _YEAR_RE.search(col)
        if m:
            year_vals.add(int(m.group(1)))
    # Cerca anche colonne hint
    for col in columns:
        if col.lower() in _YEAR_COLUMN_HINTS:
            # Non possiamo estrarre anni dal nome colonna alone,
            # ma è un segnale — li inferiamo dai sample rows
            pass
    if year_vals:
        result["year_min"] = min(year_vals)
        result["year_max"] = max(year_vals)


def _extract_year_values_from_sample(sample: list[dict], columns: list[str]) -> list[int]:
    """Extract year values from sample rows.

    Prima prova colonne numeriche con almeno 2 valori in 1900-2100,
    poi colonne con nome hint (``anno``, ``year``, ...).
    """

    def _safe_ints(vals: list) -> list[int]:
        return [int(v) for v in vals if not (isinstance(v, float) and math.isnan(v))]

    year_values: list[int] = []
    if not sample:
        return year_values

    # Strategy 1: colonne numeriche con 2+ valori nel range anni
    for col in columns:
        vals = [r.get(col) for r in sample if isinstance(r.get(col), (int, float))]
        if vals:
            y_vals = [v for v in _safe_ints(vals) if 1900 <= v <= 2100]
            if len(y_vals) >= 2:
                return y_vals

    # Strategy 2: colonne con nome hint
    for col in columns:
        if col.lower() in _YEAR_COLUMN_HINTS:
            vals = [r.get(col) for r in sample if isinstance(r.get(col), (int, float))]
            if vals:
                return _safe_ints(vals)

    return year_values
