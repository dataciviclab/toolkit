"""Preview remoto di URL CSV/TSV: HEAD → Range GET → sniff → DuckDB profile → infer.

Centralizza la logica che era divisa tra SO (``_fetch_data_preview``,
``_profile_downloaded_csv``) e toolkit (``sniff_source_file``,
``profile_with_read_cfg``).  Una chiamata restituisce tutto quello che serve
a SO per l'enrichment pre-intake.

Usage::

    from toolkit.profile.preview import preview_url

    result = preview_url("https://example.com/dati.csv")
    print(result.columns, result.granularity, result.year_min)
"""

from __future__ import annotations

import logging
import math
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from lab_connectors.http import HttpClient

from toolkit.profile.raw import profile_with_read_cfg, sniff_source_file
from toolkit.scout.http import fetch_content, probe_url_headers, resolve_preview_kind
from toolkit.scout.infer import infer_granularity

logger = logging.getLogger("toolkit.profile.preview")

# ── Public result type ────────────────────────────────────────────────────────

PreviewStatus = Literal[
    "success",
    "probe_failed",
    "unsupported_format",
    "download_failed",
    "profile_failed",
]


@dataclass
class PreviewResult:
    """Risultato strutturato di ``preview_url``.

    Solo CSV/TSV per ora — JSON ed Excel hanno casi limite che richiedono
    orchestrazione diversa.
    """

    url: str
    status: PreviewStatus = "success"
    reachable: bool = False
    http_status: int | None = None
    file_size: int | None = None
    resource_format: str | None = None

    # Sniff
    encoding_suggested: str | None = None
    delim_suggested: str | None = None
    decimal_suggested: str | None = None
    skip_suggested: int = 0

    # Profile
    columns: list[str] | None = None
    col_types: dict[str, str] | None = None
    preview_row_count: int | None = None
    robust_read_suggested: bool = False
    mapping_suggestions: dict[str, Any] = field(default_factory=dict)

    # Infer
    granularity: str = "non_determinato"
    year_min: int | None = None
    year_max: int | None = None


# ── Year extraction helpers ───────────────────────────────────────────────────

_YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20[012]\d)(?!\d)")
_YEAR_COLUMN_HINTS = frozenset(
    {"anno", "year", "data", "date", "periodo", "period", "mese", "month"}
)


def _extract_years_from_columns(columns: list[str]) -> tuple[int | None, int | None]:
    """Estrae anni da nomi colonna (es. ``Anno``, ``anno_riferimento``)."""
    vals: set[int] = set()
    for col in columns:
        m = _YEAR_RE.search(col)
        if m:
            vals.add(int(m.group(1)))
    return (min(vals), max(vals)) if vals else (None, None)


def extract_year_values_from_sample(sample: list[dict], columns: list[str]) -> list[int]:
    """Estrae anni da sample rows (valori 1900-2100 in colonne numeriche)."""
    if not sample:
        return []

    def _safe_ints(vals: list) -> list[int]:
        return [int(v) for v in vals if not (isinstance(v, float) and math.isnan(v))]

    # Strategy 1: colonne con 2+ valori nel range anni
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

    return []


# ── Orchestrator ──────────────────────────────────────────────────────────────


def preview_url(
    url: str,
    client: HttpClient | None = None,
    *,
    known_encoding: str | None = None,
    known_delim: str | None = None,
    known_decimal: str | None = None,
    known_skip: int | None = None,
) -> PreviewResult:
    """Preview remoto di un URL CSV/TSV.

    Passaggi:
    1. HEAD probe — reachability, content-type, content-length
    2. Risoluzione formato (solo CSV/TSV supportati)
    3. Range GET — download chunk (max 1MB)
    4. Sniff — encoding, delim, decimal, skip (saltabile con ``known_*``)
    5. DuckDB profile — colonne, tipi, mapping, sample rows
    6. Infer — granularità, anno minimo/massimo

    Args:
        url: URL del file CSV/TSV remoto.
        client: HttpClient opzionale (con circuit breaker).
        known_encoding: Se già noto (da inventory), salta sniff encoding.
        known_delim: Se già noto, salta sniff delim.
        known_decimal: Se già noto, salta sniff decimal.
        known_skip: Se già noto, salta sniff skip.

    Returns:
        ``PreviewResult`` con tutti i campi compilati o stato di errore.
    """
    # ── 1. HEAD probe ────────────────────────────────────────────────────────
    _owns_client = client is None
    client = client or HttpClient(timeout=(5, 10))
    try:
        try:
            probe = probe_url_headers(url, client=client)
        except (RuntimeError, Exception):
            return PreviewResult(url=url, status="probe_failed", reachable=False)

        reachable = probe.get("status_code", 0) < 400
        http_status = probe.get("status_code")
        file_size = _parse_content_length(probe)

        # ── 2. Risoluzione formato ───────────────────────────────────────────
        fmt = resolve_preview_kind(
            url,
            content_type=probe.get("content_type"),
            content_disposition=probe.get("content_disposition"),
        )
        if fmt is None or fmt.lower() not in ("csv", "tsv"):
            return PreviewResult(
                url=url,
                status="unsupported_format",
                reachable=reachable,
                http_status=http_status,
                file_size=file_size,
                resource_format=fmt.upper() if fmt else None,
            )

        fmt_lower = fmt.lower()

        # ── 3. Range GET ─────────────────────────────────────────────────────
        try:
            fetched = fetch_content(url, client=client, max_bytes=1024 * 1024)
        except RuntimeError:
            return PreviewResult(
                url=url,
                status="download_failed",
                reachable=reachable,
                http_status=http_status,
                resource_format=fmt.upper(),
            )

        content: bytes = fetched["content"]
        # Content-Length da fetch_content: su 206 con Content-Range e' la
        # dimensione reale del file; altrimenti la lunghezza del chunk.
        content_file_size = fetched.get("content_length") or len(content)
        if not file_size:
            file_size = content_file_size

        # ── 4. Sniff ─────────────────────────────────────────────────────────
        with tempfile.NamedTemporaryFile(
            suffix=".csv" if fmt_lower == "tsv" else ".csv", delete=False
        ) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)

        try:
            if known_encoding and known_delim:
                # Usa parametri noti dall'inventory — salta sniff
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

            enc = sniff.get("encoding_suggested")
            delim = sniff.get("delim_suggested")
            dec = sniff.get("decimal_suggested")
            skip = sniff.get("skip_suggested", 0)

            # ── 5. DuckDB profile ───────────────────────────────────────────
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
                logger.warning("DuckDB profile failed for %s: %s", url, exc)
                return PreviewResult(
                    url=url,
                    status="profile_failed",
                    reachable=reachable,
                    http_status=http_status,
                    file_size=file_size,
                    resource_format=fmt.upper(),
                    encoding_suggested=enc,
                    delim_suggested=delim,
                    decimal_suggested=dec,
                    skip_suggested=skip,
                )

            columns_raw = profile.get("columns_raw", [])
            duckdb_types = profile.get("duckdb_types", [])
            sample_rows = profile.get("sample_rows", [])

            col_types = dict(zip(columns_raw, duckdb_types)) if columns_raw else {}
            preview_row_count = len(sample_rows) if sample_rows else None
            mapping_suggestions = profile.get("mapping_suggestions", {})
            robust_read_suggested = profile.get("robust_read_suggested", False)

            # ── 6. Infer ─────────────────────────────────────────────────────
            combined = (
                " ".join(c.lower().replace("_", " ") for c in columns_raw) if columns_raw else ""
            )
            granularity = infer_granularity(combined)

            year_min, year_max = _extract_years_from_columns(columns_raw)

            # Fallback: anni da sample rows
            if year_min is None and sample_rows:
                year_vals = extract_year_values_from_sample(sample_rows, columns_raw)
                if year_vals:
                    year_min = min(year_vals)
                    year_max = max(year_vals)

        finally:
            tmp_path.unlink(missing_ok=True)

        return PreviewResult(
            url=url,
            status="success",
            reachable=reachable,
            http_status=http_status,
            file_size=file_size,
            resource_format=fmt.upper(),
            encoding_suggested=enc,
            delim_suggested=delim,
            decimal_suggested=dec,
            skip_suggested=skip,
            columns=columns_raw,
            col_types=col_types,
            preview_row_count=preview_row_count,
            robust_read_suggested=robust_read_suggested,
            mapping_suggestions=mapping_suggestions,
            granularity=granularity,
            year_min=year_min,
            year_max=year_max,
        )

    finally:
        if _owns_client:
            client.close()


def _parse_content_length(probe: dict) -> int | None:
    """Estrai Content-Length dal probe result."""
    raw = probe.get("content_length")
    if raw:
        try:
            return int(raw)
        except (ValueError, TypeError):
            pass
    return None
