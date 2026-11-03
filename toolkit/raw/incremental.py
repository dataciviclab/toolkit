"""Fetch incrementale per fonti append-only (stream).

Contratto ``raw.sources[].incremental`` (opzionale; assente = full):

    raw:
      sources:
        - name: voti_raw
          type: sparql
          args:
            query: |
              SELECT ?data ?voto WHERE {
                ?voto ocd:data ?data .
                {since}
              }
          incremental:
            key_expr: "?data"      # espressione SPARQL del filtro delta
            key_column: "data"     # colonna del raw esistente da cui leggere il watermark
            key_literal: string    # auto | uri | string | number

Semantica: il watermark è il valore massimo di ``key_column`` nel raw esistente
(la baseline). ``{since}`` viene sostituito con ``FILTER(key_expr > <watermark>)``
— o con la stringa vuota se la baseline non esiste (→ fetch full). Il delta
restituito viene accodato al raw esistente.

Scope: **append-only**. La query deve filtrare in modo esclusivo (``>``): il
merge è un append fedele al baseline, nessuna riscrittura delle righe storiche.
Dedup/upsert (rettifiche, annullamenti) è fuori scope — vedi ADR-006 e #534.

La baseline deve esistere nel checkout: su runner effimero va ripristinata
prima del run (stato nel data plane). Senza baseline il run è full, corretto
ma lento.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

from toolkit.core.exceptions import DownloadError
from toolkit.core.literals import render_sparql_literal

_NUMBER_RE = re.compile(r"-?\d+(\.\d+)?")

# Chiavi ammesse nel blocco ``incremental``. Fail-fast su typo.
INCREMENTAL_KEYS = {"key_expr", "key_column", "key_literal"}


def validate_incremental(cfg: dict, field_name: str) -> None:
    """Valida il blocco ``incremental``. Fail-fast su chiavi/valori errati."""
    if not isinstance(cfg, dict):
        raise ValueError(f"{field_name}.incremental must be a mapping")
    unknown = sorted(set(cfg) - INCREMENTAL_KEYS)
    if unknown:
        raise ValueError(
            f"{field_name}.incremental: unknown key(s) {unknown}; "
            f"allowed keys: {sorted(INCREMENTAL_KEYS)}"
        )
    key_expr = cfg.get("key_expr")
    key_column = cfg.get("key_column")
    if not key_expr or not isinstance(key_expr, str):
        raise ValueError(f"{field_name}.incremental.key_expr is required (es. '?data')")
    if not key_column or not isinstance(key_column, str):
        raise ValueError(f"{field_name}.incremental.key_column is required (es. 'data')")
    key_literal = cfg.get("key_literal", "auto")
    if key_literal not in {"auto", "uri", "string", "number"}:
        raise ValueError(
            f"{field_name}.incremental.key_literal must be one of: auto, uri, string, number"
        )


def build_since_clause(baseline_path: Path, incremental: dict) -> str:
    """Clausola FILTER per ``{since}``, o ``""`` se non c'è watermark.

    ``""`` (nessuna baseline o colonna vuota) → la query resta senza filtro
    → fetch full. Stessa semantica del placeholder ``{keyset}`` alla prima
    pagina.
    """
    watermark = read_watermark(baseline_path, str(incremental["key_column"]))
    if watermark is None:
        return ""
    literal = render_sparql_literal(watermark, str(incremental.get("key_literal", "auto")))
    return f"FILTER({incremental['key_expr']} > {literal})"


def read_watermark(path: Path, key_column: str) -> str | None:
    """Valore massimo di ``key_column`` nel raw esistente.

    None se il file non esiste o la colonna è tutta vuota. Numerico se tutti i
    valori sono numeri (il max stringa mentirebbe: "9" > "10"), altrimenti
    confronto lessicografico (corretto per date ISO).
    """
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if not header:
                return None
            try:
                idx = header.index(key_column)
            except ValueError as exc:
                raise DownloadError(
                    f"incremental: colonna '{key_column}' assente nel raw "
                    f"{path.name} (colonne: {header})"
                ) from exc
            values = [row[idx] for row in reader if len(row) > idx and row[idx]]
    except OSError as exc:
        raise DownloadError(f"incremental: impossibile leggere {path}: {exc}") from exc

    if not values:
        return None
    if all(_NUMBER_RE.fullmatch(v) for v in values):
        return str(max(values, key=float))
    return max(values)


def append_delta(baseline_path: Path, delta: bytes) -> bytes:
    """Accoda il delta al raw esistente, saltando l'header del delta.

    Il baseline è preservato byte-per-byte: il raw resta fedele alla fonte.
    Delta vuoto (nessun dato) → baseline invariato.
    """
    if not baseline_path.exists():
        return delta
    baseline = baseline_path.read_bytes()
    newline = delta.find(b"\n")
    if newline < 0:
        return baseline
    body = delta[newline + 1 :]
    if not body:
        return baseline
    if not baseline.endswith(b"\n"):
        baseline += b"\n"
    return baseline + body
