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
            key_expr: "STR(?data)"  # espressione SPARQL del filtro delta
            key_column: "data"      # colonna del raw esistente da cui leggere il watermark
            key_literal: string     # auto | uri | string | number

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

Confine del watermark: le righe con ``key == max`` pubblicate *dopo* il run sono
perse per sempre (batch con la stessa data). È coerente col contratto ``>``:
per dati con revisioni serve dedup/upsert (fuori scope).

La validazione del blocco (chiavi, ``{since}`` obbligatorio) sta in
``core.config`` ed è applicata al load del ``dataset.yml``: ``validate``/
``inspect`` la vedono, non solo il run.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

from toolkit.core.exceptions import DownloadError
from toolkit.core.literals import render_sparql_literal

_NUMBER_RE = re.compile(r"-?\d+(\.\d+)?")


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

    Un solo passaggio, memoria O(1): il raw può avere milioni di righe.
    """
    if not path.exists():
        return None
    max_str: str | None = None
    max_num: float | None = None
    max_num_raw: str | None = None
    all_numeric = True
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
            for row in reader:
                if len(row) <= idx or not row[idx]:
                    continue
                value = row[idx]
                if max_str is None or value > max_str:
                    max_str = value
                if all_numeric:
                    if _NUMBER_RE.fullmatch(value):
                        num = float(value)
                        if max_num is None or num > max_num:
                            max_num = num
                            max_num_raw = value
                    else:
                        all_numeric = False
    except OSError as exc:
        raise DownloadError(f"incremental: impossibile leggere {path}: {exc}") from exc

    if max_str is None:
        return None
    return max_num_raw if all_numeric else max_str


def append_delta(baseline_path: Path, delta: bytes) -> bytes:
    """Accoda il delta al raw esistente, saltando l'header del delta.

    Il baseline è preservato byte-per-byte: il raw resta fedele alla fonte.
    Delta vuoto (nessun dato) → baseline invariato. Header del delta diverso
    dal baseline (schema drift) → errore: un append produrrebbe un CSV corrotto
    in silenzio.
    """
    if not baseline_path.exists():
        return delta
    baseline = baseline_path.read_bytes()
    newline = delta.find(b"\n")
    if newline < 0:
        return baseline
    delta_header = delta[:newline]
    body = delta[newline + 1 :]
    if not body:
        return baseline
    base_newline = baseline.find(b"\n")
    if base_newline >= 0:
        base_header = baseline[:base_newline]
        if base_header.strip() != delta_header.strip():
            raise DownloadError(
                "incremental: header del delta diverso dal baseline — schema "
                f"drift, append rifiutato (baseline {base_header!r}, delta {delta_header!r})"
            )
    if not baseline.endswith(b"\n"):
        baseline += b"\n"
    return baseline + body
