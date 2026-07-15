"""Utility di preprocessing: normalizzazione numeri, colonne, e decodifica.

Centralizza i pattern piu' comuni che appaiono copiati in 4-5 candidate
di dataset-incubator::

    - normalize_number    → rimpiazza 5 copie (elezioni-*, iva-regionale)
    - normalize_columns_map → rimpiazza 4 copie (elezioni-*)
    - decode_bytes        → rimpiazza 4 copie smart_decode (elezioni-*)
"""

from __future__ import annotations

import re
from typing import Sequence


# ── normalize_number ──────────────────────────────────────────────────────

_DEFAULT_EMPTY = frozenset({"***", "-", "N.D."})


def normalize_number(
    val: str,
    *,
    strip_dot_zero: bool = True,
    empty_values: frozenset[str] | None = None,
) -> str | None:
    """Normalizza un numero in formato italiano (punti migliaia, virgola decimale).

    "1.234,56"  → "1234.56"
    "5849,00"   → "5849"        (se **strip_dot_zero** = True, default)
    "1234"      → "1234"
    "***"       → None
    "-"         → None
    "N.D."      → None

    Args:
        val: Stringa da normalizzare.
        strip_dot_zero: Se True (default), rimuove ``.0`` finale.
        empty_values: Valori considerati "vuoti" → None.
            Default: ``{"***", "-", "N.D."}``.

    Returns:
        Stringa normalizzata, o None se il valore e' vuoto/speciale.
    """
    if not isinstance(val, str):
        return None

    val = val.strip().strip('"')
    if not val:
        return None

    empty = empty_values if empty_values is not None else _DEFAULT_EMPTY
    if val in empty:
        return None

    if "," in val:
        # Rimuovi punti migliaia, converti virgola in punto
        val = val.replace(".", "").replace(",", ".")
        if strip_dot_zero:
            if val.endswith(".00"):
                val = val[:-3]
            elif val.endswith(".0"):
                val = val[:-2]
    else:
        # Rimuovi punti (se ci sono — numeri con migliaia senza decimali)
        val = val.replace(".", "")

    # Se dopo la normalizzazione non e' un numero valido, scarta
    if not _looks_like_number(val):
        return None

    return val


def _looks_like_number(val: str) -> bool:
    """True se val e' un numero valido dopo normalizzazione."""
    if not val:
        return False
    # Ammetti: cifre, singolo punto decimale, leading minus
    has_dot = False
    for i, ch in enumerate(val):
        if ch.isdigit():
            continue
        if ch == "." and not has_dot:
            has_dot = True
            continue
        if ch == "-" and i == 0:
            continue
        return False
    return True


# ── normalize_columns_map ─────────────────────────────────────────────────


def normalize_columns_map(
    header: Sequence[str],
    col_map: Sequence[tuple[re.Pattern, str]],
) -> tuple[list[str], list[int]]:
    """Applica una mappa di espressioni regolari alle colonne di un header.

    Ogni entry di **col_map** e' ``(pattern, nome_normalizzato)``.
    Per ogni colonna in **header** viene cercato il primo pattern matching;
    se matcha, la colonna viene inclusa nell'output col nome normalizzato.

    Args:
        header: Lista dei nomi colonna originali.
        col_map: Lista di ``(regex_pattern, nome_normalizzato)``.
            L'ordine determina la priorita' — vince il primo pattern che matcha.

    Returns:
        ``(nomi_normalizzati, indici_colonne_matchate)``, due liste parallele.
        - **nomi_normalizzati**: i nomi di output (uno per colonna matchata).
        - **indici_colonne_matchate**: la posizione originale in **header**.

    Example:
        >>> col_map = [
        ...     (re.compile(r"^REG(IONE)?$", re.I), "regione"),
        ...     (re.compile(r"^PROV(INCIA)?$", re.I), "provincia"),
        ...     (re.compile(r"^COMUNE$", re.I), "comune"),
        ... ]
        >>> normalize_columns_map(["REGIONE", "PROV", "COMUNE", "SKIP"], col_map)
        (["regione", "provincia", "comune"], [0, 1, 2])
    """
    norm: list[str] = []
    indices: list[int] = []

    for i, col in enumerate(header):
        col = col.strip().strip('"')
        mapped: str | None = None
        for pattern, name in col_map:
            if pattern.match(col):
                mapped = name
                break
        if mapped is not None:
            norm.append(mapped)
            indices.append(i)

    return norm, indices


# ── decode_bytes ──────────────────────────────────────────────────────────

_DEFAULT_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "iso-8859-1")


def decode_bytes(
    data: bytes,
    encodings: tuple[str, ...] | None = None,
) -> str:
    """Decodifica bytes tentando multipli encoding con fallback progressivo.

    Prova ogni encoding in ordine; al primo che non solleva
    ``UnicodeDecodeError`` si ferma. Se nessuno funziona, usa ``utf-8``
    con ``errors="replace"``.

    Args:
        data: Bytes da decodificare.
        encodings: Tuple di encoding da tentare in ordine.
            Default: ``("utf-8-sig", "utf-8", "iso-8859-1", "cp1252")``.

    Returns:
        Stringa decodificata.

    Example:
        >>> decode_bytes(b"hello")  # utf-8 direct
        'hello'
        >>> decode_bytes("café".encode("latin-1"))
        'café'
    """
    encs = encodings if encodings is not None else _DEFAULT_ENCODINGS
    for enc in encs:
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def decode_csv_bytes(
    data: bytes,
    encodings: tuple[str, ...] | None = None,
) -> str:
    """Decodifica bytes CSV tentando multipli encoding.

    Come :func:`decode_bytes` ma con default ottimizzati per CSV italiani:
    ``utf-8-sig`` → ``utf-8`` → ``latin-1``/``cp1252``.

    Args:
        data: Bytes CSV da decodificare.
        encodings: Encoding da tentare (default: stesso di decode_bytes).

    Returns:
        Stringa CSV decodificata.
    """
    return decode_bytes(data, encodings=encodings)
