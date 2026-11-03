"""Rendering di letterali SPARQL per i placeholder di paginazione e delta.

Estratto da ``toolkit.plugins.sparql``: la stessa rappresentazione
URI/stringa/numero serve sia al keyset pagination (``{keyset}``) sia al fetch
incrementale (``{since}``). Un'unica implementazione, due consumer.
"""

from __future__ import annotations

import re

from toolkit.core.exceptions import DownloadError

_NUMBER_RE = re.compile(r"-?\d+(\.\d+)?")


def quote_sparql_literal(value: str) -> str:
    """Rende ``value`` come letterale SPARQL stringa (con escape)."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def render_sparql_literal(value: str, mode: str = "auto") -> str:
    """Rappresenta un valore come letterale SPARQL.

    ``mode``:
    - ``auto`` (default): URI (http/https) → ``<uri>``, numero → nudo,
      altrimenti stringa quotata.
    - ``uri``: sempre ``<uri>``.
    - ``number``: sempre nudo (errore se non numerico).
    - ``string``: sempre quotato — necessario quando il FILTER usa
      ``STR(?var)``: il valore è una stringa, non un IRI.
    """
    v = value.strip()
    if mode == "uri":
        return f"<{v}>"
    if mode == "number":
        if not _NUMBER_RE.fullmatch(v):
            raise DownloadError(f"key_literal='number' ma l'ultimo valore non è numerico: {v!r}")
        return v
    if mode == "string":
        return quote_sparql_literal(v)
    if v.startswith(("http://", "https://")):
        return f"<{v}>"
    if _NUMBER_RE.fullmatch(v):
        return v
    return quote_sparql_literal(v)
