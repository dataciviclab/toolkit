"""Shared HTTP utilities for plugin fetch — estensioni non troncabili e troncamento byte.

Centralizza logica che era duplicata in http_file.py, http_post_file.py e ckan.py.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

# Estensioni che NON possono essere troncate con HTTP Range:
# formati binari, compressi o contenitori i cui metadati sono in coda al file.
# Campionare questi formati con Range produce file corrotti.
NON_TRUNCABLE_EXTS: set[str] = {
    ".parquet",
    ".zip",
    ".xlsx",
    ".xls",
    ".gz",
    ".bz2",
    ".7z",
    ".rar",
}


def is_non_truncable_url(url: str) -> bool:
    """Restituisce True se l'estensione del file non è troncabile in modo sicuro.

    Usa l'estensione dall'URL per decidere. URL senza estensione
    o con parametri di query sono considerati troncabili.
    """
    path = urlparse(url).path
    suffix = Path(path).suffix.lower()
    if not suffix:
        return False
    return suffix in NON_TRUNCABLE_EXTS


def truncate_at_line(content: bytes, sample_bytes: int) -> bytes:
    """Tronca il content ai primi sample_bytes, chiudendo all'ultima riga completa.

    Server che ignorano Range (rispondono 200 invece di 206) restituiscono
    tutto il file. Questa funzione taglia al limite byte garantito, poi
    arretra all'ultimo ``\\n`` per evitare CSV con quote non chiuse,
    JSON troncato, ecc.
    """
    if len(content) <= sample_bytes:
        return content
    content = content[:sample_bytes]
    last_newline = content.rfind(b"\n")
    if last_newline > 0:
        content = content[: last_newline + 1]
    return content
