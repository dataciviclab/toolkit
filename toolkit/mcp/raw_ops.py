"""Operazioni raw su file (describe, query) — logica condivisa tra CLI e MCP.

Riusa  lab_connectors.duckdb.safe_connect  per connessioni DuckDB.
Il path GCS (``gs://bucket/key``) viene risolto in URL HTTPS pubblico.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

from lab_connectors.duckdb import safe_connect

# ---------------------------------------------------------------------------
# Protezioni SQL
# ---------------------------------------------------------------------------

_BLOCKED_KEYWORDS = frozenset({
    "alter", "attach", "call", "copy", "create", "delete", "detach",
    "drop", "export", "import", "insert", "install", "load", "merge",
    "replace", "truncate", "update", "vacuum",
})
_TOKEN_RE = re.compile(r"[a-z_][a-z0-9_]*")
_MAX_ROWS_HARD_CAP = 500

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

def _normalize_path(file_path: str) -> tuple[str, bool]:
    """Ritorna (path_normalizzato, is_locale).

    Path relativi vengono risolti contro la CWD dell'utente,
    non contro la directory del modulo.
    """
    raw = (file_path or "").strip()
    if not raw:
        raise ValueError("file_path vuoto")

    lowered = raw.lower()
    if lowered.startswith("gs://"):
        bucket_and_key = raw[5:]
        if "/" not in bucket_and_key:
            raise ValueError("path GCS non valido, atteso gs://bucket/object")
        bucket, key = bucket_and_key.split("/", 1)
        return f"https://storage.googleapis.com/{quote(bucket)}/{quote(key, safe='/._-')}", False

    if lowered.startswith("https://") or lowered.startswith("http://"):
        return raw, False

    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        raise ValueError(f"File non trovato: {path}")
    return str(path), True


# ---------------------------------------------------------------------------
# Formato file
# ---------------------------------------------------------------------------

_READ_FN: dict[str, str] = {
    ".parquet": "read_parquet",
    ".csv": "read_csv",
    ".tsv": "read_csv",
    ".txt": "read_csv",
    ".json": "read_json_auto",
    ".ndjson": "read_json_auto",
    ".jsonl": "read_json_auto",
}


def _detect_read_fn(path: str) -> str:
    ext = Path(path).suffix.lower()
    fn = _READ_FN.get(ext)
    if fn is None:
        # Fallback: prova parquet, se fallisce l'utente vedrà l'errore DuckDB
        return "read_parquet"
    if fn == "read_csv" and ext == ".tsv":
        return "read_csv(auto_detect=true, delim='\\t')"
    return fn


# ---------------------------------------------------------------------------
# SQL validation
# ---------------------------------------------------------------------------


def _validate_select_sql(sql: str) -> str:
    text = (sql or "").strip()
    if not text:
        raise ValueError("sql vuoto")

    lowered = text.lower()
    if ";" in text:
        raise ValueError("Query multiple o statement terminati da ';' non consentiti")
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("Sono consentite solo query SELECT o WITH")

    # Strip commenti e stringhe letterali prima del token check
    scrubbed = re.sub(r"--.*?$", " ", text, flags=re.MULTILINE)
    scrubbed = re.sub(r"/\*.*?\*/", " ", scrubbed, flags=re.DOTALL)
    scrubbed = re.sub(r"'(?:''|[^'])*'", " ", scrubbed)
    scrubbed = re.sub(r'"(?:""|[^"])*"', " ", scrubbed)
    tokens = {token.lower() for token in _TOKEN_RE.findall(scrubbed)}

    for keyword in _BLOCKED_KEYWORDS:
        if keyword in tokens:
            raise ValueError(f"Keyword non consentita nella query: {keyword}")
    return text


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


def describe(file_path: str) -> dict[str, Any]:
    """DESCRIBE + row count di un file, autodetect formato da estensione.

    Args:
        file_path: path locale, ``gs://bucket/key`` o URL HTTPS.

    Returns:
        dict con chiavi: file, exists (bool|None), row_count, columns.
    """
    normalized_path, is_local = _normalize_path(file_path)
    read_fn = _detect_read_fn(normalized_path)
    quoted = normalized_path.replace("'", "''")
    relation = f"{read_fn}('{quoted}')"

    try:
        with safe_connect() as conn:
            describe_rows = conn.execute(
                f"DESCRIBE SELECT * FROM {relation}"
            ).fetchall()
            row_count = conn.execute(
                f"SELECT COUNT(*) AS total_rows FROM {relation}"
            ).fetchone()[0]
    except Exception as exc:
        raise RuntimeError(f"describe fallita per `{file_path}`: {exc}") from exc

    columns = [{"name": row[0], "type": row[1]} for row in describe_rows]

    return {
        "file": normalized_path,
        "exists": True if is_local else None,
        "row_count": int(row_count or 0),
        "columns": columns,
    }


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


def query(sql: str, max_rows: int = 100) -> dict[str, Any]:
    """Esegue una SELECT su un file raw via DuckDB.

    Args:
        sql: query SELECT o WITH (l'utente specifica ``read_parquet('path')`` ecc.).
        max_rows: massimo righe da ritornare (default 100, hard cap 500).

    Returns:
        dict con chiavi: columns, rows, row_count, truncated.
    """
    safe_sql = _validate_select_sql(sql)
    if max_rows <= 0:
        raise ValueError("max_rows deve essere maggiore di 0")
    safe_max_rows = min(max_rows, _MAX_ROWS_HARD_CAP)

    wrapped_sql = f"SELECT * FROM ({safe_sql}) AS q LIMIT {safe_max_rows + 1}"
    try:
        with safe_connect() as conn:
            result = conn.execute(wrapped_sql)
            columns = [item[0] for item in (result.description or [])]
            rows_raw = result.fetchall()
    except Exception as exc:
        raise RuntimeError(f"query fallita: {exc}") from exc

    truncated = len(rows_raw) > safe_max_rows
    rows = rows_raw[:safe_max_rows]
    return {
        "columns": columns,
        "rows": [list(row) for row in rows],
        "row_count": len(rows),
        "truncated": truncated,
    }
