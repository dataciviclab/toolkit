"""Operazioni DuckDB comuni su file Parquet e CSV.

Centralizza DESCRIBE, COUNT e preview per evitare SQL inline
sparso in file diversi del toolkit. Contiene sia le funzioni
per Parquet che ``csv_quick_shape`` per CSV (stesso pattern
DuckDB, formato diverso).

Rinominato da ``core/parquet.py`` — ora in ``core/duckdb_shape.py``.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb

from lab_connectors.duckdb import safe_connect
from toolkit.core.sql_utils import sql_literal


def _is_s3_path(path: str | Path) -> bool:
    """True se il path inizia con ``s3://`` (path GCS via DuckDB httpfs).

    Rileva anche ``s3:/`` (``Path()`` normalizza ``//`` in ``/``).

    I bucket pubblici ``dataciviclab-clean`` e ``dataciviclab-mart``
    sono accessibili in lettura anonima via S3-compatible API GCS.
    """
    s = str(path)
    return s.startswith("s3://") or s.startswith("s3:/")


def _s3_config() -> Mapping[str, Any]:
    """Config DuckDB per leggere bucket GCS pubblici via S3-compatible API."""
    return {
        "s3_endpoint": "storage.googleapis.com",
        "s3_region": "auto",
        "s3_access_key_id": "",
        "s3_secret_access_key": "",
        "s3_use_ssl": "true",
    }


@contextmanager
def _parquet_connect(path: Path) -> Generator[Any, None, None]:
    """Context manager DuckDB con supporto S3 se il path e' ``s3://``.

    Per path locali usa ``safe_connect()`` (backward compat).
    Per path ``s3://`` usa ``safe_connect(extensions=["httpfs"], config=...)``
    (via ``lab_connectors.duckdb`` v0.13.0).
    """
    if _is_s3_path(str(path)):
        with safe_connect(extensions=["httpfs"], config=_s3_config()) as con:
            yield con
    else:
        with safe_connect() as con:
            yield con


def _normalize_s3(path_str: str) -> str:
    """Ripristina ``s3://`` se ``Path()`` l'ha normalizzato in ``s3:/``."""
    if path_str.startswith("s3:/") and not path_str.startswith("s3://"):
        return "s3://" + path_str[4:]
    return path_str


def _display_path(path: Path) -> str:
    """Path come stringa, con ``s3://`` preservato."""
    return _normalize_s3(str(path))


def _rel(path: Path) -> str:
    """Build ``read_parquet(...)`` reference.

    Preserva il prefisso ``s3://`` — ``Path()`` normalizza ``//`` in ``/``.
    """
    return f"read_parquet('{sql_literal(_display_path(path))}')"


# ---------------------------------------------------------------------------
# CSV quick shape
# ---------------------------------------------------------------------------


def csv_quick_shape(csv_path: str | Path) -> dict[str, Any]:
    """Quick row count and column count for a CSV file via DuckDB auto-detect.

    Args:
        csv_path: Path to the CSV file.

    Returns:
        Dict with ``row_count_estimate`` (int | None) and ``column_count`` (int | None).
        Returns empty dict if file not found or unreadable (non-CSV, encoding issues, etc.).

    Note:
        Uses ``read_csv_auto`` with ``auto_detect=true``. Non fa sniff esplicito,
        approssimativo ma veloce. Per profiling completo vedi
        ``toolkit.profile.raw.profile_raw``.
    """
    path = Path(csv_path)
    if not path.exists():
        return {}
    try:
        with duckdb.connect(database=":memory:") as conn:
            conn.execute("PRAGMA disable_progress_bar")
            rel = f"read_csv_auto('{sql_literal(str(path))}', auto_detect=true)"
            describe = conn.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()
            col_count = len(describe)
            count_row = conn.execute(f"SELECT COUNT(*) FROM {rel}").fetchone()
            row_count = int(count_row[0]) if count_row else None
            return {"row_count_estimate": row_count, "column_count": col_count}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Parquet operations
# ---------------------------------------------------------------------------


def parquet_schema(path: Path) -> list[dict[str, str]]:
    """Legge lo schema di un file Parquet (nomi colonne + tipo DuckDB).

    Supporta path locali e ``s3://`` (bucket GCS pubblici).

    Returns:
        Lista di ``{"name": str, "type": str}``.
        Lista vuota se il file non esiste o non è leggibile.
    """
    if not _is_s3_path(path) and not path.exists():
        return []
    try:
        with _parquet_connect(path) as con:
            con.execute("PRAGMA disable_progress_bar")
            rows = con.execute(f"DESCRIBE SELECT * FROM {_rel(path)}").fetchall()
            return [{"name": str(r[0]), "type": str(r[1])} for r in rows]
    except Exception:
        return []


def parquet_row_count(path: Path) -> int | None:
    """Conta le righe di un file Parquet.

    Supporta path locali e ``s3://`` (bucket GCS pubblici).

    Returns:
        Numero di righe, ``None`` se il file non esiste o non è leggibile.
    """
    if not _is_s3_path(path) and not path.exists():
        return None
    try:
        with _parquet_connect(path) as con:
            con.execute("PRAGMA disable_progress_bar")
            result = con.execute(f"SELECT COUNT(*) FROM {_rel(path)}").fetchone()
            return int(result[0]) if result else None
    except Exception:
        return None


def parquet_preview(
    path: Path,
    limit: int = 10,
    sql: str | None = None,
) -> dict[str, Any]:
    """Schema + conteggio + preview righe di un file Parquet.

    Args:
        path: Path al file parquet.
        limit: Numero massimo di righe in preview (default 10).
        sql: SQL SELECT da eseguire sul parquet. Il parquet è disponibile
            come vista ``data``. Esempi::

                SELECT * FROM data WHERE anno > 2020
                SELECT regione, COUNT(*) FROM data GROUP BY regione

            Se ``None`` (default), esegue ``SELECT * FROM data LIMIT {limit}``
            (backward compat).

            Nota: solo SELECT singolo (DuckDB non supporta multi-statement
            in una chiamata ``execute()``).

    Returns:
        ``{"path": str, "column_count": int, "columns": [...],
          "row_count": int | None, "preview": [...], "truncated": bool,
          "sql": str | None}``.

    Raises:
        FileNotFoundError: se il parquet non esiste (solo quando ``sql``
            è fornito; in modalità default graceful empty dict).
        RuntimeError: se la query SQL fallisce (solo con ``sql``).
    """
    if not _is_s3_path(path) and not path.exists():
        if sql is not None:
            raise FileNotFoundError(f"Parquet non trovato: {path}")
        return {"path": _display_path(path), "column_count": 0, "columns": [],
                "row_count": None, "preview": [], "truncated": False, "sql": None}

    try:
        with _parquet_connect(path) as con:
            con.execute("PRAGMA disable_progress_bar")

            if sql is not None:
                # Registra il parquet come vista 'data' per query naturali
                con.execute(f"CREATE OR REPLACE VIEW data AS SELECT * FROM {_rel(path)}")
                source = f"({sql})"
            else:
                source = _rel(path)

            # schema
            describe = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
            columns = [{"name": str(r[0]), "type": str(r[1])} for r in describe]

            # row count
            count_row = con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()
            row_count = int(count_row[0]) if count_row else None

            # preview
            preview = con.execute(f"SELECT * FROM {source} LIMIT {int(limit)}").fetchall()
            col_names = [c["name"] for c in columns]
            preview_rows = [dict(zip(col_names, row)) for row in preview]

            return {
                "path": _display_path(path),
                "column_count": len(columns),
                "columns": columns,
                "row_count": row_count,
                "preview": preview_rows,
                "truncated": bool(row_count and row_count > limit),
                "sql": sql,
            }
    except Exception:
        if sql is not None:
            # In modalità SQL esplicito, propaga l'errore
            raise
        base = {"path": _display_path(path), "column_count": 0, "columns": [],
                "row_count": None, "preview": [], "truncated": False}
        base["sql"] = sql
        return base
