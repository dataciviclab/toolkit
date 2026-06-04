"""Operazioni DuckDB comuni su file Parquet e CSV.

Centralizza DESCRIBE, COUNT e preview per evitare SQL inline
sparso in file diversi del toolkit. Contiene sia le funzioni
per Parquet che ``csv_quick_shape`` per CSV (stesso pattern
DuckDB, formato diverso).

Rinominato da ``core/parquet.py`` — ora in ``core/duckdb_shape.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from lab_connectors.duckdb import safe_connect
from toolkit.core.sql_utils import sql_literal


def _rel(path: Path) -> str:
    return f"read_parquet('{sql_literal(str(path))}')"


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

    Returns:
        Lista di ``{"name": str, "type": str}``.
        Lista vuota se il file non esiste o non è leggibile.
    """
    if not path.exists():
        return []
    try:
        with safe_connect() as con:
            con.execute("PRAGMA disable_progress_bar")
            rows = con.execute(f"DESCRIBE SELECT * FROM {_rel(path)}").fetchall()
            return [{"name": str(r[0]), "type": str(r[1])} for r in rows]
    except Exception:
        return []


def parquet_row_count(path: Path) -> int | None:
    """Conta le righe di un file Parquet.

    Returns:
        Numero di righe, ``None`` se il file non esiste o non è leggibile.
    """
    if not path.exists():
        return None
    try:
        with safe_connect() as con:
            con.execute("PRAGMA disable_progress_bar")
            result = con.execute(f"SELECT COUNT(*) FROM {_rel(path)}").fetchone()
            return int(result[0]) if result else None
    except Exception:
        return None


def parquet_preview(
    path: Path,
    limit: int = 10,
) -> dict[str, Any]:
    """Schema + conteggio + preview righe di un file Parquet.

    Returns:
        ``{"path": str, "column_count": int, "columns": [...],
          "row_count": int | None, "preview": [...], "truncated": bool}``.
    """
    if not path.exists():
        return {"path": str(path), "column_count": 0, "columns": [],
                "row_count": None, "preview": [], "truncated": False}

    try:
        with safe_connect() as con:
            con.execute("PRAGMA disable_progress_bar")
            rel = _rel(path)

            # schema
            describe = con.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()
            columns = [{"name": str(r[0]), "type": str(r[1])} for r in describe]

            # row count
            count_row = con.execute(f"SELECT COUNT(*) FROM {rel}").fetchone()
            row_count = int(count_row[0]) if count_row else None

            # preview
            preview = con.execute(f"SELECT * FROM {rel} LIMIT {int(limit)}").fetchall()
            col_names = [c["name"] for c in columns]
            preview_rows = [dict(zip(col_names, row)) for row in preview]

            return {
                "path": str(path),
                "column_count": len(columns),
                "columns": columns,
                "row_count": row_count,
                "preview": preview_rows,
                "truncated": bool(row_count and row_count > limit),
            }
    except Exception:
        return {"path": str(path), "column_count": 0, "columns": [],
                "row_count": None, "preview": [], "truncated": False}
