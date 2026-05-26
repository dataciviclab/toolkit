"""Quick CSV shape detection — righe e colonne via DuckDB auto-detect.

Condiviso da tutti i layer (raw, cli, mcp). Non dipende da CLI ne MCP.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


def _sql_literal(value: str) -> str:
    """Escape a string for safe use inside a SQL single-quoted literal."""
    return value.replace("'", "''")


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
            rel = f"read_csv_auto('{_sql_literal(str(path))}', auto_detect=true)"
            describe = conn.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()
            col_count = len(describe)
            count_row = conn.execute(f"SELECT COUNT(*) FROM {rel}").fetchone()
            row_count = int(count_row[0]) if count_row else None
            return {"row_count_estimate": row_count, "column_count": col_count}
    except Exception:
        return {}
