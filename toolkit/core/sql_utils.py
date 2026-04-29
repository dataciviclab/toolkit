"""SQL/DuckDB utility functions shared across layers."""

from __future__ import annotations


def q_ident(value: str) -> str:
    """Quote a SQL identifier for DuckDB (handles reserved words / special chars).

    Replaces double quotes with escaped double quotes and wraps in double quotes.
    This is safe for any identifier including those containing spaces or special chars.
    """
    return '"' + value.replace('"', '""') + '"'
