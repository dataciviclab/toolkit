"""SQL/DuckDB utility functions shared across layers."""

from __future__ import annotations

from pathlib import Path


def q_ident(value: str) -> str:
    """Quote a SQL identifier for DuckDB (handles reserved words / special chars).

    Replaces double quotes with escaped double quotes and wraps in double quotes.
    This is safe for any identifier including those containing spaces or special chars.
    """
    return '"' + value.replace('"', '""') + '"'


def sql_path(p: Path) -> str:
    """Quote a file-system path for use in a DuckDB SQL string literal.

    Escapes single quotes in the resolved absolute path.
    """
    s = p.resolve().as_posix()
    return s.replace("'", "''")


def quote_list(paths: list[Path]) -> str:
    """Return a SQL comma-separated list of quoted path literals.

    Each path is quoted via :func:`sql_path` for use in DuckDB SQL statements.
    """
    return ", ".join([f"'{sql_path(p)}'" for p in paths])
