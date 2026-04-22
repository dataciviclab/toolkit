from __future__ import annotations

from pathlib import Path


def q_ident(name: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def sql_path(p: Path) -> str:
    """Quote a file-system path for use in a DuckDB SQL string literal."""
    s = p.resolve().as_posix()
    return s.replace("'", "''")


def quote_list(paths: list[Path]) -> str:
    """Return a SQL comma-separated list of quoted path literals."""
    return ", ".join([f"'{sql_path(p)}'" for p in paths])


def csv_trim_projection(columns: dict[str, str]) -> str:
    """Build a SQL projection that trims CHAR/TEXT/STRING columns."""
    exprs: list[str] = []
    for name, dtype in columns.items():
        qname = q_ident(name)
        dtype_upper = dtype.upper()
        if "CHAR" in dtype_upper or "TEXT" in dtype_upper or "STRING" in dtype_upper:
            exprs.append(f"TRIM({qname}, ' \t\r\n') AS {qname}")
        else:
            exprs.append(qname)
    return ", ".join(exprs)
