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


def sql_literal(value: str) -> str:
    """Escape a string for safe use inside a SQL single-quoted literal.

    Replaces single quotes with doubled single quotes (the SQL standard
    for escaping). Use this when interpolating arbitrary strings into
    SQL string literals to prevent injection or syntax errors.

    Example:
        ``sql_literal("it's")`` → ``"it''s"``
    """
    return value.replace("'", "''")


def sql_str(value: object) -> str:
    """Convert a value to a SQL-safe string literal.

    Wraps :func:`sql_literal` dopo conversione a stringa.
    Accetta qualsiasi tipo (``int``, ``float``, ecc.) e lo converte
    prima di eseguire l'escape.

    Example:
        ``sql_str(42)`` → ``"42"``
        ``sql_str("it's")`` → ``"it''s"``
    """
    return sql_literal(str(value))


def quote_list(paths: list[Path]) -> str:
    """Return a SQL comma-separated list of quoted path literals.

    Each path is quoted via :func:`sql_path` for use in DuckDB SQL statements.
    """
    return ", ".join([f"'{sql_path(p)}'" for p in paths])


# ---------------------------------------------------------------------------
# Column parsing utilities (formerly in read_sql_utils.py)
# ---------------------------------------------------------------------------


def parse_column_value(raw_name: str, value: str) -> tuple[str, str]:
    """Parse a columns dict value, supporting compact 'clean_name:DUCKDB_TYPE' format.

    Examples:
        "VARCHAR"                          -> ("column_name", "VARCHAR")
        "anno_di_imposta:VARCHAR"         -> ("anno_di_imposta", "VARCHAR")
        "numero_contribuenti:DOUBLE"      -> ("numero_contribuenti", "DOUBLE")
    """
    if ":" in value:
        clean_name, dtype = value.rsplit(":", 1)
        return clean_name.strip(), dtype.strip()
    return raw_name, value.strip()


def csv_trim_projection(columns: dict[str, str]) -> str:
    """Build a SQL projection that renames and optionally trims CHAR/TEXT columns.

    Supports compact format in columns dict values: "clean_name:DUCKDB_TYPE".
    When the clean name differs from the raw name, produces "raw_name AS clean_name".
    Text-type columns are trimmed of surrounding whitespace.

    Examples:
        {"column00": "VARCHAR"}                          -> '"column00" AS "column00"'
        {"column00": "anno_di_imposta:VARCHAR"}          -> 'TRIM("column00", \' \\t\\r\\n\') AS "anno_di_imposta"'
        {"column00": "numero:DOUBLE"}                    -> '"column00" AS "numero"'
    """
    exprs: list[str] = []
    for raw_name, value in columns.items():
        clean_name, dtype = parse_column_value(raw_name, value)
        qraw = q_ident(raw_name)
        qclean = q_ident(clean_name)
        dtype_upper = dtype.upper()
        if "CHAR" in dtype_upper or "TEXT" in dtype_upper or "STRING" in dtype_upper:
            exprs.append(f"TRIM({qraw}, ' \t\r\n') AS {qclean}")
        else:
            exprs.append(f"{qraw} AS {qclean}")
    return ", ".join(exprs)
