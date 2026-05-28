"""SQL utility functions specific to the clean layer.

Layer-specific SQL helpers (not shared across layers).
For shared utilities like ``q_ident``, ``sql_path``, ``quote_list``
see ``toolkit.core.sql_utils``.
"""

from __future__ import annotations

from toolkit.core.sql_utils import q_ident


def _parse_column_value(raw_name: str, value: str) -> tuple[str, str]:
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
        {"column00": "anno_di_imposta:VARCHAR"}          -> 'TRIM("column00", \' \t\\r\\n\') AS "anno_di_imposta"'
        {"column00": "numero:DOUBLE"}                    -> '"column00" AS "numero"'
    """
    exprs: list[str] = []
    for raw_name, value in columns.items():
        clean_name, dtype = _parse_column_value(raw_name, value)
        qraw = q_ident(raw_name)
        qclean = q_ident(clean_name)
        dtype_upper = dtype.upper()
        if "CHAR" in dtype_upper or "TEXT" in dtype_upper or "STRING" in dtype_upper:
            exprs.append(f"TRIM({qraw}, ' \t\r\n') AS {qclean}")
        else:
            exprs.append(f"{qraw} AS {qclean}")
    return ", ".join(exprs)
