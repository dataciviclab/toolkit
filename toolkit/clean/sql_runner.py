from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def _sql_path(p: Path) -> str:
    """
    Convert a filesystem path into a SQL-safe path string for DuckDB.
    Uses forward slashes (safe on Windows) and escapes single quotes.
    """
    s = p.resolve().as_posix()
    return s.replace("'", "''")


def _sql_str(value: object) -> str:
    """
    Escape a generic Python value into SQL single-quoted content.
    """
    return str(value).replace("'", "''")


def _quote_list(paths: list[Path]) -> str:
    return ", ".join([f"'{_sql_path(p)}'" for p in paths])


def _normalize_encoding(enc: str | None) -> str | None:
    if enc is None:
        return None
    e = enc.strip()
    # common aliases
    if e.lower() == "latin1":
        return "latin-1"
    if e.lower() == "utf8":
        return "utf-8"
    if e.lower() in {"win1252", "windows1252"}:
        return "CP1252"
    return e


def _filter_input_files(input_files: list[Path]) -> list[Path]:
    """
    Keep only data files (csv/tsv/txt/php/parquet + gz).
    Exclude metadata files (json/md/yml) commonly produced by RAW.
    """
    allowed = {".csv", ".tsv", ".txt", ".php", ".parquet", ".gz"}
    out = []
    for p in input_files:
        if not p.is_file():
            continue
        if p.suffix.lower() in {".json", ".md", ".yml", ".yaml"}:
            continue
        if p.suffix.lower() in allowed:
            out.append(p)
    return out


# -------------------------------------------------------------------
# Core relation builder
# -------------------------------------------------------------------

def _read_relation(
    con: duckdb.DuckDBPyConnection,
    input_files: list[Path],
    read_cfg: dict[str, Any] | None,
) -> None:
    """
    Create VIEW raw_input from input files.

    Supports:
    - Parquet
    - CSV/TSV/TXT via read_csv_auto (default)
    - CSV with explicit read options
    """

    read_cfg = dict(read_cfg or {})
    input_files = _filter_input_files(input_files)

    if not input_files:
        raise FileNotFoundError("No supported input files found for CLEAN (csv/tsv/txt/php/parquet).")

    exts = {p.suffix.lower() for p in input_files}

    # -----------------------
    # PARQUET
    # -----------------------
    if exts <= {".parquet"}:
        if len(input_files) == 1:
            con.execute(
                f"CREATE VIEW raw_input AS "
                f"SELECT * FROM read_parquet('{_sql_path(input_files[0])}');"
            )
        else:
            paths = _quote_list(input_files)
            con.execute(
                f"CREATE VIEW raw_input AS "
                f"SELECT * FROM read_parquet([{paths}]);"
            )
        return

    # -----------------------
    # CSV / TXT (and .php used as csv)
    # -----------------------
    paths = _quote_list(input_files)

    # If explicit read options provided
    if read_cfg:
        sep = read_cfg.get("sep")
        delim = read_cfg.get("delim")
        decimal = read_cfg.get("decimal")
        encoding = _normalize_encoding(read_cfg.get("encoding"))
        header = read_cfg.get("header", True)
        skip = read_cfg.get("skip")
        nullstr = read_cfg.get("nullstr")
        dateformat = read_cfg.get("dateformat")
        timestampformat = read_cfg.get("timestampformat")

        # NEW robust options
        strict_mode = read_cfg.get("strict_mode")
        ignore_errors = read_cfg.get("ignore_errors")
        null_padding = read_cfg.get("null_padding")
        max_line_size = read_cfg.get("max_line_size")
        quote = read_cfg.get("quote")
        escape = read_cfg.get("escape")
        comment = read_cfg.get("comment")
        compression = read_cfg.get("compression")

        opts = ["union_by_name=true"]

        if sep is not None:
            opts.append(f"sep='{_sql_str(sep)}'")
        elif delim is not None:
            opts.append(f"sep='{_sql_str(delim)}'")

        if decimal is not None:
            opts.append(f"decimal_separator='{_sql_str(decimal)}'")

        if encoding is not None:
            opts.append(f"encoding='{_sql_str(encoding)}'")

        if header is not None:
            opts.append(f"header={'true' if bool(header) else 'false'}")

        if skip is not None:
            opts.append(f"skip={int(skip)}")

        if nullstr is not None:
            if isinstance(nullstr, list):
                xs = ", ".join([f"'{_sql_str(x)}'" for x in nullstr])
                opts.append(f"nullstr=[{xs}]")
            else:
                opts.append(f"nullstr='{_sql_str(nullstr)}'")

        if dateformat is not None:
            opts.append(f"dateformat='{_sql_str(dateformat)}'")

        if timestampformat is not None:
            opts.append(f"timestampformat='{_sql_str(timestampformat)}'")

        # robust additions
        if strict_mode is not None:
            opts.append(f"strict_mode={'true' if bool(strict_mode) else 'false'}")

        if ignore_errors is not None:
            opts.append(f"ignore_errors={'true' if bool(ignore_errors) else 'false'}")

        if null_padding is not None:
            opts.append(f"null_padding={'true' if bool(null_padding) else 'false'}")

        if max_line_size is not None:
            opts.append(f"max_line_size={int(max_line_size)}")

        if quote is not None:
            opts.append(f"quote='{_sql_str(quote)}'")

        if escape is not None:
            opts.append(f"escape='{_sql_str(escape)}'")

        if comment is not None:
            opts.append(f"comment='{_sql_str(comment)}'")

        if compression is not None:
            opts.append(f"compression='{_sql_str(compression)}'")

        opt_sql = ", ".join(opts)

        # Use explicit read_csv with options
        con.execute(
            f"CREATE VIEW raw_input AS "
            f"SELECT * FROM read_csv([{paths}], {opt_sql});"
        )
        return

    # Default auto detection
    # Note: this can fail on “dirty” CSVs. In that case, user should provide read_cfg.
    con.execute(
        f"CREATE VIEW raw_input AS "
        f"SELECT * FROM read_csv_auto([{paths}], union_by_name=true);"
    )


# -------------------------------------------------------------------
# Public runner
# -------------------------------------------------------------------

def run_sql(
    input_files: list[Path],
    sql_query: str,
    output_path: Path,
    read_cfg: dict[str, Any] | None = None,
) -> None:
    con = duckdb.connect(":memory:")

    _read_relation(con, input_files, read_cfg)

    con.execute(f"CREATE TABLE clean_out AS {sql_query}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    con.execute(
        f"COPY clean_out TO '{_sql_path(output_path)}' (FORMAT PARQUET);"
    )