from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import yaml
from toolkit.clean.read_config import (
    _read_source_mode,
    _split_read_cfg,
    filter_suggested_read,
    load_suggested_read,
    resolve_clean_read_cfg,
)
from toolkit.core.csv_read import (
    csv_read_option_strings,
    normalize_encoding,
    normalize_read_cfg,
    robust_preset,
)


SUPPORTED_INPUT_EXTS = {
    ".csv",
    ".tsv",
    ".txt",
    ".parquet",
    ".csv.gz",
    ".tsv.gz",
    ".txt.gz",
    ".xlsx",
    ".nt.gz",
}


@dataclass(frozen=True)
class ReadInfo:
    source: str
    params_used: dict[str, Any]





def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_path(p: Path) -> str:
    s = p.resolve().as_posix()
    return s.replace("'", "''")


def quote_list(paths: list[Path]) -> str:
    return ", ".join([f"'{sql_path(p)}'" for p in paths])


def csv_trim_projection(columns: dict[str, str]) -> str:
    exprs: list[str] = []
    for name, dtype in columns.items():
        qname = q_ident(name)
        dtype_upper = dtype.upper()
        if "CHAR" in dtype_upper or "TEXT" in dtype_upper or "STRING" in dtype_upper:
            exprs.append(f"TRIM({qname}, ' \t\r\n') AS {qname}")
        else:
            exprs.append(qname)
    return ", ".join(exprs)


def _csv_read_options(
    read_cfg: dict[str, Any],
) -> tuple[list[str], dict[str, Any], dict[str, str] | None]:
    """Build DuckDB read_csv options, track params, extract source columns.

    Delegates the option-string building to the shared
    ``csv_read_option_strings`` in ``core/csv_read.py``.
    The header/skip override logic (triggered when explicit columns are
    provided) stays here because it is a runtime concern specific to the
    clean layer.
    """
    header = read_cfg.get("header", True)
    skip = read_cfg.get("skip")
    columns = read_cfg.get("columns")

    source_columns = dict(columns) if columns else None

    # Runtime override: when explicit columns are set, force header=false
    # and bump skip by 1 so the header row is consumed as data.
    parser_header = bool(header)
    parser_skip = int(skip) if skip is not None else None
    if source_columns and parser_header:
        parser_header = False
        parser_skip = (parser_skip or 0) + 1

    # Build the shared option strings, then prepend union_by_name and
    # append header/skip (which are layer-specific).
    opts = ["union_by_name=true"] + csv_read_option_strings(read_cfg)
    opts.append(f"header={'true' if parser_header else 'false'}")
    if parser_skip is not None:
        opts.append(f"skip={parser_skip}")

    # Build params_used for logging/metadata
    params_used: dict[str, Any] = {}
    for key in (
        "delim",
        "sep",
        "encoding",
        "decimal",
        "nullstr",
        "auto_detect",
        "strict_mode",
        "ignore_errors",
        "null_padding",
        "parallel",
        "quote",
        "escape",
        "comment",
        "max_line_size",
        "columns",
    ):
        val = read_cfg.get(key)
        if val is not None:
            if key == "columns" and isinstance(val, dict):
                params_used[key] = dict(val)
            else:
                params_used[key] = val
    params_used["header"] = parser_header
    if parser_skip is not None:
        params_used["skip"] = parser_skip

    return opts, params_used, source_columns


def _execute_csv_read(
    con: duckdb.DuckDBPyConnection,
    input_files: list[Path],
    read_cfg: dict[str, Any],
) -> dict[str, Any]:
    if read_cfg.get("normalize_rows_to_columns"):
        return _execute_normalized_csv_read(con, input_files, read_cfg)

    paths = quote_list(input_files)
    trim_whitespace = read_cfg.get("trim_whitespace", True)
    sample_size = read_cfg.get("sample_size")

    opts, params_used, source_columns = _csv_read_options(read_cfg)
    if sample_size is not None:
        opts.append(f"sample_size={int(sample_size)}")
        params_used["sample_size"] = int(sample_size)

    opt_sql = ", ".join(opts)
    if source_columns:
        con.execute(
            f"CREATE OR REPLACE VIEW raw_input_source AS "
            f"SELECT * FROM read_csv([{paths}], {opt_sql});"
        )
        if trim_whitespace:
            projection = csv_trim_projection(source_columns)
        else:
            projection = ", ".join(q_ident(name) for name in source_columns)
        con.execute(
            f"CREATE OR REPLACE VIEW raw_input AS SELECT {projection} FROM raw_input_source;"
        )
    else:
        con.execute(
            f"CREATE OR REPLACE VIEW raw_input AS SELECT * FROM read_csv([{paths}], {opt_sql});"
        )
    params_used["trim_whitespace"] = bool(trim_whitespace)
    return params_used


def _normalized_csv_reader_kwargs(read_cfg: dict[str, Any]) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "delimiter": read_cfg.get("delim") or ",",
    }
    quote = read_cfg.get("quote")
    if quote is not None:
        kwargs["quotechar"] = quote
    escape = read_cfg.get("escape")
    if escape is not None:
        kwargs["escapechar"] = escape
    return kwargs


def _load_normalized_csv_frame(
    input_file: Path,
    read_cfg: dict[str, Any],
    columns: dict[str, str],
) -> pd.DataFrame:
    encoding = normalize_encoding(read_cfg.get("encoding")) or "utf-8"
    trim_whitespace = bool(read_cfg.get("trim_whitespace", True))
    header = bool(read_cfg.get("header", True))
    skip = int(read_cfg.get("skip") or 0)
    expected_names = list(columns.keys())
    expected_len = len(expected_names)
    skip_rows = skip + (1 if header else 0)

    rows: list[list[Any]] = []
    with input_file.open("r", encoding=encoding, newline="") as handle:
        reader = csv.reader(handle, **_normalized_csv_reader_kwargs(read_cfg))
        for _ in range(skip_rows):
            try:
                next(reader)
            except StopIteration:
                break
        for row_number, row in enumerate(reader, start=skip_rows + 1):
            if len(row) > expected_len:
                raise ValueError(
                    "CSV row wider than configured columns while normalize_rows_to_columns=true. "
                    f"file={input_file} row={row_number} configured={expected_len} actual={len(row)}"
                )
            if len(row) < expected_len:
                row = list(row) + [""] * (expected_len - len(row))
            else:
                row = list(row)
            if trim_whitespace:
                row = [value.strip() if isinstance(value, str) else value for value in row]
            rows.append(row)

    return pd.DataFrame(rows, columns=expected_names)


def _execute_normalized_csv_read(
    con: duckdb.DuckDBPyConnection,
    input_files: list[Path],
    read_cfg: dict[str, Any],
) -> dict[str, Any]:
    columns = read_cfg.get("columns")
    if not columns:
        raise ValueError("clean.read.normalize_rows_to_columns=true requires clean.read.columns")

    frames = [
        _load_normalized_csv_frame(input_file, read_cfg, columns) for input_file in input_files
    ]
    combined = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    con.register("raw_input_df", combined)
    con.execute("CREATE OR REPLACE VIEW raw_input AS SELECT * FROM raw_input_df;")

    params_used: dict[str, Any] = {
        "columns": dict(columns),
        "normalize_rows_to_columns": True,
        "trim_whitespace": bool(read_cfg.get("trim_whitespace", True)),
        "header": bool(read_cfg.get("header", True)),
    }
    if read_cfg.get("delim") is not None:
        params_used["delim"] = read_cfg.get("delim")
    if read_cfg.get("encoding") is not None:
        params_used["encoding"] = normalize_encoding(read_cfg.get("encoding"))
    if read_cfg.get("skip") is not None:
        params_used["skip"] = int(read_cfg.get("skip"))
    if read_cfg.get("quote") is not None:
        params_used["quote"] = read_cfg.get("quote")
    if read_cfg.get("escape") is not None:
        params_used["escape"] = read_cfg.get("escape")
    return params_used


def _execute_parquet_read(
    con: duckdb.DuckDBPyConnection,
    input_files: list[Path],
) -> ReadInfo:
    if len(input_files) == 1:
        con.execute(
            f"CREATE VIEW raw_input AS SELECT * FROM read_parquet('{sql_path(input_files[0])}');"
        )
    else:
        paths = quote_list(input_files)
        con.execute(f"CREATE VIEW raw_input AS SELECT * FROM read_parquet([{paths}]);")
    return ReadInfo(source="parquet", params_used={})


def _normalize_excel_sheet_name(value: Any) -> str | int:
    if value is None:
        return 0
    if isinstance(value, bool):
        raise ValueError("clean.read.sheet_name must be a string, integer, or null")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        return text
    raise ValueError("clean.read.sheet_name must be a string, integer, or null")


def _trim_excel_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(
        lambda column: column.map(lambda value: value.strip() if isinstance(value, str) else value)
    )


def _load_excel_frame(
    input_file: Path,
    read_cfg: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    header = bool(read_cfg.get("header", True))
    skip = int(read_cfg["skip"]) if read_cfg.get("skip") is not None else 0
    trim_whitespace = read_cfg.get("trim_whitespace", True)
    columns = read_cfg.get("columns")
    sheet_name = _normalize_excel_sheet_name(read_cfg.get("sheet_name"))

    df = pd.read_excel(
        input_file,
        sheet_name=sheet_name,
        header=0 if header else None,
        skiprows=skip,
        dtype=object,
        engine="openpyxl",
    )

    if columns:
        expected_columns = list(columns.keys())
        if len(expected_columns) != len(df.columns):
            raise ValueError(
                "Excel input columns mismatch. "
                f"Configured={len(expected_columns)} detected={len(df.columns)} file={input_file}"
            )
        df.columns = expected_columns
    elif not header:
        df.columns = [f"col{i}" for i in range(len(df.columns))]

    if trim_whitespace:
        df = _trim_excel_dataframe(df)

    return df, {
        "sheet_name": sheet_name,
        "header": header,
        "skip": skip,
        "trim_whitespace": bool(trim_whitespace),
        "columns": dict(columns) if columns else None,
    }


def _execute_excel_read(
    con: duckdb.DuckDBPyConnection,
    input_files: list[Path],
    read_cfg: dict[str, Any],
    *,
    logger,
) -> ReadInfo:
    frames: list[pd.DataFrame] = []
    params_used: dict[str, Any] | None = None

    for input_file in input_files:
        frame, frame_params = _load_excel_frame(input_file, read_cfg)
        frames.append(frame)
        if params_used is None:
            params_used = frame_params

    combined = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    con.register("raw_input_df", combined)
    con.execute("CREATE OR REPLACE VIEW raw_input AS SELECT * FROM raw_input_df;")

    used = dict(params_used or {})
    if used.get("columns") is None:
        used.pop("columns", None)
    logger.info(
        "read_excel params used: source=excel params=%s",
        json.dumps(used, ensure_ascii=False, sort_keys=True),
    )
    return ReadInfo(source="excel", params_used=used)


def _validate_read_mode(mode: str) -> str:
    normalized_mode = str(mode or "fallback")
    if normalized_mode not in {"strict", "fallback", "robust"}:
        raise ValueError("clean.read_mode must be one of: strict, fallback, robust")
    return normalized_mode


def _read_failure_message(
    *,
    input_file: Path,
    read_cfg: dict[str, Any],
) -> str:
    return (
        "Failed to read CLEAN CSV input. "
        f"selected_input={input_file} "
        f"read_cfg={json.dumps(read_cfg, ensure_ascii=False, sort_keys=True)}. "
        "Try setting clean.read.columns or clean.read.source, "
        "or adjusting quote/escape/comment/ignore_errors"
    )


def _execute_csv_mode(
    con: duckdb.DuckDBPyConnection,
    input_files: list[Path],
    read_cfg: dict[str, Any],
    *,
    source: str,
    logger,
) -> ReadInfo:
    params_used = _execute_csv_read(con, input_files, read_cfg)
    logger.info(
        "read_csv params used: source=%s params=%s",
        source,
        json.dumps(params_used, ensure_ascii=False, sort_keys=True),
    )
    return ReadInfo(source=source, params_used=params_used)


def _read_csv_relation(
    con: duckdb.DuckDBPyConnection,
    input_files: list[Path],
    read_cfg: dict[str, Any],
    *,
    mode: str,
    logger,
) -> ReadInfo:
    if mode == "robust":
        return _execute_csv_mode(
            con,
            input_files,
            robust_preset(read_cfg),
            source="robust",
            logger=logger,
        )

    try:
        return _execute_csv_mode(
            con,
            input_files,
            read_cfg,
            source="strict",
            logger=logger,
        )
    except Exception as exc:
        if mode == "strict":
            raise ValueError(
                _read_failure_message(input_file=input_files[0], read_cfg=read_cfg)
            ) from exc

        short_msg = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "strict read failed, falling back to robust | input=%s exc=%s",
            input_files[0],
            short_msg,
        )
        robust_cfg = robust_preset(read_cfg)
        try:
            return _execute_csv_mode(
                con,
                input_files,
                robust_cfg,
                source="robust",
                logger=logger,
            )
        except Exception as robust_exc:
            raise ValueError(
                _read_failure_message(input_file=input_files[0], read_cfg=robust_cfg)
            ) from robust_exc


def read_raw_to_relation(
    con: duckdb.DuckDBPyConnection,
    input_files: list[Path],
    params: dict[str, Any] | None,
    mode: str,
    logger,
) -> ReadInfo:
    read_cfg = normalize_read_cfg(params)
    if not input_files:
        raise FileNotFoundError(
            "No supported input files found for CLEAN "
            f"(expected one of: {sorted(SUPPORTED_INPUT_EXTS)})."
        )

    exts = {p.suffix.lower() for p in input_files}
    if exts <= {".parquet"}:
        info = _execute_parquet_read(con, input_files)
        logger.info("read_csv params used: source=parquet params={}")
        return info
    if exts <= {".xlsx"}:
        return _execute_excel_read(con, input_files, read_cfg, logger=logger)

    normalized_mode = _validate_read_mode(mode)
    return _read_csv_relation(
        con,
        input_files,
        read_cfg,
        mode=normalized_mode,
        logger=logger,
    )
