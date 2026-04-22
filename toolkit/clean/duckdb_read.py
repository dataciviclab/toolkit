from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
from toolkit.clean.read_config import resolve_clean_read_cfg as _resolve_clean_read_cfg
from toolkit.clean.read_csv_normalized import _execute_normalized_csv_read
from toolkit.clean.read_sql_utils import (
    csv_trim_projection,
    q_ident,
    quote_list,
    sql_path,
)

from toolkit.clean.read_excel import _execute_excel_read
from toolkit.core.csv_read import (
    csv_read_option_strings,
    normalize_read_cfg,
    robust_preset,
)

# Re-exported for backward compat — consumers import resolve_clean_read_cfg from duckdb_read
resolve_clean_read_cfg = _resolve_clean_read_cfg


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
        result = _execute_excel_read(con, input_files, read_cfg, logger=logger)
        return ReadInfo(source=result["source"], params_used=result["params_used"])

    normalized_mode = _validate_read_mode(mode)
    return _read_csv_relation(
        con,
        input_files,
        read_cfg,
        mode=normalized_mode,
        logger=logger,
    )
