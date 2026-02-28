from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import yaml
from toolkit.core.csv_read import (
    READ_SELECTION_KEYS,
    READ_SOURCE_MODES,
    filter_suggested_format_keys,
    merge_read_cfg,
    normalize_encoding,
    normalize_read_cfg,
    robust_preset,
    sql_str,
)


SUPPORTED_INPUT_EXTS = {".csv", ".tsv", ".txt", ".parquet", ".csv.gz", ".tsv.gz", ".txt.gz"}


@dataclass(frozen=True)
class ReadInfo:
    source: str
    params_used: dict[str, Any]


def load_suggested_read(raw_year_dir: Path) -> dict[str, Any] | None:
    suggested_path = raw_year_dir / "_profile" / "suggested_read.yml"
    if not suggested_path.exists():
        return None

    payload = yaml.safe_load(suggested_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None

    clean_cfg = payload.get("clean")
    if not isinstance(clean_cfg, dict):
        return None

    read_cfg = clean_cfg.get("read")
    if not isinstance(read_cfg, dict):
        return None

    return dict(read_cfg)


def filter_suggested_read(cfg: dict[str, Any] | None) -> dict[str, Any]:
    return filter_suggested_format_keys(cfg)


def resolve_clean_read_cfg(
    raw_year_dir: Path,
    clean_cfg: dict[str, Any],
    logger=None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    raw_read_cfg = clean_cfg.get("read")
    read_source = clean_cfg.get("read_source")
    explicit_cfg: dict[str, Any] = {}

    if raw_read_cfg is None:
        pass
    elif isinstance(raw_read_cfg, str):
        if logger is not None:
            logger.warning(
                "clean.read scalar form is deprecated; use clean.read.source"
            )
        read_source = raw_read_cfg
    elif isinstance(raw_read_cfg, dict):
        explicit_cfg = dict(raw_read_cfg)
        nested_source = explicit_cfg.pop("source", None)
        if nested_source is not None:
            read_source = nested_source
    else:
        raise ValueError("clean.read must be either a mapping (dict) or one of: auto, config_only")

    normalized_source = str(read_source or "auto")
    if normalized_source not in READ_SOURCE_MODES:
        raise ValueError("clean.read source must be one of: auto, config_only")

    selection_cfg = dict(explicit_cfg)
    relation_overrides = {
        key: value for key, value in explicit_cfg.items() if key not in READ_SELECTION_KEYS
    }

    suggested_cfg = load_suggested_read(raw_year_dir)
    filtered_suggested = filter_suggested_read(suggested_cfg)
    if normalized_source == "auto" and filtered_suggested and logger is not None:
        logger.info(
            "CLEAN read hints loaded from suggested_read.yml: %s",
            json.dumps(filtered_suggested, ensure_ascii=False, sort_keys=True),
        )

    merged_relation_cfg, params_source = merge_read_cfg(
        source=normalized_source,
        suggested=suggested_cfg,
        overrides=relation_overrides,
    )

    return selection_cfg, merged_relation_cfg, params_source


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


def _csv_read_options(read_cfg: dict[str, Any]) -> tuple[list[str], dict[str, Any], dict[str, str] | None]:
    delim = read_cfg.get("delim")
    encoding = normalize_encoding(read_cfg.get("encoding"))
    decimal = read_cfg.get("decimal")
    header = read_cfg.get("header", True)
    skip = read_cfg.get("skip")
    nullstr = read_cfg.get("nullstr")
    auto_detect = read_cfg.get("auto_detect")
    strict_mode = read_cfg.get("strict_mode")
    ignore_errors = read_cfg.get("ignore_errors")
    null_padding = read_cfg.get("null_padding")
    quote = read_cfg.get("quote")
    escape = read_cfg.get("escape")
    comment = read_cfg.get("comment")
    columns = read_cfg.get("columns")

    opts = ["union_by_name=true"]
    params_used: dict[str, Any] = {}

    if delim is not None:
        opts.append(f"sep='{sql_str(delim)}'")
        params_used["delim"] = delim
    if encoding is not None:
        opts.append(f"encoding='{sql_str(encoding)}'")
        params_used["encoding"] = encoding
    if decimal is not None:
        opts.append(f"decimal_separator='{sql_str(decimal)}'")
        params_used["decimal"] = decimal
    if nullstr is not None:
        if isinstance(nullstr, list):
            xs = ", ".join([f"'{sql_str(x)}'" for x in nullstr])
            opts.append(f"nullstr=[{xs}]")
        else:
            opts.append(f"nullstr='{sql_str(nullstr)}'")
        params_used["nullstr"] = nullstr
    if auto_detect is not None:
        opts.append(f"auto_detect={'true' if bool(auto_detect) else 'false'}")
        params_used["auto_detect"] = bool(auto_detect)
    if strict_mode is not None:
        opts.append(f"strict_mode={'true' if bool(strict_mode) else 'false'}")
        params_used["strict_mode"] = bool(strict_mode)
    if ignore_errors is not None:
        opts.append(f"ignore_errors={'true' if bool(ignore_errors) else 'false'}")
        params_used["ignore_errors"] = bool(ignore_errors)
    if null_padding is not None:
        opts.append(f"null_padding={'true' if bool(null_padding) else 'false'}")
        params_used["null_padding"] = bool(null_padding)
    if quote is not None:
        opts.append(f"quote='{sql_str(quote)}'")
        params_used["quote"] = quote
    if escape is not None:
        opts.append(f"escape='{sql_str(escape)}'")
        params_used["escape"] = escape
    if comment is not None:
        opts.append(f"comment='{sql_str(comment)}'")
        params_used["comment"] = comment

    source_columns = None
    if columns:
        source_columns = dict(columns)
        if "__extra" not in source_columns:
            source_columns["__extra"] = "VARCHAR"
        cols_sql = ", ".join(
            [f"'{sql_str(name)}': '{sql_str(dtype)}'" for name, dtype in source_columns.items()]
        )
        opts.append(f"columns={{ {cols_sql} }}")
        params_used["columns"] = dict(source_columns)

    parser_header = bool(header)
    parser_skip = int(skip) if skip is not None else None
    if source_columns and parser_header:
        parser_header = False
        parser_skip = (parser_skip or 0) + 1

    opts.append(f"header={'true' if parser_header else 'false'}")
    params_used["header"] = parser_header

    if parser_skip is not None:
        opts.append(f"skip={parser_skip}")
        params_used["skip"] = parser_skip

    return opts, params_used, source_columns


def _execute_csv_read(
    con: duckdb.DuckDBPyConnection,
    input_files: list[Path],
    read_cfg: dict[str, Any],
) -> dict[str, Any]:
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
            f"CREATE OR REPLACE VIEW raw_input AS "
            f"SELECT {projection} FROM raw_input_source;"
        )
    else:
        con.execute(
            f"CREATE OR REPLACE VIEW raw_input AS "
            f"SELECT * FROM read_csv([{paths}], {opt_sql});"
        )
    params_used["trim_whitespace"] = bool(trim_whitespace)
    return params_used


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
        if len(input_files) == 1:
            con.execute(
                f"CREATE VIEW raw_input AS "
                f"SELECT * FROM read_parquet('{sql_path(input_files[0])}');"
            )
        else:
            paths = quote_list(input_files)
            con.execute(
                f"CREATE VIEW raw_input AS "
                f"SELECT * FROM read_parquet([{paths}]);"
            )
        logger.info("read_csv params used: source=parquet params={}")
        return ReadInfo(source="parquet", params_used={})

    normalized_mode = str(mode or "fallback")
    if normalized_mode not in {"strict", "fallback", "robust"}:
        raise ValueError("clean.read_mode must be one of: strict, fallback, robust")

    try:
        if normalized_mode == "robust":
            robust_cfg = robust_preset(read_cfg)
            params_used = _execute_csv_read(con, input_files, robust_cfg)
            logger.info(
                "read_csv params used: source=robust params=%s",
                json.dumps(params_used, ensure_ascii=False, sort_keys=True),
            )
            return ReadInfo(source="robust", params_used=params_used)

        params_used = _execute_csv_read(con, input_files, read_cfg)
        logger.info(
            "read_csv params used: source=strict params=%s",
            json.dumps(params_used, ensure_ascii=False, sort_keys=True),
        )
        return ReadInfo(source="strict", params_used=params_used)
    except Exception as exc:
        if normalized_mode == "strict":
            raise ValueError(
                "Failed to read CLEAN CSV input. "
                f"selected_input={input_files[0]} "
                f"read_cfg={json.dumps(read_cfg, ensure_ascii=False, sort_keys=True)}. "
                "Try setting clean.read.columns or clean.read.source, "
                "or adjusting quote/escape/comment/ignore_errors"
            ) from exc

        short_msg = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "strict read failed, falling back to robust | input=%s exc=%s",
            input_files[0],
            short_msg,
        )
        robust_cfg = robust_preset(read_cfg)
        try:
            params_used = _execute_csv_read(con, input_files, robust_cfg)
            logger.info(
                "read_csv params used: source=robust params=%s",
                json.dumps(params_used, ensure_ascii=False, sort_keys=True),
            )
            return ReadInfo(source="robust", params_used=params_used)
        except Exception as robust_exc:
            raise ValueError(
                "Failed to read CLEAN CSV input. "
                f"selected_input={input_files[0]} "
                f"read_cfg={json.dumps(robust_cfg, ensure_ascii=False, sort_keys=True)}. "
                "Try setting clean.read.columns or clean.read.source, "
                "or adjusting quote/escape/comment/ignore_errors"
            ) from robust_exc
