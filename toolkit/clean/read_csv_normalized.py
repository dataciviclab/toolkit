from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pandas as pd

from toolkit.core.csv_read import normalize_encoding


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
    con,
    input_files: list[Path],
    read_cfg: dict[str, Any],
) -> dict[str, Any]:
    """Execute CSV read with normalize_rows_to_columns=true.

    Reads each input file with the CSV reader into a pandas DataFrame,
    concatenates if multiple files, registers as DuckDB view ``raw_input``.
    """
    import duckdb

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
