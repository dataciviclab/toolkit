from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _normalize_excel_sheet_name(value: Any) -> str | int:
    """Normalize sheet_name config value to a string or integer for pd.read_excel."""
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
    """Strip whitespace from all string values in a DataFrame."""
    return df.apply(
        lambda column: column.map(lambda value: value.strip() if isinstance(value, str) else value)
    )


def _load_excel_frame(
    input_file: Path,
    read_cfg: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load a single Excel file into a DataFrame with configured columns / header / skip."""
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
    con,
    input_files: list[Path],
    read_cfg: dict[str, Any],
    *,
    logger,
) -> dict[str, Any]:
    """Execute Excel read: load each file, concatenate, register as DuckDB view ``raw_input``."""
    import json

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
    return {"source": "excel", "params_used": used}
