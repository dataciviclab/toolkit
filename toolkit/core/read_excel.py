from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pandas as pd


from toolkit.core.constants import RAW_INPUT_VIEW, RAW_INPUT_DF_VIEW


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
    import pandas as pd

    header = bool(read_cfg.get("header", True))
    skip = int(read_cfg["skip"]) if read_cfg.get("skip") is not None else 0
    trim_whitespace = read_cfg.get("trim_whitespace", True)
    columns = read_cfg.get("columns")
    sheet_name = _normalize_excel_sheet_name(read_cfg.get("sheet_name"))

    ext = input_file.suffix.lower()
    # Literal richiesto dagli overload pandas (engine non è str generico)
    if ext == ".xls":
        engine: Literal["xlrd", "openpyxl", "odf"] = "xlrd"
    elif ext == ".ods":
        engine = "odf"
    else:
        engine = "openpyxl"
    df = pd.read_excel(
        input_file,
        sheet_name=sheet_name,
        header=0 if header else None,
        skiprows=skip,
        dtype=object,
        engine=engine,
    )

    if columns:
        from toolkit.core.sql_utils import parse_column_value

        # Supporta formato compatto "clean_name:DUCKDB_TYPE" come nel path CSV
        resolved_names: list[str] = []
        for raw_name, value in columns.items():
            clean_name, _ = parse_column_value(raw_name, value)
            resolved_names.append(clean_name)
        if len(resolved_names) != len(df.columns):
            raise ValueError(
                "Excel input columns mismatch. "
                f"Configured={len(resolved_names)} detected={len(df.columns)} file={input_file}"
            )
        df.columns = resolved_names
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
    import pandas as pd

    frames: list[pd.DataFrame] = []
    params_used: dict[str, Any] | None = None

    for input_file in input_files:
        frame, frame_params = _load_excel_frame(input_file, read_cfg)
        frames.append(frame)
        if params_used is None:
            params_used = frame_params

    combined = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    con.register(RAW_INPUT_DF_VIEW, combined)
    con.execute(f"CREATE OR REPLACE VIEW {RAW_INPUT_VIEW} AS SELECT * FROM {RAW_INPUT_DF_VIEW};")

    used = dict(params_used or {})
    if used.get("columns") is None:
        used.pop("columns", None)
    source_label = "excel"
    if input_files and input_files[0].suffix.lower() == ".ods":
        source_label = "ods"
    logger.info(
        "read_excel params used: source=%s params=%s",
        source_label,
        json.dumps(used, ensure_ascii=False, sort_keys=True),
    )
    return {"source": source_label, "params_used": used}
