from __future__ import annotations

from typing import Any


ALLOWED_READ_CSV_KEYS = {
    "delim",
    "header",
    "encoding",
    "decimal",
    "skip",
    "auto_detect",
    "quote",
    "escape",
    "comment",
    "ignore_errors",
    "strict_mode",
    "null_padding",
    "parallel",
    "nullstr",
    "mode",
    "glob",
    "prefer_from_raw_run",
    "allow_ambiguous",
    "include",
    "csv",
    "columns",
    "normalize_rows_to_columns",
    "trim_whitespace",
    "sample_size",
    "sheet_name",
}
ALLOWED_NESTED_CSV_KEYS = {
    "delim",
    "header",
    "encoding",
    "decimal",
    "skip",
    "auto_detect",
    "quote",
    "escape",
    "comment",
    "ignore_errors",
    "strict_mode",
    "null_padding",
    "parallel",
    "nullstr",
    "columns",
    "normalize_rows_to_columns",
    "trim_whitespace",
    "sheet_name",
}
FORMAT_HINT_KEYS = {
    "delim",
    "header",
    "encoding",
    "decimal",
    "skip",
    "quote",
    "escape",
    "comment",
    "nullstr",
    "parallel",
    "columns",
    "normalize_rows_to_columns",
    "trim_whitespace",
}
READ_SELECTION_KEYS = {"mode", "glob", "prefer_from_raw_run", "allow_ambiguous", "include"}
READ_SOURCE_MODES = {"auto", "config_only"}


def sql_str(value: object) -> str:
    return str(value).replace("'", "''")


def normalize_encoding(enc: str | None) -> str | None:
    if enc is None:
        return None
    e = enc.strip()
    if e.lower() == "latin1":
        return "latin-1"
    if e.lower() == "utf8":
        return "utf-8"
    if e.lower() in {"win1252", "windows1252"}:
        return "CP1252"
    return e


def normalize_columns_spec(columns: object) -> dict[str, str] | None:
    if columns is None:
        return None
    if isinstance(columns, dict):
        normalized: dict[str, str] = {}
        for name, dtype in columns.items():
            if not isinstance(name, str) or not isinstance(dtype, str):
                raise ValueError("clean.read.columns mapping must be {name: duckdb_type}")
            normalized[name] = dtype
        return normalized
    if isinstance(columns, list):
        normalized = {}
        for item in columns:
            if not isinstance(item, dict):
                raise ValueError(
                    "clean.read.columns list entries must be mappings with name and type"
                )
            name = item.get("name")
            dtype = item.get("type")
            if not isinstance(name, str) or not isinstance(dtype, str):
                raise ValueError(
                    "clean.read.columns list entries must include string name and type"
                )
            normalized[name] = dtype
        return normalized
    raise ValueError(
        "clean.read.columns must be a mapping or a list of {name, type} mappings"
    )


def normalize_read_cfg(read_cfg: dict[str, Any] | None) -> dict[str, Any]:
    cfg = dict(read_cfg or {})
    csv_cfg = cfg.get("csv") or {}
    if csv_cfg and not isinstance(csv_cfg, dict):
        raise ValueError(
            "clean.read must be a mapping (dict) in dataset.yml; "
            "legacy clean.read.csv must also be a mapping if used"
        )

    unknown_top = sorted(set(cfg.keys()) - ALLOWED_READ_CSV_KEYS)
    if unknown_top:
        raise ValueError(
            "Unsupported clean.read options for CSV reader: "
            f"{unknown_top}. Allowed keys: {sorted(ALLOWED_READ_CSV_KEYS)}"
        )

    if csv_cfg:
        unknown_nested = sorted(set(csv_cfg.keys()) - ALLOWED_NESTED_CSV_KEYS)
        if unknown_nested:
            raise ValueError(
                "Unsupported legacy clean.read.csv options: "
                f"{unknown_nested}. Allowed keys: {sorted(ALLOWED_NESTED_CSV_KEYS)}"
            )

    merged = dict(csv_cfg)
    for key in ALLOWED_NESTED_CSV_KEYS:
        if key in cfg:
            merged[key] = cfg[key]
    merged["columns"] = normalize_columns_spec(merged.get("columns"))
    return merged


def filter_suggested_format_keys(cfg: dict[str, Any] | None) -> dict[str, Any]:
    filtered = {key: value for key, value in dict(cfg or {}).items() if key in FORMAT_HINT_KEYS}
    return normalize_read_cfg(filtered)


def merge_read_cfg(
    *,
    source: str,
    suggested: dict[str, Any] | None = None,
    overrides: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    normalized_source = str(source or "auto")
    if normalized_source not in READ_SOURCE_MODES:
        raise ValueError("clean.read source must be one of: auto, config_only")

    merged = normalize_read_cfg({})
    params_source = ["defaults"]

    if normalized_source == "auto":
        filtered_suggested = filter_suggested_format_keys(suggested)
        if filtered_suggested:
            merged.update(filtered_suggested)
            params_source.append("suggested")

    normalized_overrides = normalize_read_cfg(overrides)
    override_values = {
        key: value for key, value in normalized_overrides.items() if value is not None
    }
    if override_values:
        merged.update(override_values)
        params_source.append("config_overrides")

    return merged, params_source


def robust_preset(read_cfg: dict[str, Any] | None) -> dict[str, Any]:
    robust = dict(read_cfg or {})
    robust.setdefault("ignore_errors", True)
    robust.setdefault("null_padding", True)
    robust.setdefault("strict_mode", False)
    robust.setdefault("sample_size", -1)
    return robust
