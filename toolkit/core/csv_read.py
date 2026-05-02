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
    "columns",
    "normalize_rows_to_columns",
    "trim_whitespace",
    "sample_size",
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
    # ISO-8859-1 variants — normalize to DuckDB's expected form
    if e.lower() in {"iso-8859-1", "iso8859-1"}:
        return "latin-1"
    # ASCII normalization
    if e.lower() == "ascii":
        return "us-ascii"
    return e


def normalize_columns_spec(columns: object) -> dict[str, str] | None:
    if columns is None:
        return None
    if isinstance(columns, dict):
        normalized: dict[str, str] = {}
        for name, dtype in columns.items():
            if not isinstance(name, str) and not isinstance(dtype, str):
                raise ValueError(
                    f"clean.read.columns mapping must be {{name: duckdb_type}}, "
                    f"got column name={name!r} (type={type(name).__name__}) and "
                    f"dtype={dtype!r} (type={type(dtype).__name__})"
                )
            if not isinstance(name, str):
                raise ValueError(
                    f"clean.read.columns mapping must be {{name: duckdb_type}}, "
                    f"got column name={name!r} with type={type(name).__name__}"
                )
            if not isinstance(dtype, str):
                raise ValueError(
                    f"clean.read.columns mapping must be {{name: duckdb_type}}, "
                    f"got dtype={dtype!r} with type={type(dtype).__name__} for column '{name}'"
                )
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
                    f"clean.read.columns list entries must include string name and type, "
                    f"got name={name!r} with type={type(name).__name__}, "
                    f"dtype={dtype!r} with type={type(dtype).__name__}"
                )
            normalized[name] = dtype
        return normalized
    raise ValueError("clean.read.columns must be a mapping or a list of {name, type} mappings")


def _validate_nullstr(value: object | None) -> None:
    """Validate nullstr: must be a string or a list of strings.

    None is allowed (no null marker). Numbers and other types are rejected
    because they would be stringified incorrectly by DuckDB.
    """
    if value is None:
        return
    if isinstance(value, str):
        return
    if isinstance(value, list):
        for i, item in enumerate(value):
            if not isinstance(item, str):
                raise ValueError(
                    f"clean.read.nullstr list entries must be strings, "
                    f"got item {i}: {item!r} with type={type(item).__name__}"
                )
        return
    raise ValueError(
        f"clean.read.nullstr must be a string or list of strings, "
        f"got {value!r} with type={type(value).__name__}"
    )


def normalize_read_cfg(read_cfg: dict[str, Any] | None) -> dict[str, Any]:
    cfg = dict(read_cfg or {})
    if "csv" in cfg:
        raise ValueError("clean.read.csv is no longer supported; use clean.read.* directly")

    unknown_top = sorted(set(cfg.keys()) - ALLOWED_READ_CSV_KEYS)
    if unknown_top:
        raise ValueError(
            "Unsupported clean.read options for CSV reader: "
            f"{unknown_top}. Allowed keys: {sorted(ALLOWED_READ_CSV_KEYS)}"
        )
    _validate_nullstr(cfg.get("nullstr"))
    cfg["columns"] = normalize_columns_spec(cfg.get("columns"))
    return cfg


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


def csv_read_option_strings(read_cfg: dict[str, Any]) -> list[str]:
    """Build a list of DuckDB read_csv option strings from a config dict.

    This is the single canonical place where read_csv options are converted
    to SQL fragments.  Callers can prepend their own fixed options (e.g.
    ``union_by_name=true``) and append caller-specific ones (e.g. columns).

    Supported keys:
    ``delim``, ``sep``, ``encoding``, ``decimal``, ``nullstr``,
    ``auto_detect``, ``strict_mode``, ``ignore_errors``, ``null_padding``,
    ``parallel``, ``quote``, ``escape``, ``comment``, ``max_line_size``,
    ``columns``.

    ``header`` and ``skip`` are intentionally NOT handled here because their
    effective values can be overridden at runtime by the clean layer when
    explicit columns are provided (see ``duckdb_read._csv_read_options``).
    """
    opts: list[str] = []

    delim = read_cfg.get("sep") or read_cfg.get("delim")
    if delim is not None:
        opts.append(f"sep='{sql_str(str(delim))}'")

    encoding = normalize_encoding(read_cfg.get("encoding"))
    if encoding is not None:
        opts.append(f"encoding='{sql_str(encoding)}'")

    decimal = read_cfg.get("decimal")
    if decimal is not None:
        opts.append(f"decimal_separator='{sql_str(str(decimal))}'")

    nullstr = read_cfg.get("nullstr")
    if nullstr is not None:
        if isinstance(nullstr, list):
            xs = ", ".join([f"'{sql_str(x)}'" for x in nullstr])
            opts.append(f"nullstr=[{xs}]")
        else:
            opts.append(f"nullstr='{sql_str(nullstr)}'")

    auto_detect = read_cfg.get("auto_detect")
    if auto_detect is not None:
        opts.append(f"auto_detect={'true' if bool(auto_detect) else 'false'}")

    strict_mode = read_cfg.get("strict_mode")
    if strict_mode is not None:
        opts.append(f"strict_mode={'true' if bool(strict_mode) else 'false'}")

    ignore_errors = read_cfg.get("ignore_errors")
    if ignore_errors is not None:
        opts.append(f"ignore_errors={'true' if bool(ignore_errors) else 'false'}")

    null_padding = read_cfg.get("null_padding")
    if null_padding is not None:
        opts.append(f"null_padding={'true' if bool(null_padding) else 'false'}")

    parallel = read_cfg.get("parallel")
    if parallel is not None:
        opts.append(f"parallel={'true' if bool(parallel) else 'false'}")

    quote = read_cfg.get("quote")
    if quote is not None:
        opts.append(f"quote='{sql_str(str(quote))}'")

    escape = read_cfg.get("escape")
    if escape is not None:
        opts.append(f"escape='{sql_str(str(escape))}'")

    comment = read_cfg.get("comment")
    if comment is not None:
        opts.append(f"comment='{sql_str(str(comment))}'")

    max_line_size = read_cfg.get("max_line_size")
    if max_line_size is not None:
        opts.append(f"max_line_size={int(max_line_size)}")

    columns = read_cfg.get("columns")
    if columns:
        if isinstance(columns, dict):
            cols_sql = ", ".join(
                [f"'{sql_str(name)}': '{sql_str(dtype)}'" for name, dtype in columns.items()]
            )
            opts.append(f"columns={{ {cols_sql} }}")

    return opts
