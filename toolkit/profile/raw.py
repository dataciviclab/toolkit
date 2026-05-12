"""RAW profiling entry point.

Orchestrates encoding/delimiter sniffing, DuckDB-based column profiling,
and mapping suggestion generation. Internal sniffing logic lives in
``_sniff_encoding``, ``_sniff_delimiter``, and ``_column_profile``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
from lab_connectors.duckdb import safe_connect

from toolkit.core.csv_read import (
    csv_read_option_strings,
    normalize_read_cfg,
    robust_preset,
    sql_str,
)
from toolkit.core.io import write_json_atomic
from toolkit.profile._sniff_encoding import is_binary_file as _is_binary_file, sniff_encoding
from toolkit.profile._sniff_delimiter import sniff_decimal, sniff_delim, suggest_skip
from toolkit.profile._column_profile import _build_mapping_suggestions, _normalize_colname


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _raw_files(raw_dir: Path) -> list[Path]:
    return sorted([p for p in raw_dir.glob("*") if p.is_file()])


def _preview_columns(header_line: str | None, delim: str | None) -> list[str]:
    if not header_line or not delim:
        return []
    parts = [segment.strip() for segment in header_line.split(delim)]
    return [_normalize_colname(part) for part in parts if part.strip()]


def sniff_source_file(filepath: Path) -> Dict[str, Any]:
    """Pure source sniffing: encoding, delimiter, decimal, skip, header.

    Does not read the file with DuckDB — only inspects raw bytes/text to
    produce suggested parse parameters and the header line at the skip offset.

    Returns
    -------
    dict with keys:
        - file_used (str): filename
        - encoding_suggested (str): detected encoding
        - delim_suggested (str): detected delimiter
        - decimal_suggested (str): detected decimal separator
        - skip_suggested (int): suggested skip rows
        - header_line (str | None): header text at skip offset
        - true_header_line (str | None): header text at line 0 (for mismatch detection)
        - columns_preview (list[str]): normalised column names from header_line
        - warnings (list[str]): issues detected during sniffing
        - is_binary_file (str | None): 'xlsx' / 'xls' if binary format detected
    """
    # Binary files (XLSX/XLS) cannot be decoded as text — return early.
    binary_fmt = _is_binary_file(filepath)
    if binary_fmt:
        return {
            "file_used": filepath.name,
            "encoding_suggested": None,
            "delim_suggested": None,
            "decimal_suggested": None,
            "skip_suggested": 0,
            "header_line": None,
            "true_header_line": None,
            "columns_preview": [],
            "warnings": [f"binary_file_detected: {binary_fmt}"],
            "is_binary_file": binary_fmt,
        }

    enc, txt = sniff_encoding(filepath)
    delim = sniff_delim(txt)
    dec = sniff_decimal(txt)
    skip = suggest_skip(txt, delim)
    warnings: list[str] = []

    if skip:
        warnings.append(
            "header_preamble_detected: first non-empty line looks like a title row, consider skip: 1"
        )

    header_line: str | None = None
    true_header_line: str | None = None

    # Read header at skip offset (where DuckDB would start reading data).
    # This preserves backward-compatible header_line for suggested_read.
    try:
        with filepath.open("r", encoding=enc, errors="replace") as f:
            for _ in range(skip):
                f.readline()
            header_line = f.readline().rstrip("\n\r")
    except Exception as exc:
        warnings.append(f"header_read_failed: {type(exc).__name__}: {exc}")

    # Also read the true file header from line 0 (independent of skip).
    # This is the ground-truth header row used for mismatch detection:
    # if header at line 0 has fewer tokens than data columns returned by
    # DESCRIBE, the file has extra cols in data rows (IRPEF comunale pattern).
    try:
        with filepath.open("r", encoding=enc, errors="replace") as f:
            true_header_line = f.readline().rstrip("\n\r")
    except Exception:
        pass  # already logged above; degrade gracefully

    return {
        "file_used": filepath.name,
        "encoding_suggested": enc,
        "delim_suggested": delim,
        "decimal_suggested": dec,
        "skip_suggested": skip,
        "header_line": header_line,
        "true_header_line": true_header_line,
        "columns_preview": _preview_columns(header_line, delim),
        "warnings": warnings,
        "is_binary_file": None,
    }


# Backward-compatible alias — new code should use sniff_source_file directly.
build_profile_hints = sniff_source_file


def build_suggested_read_cfg(
    profile: "RawProfile | Dict[str, Any]",
    read_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    data = profile if isinstance(profile, dict) else asdict(profile)
    cfg: Dict[str, Any] = {}

    source_cfg = dict(read_cfg or {})
    for key in (
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
        "nullstr",
        "columns",
        "trim_whitespace",
        "sample_size",
    ):
        if key in source_cfg:
            cfg[key] = source_cfg[key]

    if "delim" not in cfg and data.get("delim_suggested") is not None:
        cfg["delim"] = data["delim_suggested"]
    if "decimal" not in cfg and data.get("decimal_suggested") is not None:
        cfg["decimal"] = data["decimal_suggested"]
    if "encoding" not in cfg and data.get("encoding_suggested") is not None:
        cfg["encoding"] = data["encoding_suggested"]
    if "skip" not in cfg and int(data.get("skip_suggested") or 0) > 0:
        cfg["skip"] = int(data["skip_suggested"])

    cfg.setdefault("header", True)

    if data.get("robust_read_suggested"):
        cfg.setdefault("auto_detect", False)
        cfg.setdefault("strict_mode", False)
        cfg.setdefault("null_padding", True)
        cfg.setdefault("ignore_errors", True)

    return normalize_read_cfg(cfg)


def write_suggested_read_yml(out_dir: Path, profile: "RawProfile | Dict[str, Any]") -> Path:
    _safe_mkdir(out_dir)
    suggested_read = build_suggested_read_cfg(profile)

    lines = ["clean:", "  read:"]
    for key, value in suggested_read.items():
        if isinstance(value, str):
            escaped = value.replace('"', '\\"')
            rendered = f'"{escaped}"'
        elif isinstance(value, bool):
            rendered = "true" if value else "false"
        elif value is None:
            rendered = "null"
        else:
            rendered = str(value)
        lines.append(f"    {key}: {rendered}")

    p = out_dir / "suggested_read.yml"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def _pick_data_file(files: List[Path]) -> Path:
    preferred = [p for p in files if p.suffix.lower() in {".csv", ".tsv", ".txt", ".php", ".gz"}]
    if preferred:
        return preferred[0]
    for p in files:
        if p.suffix.lower() not in {".json", ".md", ".yml", ".yaml"}:
            return p
    return files[0]


def _effective_profile_read_cfg(
    read_cfg: Optional[Dict[str, Any]],
    *,
    encoding: str,
    delim: Optional[str],
    decimal: Optional[str],
    skip: int,
) -> dict[str, Any]:
    effective_read_cfg = dict(read_cfg) if isinstance(read_cfg, dict) else {}
    effective_read_cfg.pop("source", None)
    if "delim" not in effective_read_cfg and "sep" not in effective_read_cfg and delim:
        effective_read_cfg["delim"] = delim
    if "encoding" not in effective_read_cfg and encoding:
        effective_read_cfg["encoding"] = encoding
    if "decimal" not in effective_read_cfg and decimal:
        effective_read_cfg["decimal"] = decimal
    if "skip" not in effective_read_cfg and skip:
        effective_read_cfg["skip"] = skip
    effective_read_cfg.setdefault("header", True)
    return effective_read_cfg


def _profile_view(
    con: duckdb.DuckDBPyConnection,
    file0: Path,
    *,
    effective_read_cfg: dict[str, Any],
) -> None:
    opts = csv_read_option_strings(effective_read_cfg, include_header_skip=True)
    opt_sql = f"union_by_name=true, {', '.join(opts)}"
    con.execute(
        f"CREATE OR REPLACE VIEW v AS SELECT * FROM read_csv('{sql_str(str(file0))}', {opt_sql});"
    )


def _describe_columns(con: duckdb.DuckDBPyConnection) -> tuple[list[str], list[str], list[str]]:
    cols = con.execute("DESCRIBE v").fetchall()
    columns_raw = [r[0] for r in cols]
    columns_norm = [_normalize_colname(c) for c in columns_raw]
    duckdb_types = [r[1] for r in cols]  # DuckDB-inferred types
    return columns_raw, columns_norm, duckdb_types


def _sample_profile_rows(
    con: duckdb.DuckDBPyConnection,
    columns_raw: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    df = con.execute("SELECT * FROM v LIMIT 50").fetchdf()
    sample_rows = df.to_dict(orient="records")

    # Single-pass missingness: one query with all columns in CASE expressions.
    # Avoids O(N) separate queries (N = columns), which was slow for 50+ column files.
    missingness_top: list[dict[str, Any]] = []
    cols_to_profile = columns_raw[:200]
    if cols_to_profile:
        case_exprs: list[str] = []
        for c in cols_to_profile:
            col_ref = f'"{c}"'
            case_exprs.append(
                f'SUM(CASE WHEN {col_ref} IS NULL OR TRIM(CAST({col_ref} AS VARCHAR)) = \'\' THEN 1 ELSE 0 END) AS "{c}__missing"'
            )

        row = con.execute(
            f"SELECT COUNT(*) AS n_total, {', '.join(case_exprs)} FROM v"
        ).fetchone()
        if row is not None:
            n_total = int(row[0])
            if n_total > 0:
                for i, c in enumerate(cols_to_profile):
                    nmiss = int(row[i + 1])
                    if nmiss > 0:
                        missingness_top.append({"column": c, "missing_pct": float(nmiss) / float(n_total) * 100.0})

                missingness_top = sorted(missingness_top, key=lambda x: -x["missing_pct"])[:25]

    return sample_rows, missingness_top


def _profile_excel(file0: Path, read_cfg: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Profile an Excel file using the same pandas reader as ``clean.read_excel``.

    Reuses ``_load_excel_frame`` from ``clean.read_excel`` to stay in sync
    with the clean runtime's Excel handling (sheet_name, header, skip, columns,
    trim_whitespace, mismatch detection).

    Returns the same dict shape as ``profile_with_read_cfg``.
    """
    from toolkit.clean.read_excel import _load_excel_frame

    cfg = read_cfg or {}
    try:
        df, _ = _load_excel_frame(file0, cfg)
    except Exception as exc:
        return {
            "columns_raw": [],
            "columns_norm": [],
            "duckdb_types": [],
            "sample_rows": [],
            "missingness_top": [],
            "mapping_suggestions": {},
            "warnings": [f"excel_profile_failed: {type(exc).__name__}: {exc}"],
            "robust_read_suggested": True,
        }

    # Normalize column names
    columns_raw = list(df.columns)
    columns_norm = [_normalize_colname(str(c)) for c in columns_raw]

    # Sample rows
    sample_rows = df.head(5).fillna("").to_dict(orient="records")

    # Missingness
    n = len(df)
    missingness_top: list[dict[str, Any]] = []
    if n > 0:
        for col in columns_raw:
            nmiss = int(df[col].isna().sum())
            if nmiss > 0:
                missingness_top.append({"column": str(col), "missing_pct": float(nmiss) / float(n) * 100.0})
    missingness_top = sorted(missingness_top, key=lambda x: -x["missing_pct"])[:25]

    # Mapping suggestions — no DuckDB types for Excel, use empty
    mapping_suggestions: Dict[str, Any] = {}
    duckdb_types: List[str] = []

    return {
        "columns_raw": columns_raw,
        "columns_norm": columns_norm,
        "duckdb_types": duckdb_types,
        "sample_rows": sample_rows,
        "missingness_top": missingness_top,
        "mapping_suggestions": mapping_suggestions,
        "warnings": [],
        "robust_read_suggested": False,
    }


def profile_with_read_cfg(
    file0: Path,
    sniff_hints: Dict[str, Any],
    effective_read_cfg: dict[str, Any],
) -> Dict[str, Any]:
    """Profile a file using DuckDB with a specific read configuration.

    This is the "runtime" half of profiling: it reads the file exactly as
    ``clean.read`` would and returns column-level statistics.

    Parameters
    ----------
    file0:
        Path to the data file.
    sniff_hints:
        Output of ``sniff_source_file`` — provides ``true_header_line`` for
        mismatch detection and carries the sniff-level warnings.
    effective_read_cfg:
        Fully-resolved read configuration (encoding, delim, skip, etc.)
        to pass to DuckDB's ``read_csv``.

    Returns
    -------
    dict with keys:
        - columns_raw (list[str])
        - columns_norm (list[str])
        - duckdb_types (list[str])
        - sample_rows (list[dict])
        - missingness_top (list[dict])
        - mapping_suggestions (dict)
        - warnings (list[str]): extended with runtime-specific warnings
        - robust_read_suggested (bool): True if robust preset was needed
    """
    true_header_line: str | None = sniff_hints.get("true_header_line")
    warnings = list(sniff_hints.get("warnings") or [])
    robust_read_suggested = False

    try:
        with safe_connect() as con:
            try:
                _profile_view(
                    con,
                    file0,
                    effective_read_cfg=effective_read_cfg,
                )
            except Exception as e:
                warnings.append(f"profile_read_retry: {type(e).__name__}: {e}")
                robust_read_suggested = True
                fallback_cfg = robust_preset(effective_read_cfg)
                fallback_cfg.setdefault("auto_detect", False)
                _profile_view(
                    con,
                    file0,
                    effective_read_cfg=fallback_cfg,
                )

            columns_raw, columns_norm, duckdb_types = _describe_columns(con)

            # Detect column-count mismatch between header and data.
            # When true_header_line (ground truth at line 0) has fewer tokens
            # than what DESCRIBE returns, the file has more columns in data
            # rows than in the header row (IRPEF comunale pattern:
            # header=50 cols, data rows=52 cols).
            if true_header_line is not None:
                true_header_tokens = true_header_line.count(effective_read_cfg.get("delim") or ";") + 1
                if len(columns_raw) > true_header_tokens:
                    warnings.append(
                        f"header_data_cols_mismatch: header has {true_header_tokens} tokens, "
                        f"data has {len(columns_raw)} columns; retrying with null_padding=true"
                    )
                    robust_read_suggested = True
                    fallback_cfg = robust_preset(effective_read_cfg)
                    fallback_cfg.setdefault("auto_detect", False)
                    fallback_cfg.setdefault("null_padding", True)
                    _profile_view(
                        con,
                        file0,
                        effective_read_cfg=fallback_cfg,
                    )
                    columns_raw, columns_norm, duckdb_types = _describe_columns(con)

            duckdb_type_map: dict[str, str] = {
                raw: dtype for raw, dtype in zip(columns_raw, duckdb_types)
            }
            sample_rows, missingness_top = _sample_profile_rows(con, columns_raw)
            mapping_suggestions = _build_mapping_suggestions(
                columns_raw, sample_rows, duckdb_types=duckdb_type_map
            )
    except Exception as e:
        warnings.append(f"profile_failed: {type(e).__name__}: {e}")
        warnings.append(
            "python_fallback_used: suggested_read generated from lightweight sniffing only"
        )
        columns_raw, columns_norm, duckdb_types = [], [], []
        sample_rows, missingness_top = [], []
        mapping_suggestions = {}
        robust_read_suggested = True

    return {
        "columns_raw": columns_raw,
        "columns_norm": columns_norm,
        "duckdb_types": duckdb_types,
        "sample_rows": sample_rows,
        "missingness_top": missingness_top,
        "mapping_suggestions": mapping_suggestions,
        "warnings": warnings,
        "robust_read_suggested": robust_read_suggested,
    }


@dataclass
class RawProfile:
    dataset: str
    year: int
    file_used: str

    encoding_suggested: Optional[str]
    delim_suggested: Optional[str]
    decimal_suggested: Optional[str]
    skip_suggested: int
    robust_read_suggested: bool

    header_line: Optional[str]
    columns_raw: List[str]
    columns_norm: List[str]

    missingness_top: List[Dict[str, Any]]
    sample_rows: List[Dict[str, Any]]
    mapping_suggestions: Dict[str, Any]

    warnings: List[str]


def profile_raw(
    raw_dir: Path,
    dataset: str,
    year: int,
    read_cfg: Optional[Dict[str, Any]] = None,
    *,
    primary_file: Path | None = None,
) -> RawProfile:
    """Profile a RAW directory.

    This is a facade that firstsniffs the source file (encoding, delimiter,
    skip, header) then profiles it with DuckDB using the resolved read
    configuration.

    Parameters
    ----------
    raw_dir:
        Directory containing RAW files.
    dataset:
        Dataset slug.
    year:
        Year of the dataset.
    read_cfg:
        Optional explicit read configuration. Values here override the
        sniffed suggestions.
    primary_file:
        Optional explicit path to the primary source file. When provided,
        takes precedence over the alphabetical glob fallback. Should match
        the ``primary_output_file`` from the RAW manifest (via
        ``_choose_primary_output``).

    Returns
    -------
    RawProfile
    """
    files = _raw_files(raw_dir)
    if not files:
        raise FileNotFoundError(f"No RAW files found in {raw_dir}")

    file0 = primary_file if primary_file is not None else _pick_data_file(files)

    # Phase 1: pure source sniffing
    sniff_hints = sniff_source_file(file0)

    enc = sniff_hints["encoding_suggested"]
    delim = sniff_hints["delim_suggested"]
    dec = sniff_hints["decimal_suggested"]
    skip = sniff_hints["skip_suggested"]
    header_line = sniff_hints["header_line"]

    # Phase 1b: Excel files — profile via pandas (same reader as clean runtime)
    binary_fmt = sniff_hints.get("is_binary_file")
    if binary_fmt in ("xlsx", "xls"):
        runtime_result = _profile_excel(file0, read_cfg)
        return RawProfile(
            dataset=dataset,
            year=year,
            file_used=str(file0.name),
            encoding_suggested=None,
            delim_suggested=None,
            decimal_suggested=None,
            skip_suggested=skip,
            robust_read_suggested=runtime_result["robust_read_suggested"],
            header_line=None,
            columns_raw=runtime_result["columns_raw"],
            columns_norm=runtime_result["columns_norm"],
            missingness_top=runtime_result["missingness_top"],
            sample_rows=runtime_result["sample_rows"],
            mapping_suggestions=runtime_result["mapping_suggestions"],
            warnings=runtime_result["warnings"],
        )
    # ZIP or other unsupported binary — return empty profile with warning
    if binary_fmt == "zip":
        return RawProfile(
            dataset=dataset,
            year=year,
            file_used=str(file0.name),
            encoding_suggested=None,
            delim_suggested=None,
            decimal_suggested=None,
            skip_suggested=skip,
            robust_read_suggested=True,
            header_line=None,
            columns_raw=[],
            columns_norm=[],
            missingness_top=[],
            sample_rows=[],
            mapping_suggestions={},
            warnings=["binary_file_not_supported: zip — use a different source format"],
        )

    # Phase 2: resolve effective read cfg (sniff + user override)
    effective_read_cfg = _effective_profile_read_cfg(
        read_cfg,
        encoding=enc,
        delim=delim,
        decimal=dec,
        skip=skip,
    )

    # Phase 3: DuckDB runtime profiling
    runtime_result = profile_with_read_cfg(file0, sniff_hints, effective_read_cfg)

    return RawProfile(
        dataset=dataset,
        year=year,
        file_used=str(file0.name),
        encoding_suggested=enc,
        delim_suggested=delim,
        decimal_suggested=dec,
        skip_suggested=skip,
        robust_read_suggested=runtime_result["robust_read_suggested"],
        header_line=header_line,
        columns_raw=runtime_result["columns_raw"],
        columns_norm=runtime_result["columns_norm"],
        missingness_top=runtime_result["missingness_top"],
        sample_rows=runtime_result["sample_rows"],
        mapping_suggestions=runtime_result["mapping_suggestions"],
        warnings=runtime_result["warnings"],
    )


def write_raw_profile(
    out_dir: Path,
    profile: RawProfile,
    *,
    write_canonical: bool = True,
) -> Dict[str, Path]:
    _safe_mkdir(out_dir)

    p_raw_json = out_dir / "raw_profile.json"
    payload = asdict(profile)
    written: Dict[str, Path] = {}

    if write_canonical:
        write_json_atomic(p_raw_json, payload)
        written["raw_json"] = p_raw_json

    return written
