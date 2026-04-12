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

from toolkit.core.csv_read import (
    csv_read_option_strings,
    normalize_read_cfg,
    robust_preset,
    sql_str,
)
from toolkit.core.io import write_json_atomic
from toolkit.profile._sniff_encoding import sniff_encoding
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


def build_profile_hints(filepath: Path) -> Dict[str, Any]:
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
    try:
        with filepath.open("r", encoding=enc, errors="replace") as f:
            for _ in range(skip):
                f.readline()
            header_line = f.readline().rstrip("\n\r")
    except Exception as exc:
        warnings.append(f"header_read_failed: {type(exc).__name__}: {exc}")

    return {
        "file_used": filepath.name,
        "encoding_suggested": enc,
        "delim_suggested": delim,
        "decimal_suggested": dec,
        "skip_suggested": skip,
        "header_line": header_line,
        "columns_preview": _preview_columns(header_line, delim),
        "warnings": warnings,
    }


def _build_read_csv_opts(read_cfg: Dict[str, Any]) -> str:
    opts = ["union_by_name=true"] + csv_read_option_strings(read_cfg)

    header = read_cfg.get("header", True)
    opts.append(f"header={'true' if bool(header) else 'false'}")

    skip_n = read_cfg.get("skip")
    if skip_n is not None:
        opts.append(f"skip={int(skip_n)}")

    return ", ".join(opts)


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


def _read_header_line(file0: Path, *, encoding: str, skip_n: int) -> str | None:
    try:
        with file0.open("r", encoding=encoding, errors="replace") as f:
            for _ in range(skip_n):
                f.readline()
            return f.readline().rstrip("\n\r")
    except Exception:
        return None


def _profile_view(
    con: duckdb.DuckDBPyConnection,
    file0: Path,
    *,
    effective_read_cfg: dict[str, Any],
) -> None:
    opt_sql = _build_read_csv_opts(effective_read_cfg)
    con.execute(
        f"CREATE OR REPLACE VIEW v AS SELECT * FROM read_csv('{sql_str(str(file0))}', {opt_sql});"
    )


def _describe_columns(con: duckdb.DuckDBPyConnection) -> tuple[list[str], list[str]]:
    cols = con.execute("DESCRIBE v").fetchall()
    columns_raw = [r[0] for r in cols]
    columns_norm = [_normalize_colname(c) for c in columns_raw]
    return columns_raw, columns_norm


def _sample_profile_rows(
    con: duckdb.DuckDBPyConnection,
    columns_raw: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    df = con.execute("SELECT * FROM v LIMIT 50").fetchdf()
    sample_rows = df.to_dict(orient="records")

    missingness_top: list[dict[str, Any]] = []
    for c in columns_raw[:200]:
        n, nmiss = con.execute(
            f"""
            SELECT
              COUNT(*) AS n,
              SUM(CASE WHEN "{c}" IS NULL OR TRIM(CAST("{c}" AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS n_missing
            FROM v
            """
        ).fetchone()
        if n:
            missingness_top.append({"column": c, "missing_pct": float(nmiss) / float(n) * 100.0})

    missingness_top = sorted(missingness_top, key=lambda x: -x["missing_pct"])[:25]
    return sample_rows, missingness_top


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
    raw_dir: Path, dataset: str, year: int, read_cfg: Optional[Dict[str, Any]] = None
) -> RawProfile:
    files = _raw_files(raw_dir)
    if not files:
        raise FileNotFoundError(f"No RAW files found in {raw_dir}")

    file0 = _pick_data_file(files)
    enc, txt = sniff_encoding(file0)
    delim = sniff_delim(txt)
    dec = sniff_decimal(txt)
    skip = suggest_skip(txt, delim)
    effective_read_cfg = _effective_profile_read_cfg(
        read_cfg,
        encoding=enc,
        delim=delim,
        decimal=dec,
        skip=skip,
    )

    warnings: List[str] = []
    header_line: Optional[str] = None
    columns_raw: List[str] = []
    columns_norm: List[str] = []
    sample_rows: List[Dict[str, Any]] = []
    missingness_top: List[Dict[str, Any]] = []
    mapping_suggestions: Dict[str, Any] = {}
    robust_read_suggested = False

    if skip:
        warnings.append(
            "header_preamble_detected: first non-empty line looks like a title row, consider skip: 1"
        )

    skip_n = int(effective_read_cfg.get("skip") or 0)
    header_line = _read_header_line(
        file0,
        encoding=effective_read_cfg.get("encoding") or enc,
        skip_n=skip_n,
    )
    if header_line is None:
        warnings.append("header_read_failed: could not read header line")

    con = duckdb.connect(":memory:")
    try:
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

        columns_raw, columns_norm = _describe_columns(con)
        sample_rows, missingness_top = _sample_profile_rows(con, columns_raw)
        mapping_suggestions = _build_mapping_suggestions(columns_raw, sample_rows)

    except Exception as e:
        warnings.append(f"profile_failed: {type(e).__name__}: {e}")
        warnings.append(
            "python_fallback_used: suggested_read generated from lightweight sniffing only"
        )
    finally:
        con.close()

    return RawProfile(
        dataset=dataset,
        year=year,
        file_used=str(file0.name),
        encoding_suggested=enc,
        delim_suggested=delim,
        decimal_suggested=dec,
        skip_suggested=skip,
        robust_read_suggested=robust_read_suggested,
        header_line=header_line,
        columns_raw=columns_raw,
        columns_norm=columns_norm,
        missingness_top=missingness_top,
        sample_rows=sample_rows,
        mapping_suggestions=mapping_suggestions,
        warnings=warnings,
    )


def write_raw_profile(
    out_dir: Path,
    profile: RawProfile,
    *,
    write_canonical: bool = True,
    write_legacy_alias: bool = True,
) -> Dict[str, Path]:
    _safe_mkdir(out_dir)

    p_raw_json = out_dir / "raw_profile.json"
    p_json = out_dir / "profile.json"
    payload = asdict(profile)
    written: Dict[str, Path] = {}

    if write_canonical:
        write_json_atomic(p_raw_json, payload)
        written["raw_json"] = p_raw_json
    if write_legacy_alias:
        write_json_atomic(p_json, payload)
        written["json"] = p_json

    return written
