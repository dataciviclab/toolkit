from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
from toolkit.core.csv_read import normalize_read_cfg, robust_preset, sql_str
from toolkit.core.io import write_json_atomic

COMMON_DELIMS = [";", ",", "\t", "|"]
COMMON_ENCODINGS = ["utf-8", "latin-1", "windows-1252", "CP1252"]

# tokens tipici che vogliamo suggerire come nullify
NULL_TOKENS_DEFAULT = ["", "-", "n.d.", "n.d", "ND", "NA", "N/A", "null", "NULL"]


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _raw_files(raw_dir: Path) -> list[Path]:
    return sorted([p for p in raw_dir.glob("*") if p.is_file()])


def _try_decode(filepath: Path, enc: str) -> Optional[str]:
    try:
        with filepath.open("r", encoding=enc, errors="strict") as f:
            return f.read(200_000)
    except Exception:
        return None


def sniff_encoding(filepath: Path) -> Tuple[str, str]:
    for enc in COMMON_ENCODINGS:
        txt = _try_decode(filepath, enc)
        if txt is not None:
            return enc, txt
    with filepath.open("r", encoding="utf-8", errors="replace") as f:
        return "utf-8", f.read(200_000)


def sniff_delim(sample_text: str) -> Optional[str]:
    lines = [ln for ln in sample_text.splitlines() if ln.strip()][:25]
    if not lines:
        return None
    scores = {}
    for d in COMMON_DELIMS:
        counts = [ln.count(d) for ln in lines]
        non_zero = [c for c in counts if c > 0]
        if not non_zero:
            continue
        variance = max(non_zero) - min(non_zero)
        scores[d] = (len(non_zero), -variance, sum(non_zero))
    if not scores:
        return None
    return sorted(scores.items(), key=lambda kv: (kv[1][0], kv[1][1], kv[1][2]), reverse=True)[0][0]


def sniff_decimal(sample_text: str) -> Optional[str]:
    chunk = sample_text[:200_000]
    comma_dec = len(re.findall(r"\d+,\d{1,3}\b", chunk))
    dot_dec = len(re.findall(r"\d+\.\d{1,3}\b", chunk))
    if comma_dec == 0 and dot_dec == 0:
        return None
    return "," if comma_dec >= dot_dec else "."


def _normalize_colname(c: str) -> str:
    c = c.strip()
    c = re.sub(r"\s+", " ", c)
    return c


def suggest_skip(sample_text: str, delim: Optional[str]) -> int:
    if not delim:
        return 0
    lines = [ln for ln in sample_text.splitlines() if ln.strip()][:5]
    if len(lines) < 2:
        return 0
    first_count = lines[0].count(delim)
    second_count = lines[1].count(delim)
    if first_count == 0 and second_count > 0:
        return 1
    if first_count < second_count and first_count <= 1 and second_count >= 3:
        return 1
    return 0


def _sniff_file_profile(file0: Path) -> tuple[str, str, Optional[str], Optional[str], int]:
    enc, txt = sniff_encoding(file0)
    delim = sniff_delim(txt)
    dec = sniff_decimal(txt)
    skip = suggest_skip(txt, delim)
    return enc, txt, delim, dec, skip


def _build_read_csv_opts(read_cfg: Dict[str, Any]) -> str:
    opts = ["union_by_name=true"]

    sep = read_cfg.get("sep") or read_cfg.get("delim")
    if sep is not None:
        opts.append(f"sep='{sql_str(str(sep))}'")

    encoding = read_cfg.get("encoding")
    if encoding is not None:
        opts.append(f"encoding='{sql_str(str(encoding))}'")

    decimal_sep = read_cfg.get("decimal")
    if decimal_sep is not None:
        opts.append(f"decimal_separator='{sql_str(str(decimal_sep))}'")

    header = read_cfg.get("header", True)
    opts.append(f"header={'true' if bool(header) else 'false'}")

    skip_n = read_cfg.get("skip")
    if skip_n is not None:
        opts.append(f"skip={int(skip_n)}")

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

    max_line_size = read_cfg.get("max_line_size")
    if max_line_size is not None:
        opts.append(f"max_line_size={int(max_line_size)}")

    quote = read_cfg.get("quote")
    if quote is not None:
        opts.append(f"quote='{sql_str(str(quote))}'")

    escape = read_cfg.get("escape")
    if escape is not None:
        opts.append(f"escape='{sql_str(str(escape))}'")

    comment = read_cfg.get("comment")
    if comment is not None:
        opts.append(f"comment='{sql_str(str(comment))}'")

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


def _pick_data_file(files: List[Path]) -> Path:
    # prefer csv-like, exclude metadata/validation json
    preferred = [p for p in files if p.suffix.lower() in {".csv", ".tsv", ".txt", ".php", ".gz"}]
    if preferred:
        return preferred[0]
    # fallback: first non-json/yml/md
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


def _sample_values(sample_rows: List[Dict[str, Any]], col: str, limit: int = 25) -> List[str]:
    vals: List[str] = []
    for r in sample_rows:
        if col not in r:
            continue
        v = r.get(col)
        if v is None:
            continue
        s = str(v).strip()
        if s == "":
            continue
        vals.append(s)
        if len(vals) >= limit:
            break
    return vals


def _detect_parse_kind(values: List[str]) -> Optional[str]:
    # percent if many values include '%'
    pct = sum(1 for v in values if "%" in v)
    if pct >= max(2, int(len(values) * 0.3)):
        return "percent_it"

    # number_it if we see patterns like 1.234,56 or 123,45
    it_like = 0
    for v in values:
        v2 = v.replace(" ", "")
        if re.search(r"\d{1,3}(\.\d{3})+,\d+", v2):  # 1.234,56
            it_like += 1
        elif re.search(r"\d+,\d{1,3}\b", v2):  # 123,4
            it_like += 1
    if it_like >= max(2, int(len(values) * 0.3)):
        return "number_it"

    return None


def _detect_type(values: List[str], parse_kind: Optional[str]) -> str:
    # If parse_kind is numeric-ish, it will be float
    if parse_kind in ("percent_it", "number_it"):
        return "float"

    # try int
    int_like = 0
    float_like = 0
    for v in values:
        v2 = v.replace(" ", "")
        if re.fullmatch(r"-?\d+", v2):
            int_like += 1
        elif re.fullmatch(r"-?\d+\.\d+", v2) or re.fullmatch(r"-?\d+,\d+", v2):
            float_like += 1

    if int_like >= max(2, int(len(values) * 0.6)):
        return "int"
    if (int_like + float_like) >= max(2, int(len(values) * 0.6)):
        return "float"

    return "str"


def _suggest_nullify(values: List[str]) -> List[str]:
    hits = set()
    for v in values:
        if v in NULL_TOKENS_DEFAULT:
            hits.add(v)
    # mantieni ordine “standard”
    out = [t for t in NULL_TOKENS_DEFAULT if t in hits]
    return out


def _suggest_normalize(colname: str, detected_type: str) -> Optional[List[str]]:
    # su stringhe suggeriamo sempre trim + collapse_spaces
    if detected_type == "str":
        # title utile su nomi geografici
        if any(k in colname.lower() for k in ["comune", "prov", "reg", "nome", "citt"]):
            return ["trim", "title", "collapse_spaces"]
        return ["trim", "collapse_spaces"]
    return None


def _build_mapping_suggestions(columns: List[str], sample_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for col in columns:
        vals = _sample_values(sample_rows, col, limit=30)
        parse_kind = _detect_parse_kind(vals)
        dtype = _detect_type(vals, parse_kind)
        nullify = _suggest_nullify(vals)
        normalize = _suggest_normalize(col, dtype)

        spec: Dict[str, Any] = {"from": col, "type": dtype}

        if nullify:
            spec["nullify"] = nullify
        if normalize:
            spec["normalize"] = normalize
        if parse_kind:
            spec["parse"] = {"kind": parse_kind}

        out[col] = spec
    return out


def _profile_view(
    con: duckdb.DuckDBPyConnection,
    file0: Path,
    *,
    effective_read_cfg: dict[str, Any],
) -> None:
    opt_sql = _build_read_csv_opts(effective_read_cfg)
    con.execute(
        f"CREATE OR REPLACE VIEW v AS "
        f"SELECT * FROM read_csv('{sql_str(str(file0))}', {opt_sql});"
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


def profile_raw(raw_dir: Path, dataset: str, year: int, read_cfg: Optional[Dict[str, Any]] = None) -> RawProfile:
    files = _raw_files(raw_dir)
    if not files:
        raise FileNotFoundError(f"No RAW files found in {raw_dir}")

    file0 = _pick_data_file(files)
    enc, txt, delim, dec, skip = _sniff_file_profile(file0)
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
        warnings.append("header_preamble_detected: first non-empty line looks like a title row, consider skip: 1")

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
