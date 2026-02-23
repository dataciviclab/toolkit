from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

COMMON_DELIMS = [";", ",", "\t", "|"]
COMMON_ENCODINGS = ["utf-8", "latin-1", "windows-1252", "CP1252"]

# tokens tipici che vogliamo suggerire come nullify
NULL_TOKENS_DEFAULT = ["", "-", "n.d.", "n.d", "ND", "NA", "N/A", "null", "NULL"]


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


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


@dataclass
class RawProfile:
    dataset: str
    year: int
    file_used: str

    encoding_suggested: Optional[str]
    delim_suggested: Optional[str]
    decimal_suggested: Optional[str]

    header_line: Optional[str]
    columns_raw: List[str]
    columns_norm: List[str]

    missingness_top: List[Dict[str, Any]]
    sample_rows: List[Dict[str, Any]]
    mapping_suggestions: Dict[str, Any]

    warnings: List[str]


def profile_raw(raw_dir: Path, dataset: str, year: int, read_cfg: Optional[Dict[str, Any]] = None) -> RawProfile:
    files = sorted([p for p in raw_dir.glob("*") if p.is_file()])
    if not files:
        raise FileNotFoundError(f"No RAW files found in {raw_dir}")

    file0 = _pick_data_file(files)

    enc, txt = sniff_encoding(file0)
    delim = sniff_delim(txt)
    dec = sniff_decimal(txt)

    effective_read_cfg = dict(read_cfg or {})
    if "delim" not in effective_read_cfg and "sep" not in effective_read_cfg and delim:
        effective_read_cfg["delim"] = delim
    if "encoding" not in effective_read_cfg and enc:
        effective_read_cfg["encoding"] = enc
    if "decimal" not in effective_read_cfg and dec:
        effective_read_cfg["decimal"] = dec
    effective_read_cfg.setdefault("header", True)

    warnings: List[str] = []
    header_line: Optional[str] = None
    columns_raw: List[str] = []
    columns_norm: List[str] = []
    sample_rows: List[Dict[str, Any]] = []
    missingness_top: List[Dict[str, Any]] = []
    mapping_suggestions: Dict[str, Any] = {}

    # header line (respect skip)
    try:
        skip_n = int(effective_read_cfg.get("skip") or 0)
        with file0.open("r", encoding=effective_read_cfg.get("encoding") or enc, errors="replace") as f:
            for _ in range(skip_n):
                f.readline()
            header_line = f.readline().rstrip("\n\r")
    except Exception as e:
        warnings.append(f"header_read_failed: {type(e).__name__}: {e}")

    con = duckdb.connect(":memory:")
    try:
        opts = ["union_by_name=true"]

        sep = effective_read_cfg.get("sep") or effective_read_cfg.get("delim")
        if sep is not None:
            opts.append(f"sep='{sep}'")

        encoding = effective_read_cfg.get("encoding")
        if encoding is not None:
            opts.append(f"encoding='{encoding}'")

        decimal_sep = effective_read_cfg.get("decimal")
        if decimal_sep is not None:
            opts.append(f"decimal_separator='{decimal_sep}'")

        header = effective_read_cfg.get("header", True)
        opts.append(f"header={'true' if bool(header) else 'false'}")

        skip_n = effective_read_cfg.get("skip")
        if skip_n is not None:
            opts.append(f"skip={int(skip_n)}")

        # strict / ignore / null padding / max line size
        strict_mode = effective_read_cfg.get("strict_mode")
        if strict_mode is not None:
            opts.append(f"strict_mode={'true' if bool(strict_mode) else 'false'}")

        ignore_errors = effective_read_cfg.get("ignore_errors")
        if ignore_errors is not None:
            opts.append(f"ignore_errors={'true' if bool(ignore_errors) else 'false'}")

        null_padding = effective_read_cfg.get("null_padding")
        if null_padding is not None:
            opts.append(f"null_padding={'true' if bool(null_padding) else 'false'}")

        max_line_size = effective_read_cfg.get("max_line_size")
        if max_line_size is not None:
            opts.append(f"max_line_size={int(max_line_size)}")

        # quote / escape / comment
        quote = effective_read_cfg.get("quote")
        if quote is not None:
            opts.append(f"quote='{quote}'")

        escape = effective_read_cfg.get("escape")
        if escape is not None:
            opts.append(f"escape='{escape}'")

        comment = effective_read_cfg.get("comment")
        if comment is not None:
            opts.append(f"comment='{comment}'")

        opt_sql = ", ".join(opts)
        con.execute(f"CREATE OR REPLACE VIEW v AS SELECT * FROM read_csv_auto('{file0}', {opt_sql});")

        cols = con.execute("DESCRIBE v").fetchall()
        columns_raw = [r[0] for r in cols]
        columns_norm = [_normalize_colname(c) for c in columns_raw]

        df = con.execute("SELECT * FROM v LIMIT 50").fetchdf()
        sample_rows = df.to_dict(orient="records")

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

        mapping_suggestions = _build_mapping_suggestions(columns_raw, sample_rows)

    except Exception as e:
        warnings.append(f"profile_failed: {type(e).__name__}: {e}")
    finally:
        con.close()

    return RawProfile(
        dataset=dataset,
        year=year,
        file_used=str(file0.name),
        encoding_suggested=enc,
        delim_suggested=delim,
        decimal_suggested=dec,
        header_line=header_line,
        columns_raw=columns_raw,
        columns_norm=columns_norm,
        missingness_top=missingness_top,
        sample_rows=sample_rows,
        mapping_suggestions=mapping_suggestions,
        warnings=warnings,
    )


def write_raw_profile(out_dir: Path, profile: RawProfile) -> Dict[str, Path]:
    _safe_mkdir(out_dir)

    p_json = out_dir / "profile.json"
    p_json.write_text(json.dumps(asdict(profile), ensure_ascii=False, indent=2), encoding="utf-8")

    return {"json": p_json}