"""Column profiling and mapping suggestion utilities."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

# tokens tipici che vogliamo suggerire come nullify
NULL_TOKENS_DEFAULT = ["", "-", "n.d.", "n.d", "ND", "NA", "N/A", "null", "NULL"]


def _normalize_colname(c: str) -> str:
    c = c.strip()
    c = re.sub(r"\s+", " ", c)
    return c


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
    pct = sum(1 for v in values if "%" in v)
    if pct >= max(2, int(len(values) * 0.3)):
        return "percent_it"

    it_like = 0
    for v in values:
        v2 = v.replace(" ", "")
        if re.search(r"\d{1,3}(\.\d{3})+,\d+", v2):
            it_like += 1
        elif re.search(r"\d+,\d{1,3}\b", v2):
            it_like += 1
    if it_like >= max(2, int(len(values) * 0.3)):
        return "number_it"

    return None


def _detect_type(values: List[str], parse_kind: Optional[str]) -> str:
    if parse_kind in ("percent_it", "number_it"):
        return "float"

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
    out = [t for t in NULL_TOKENS_DEFAULT if t in hits]
    return out


def _suggest_normalize(colname: str, detected_type: str) -> Optional[List[str]]:
    if detected_type == "str":
        if any(k in colname.lower() for k in ["comune", "prov", "reg", "nome", "citt"]):
            return ["trim", "title", "collapse_spaces"]
        return ["trim", "collapse_spaces"]
    return None


def _build_mapping_suggestions(
    columns: list[str],
    sample_rows: list[dict[str, Any]],
    duckdb_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build mapping suggestions for each column.

    When duckdb_types is provided (raw_name -> DuckDB type string), those types
    are used instead of regex-based type inference. This produces more accurate
    type suggestions since DuckDB has already inferred types from the full file.

    Each entry: {"from": col, "type": duckdb_type_or_inferred, ...}
    """
    out: dict[str, Any] = {}
    for col in columns:
        vals = _sample_values(sample_rows, col, limit=30)
        parse_kind = _detect_parse_kind(vals)

        # Use DuckDB-inferred type when available and specific.
        # DuckDB VARCHAR is too generic — only use it as definitive when DuckDB
        # found a specific type (int/float/date/bool).  For VARCHAR fall back to
        # regex-based _detect_type so that heuristics like number_it/percent_it
        # can surface (DuckDB doesn't detect Italian decimal/comma formats).
        if duckdb_types and col in duckdb_types:
            duckdb_t = duckdb_types[col].upper()
            is_varchar = duckdb_t in ("VARCHAR", "CHAR", "TEXT", "STRING", "UUID")
            if not is_varchar:
                # DuckDB found a specific type — use it
                dtype = _duckdb_type_to_mapping_type(duckdb_types[col])
            else:
                # DuckDB says VARCHAR — fall back to regex inference for more detail
                dtype = _detect_type(vals, parse_kind)
        else:
            dtype = _detect_type(vals, parse_kind)

        nullify = _suggest_nullify(vals)
        normalize = _suggest_normalize(col, dtype)
        spec: dict[str, Any] = {"from": col, "type": dtype}

        if nullify:
            spec["nullify"] = nullify
        if normalize:
            spec["normalize"] = normalize
        if parse_kind:
            spec["parse"] = {"kind": parse_kind}

        out[col] = spec
    return out


def _duckdb_type_to_mapping_type(duckdb_type: str) -> str:
    """Map DuckDB DESCRIBE type to mapping suggestion type.

    DuckDB types: VARCHAR, BIGINT, DOUBLE, DATE, TIMESTAMP, BOOLEAN, etc.
    Mapping types: str, int, float, date, bool.
    """
    t = duckdb_type.upper()
    if t in ("VARCHAR", "CHAR", "TEXT", "STRING", "UUID"):
        return "str"
    if t in ("INTEGER", "BIGINT", "SMALLINT", "TINYINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT", "HUGEINT", "UHUGEINT"):
        return "int"
    if t in ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC", "UHUGGINT"):
        return "float"
    if t in ("DATE", "TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_US"):
        return "date"
    if t in ("BOOLEAN", "BOOL"):
        return "bool"
    return "str"
