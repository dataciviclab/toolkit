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
    columns: List[str], sample_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
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
