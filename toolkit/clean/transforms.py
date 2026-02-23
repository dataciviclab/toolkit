from __future__ import annotations

from typing import Any, Dict, List, Optional


def sql_ident(name: str) -> str:
    return f'"{name}"'


def apply_nullify(expr: str, nullify: Optional[List[str]]) -> str:
    if not nullify:
        return expr
    toks = ", ".join([f"'{t}'" for t in nullify])
    return f"CASE WHEN TRIM(CAST({expr} AS VARCHAR)) IN ({toks}) THEN NULL ELSE {expr} END"


def apply_replace(expr: str, replace: Optional[Dict[str, str]]) -> str:
    if not replace:
        return expr
    out = expr
    for k, v in replace.items():
        out = f"REPLACE({out}, '{k}', '{v}')"
    return out


def apply_normalize(expr: str, normalize: Optional[List[str]]) -> str:
    if not normalize:
        return expr
    out = expr
    for op in normalize:
        if op == "trim":
            out = f"TRIM({out})"
        elif op == "upper":
            out = f"UPPER({out})"
        elif op == "lower":
            out = f"LOWER({out})"
        elif op == "title":
            out = f"INITCAP({out})"
        elif op == "collapse_spaces":
            out = f"REGEXP_REPLACE({out}, '\\s+', ' ')"
        elif op == "remove_accents":
            # Placeholder: no-op for now (future: UDF)
            out = out
        else:
            raise ValueError(f"Unknown normalize op: {op}")
    return out


def _parse_number_it(expr: str) -> str:
    # "1.234,56" -> 1234.56
    return f"REPLACE(REPLACE({expr}, '.', ''), ',', '.')"


def _parse_percent_it(expr: str) -> str:
    # "12,3%" -> 12.3
    return _parse_number_it(f"REPLACE({expr}, '%', '')")


def apply_parse(expr: str, parse: Optional[Dict[str, Any]]) -> str:
    if not parse:
        return expr
    kind = parse.get("kind")
    if kind == "number_it":
        return _parse_number_it(expr)
    if kind == "percent_it":
        return _parse_percent_it(expr)
    raise ValueError(f"Unknown parse kind: {kind}")


def apply_cast(expr: str, target_type: str) -> str:
    t = target_type.lower()
    if t in ("int", "integer"):
        return f"CAST({expr} AS INTEGER)"
    if t in ("float", "double"):
        return f"CAST({expr} AS DOUBLE)"
    if t in ("str", "string"):
        return f"CAST({expr} AS VARCHAR)"
    if t == "date":
        return f"CAST({expr} AS DATE)"
    raise ValueError(f"Unknown target type: {target_type}")