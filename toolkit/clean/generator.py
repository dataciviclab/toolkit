from __future__ import annotations

from typing import Any, Dict, Optional

ALLOWED_TYPES = {"int", "integer", "float", "double", "str", "string", "date"}


def sql_ident(name: str) -> str:
    return f'"{name}"'


def apply_nullify(expr: str, nullify: Optional[list[str]]) -> str:
    if not nullify:
        return expr
    toks = ", ".join([f"'{t}'" for t in nullify])
    return f"CASE WHEN TRIM(CAST({expr} AS VARCHAR)) IN ({toks}) THEN NULL ELSE {expr} END"


def apply_replace(expr: str, replace: Optional[dict[str, str]]) -> str:
    if not replace:
        return expr
    out = expr
    for k, v in replace.items():
        out = f"REPLACE({out}, '{k}', '{v}')"
    return out


def apply_normalize(expr: str, normalize: Optional[list[str]]) -> str:
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


def apply_parse(expr: str, parse: Optional[dict[str, Any]]) -> str:
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


def _validate_mapping(mapping: Dict[str, Dict[str, Any]]) -> None:
    if not isinstance(mapping, dict) or not mapping:
        raise ValueError("clean.mapping must be a non-empty dict")

    for out_col, spec in mapping.items():
        if not isinstance(out_col, str) or not out_col.strip():
            raise ValueError("Mapping keys (output columns) must be non-empty strings")
        if not isinstance(spec, dict):
            raise ValueError(f"Mapping for '{out_col}' must be a dict")

        src = spec.get("from")
        if not isinstance(src, str) or not src.strip():
            raise ValueError(f"Mapping for '{out_col}' missing/invalid 'from'")

        t = str(spec.get("type", "str")).lower()
        if t not in ALLOWED_TYPES:
            raise ValueError(
                f"Mapping for '{out_col}' has invalid type '{t}' (allowed: {sorted(ALLOWED_TYPES)})"
            )

        if (norm := spec.get("normalize")) is not None and not isinstance(norm, list):
            raise ValueError(f"normalize for '{out_col}' must be a list")

        if (nullify := spec.get("nullify")) is not None and not isinstance(nullify, list):
            raise ValueError(f"nullify for '{out_col}' must be a list")

        if (replace := spec.get("replace")) is not None and not isinstance(replace, dict):
            raise ValueError(f"replace for '{out_col}' must be a dict")

        if (parse := spec.get("parse")) is not None and not isinstance(parse, dict):
            raise ValueError(f"parse for '{out_col}' must be a dict")


def _field_expr(out_col: str, spec: Dict[str, Any]) -> str:
    src = spec["from"]
    expr = sql_ident(src)

    # replace -> nullify
    expr = apply_replace(expr, spec.get("replace"))
    expr = apply_nullify(expr, spec.get("nullify"))

    t = str(spec.get("type", "str")).lower()

    # Strings: cast + normalize
    if t in ("str", "string"):
        expr = apply_cast(expr, "str")
        expr = apply_normalize(expr, spec.get("normalize"))
        expr = apply_cast(expr, "str")
        return f"{expr} AS {out_col}"

    # Non-strings: parse from trimmed string
    expr = f"TRIM(CAST({expr} AS VARCHAR))"
    expr = apply_parse(expr, spec.get("parse"))
    expr = apply_cast(expr, t)
    return f"{expr} AS {out_col}"


def generate_clean_sql(
    dataset: str,
    year: int,
    mapping: Dict[str, Dict[str, Any]],
    derive: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """
    Generate a CLEAN SQL query (NO trailing semicolon) that reads from view `raw_input`.
    """
    _validate_mapping(mapping)

    lines: list[str] = []
    lines.append("-- AUTO-GENERATED BY toolkit.clean.generator")
    lines.append(f"-- dataset: {dataset}")
    lines.append(f"-- year: {year}")
    lines.append("-- source view: raw_input")
    lines.append("")

    lines.append("WITH src AS (")
    lines.append("  SELECT * FROM raw_input")
    lines.append("), mapped AS (")
    lines.append("  SELECT")

    fields = []
    for out_col, spec in mapping.items():
        fields.append("    " + _field_expr(out_col, spec))
    lines.append(",\n".join(fields))
    lines.append("  FROM src")
    lines.append(")")

    if derive:
        lines.append(", derived AS (")
        lines.append("  SELECT")
        lines.append("    *,")
        derived_cols = []
        for k, spec in derive.items():
            expr = spec.get("expr")
            if not isinstance(expr, str) or not expr.strip():
                raise ValueError(f"derive.{k} missing/invalid expr")
            derived_cols.append(f"    ({expr}) AS {k}")
        lines.append(",\n".join(derived_cols))
        lines.append("  FROM mapped")
        lines.append(")")
        lines.append("SELECT * FROM derived")
    else:
        lines.append("SELECT * FROM mapped")

    return "\n".join(lines) + "\n"
