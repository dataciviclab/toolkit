from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str]
    warnings: list[str]
    summary: dict[str, Any]


def _missing_required_columns(actual: list[str], required: list[str]) -> list[str]:
    actual_set = set(actual)
    return [c for c in required if c not in actual_set]


def _q_ident(col: str) -> str:
    """Quote identifier for DuckDB (handles reserved words / special chars)."""
    return '"' + col.replace('"', '""') + '"'


def validate_clean(
    parquet_path: str | Path,
    required: list[str] | None = None,
    *,
    primary_key: list[str] | None = None,
    not_null: list[str] | None = None,
    ranges: dict[str, dict[str, float]] | None = None,
    max_null_pct: dict[str, float] | None = None,
    min_rows: int | None = None,
) -> ValidationResult:
    """
    Validate CLEAN parquet output with optional rule-based checks.

    Backward compatible:
    - If you only pass `required`, it behaves like the previous validator
      (exists + required columns + row_count > 0).

    Rule options:
    - primary_key: list of columns that must be unique as a group
    - not_null: list of columns that must have 0 NULLs
    - ranges: {"col": {"min": 0, "max": 100}} (min/max are optional)
    - max_null_pct: {"col": 0.05} (5% max NULLs)
    - min_rows: minimum row count allowed
    """
    required = list(required or [])
    primary_key = list(primary_key or [])
    not_null = list(not_null or [])
    ranges = dict(ranges or {})
    max_null_pct = dict(max_null_pct or {})

    errors: list[str] = []
    warnings: list[str] = []

    p = Path(parquet_path)
    if not p.exists():
        return ValidationResult(
            ok=False,
            errors=[f"Missing CLEAN parquet: {p}"],
            warnings=[],
            summary={"path": str(p)},
        )

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{p.as_posix()}')")

    # Columns
    cols = [r[0] for r in con.execute("DESCRIBE t").fetchall()]

    # Required columns
    missing = _missing_required_columns(cols, required)
    if missing:
        errors.append(f"Missing required columns: {missing}")

    # Row count
    row_count = int(con.execute("SELECT COUNT(*) FROM t").fetchone()[0])
    if row_count == 0:
        errors.append("CLEAN parquet has 0 rows")
    if min_rows is not None and row_count < int(min_rows):
        errors.append(f"CLEAN row_count too small: {row_count} < {min_rows}")

    # Not-null checks
    for c in not_null:
        if c not in cols:
            warnings.append(f"Not-null rule column missing in data: '{c}'")
            continue
        qc = _q_ident(c)
        nnull = int(con.execute(f"SELECT COUNT(*) FROM t WHERE {qc} IS NULL").fetchone()[0])
        if nnull > 0:
            errors.append(f"Column '{c}' has NULLs: {nnull}")

    # Null percentage thresholds
    if row_count > 0:
        for c, thr in max_null_pct.items():
            if c not in cols:
                warnings.append(f"Null-pct rule column missing in data: '{c}'")
                continue
            qc = _q_ident(c)
            nnull = int(con.execute(f"SELECT COUNT(*) FROM t WHERE {qc} IS NULL").fetchone()[0])
            pct = nnull / row_count
            if pct > float(thr):
                errors.append(f"Column '{c}' null_pct too high: {pct:.3%} > {float(thr):.3%}")

    # Primary key uniqueness
    if primary_key:
        if not all(c in cols for c in primary_key):
            warnings.append(f"Primary key columns not all present: {primary_key}")
        else:
            key_expr = ", ".join(_q_ident(c) for c in primary_key)
            dup_groups = int(
                con.execute(
                    f"""
                    SELECT COUNT(*) FROM (
                      SELECT {key_expr}, COUNT(*) AS n
                      FROM t
                      GROUP BY {key_expr}
                      HAVING COUNT(*) > 1
                    ) d
                    """
                ).fetchone()[0]
            )
            if dup_groups > 0:
                errors.append(f"Primary key duplicates found for {primary_key}: groups={dup_groups}")

    # Range checks (violation = below min OR above max)
    for c, rule in ranges.items():
        if c not in cols:
            warnings.append(f"Range rule column missing in data: '{c}'")
            continue

        qc = _q_ident(c)
        violations: list[str] = []
        if "min" in rule:
            violations.append(f"{qc} < {float(rule['min'])}")
        if "max" in rule:
            violations.append(f"{qc} > {float(rule['max'])}")

        if not violations:
            warnings.append(f"Range rule for '{c}' has no min/max, skipping")
            continue

        where = f"{qc} IS NOT NULL AND (" + " OR ".join(violations) + ")"
        bad = int(con.execute(f"SELECT COUNT(*) FROM t WHERE {where}").fetchone()[0])
        if bad > 0:
            errors.append(f"Range check failed for '{c}': bad_rows={bad} rules={rule}")

    con.close()

    return ValidationResult(
        ok=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        summary={
            "path": str(p),
            "row_count": row_count,
            "columns": cols,
            "required": required,
            "primary_key": primary_key,
            "not_null": not_null,
            "ranges": ranges,
            "max_null_pct": max_null_pct,
            "min_rows": min_rows,
        },
    )