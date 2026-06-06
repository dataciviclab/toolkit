"""Column-level validation rules for clean/mart parquet files.

Canonical location for reusable validation rules.
Consumed by ``clean.validate`` and ``mart.validate``.
"""

from __future__ import annotations

from typing import Any

import duckdb

from toolkit.core.sql_utils import q_ident


def _prefixed(prefix: str, msg: str) -> str:
    """Prepend prefix if non-empty."""
    return f"{prefix}{msg}" if prefix else msg


def check_not_null(
    con: duckdb.DuckDBPyConnection,
    table: str,
    columns: list[str],
    cols: list[str],
    prefix: str = "",
) -> tuple[list[str], list[str]]:
    """Check not-null constraints. Returns (errors, warnings)."""
    errors: list[str] = []
    warnings: list[str] = []
    for c in columns:
        if c not in cols:
            warnings.append(_prefixed(prefix, f"Not-null rule column missing in data: '{c}'"))
            continue
        qc = q_ident(c)
        nnull = int(con.execute(f"SELECT COUNT(*) FROM {table} WHERE {qc} IS NULL").fetchone()[0])
        if nnull > 0:
            errors.append(_prefixed(prefix, f"Column '{c}' has NULLs: {nnull}"))
    return errors, warnings


def check_primary_key(
    con: duckdb.DuckDBPyConnection,
    table: str,
    pk: list[str],
    cols: list[str],
    prefix: str = "",
) -> tuple[list[str], list[str]]:
    """Check primary key uniqueness. Returns (errors, warnings)."""
    errors: list[str] = []
    warnings: list[str] = []
    if not pk:
        return errors, warnings
    if not all(c in cols for c in pk):
        warnings.append(_prefixed(prefix, f"Primary key columns not all present: {pk}"))
    else:
        key_expr = ", ".join(q_ident(c) for c in pk)
        dup_groups = int(
            con.execute(
                f"""
                SELECT COUNT(*) FROM (
                  SELECT {key_expr}, COUNT(*) AS n
                  FROM {table}
                  GROUP BY {key_expr}
                  HAVING COUNT(*) > 1
                ) d
                """
            ).fetchone()[0]
        )
        if dup_groups > 0:
            errors.append(
                _prefixed(prefix, f"Primary key duplicates found for {pk}: groups={dup_groups}")
            )
    return errors, warnings


def check_ranges(
    con: duckdb.DuckDBPyConnection,
    table: str,
    ranges: dict[str, Any],
    cols: list[str],
    prefix: str = "",
) -> tuple[list[str], list[str]]:
    """Check min/max range constraints. Returns (errors, warnings)."""
    errors: list[str] = []
    warnings: list[str] = []
    for c, rule in ranges.items():
        if c not in cols:
            warnings.append(_prefixed(prefix, f"Range rule column missing in data: '{c}'"))
            continue

        qc = q_ident(c)
        violations: list[str] = []
        if rule.min is not None:
            violations.append(f"{qc} < {rule.min}")
        if rule.max is not None:
            violations.append(f"{qc} > {rule.max}")

        if not violations:
            warnings.append(_prefixed(prefix, f"Range rule for '{c}' has no min/max, skipping"))
            continue

        where = f"{qc} IS NOT NULL AND (" + " OR ".join(violations) + ")"
        bad = int(con.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0])
        if bad > 0:
            errors.append(
                _prefixed(
                    prefix,
                    f"Range check failed for '{c}': bad_rows={bad} "
                    f"rules={{'min': {rule.min}, 'max': {rule.max}}}",
                )
            )
    return errors, warnings


def check_max_null_pct(
    con: duckdb.DuckDBPyConnection,
    table: str,
    max_null_pct: dict[str, float],
    cols: list[str],
    row_count: int,
    prefix: str = "",
) -> tuple[list[str], list[str]]:
    """Check max null percentage constraints. Returns (errors, warnings)."""
    errors: list[str] = []
    warnings: list[str] = []
    if row_count == 0:
        return errors, warnings
    for c, thr in max_null_pct.items():
        if c not in cols:
            warnings.append(_prefixed(prefix, f"Null-pct rule column missing in data: '{c}'"))
            continue
        qc = q_ident(c)
        nnull = int(con.execute(f"SELECT COUNT(*) FROM {table} WHERE {qc} IS NULL").fetchone()[0])
        pct = nnull / row_count
        if pct > thr:
            errors.append(
                _prefixed(prefix, f"Column '{c}' null_pct too high: {pct:.3%} > {thr:.3%}")
            )
    return errors, warnings
