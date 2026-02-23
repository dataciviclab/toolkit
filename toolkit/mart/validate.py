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


def _q_ident(col: str) -> str:
    """Quote identifier for DuckDB (handles reserved words / special chars)."""
    return '"' + col.replace('"', '""') + '"'


def validate_mart(
    mart_dir: str | Path,
    required_tables: list[str] | None = None,
    *,
    table_rules: dict[str, dict[str, Any]] | None = None,
) -> ValidationResult:
    """
    Validate MART folder with optional per-table rules.

    Backward compatible:
    - If you only pass required_tables, it checks presence + counts rows best-effort.

    Rules structure:
    table_rules:
      mart_table_name:
        required_columns: [..]
        not_null: [..]
        primary_key: [..]
        ranges:
          col: {min: 0, max: 100}
    """
    required_tables = list(required_tables or [])
    table_rules = dict(table_rules or {})

    errors: list[str] = []
    warnings: list[str] = []

    d = Path(mart_dir)
    if not d.exists():
        return ValidationResult(ok=False, errors=[f"Missing MART dir: {d}"], warnings=[], summary={"dir": str(d)})

    existing_files = sorted(d.glob("*.parquet"))
    existing_tables = sorted([p.stem for p in existing_files])

    # Required tables presence
    missing = [t for t in required_tables if t not in existing_tables]
    if missing:
        errors.append(f"Missing required MART tables: {missing}")

    con = duckdb.connect(":memory:")
    row_counts: dict[str, int] = {}
    per_table: dict[str, Any] = {}

    for p in existing_files:
        name = p.stem

        try:
            rc = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{p.as_posix()}')").fetchone()[0])
            row_counts[name] = rc
        except Exception as e:
            warnings.append(f"Could not count rows for {p.name}: {e}")
            continue

        rules = table_rules.get(name, {})
        if not rules:
            continue

        con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{p.as_posix()}')")
        cols = [r[0] for r in con.execute("DESCRIBE t").fetchall()]

        # required columns
        req_cols = list(rules.get("required_columns") or [])
        miss_cols = [c for c in req_cols if c not in cols]
        if miss_cols:
            errors.append(f"[{name}] Missing required columns: {miss_cols}")

        # not null
        for c in (rules.get("not_null") or []):
            if c not in cols:
                warnings.append(f"[{name}] Not-null rule column missing: '{c}'")
                continue
            qc = _q_ident(c)
            nnull = int(con.execute(f"SELECT COUNT(*) FROM t WHERE {qc} IS NULL").fetchone()[0])
            if nnull > 0:
                errors.append(f"[{name}] Column '{c}' has NULLs: {nnull}")

        # primary key duplicates
        pk = list(rules.get("primary_key") or [])
        if pk:
            if not all(c in cols for c in pk):
                warnings.append(f"[{name}] Primary key columns not all present: {pk}")
            else:
                key_expr = ", ".join(_q_ident(c) for c in pk)
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
                    errors.append(f"[{name}] PK duplicates for {pk}: groups={dup_groups}")

        # ranges (violation = below min OR above max)
        for c, rule in (rules.get("ranges") or {}).items():
            if c not in cols:
                warnings.append(f"[{name}] Range rule column missing: '{c}'")
                continue

            qc = _q_ident(c)
            violations: list[str] = []
            if "min" in rule:
                violations.append(f"{qc} < {float(rule['min'])}")
            if "max" in rule:
                violations.append(f"{qc} > {float(rule['max'])}")

            if not violations:
                warnings.append(f"[{name}] Range rule for '{c}' has no min/max, skipping")
                continue

            where = f"{qc} IS NOT NULL AND (" + " OR ".join(violations) + ")"
            bad = int(con.execute(f"SELECT COUNT(*) FROM t WHERE {where}").fetchone()[0])
            if bad > 0:
                errors.append(f"[{name}] Range check failed for '{c}': bad_rows={bad} rules={rule}")

        per_table[name] = {"columns": cols, "rules": rules}

    con.close()

    return ValidationResult(
        ok=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        summary={
            "dir": str(d),
            "tables": existing_tables,
            "required_tables": required_tables,
            "row_counts": row_counts,
            "table_rules": table_rules,
            "per_table": per_table,
        },
    )