from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from toolkit.core.config_models import MartTableRuleConfig, MartValidationSpec
from toolkit.core.metadata import write_manifest
from toolkit.core.paths import layer_year_dir, to_root_relative
from toolkit.core.validation import (
    ValidationResult,
    build_validation_summary,
    required_columns_check,
    write_validation_json,
)


def _q_ident(col: str) -> str:
    """Quote identifier for DuckDB (handles reserved words / special chars)."""
    return '"' + col.replace('"', '""') + '"'


def validate_mart(
    mart_dir: str | Path,
    required_tables: list[str] | None = None,
    *,
    root: str | Path | None = None,
    table_rules: dict[str, MartTableRuleConfig | dict[str, Any]] | None = None,
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
    spec = MartValidationSpec.model_validate(
        {
            "required_tables": required_tables,
            "validate": {
                "table_rules": table_rules or {},
            },
        }
    )
    required_tables = spec.required_tables
    table_rules = spec.validate.table_rules

    errors: list[str] = []
    warnings: list[str] = []

    d = Path(mart_dir)
    dir_value = to_root_relative(d, Path(root)) if root is not None else str(d)
    if not d.exists():
        return ValidationResult(ok=False, errors=[f"Missing MART dir: {d}"], warnings=[], summary={"dir": dir_value})

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

        rules = table_rules.get(name)
        if not rules:
            continue

        min_rows = rules.min_rows
        if min_rows is not None and rc < min_rows:
            errors.append(f"[{name}] row_count too small: {rc} < {min_rows}")

        con.execute(f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{p.as_posix()}')")
        cols = [r[0] for r in con.execute("DESCRIBE t").fetchall()]

        # required columns
        req_cols = rules.required_columns
        required_result = required_columns_check(cols, req_cols)
        errors.extend([f"[{name}] {error}" for error in required_result.errors])

        # not null
        for c in rules.not_null:
            if c not in cols:
                warnings.append(f"[{name}] Not-null rule column missing: '{c}'")
                continue
            qc = _q_ident(c)
            nnull = int(con.execute(f"SELECT COUNT(*) FROM t WHERE {qc} IS NULL").fetchone()[0])
            if nnull > 0:
                errors.append(f"[{name}] Column '{c}' has NULLs: {nnull}")

        # primary key duplicates
        pk = rules.primary_key
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
        for c, rule in rules.ranges.items():
            if c not in cols:
                warnings.append(f"[{name}] Range rule column missing: '{c}'")
                continue

            qc = _q_ident(c)
            violations: list[str] = []
            if rule.min is not None:
                violations.append(f"{qc} < {rule.min}")
            if rule.max is not None:
                violations.append(f"{qc} > {rule.max}")

            if not violations:
                warnings.append(f"[{name}] Range rule for '{c}' has no min/max, skipping")
                continue

            where = f"{qc} IS NOT NULL AND (" + " OR ".join(violations) + ")"
            bad = int(con.execute(f"SELECT COUNT(*) FROM t WHERE {where}").fetchone()[0])
            if bad > 0:
                errors.append(
                    f"[{name}] Range check failed for '{c}': bad_rows={bad} "
                    f"rules={{'min': {rule.min}, 'max': {rule.max}}}"
                )

        per_table[name] = {
            "columns": cols,
            "rules": {
                "required_columns": rules.required_columns,
                "not_null": rules.not_null,
                "primary_key": rules.primary_key,
                "ranges": {
                    column: {"min": range_rule.min, "max": range_rule.max}
                    for column, range_rule in rules.ranges.items()
                },
                "min_rows": rules.min_rows,
            },
        }

    con.close()

    return ValidationResult(
        ok=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        summary={
            "dir": dir_value,
            "tables": existing_tables,
            "required_tables": required_tables,
            "row_counts": row_counts,
            "table_rules": {
                table: {
                    "required_columns": rule.required_columns,
                    "not_null": rule.not_null,
                    "primary_key": rule.primary_key,
                    "ranges": {
                        column: {"min": range_rule.min, "max": range_rule.max}
                        for column, range_rule in rule.ranges.items()
                    },
                    "min_rows": rule.min_rows,
                }
                for table, rule in table_rules.items()
            },
            "per_table": per_table,
        },
    )


def run_mart_validation(cfg, year: int, logger) -> dict[str, Any]:
    mart_dir = layer_year_dir(cfg.root, "mart", cfg.dataset, year)

    mart_cfg: dict[str, Any] = cfg.mart or {}
    spec = MartValidationSpec.model_validate(mart_cfg)

    result = validate_mart(
        mart_dir,
        required_tables=spec.required_tables,
        root=cfg.root,
        table_rules=spec.validate.table_rules,
    )

    report = write_validation_json(Path(mart_dir) / "_validate" / "mart_validation.json", result)
    metadata = json.loads((mart_dir / "metadata.json").read_text(encoding="utf-8"))
    write_manifest(
        mart_dir,
        metadata_path="metadata.json",
        validation_path="_validate/mart_validation.json",
        outputs=metadata.get("outputs", []),
        ok=result.ok,
        errors_count=len(result.errors),
        warnings_count=len(result.warnings),
    )
    logger.info(f"VALIDATE MART -> {report} (ok={result.ok})")
    return build_validation_summary(result)
