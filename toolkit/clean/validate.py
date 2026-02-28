from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from toolkit.core.config_models import CleanValidationSpec, RangeRuleConfig
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


def validate_clean(
    parquet_path: str | Path,
    required: list[str] | None = None,
    *,
    root: str | Path | None = None,
    primary_key: list[str] | None = None,
    not_null: list[str] | None = None,
    ranges: dict[str, RangeRuleConfig | dict[str, float]] | None = None,
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
    spec = CleanValidationSpec.model_validate(
        {
            "required_columns": required,
            "validate": {
                "primary_key": primary_key,
                "not_null": not_null,
                "ranges": ranges or {},
                "max_null_pct": max_null_pct or {},
                "min_rows": min_rows,
            },
        }
    )
    required = spec.required_columns
    rules = spec.validate
    primary_key = rules.primary_key
    not_null = rules.not_null
    ranges = rules.ranges
    max_null_pct = rules.max_null_pct
    min_rows = rules.min_rows

    errors: list[str] = []
    warnings: list[str] = []

    p = Path(parquet_path)
    path_value = to_root_relative(p, Path(root)) if root is not None else str(p)
    if not p.exists():
        return ValidationResult(
            ok=False,
            errors=[f"Missing CLEAN parquet: {p}"],
            warnings=[],
            summary={"path": path_value},
        )

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{p.as_posix()}')")

    # Columns
    cols = [r[0] for r in con.execute("DESCRIBE t").fetchall()]

    # Required columns
    required_result = required_columns_check(cols, required)
    errors.extend(required_result.errors)

    # Row count
    row_count = int(con.execute("SELECT COUNT(*) FROM t").fetchone()[0])
    if row_count == 0:
        errors.append("CLEAN parquet has 0 rows")
    if min_rows is not None and row_count < min_rows:
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
            if pct > thr:
                errors.append(f"Column '{c}' null_pct too high: {pct:.3%} > {thr:.3%}")

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
        if rule.min is not None:
            violations.append(f"{qc} < {rule.min}")
        if rule.max is not None:
            violations.append(f"{qc} > {rule.max}")

        if not violations:
            warnings.append(f"Range rule for '{c}' has no min/max, skipping")
            continue

        where = f"{qc} IS NOT NULL AND (" + " OR ".join(violations) + ")"
        bad = int(con.execute(f"SELECT COUNT(*) FROM t WHERE {where}").fetchone()[0])
        if bad > 0:
            errors.append(
                f"Range check failed for '{c}': bad_rows={bad} "
                f"rules={{'min': {rule.min}, 'max': {rule.max}}}"
            )

    con.close()

    return ValidationResult(
        ok=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        summary={
            "path": path_value,
            "row_count": row_count,
            "columns": cols,
            "required": required,
            "primary_key": primary_key,
            "not_null": not_null,
            "ranges": {
                column: {"min": rule.min, "max": rule.max}
                for column, rule in ranges.items()
            },
            "max_null_pct": max_null_pct,
            "min_rows": min_rows,
        },
    )


def run_clean_validation(cfg, year: int, logger) -> dict[str, Any]:
    out_dir = layer_year_dir(cfg.root, "clean", cfg.dataset, year)
    parquet = out_dir / f"{cfg.dataset}_{year}_clean.parquet"

    clean_cfg: dict[str, Any] = cfg.clean or {}
    spec = CleanValidationSpec.model_validate(
        {
            "required_columns": clean_cfg.get("required_columns"),
            "validate": clean_cfg.get("validate"),
        }
    )

    result = validate_clean(
        parquet,
        required=spec.required_columns,
        root=cfg.root,
        primary_key=spec.validate.primary_key,
        not_null=spec.validate.not_null,
        ranges=spec.validate.ranges,
        max_null_pct=spec.validate.max_null_pct,
        min_rows=spec.validate.min_rows,
    )

    report = write_validation_json(Path(out_dir) / "_validate" / "clean_validation.json", result)
    metadata = json.loads((out_dir / "metadata.json").read_text(encoding="utf-8"))
    write_manifest(
        out_dir,
        metadata_path="metadata.json",
        validation_path="_validate/clean_validation.json",
        outputs=metadata.get("outputs", []),
        ok=result.ok,
        errors_count=len(result.errors),
        warnings_count=len(result.warnings),
    )
    logger.info(f"VALIDATE CLEAN -> {report} (ok={result.ok})")
    return build_validation_summary(result)
