from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lab_connectors.duckdb import safe_connect

from toolkit.core.column_rules import (
    check_max_null_pct,
    check_not_null,
    check_primary_key,
    check_ranges,
)
from toolkit.core.config_models import MartTableRuleConfig, MartValidationSpec
from toolkit.core.metadata import merge_layer_manifest
from toolkit.core.paths import MART_VALIDATION, layer_year_dir, to_root_relative
from toolkit.core.validation import (
    ValidationResult,
    build_validation_summary,
    check_transitions,
    required_columns_check,
    write_validation_json,
)


def validate_mart(
    mart_dir: str | Path,
    required_tables: list[str] | None = None,
    *,
    root: str | Path | None = None,
    table_rules: dict[str, MartTableRuleConfig | dict[str, Any]] | None = None,
    declared_tables: list[str] | None = None,
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
    declared_tables = sorted(set(declared_tables or []))
    orphan_rules: list[str] = []

    # Required tables presence
    missing = [t for t in required_tables if t not in existing_tables]
    if missing:
        errors.append(f"Missing required MART tables: {missing}")

    if declared_tables:
        orphan_rules = sorted(table for table in table_rules.keys() if table not in declared_tables)
        if orphan_rules:
            warnings.append(
                "MART table_rules reference tables not declared in mart.tables: "
                f"{orphan_rules}"
            )

    with safe_connect() as con:
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

            # not null — centralizzato in core.column_rules
            prefix = f"[{name}] "
            err_warn = check_not_null(con, "t", rules.not_null, cols, prefix=prefix)
            errors.extend(err_warn[0])
            warnings.extend(err_warn[1])

            # primary key duplicates — centralizzato in core.column_rules
            err_warn = check_primary_key(con, "t", rules.primary_key, cols, prefix=prefix)
            errors.extend(err_warn[0])
            warnings.extend(err_warn[1])

            # ranges — centralizzato in core.column_rules
            err_warn = check_ranges(con, "t", rules.ranges, cols, prefix=prefix)
            errors.extend(err_warn[0])
            warnings.extend(err_warn[1])

            # max_null_pct — centralizzato in core.column_rules
            if rules.max_null_pct:
                err_warn = check_max_null_pct(con, "t", rules.max_null_pct, cols, rc, prefix=prefix)
                errors.extend(err_warn[0])
                warnings.extend(err_warn[1])

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
                    "max_null_pct": rules.max_null_pct,
                    "min_rows": rules.min_rows,
                },
            }

    return ValidationResult(
        ok=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        summary={
            "dir": dir_value,
            "tables": existing_tables,
            "required_tables": required_tables,
            "declared_tables": declared_tables,
            "row_counts": row_counts,
            "orphan_table_rules": orphan_rules,
            "table_rules": {
                table: {
                    "required_columns": rule.required_columns,
                    "not_null": rule.not_null,
                    "primary_key": rule.primary_key,
                    "ranges": {
                        column: {"min": range_rule.min, "max": range_rule.max}
                        for column, range_rule in rule.ranges.items()
                    },
                    "max_null_pct": rule.max_null_pct,
                    "min_rows": rule.min_rows,
                }
                for table, rule in table_rules.items()
            },
            "per_table": per_table,
        },
    )


def run_mart_validation(cfg, year: int, logger, *, sample_mode: bool = False) -> dict[str, Any]:
    mart_dir = layer_year_dir(cfg.root, "mart", cfg.dataset, year)

    mart_cfg: dict[str, Any] = cfg.mart or {}
    declared_tables = [
        table.get("name")
        for table in mart_cfg.get("tables", [])
        if isinstance(table, dict) and table.get("name")
    ]
    spec = MartValidationSpec.model_validate(
        {
            "required_tables": mart_cfg.get("required_tables"),
            "validate": mart_cfg.get("validate") or {},
        }
    )

    # In sample mode, min_rows non e' applicabile (campione non rappresentativo).
    if sample_mode:
        for rule in spec.validate.table_rules.values():
            rule.min_rows = None

    result = validate_mart(
        mart_dir,
        required_tables=spec.required_tables,
        root=cfg.root,
        table_rules=spec.validate.table_rules,
        declared_tables=declared_tables,
    )

    metadata = json.loads((mart_dir / "metadata.json").read_text(encoding="utf-8"))
    transition_report = check_transitions(
        metadata.get("transition_profiles") or [],
        spec.validate.transition,
    )
    if transition_report["warning_messages"]:
        result = ValidationResult(
            ok=result.ok,
            errors=result.errors,
            warnings=result.warnings + transition_report["warning_messages"],
            summary=result.summary,
            sections={"transition": transition_report},
        )
    else:
        result = ValidationResult(
            ok=result.ok,
            errors=result.errors,
            warnings=result.warnings,
            summary=result.summary,
            sections={"transition": transition_report},
        )

    report = write_validation_json(Path(mart_dir) / MART_VALIDATION, result)
    merge_layer_manifest(
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
