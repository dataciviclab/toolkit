"""Clean layer validation — validate_clean, validate_promotion, run_clean_validation."""

from __future__ import annotations

import json
import re as _re
from pathlib import Path
from typing import Any

import duckdb

from toolkit.clean._column_rules import (
    _check_max_null_pct,
    _check_not_null,
    _check_primary_key,
    _check_ranges,
)
from toolkit.clean._helpers import _input_files_from_clean_metadata, _profile_raw_input
from toolkit.clean.duckdb_read import read_raw_to_relation
from toolkit.core.config_models import CleanValidationSpec, RangeRuleConfig, TransitionConfig
from toolkit.core.layer_profile import compare_layer_profiles, profile_relation
from toolkit.core.metadata import write_layer_manifest
from toolkit.core.paths import layer_year_dir, to_root_relative
from toolkit.core.sql_utils import q_ident
from toolkit.core.validation import (
    ValidationResult,
    build_validation_summary,
    check_transitions,
    required_columns_check,
    write_validation_json,
)


def _clean_validation_spec(
    *,
    required: list[str] | None = None,
    primary_key: list[str] | None = None,
    not_null: list[str] | None = None,
    ranges: dict[str, RangeRuleConfig | dict[str, float]] | None = None,
    max_null_pct: dict[str, float] | None = None,
    min_rows: int | None = None,
) -> CleanValidationSpec:
    return CleanValidationSpec.model_validate(
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
    spec = _clean_validation_spec(
        required=required,
        primary_key=primary_key,
        not_null=not_null,
        ranges=ranges,
        max_null_pct=max_null_pct,
        min_rows=min_rows,
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
    try:
        con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{p.as_posix()}')")

        cols = [r[0] for r in con.execute("DESCRIBE t").fetchall()]

        required_result = required_columns_check(cols, required)
        errors.extend(required_result.errors)

        row_count = int(con.execute("SELECT COUNT(*) FROM t").fetchone()[0])
        if row_count == 0:
            errors.append("CLEAN parquet has 0 rows")
        if min_rows is not None and row_count < min_rows:
            errors.append(f"CLEAN row_count too small: {row_count} < {min_rows}")

        err_warn = _check_not_null(con, "t", not_null, cols)
        errors.extend(err_warn[0])
        warnings.extend(err_warn[1])

        if row_count > 0:
            err_warn = _check_max_null_pct(con, "t", max_null_pct, cols, row_count)
            errors.extend(err_warn[0])
            warnings.extend(err_warn[1])

        err_warn = _check_primary_key(con, "t", primary_key, cols)
        errors.extend(err_warn[0])
        warnings.extend(err_warn[1])

        err_warn = _check_ranges(con, "t", ranges, cols)
        errors.extend(err_warn[0])
        warnings.extend(err_warn[1])
    finally:
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


def validate_promotion(
    raw_dir: str | Path,
    clean_dir: str | Path,
    *,
    root: str | Path | None = None,
    transition: TransitionConfig | None = None,
    logger=None,
) -> ValidationResult:
    raw_path = Path(raw_dir)
    clean_path = Path(clean_dir)
    raw_value = to_root_relative(raw_path, Path(root)) if root is not None else str(raw_path)
    clean_value = to_root_relative(clean_path, Path(root)) if root is not None else str(clean_path)

    errors: list[str] = []
    warnings: list[str] = []

    clean_metadata_path = clean_path / "metadata.json"
    if not raw_path.exists():
        errors.append(f"Missing RAW dir: {raw_path}")
    if not clean_metadata_path.exists():
        errors.append(f"Missing CLEAN metadata: {clean_metadata_path}")
    if errors:
        return ValidationResult(
            ok=False,
            errors=errors,
            warnings=warnings,
            summary={"raw_dir": raw_value, "clean_dir": clean_value},
        )

    clean_metadata = json.loads(clean_metadata_path.read_text(encoding="utf-8"))
    input_files = _input_files_from_clean_metadata(raw_path, clean_metadata)
    missing_inputs = [path for path in input_files if not path.exists()]
    if missing_inputs:
        errors.append(f"Missing RAW input files used by CLEAN: {[p.name for p in missing_inputs]}")

    clean_profile = clean_metadata.get("output_profile")
    if not isinstance(clean_profile, dict):
        errors.append("CLEAN metadata missing output_profile")

    if errors:
        return ValidationResult(
            ok=False,
            errors=errors,
            warnings=warnings,
            summary={"raw_dir": raw_value, "clean_dir": clean_value},
        )

    read_cfg = clean_metadata.get("read_params_used") or {}
    read_mode = str(clean_metadata.get("read_source_used") or "fallback")

    profile_dir = raw_path / "_profile"
    saved_profile_path = profile_dir / "raw_profile.json"
    if not saved_profile_path.exists():
        saved_profile_path = profile_dir / "profile.json"

    if saved_profile_path.exists():
        try:
            saved = json.loads(saved_profile_path.read_text(encoding="utf-8"))
            raw_profile = {
                "row_count": saved.get("row_count"),
                "columns": [{"name": c, "type": "VARCHAR"} for c in (saved.get("columns_raw") or [])],
            }
        except Exception:
            raw_profile = _profile_raw_input(input_files, read_cfg, read_mode, logger)
    else:
        raw_profile = _profile_raw_input(input_files, read_cfg, read_mode, logger)
    transition_profile = compare_layer_profiles(
        raw_profile,
        clean_profile,
        source_layer="raw",
        target_layer="clean",
        target_name="clean",
    )
    transition_report = check_transitions(
        [transition_profile] if transition_profile is not None else [],
        transition or TransitionConfig(),
    )
    warnings.extend(transition_report["warning_messages"])

    return ValidationResult(
        ok=True,
        errors=[],
        warnings=warnings,
        summary={
            "raw_dir": raw_value,
            "clean_dir": clean_value,
            "input_files": [path.name for path in input_files],
            "raw_row_count": raw_profile.get("row_count"),
            "clean_row_count": clean_profile.get("row_count"),
            "raw_col_count": len(raw_profile.get("columns") or []),
        },
        sections={"transition": transition_report},
    )


def run_clean_validation(cfg, year: int, logger) -> dict[str, Any]:
    out_dir = layer_year_dir(cfg.root, "clean", cfg.dataset, year)
    parquet = out_dir / f"{cfg.dataset}_{year}_clean.parquet"

    clean_cfg: dict[str, Any] = getattr(cfg, "clean", {}) or {}
    spec = CleanValidationSpec.model_validate(
        {
            "required_columns": clean_cfg.get("required_columns"),
            "validate": clean_cfg.get("validate") or {},
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

    # cross-layer raw→clean check (row retention, column coverage)
    raw_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, year)
    promotion_result = validate_promotion(
        raw_dir,
        out_dir,
        root=cfg.root,
        transition=spec.validate.promotion,
        logger=logger,
    )
    merged_errors = result.errors + promotion_result.errors
    merged_warnings = result.warnings + promotion_result.warnings

    clean_cols = result.summary.get("columns") or []
    raw_row_count = promotion_result.summary.get("raw_row_count")
    clean_row_count = promotion_result.summary.get("clean_row_count") or result.summary.get("row_count")
    raw_col_count = promotion_result.summary.get("raw_col_count")

    # scaffold check: legge profile raw (canonical prima, fallback legacy alias)
    # Usa columns_raw dal profile come source of truth per raw_col_count, bypassing
    # _profile_raw_input che potrebbe rileggere il CSV con parametri header errati
    _profile_dir = raw_dir / "_profile"
    profile_path = _profile_dir / "raw_profile.json"
    if not profile_path.exists():
        profile_path = _profile_dir / "profile.json"
    trusted_raw_cols: list[str] = []
    if profile_path.exists():
        try:
            def _to_snake(n: str) -> str:
                s = _re.sub(r"([a-z])([A-Z])", r"\1_\2", n.strip())
                s = _re.sub(r"[^a-zA-Z0-9]+", "_", s)
                return _re.sub(r"_+", "_", s).lower().strip("_") or "col"

            raw_profile = json.loads(profile_path.read_text(encoding="utf-8"))
            trusted_raw_cols = raw_profile.get("columns_raw") or []
            scaffold_cols = {_to_snake(c) for c in trusted_raw_cols}
            clean_cols_set = set(clean_cols)
            unmapped = sorted(scaffold_cols - clean_cols_set)
            if unmapped:
                merged_warnings.append(
                    f"[scaffold] {len(unmapped)} colonne raw non mappate nel clean "
                    f"(drop senza -- DROP: <motivo>?): {unmapped}"
                )
        except Exception:
            pass

    actual_raw_col_count: int | None = len(trusted_raw_cols) if trusted_raw_cols else None
    raw_missing_columns: list[str] = []

    if not trusted_raw_cols:
        # Find raw file(s) to probe actual column count — parquet preferred, CSV fallback
        _raw_file: Path | None = None
        for _pattern in ("*.parquet", "*.csv"):
            _candidates = list(raw_dir.glob(_pattern))
            if _candidates:
                _raw_file = _candidates[0]
                break
        if _raw_file is not None:
            try:
                _con = duckdb.connect(":memory:")
                try:
                    if _raw_file.suffix == ".parquet":
                        _query = f'DESCRIBE SELECT * FROM read_parquet("{_raw_file.as_posix()}")'
                    else:
                        _csv_path = _raw_file.as_posix()
                        _query = (
                            f"DESCRIBE SELECT * FROM read_csv(\"{_csv_path}\", auto_detect=true)"
                        )
                    _col_rows = _con.execute(_query).fetchall()
                    _actual_raw_col_names = [str(r[0]) for r in _col_rows]
                    actual_raw_col_count = len(_actual_raw_col_names)

                    # Infer expected columns from config
                    _read_cfg = clean_cfg.get("read") or {}
                    _expected_cols: list[str] = []
                    if _read_cfg.get("normalize_rows_to_columns"):
                        _col_defs = _read_cfg.get("columns") or {}
                        if isinstance(_col_defs, dict) and not _col_defs:
                            _expected_cols = clean_cols
                        elif isinstance(_col_defs, dict):
                            _expected_cols = list(_col_defs.keys())
                        elif isinstance(_col_defs, list):
                            _expected_cols = _col_defs
                    if _expected_cols:
                        _actual_set = set(_actual_raw_col_names)
                        raw_missing_columns = sorted(
                            c for c in _expected_cols if c not in _actual_set
                        )
                finally:
                    _con.close()
            except Exception:
                pass

    row_drop_pct = (
        round((raw_row_count - clean_row_count) / raw_row_count * 100, 2)
        if raw_row_count and clean_row_count is not None and raw_row_count > 0
        else None
    )
    col_drop_count = (raw_col_count - len(clean_cols)) if raw_col_count is not None else None

    rules = {k: v for k, v in {
        "required": spec.required_columns or [],
        "primary_key": spec.validate.primary_key or [],
        "not_null": spec.validate.not_null or [],
        "ranges": {c: {"min": r.min, "max": r.max} for c, r in (spec.validate.ranges or {}).items()},
        "max_null_pct": spec.validate.max_null_pct or {},
        "min_rows": spec.validate.min_rows,
    }.items() if v not in ([], {}, None)}

    merged_summary = {
        "dataset": cfg.dataset,
        "year": year,
        "stats": {
            "raw_rows": raw_row_count,
            "clean_rows": clean_row_count,
            "row_drop_pct": row_drop_pct,
            "raw_cols": raw_col_count,
            "clean_cols": len(clean_cols),
            "col_drop_count": col_drop_count,
            **({"actual_raw_cols": actual_raw_col_count} if actual_raw_col_count is not None else {}),
            **({"raw_missing_columns": raw_missing_columns} if raw_missing_columns else {}),
        },
        "columns": clean_cols,
        **({"rules": rules} if rules else {}),
    }
    merged_sections = {**(result.sections or {}), **(promotion_result.sections or {})}
    result = ValidationResult(
        ok=len(merged_errors) == 0,
        errors=merged_errors,
        warnings=merged_warnings,
        summary=merged_summary,
        sections=merged_sections,
    )

    report = write_validation_json(Path(out_dir) / "_validate" / "clean_validation.json", result)
    metadata = json.loads((out_dir / "metadata.json").read_text(encoding="utf-8"))
    write_layer_manifest(
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