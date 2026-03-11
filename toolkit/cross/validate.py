from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from toolkit.core.metadata import file_record, write_layer_manifest
from toolkit.core.paths import layer_dataset_dir, to_root_relative
from toolkit.core.validation import ValidationResult, build_validation_summary, write_validation_json


def validate_cross_outputs(
    cross_dir: str | Path,
    *,
    required_tables: list[str] | None = None,
    root: str | Path | None = None,
    years: list[int] | None = None,
) -> ValidationResult:
    required = sorted(set(required_tables or []))
    d = Path(cross_dir)
    dir_value = to_root_relative(d, Path(root)) if root is not None else str(d)
    if not d.exists():
        return ValidationResult(
            ok=False,
            errors=[f"Missing CROSS dir: {d}"],
            warnings=[],
            summary={
                "dir": dir_value,
                "years": list(years or []),
                "tables": [],
                "required_tables": required,
                "row_counts": {},
            },
        )

    existing_files = sorted(d.glob("*.parquet"))
    existing_tables = sorted(path.stem for path in existing_files)
    missing = [table for table in required if table not in existing_tables]

    errors: list[str] = []
    warnings: list[str] = []
    if missing:
        errors.append(f"Missing required CROSS tables: {missing}")

    con = duckdb.connect(":memory:")
    try:
        row_counts: dict[str, int] = {}
        for path in existing_files:
            try:
                row_counts[path.stem] = int(
                    con.execute(f"SELECT COUNT(*) FROM read_parquet('{path.as_posix()}')").fetchone()[0]
                )
            except Exception as exc:
                warnings.append(f"Could not count rows for {path.name}: {exc}")
    finally:
        con.close()

    return ValidationResult(
        ok=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        summary={
            "dir": dir_value,
            "years": list(years or []),
            "tables": existing_tables,
            "required_tables": required,
            "row_counts": row_counts,
        },
    )


def run_cross_validation(cfg, years: list[int], logger) -> dict[str, Any]:
    cross_dir = layer_dataset_dir(cfg.root, "cross", cfg.dataset)
    required_tables = [
        table.get("name")
        for table in (cfg.cross_year or {}).get("tables", [])
        if isinstance(table, dict) and table.get("name")
    ]

    result = validate_cross_outputs(
        cross_dir,
        required_tables=required_tables,
        root=cfg.root,
        years=years,
    )

    report = write_validation_json(Path(cross_dir) / "_validate" / "cross_validation.json", result)
    metadata_path = Path(cross_dir) / "metadata.json"
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        outputs = metadata.get("outputs", [])
    else:
        outputs = [file_record(path) for path in sorted(Path(cross_dir).glob("*.parquet"))]
    write_layer_manifest(
        cross_dir,
        metadata_path="metadata.json",
        validation_path="_validate/cross_validation.json",
        outputs=outputs,
        ok=result.ok,
        errors_count=len(result.errors),
        warnings_count=len(result.warnings),
    )
    logger.info("VALIDATE CROSS_YEAR -> %s (ok=%s)", report, result.ok)
    return build_validation_summary(result)
