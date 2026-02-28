from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from toolkit.core.exceptions import ValidationError
from toolkit.core.io import write_json_atomic


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def ensure(self) -> "ValidationResult":
        if not self.ok:
            raise ValidationError(str(self.errors or self.summary))
        return self


def write_validation_json(path: str | Path, result: ValidationResult) -> Path:
    out = Path(path)
    payload = {
        "validation_schema_version": 1,
        "ok": result.ok,
        "errors": result.errors,
        "warnings": result.warnings,
        "summary": result.summary,
    }
    write_json_atomic(out, payload)
    return out


def required_columns_check(actual: Iterable[str], required: Iterable[str]) -> ValidationResult:
    actual_list = list(actual)
    required_list = list(required)
    actual_set = set(actual_list)
    missing = [column for column in required_list if column not in actual_set]
    return ValidationResult(
        ok=(len(missing) == 0),
        errors=[] if not missing else [f"Missing required columns: {missing}"],
        warnings=[],
        summary={
            "required": required_list,
            "actual": actual_list,
            "missing": missing,
        },
    )


def build_validation_summary(result: ValidationResult) -> dict[str, Any]:
    errors_count = len(result.errors)
    warnings_count = len(result.warnings)
    return {
        "passed": result.ok,
        "errors_count": errors_count,
        "warnings_count": warnings_count,
        "checks": [
            {
                "name": "errors",
                "status": "passed" if errors_count == 0 else "failed",
                "details": f"errors={errors_count}",
            },
            {
                "name": "warnings",
                "status": "passed" if warnings_count == 0 else "warning",
                "details": f"warnings={warnings_count}",
            },
        ],
    }
