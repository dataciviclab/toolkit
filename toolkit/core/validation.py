from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from toolkit.core.config_models import TransitionConfig
from toolkit.core.exceptions import ValidationError
from toolkit.core.io import write_json_atomic


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    sections: dict[str, Any] = field(default_factory=dict)

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
    for key, section in result.sections.items():
        if isinstance(section, dict):
            payload[key] = {k: v for k, v in section.items() if k != "warning_messages"}
        else:
            payload[key] = section
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
    out: dict[str, Any] = {
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
    if "stats" in result.summary:
        out["stats"] = result.summary["stats"]
    return out


def check_transitions(
    transition_profiles: list[dict[str, Any]],
    transition_cfg: TransitionConfig,
) -> dict[str, Any]:
    warning_messages: list[str] = []
    structured_warnings: list[dict[str, Any]] = []
    for profile in transition_profiles:
        target_name = profile.get("target_name", "?")
        source_layer = profile.get("from") or "clean"
        target_layer = profile.get("to") or "mart"
        source_rows = profile.get("source_row_count") or 0
        target_rows = profile.get("target_row_count") or 0
        removed = profile.get("removed_columns") or []

        if (
            transition_cfg.max_row_drop_pct is not None
            and source_rows > 0
            and target_rows < source_rows
        ):
            drop_pct = (source_rows - target_rows) / source_rows * 100
            if drop_pct > transition_cfg.max_row_drop_pct:
                message = (
                    f"[transition:{target_name}] row drop {drop_pct:.1f}% "
                    f"exceeds threshold {transition_cfg.max_row_drop_pct}% "
                    f"({source_layer}={source_rows} -> {target_layer}={target_rows})"
                )
                warning_messages.append(message)
                structured_warnings.append(
                    {
                        "kind": "row_drop_pct",
                        "target_name": target_name,
                        "source_row_count": source_rows,
                        "target_row_count": target_rows,
                        "drop_pct": round(drop_pct, 1),
                        "threshold_pct": transition_cfg.max_row_drop_pct,
                        "message": message,
                    }
                )

        if transition_cfg.warn_removed_columns and removed:
            message = f"[transition:{target_name}] columns removed from {source_layer}: {removed}"
            warning_messages.append(message)
            structured_warnings.append(
                {
                    "kind": "removed_columns",
                    "target_name": target_name,
                    "removed_columns": removed,
                    "message": message,
                }
            )

    return {
        "enabled": (
            transition_cfg.max_row_drop_pct is not None
            or transition_cfg.warn_removed_columns
        ),
        "config": {
            "max_row_drop_pct": transition_cfg.max_row_drop_pct,
            "warn_removed_columns": transition_cfg.warn_removed_columns,
        },
        "profiles_count": len(transition_profiles),
        "warnings_count": len(structured_warnings),
        "warnings": structured_warnings,
        "warning_messages": warning_messages,
    }
