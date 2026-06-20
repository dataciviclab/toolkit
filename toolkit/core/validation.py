from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from toolkit.core.config_models.mart import TransitionConfig
from toolkit.core.io import write_json_atomic


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    sections: dict[str, Any] = field(default_factory=dict)


def write_validation_json(path: str | Path, result: ValidationResult) -> Path:
    out = Path(path)
    quality_score = _compute_quality_score(len(result.errors), len(result.warnings))
    payload = {
        "validation_schema_version": 1,
        "ok": result.ok,
        "quality_score": quality_score,
        "quality_verdict": _quality_verdict(quality_score),
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


def _compute_quality_score(errors_count: int, warnings_count: int) -> int:
    """Quality score 0-100 con gradiente: errors pesano -20, warnings pesano -5.

    Un dataset perfetto (0 errori, 0 warnings) → 100.
    Un dataset con 1 errore → 80.
    Un dataset con 3 errori → 40.
    Un dataset con 0 errori ma 6 warnings → 70 (visibile ma non bloccante).
    """
    score = 100 - errors_count * 20 - warnings_count * 5
    return max(0, min(100, score))


def _quality_verdict(score: int) -> str:
    if score >= 80:
        return "buona"
    if score >= 50:
        return "accettabile"
    return "scarsa"


_MAX_MSGS_IN_RUN_RECORD = 20


def build_validation_summary(result: ValidationResult) -> dict[str, Any]:
    errors_count = len(result.errors)
    warnings_count = len(result.warnings)
    quality_score = _compute_quality_score(errors_count, warnings_count)
    out: dict[str, Any] = {
        "passed": result.ok,
        "errors_count": errors_count,
        "warnings_count": warnings_count,
        "quality_score": quality_score,
        "quality_verdict": _quality_verdict(quality_score),
        "errors": result.errors[:_MAX_MSGS_IN_RUN_RECORD],
        "warnings": result.warnings[:_MAX_MSGS_IN_RUN_RECORD],
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
    error_messages: list[str] = []
    structured_errors: list[dict[str, Any]] = []
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
                if transition_cfg.fail_on_row_drop_exceeded:
                    error_messages.append(message)
                    structured_errors.append(
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
                else:
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
            transition_cfg.max_row_drop_pct is not None or transition_cfg.warn_removed_columns
        ),
        "config": {
            "max_row_drop_pct": transition_cfg.max_row_drop_pct,
            "warn_removed_columns": transition_cfg.warn_removed_columns,
            "fail_on_row_drop_exceeded": transition_cfg.fail_on_row_drop_exceeded,
        },
        "profiles_count": len(transition_profiles),
        "errors_count": len(structured_errors),
        "errors": structured_errors,
        "error_messages": error_messages,
        "warnings_count": len(structured_warnings),
        "warnings": structured_warnings,
        "warning_messages": warning_messages,
    }
