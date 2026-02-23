# toolkit/core/validators.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from toolkit.core.exceptions import ValidationError


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    code: str
    details: dict

    def ensure(self) -> "ValidationResult":
        if not self.ok:
            raise ValidationError(f"[{self.code}] {self.details}")
        return self


def required_columns(actual: Iterable[str], required: Iterable[str]) -> ValidationResult:
    actual_set = set(actual)
    missing = [c for c in required if c not in actual_set]
    return ValidationResult(
        ok=(len(missing) == 0),
        code="required_columns",
        details={"missing": missing},
    )