# toolkit/clean/report.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from toolkit.clean.validate import ValidationResult


def write_clean_validation(out_dir: str | Path, result: ValidationResult) -> Path:
    """
    Scrive un report JSON della validazione CLEAN in:
      <out_dir>/_validate/clean_validation.json
    Ritorna il path del report.
    """
    d = Path(out_dir) / "_validate"
    d.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "ok": result.ok,
        "errors": result.errors,
        "warnings": result.warnings,
        "summary": result.summary,
    }

    report_path = d / "clean_validation.json"
    report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return report_path