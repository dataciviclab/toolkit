from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json


TEXT_EXT = {".csv", ".txt", ".tsv", ".json", ".xml", ".html"}
CSV_EXT = {".csv", ".tsv"}


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str]
    warnings: list[str]
    summary: dict[str, Any]


def _looks_like_text(b: bytes) -> bool:
    # Heuristic: bytes mostly printable or whitespace
    # If there are many null bytes, it's likely binary
    if not b:
        return True
    if b.count(b"\x00") > 0:
        return False
    sample = b[:4096]
    # count non-printable
    non_printable = 0
    for ch in sample:
        if ch in (9, 10, 13):  # tab, lf, cr
            continue
        if 32 <= ch <= 126:  # basic ASCII printable
            continue
        non_printable += 1
    return (non_printable / max(1, len(sample))) < 0.10


def _starts_with_html(b: bytes) -> bool:
    head = b[:1024].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html")


def validate_raw_output(out_dir: Path, files_written: list[dict]) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    if not files_written:
        errors.append("No files were written in RAW output.")
        return ValidationResult(
            ok=False,
            errors=errors,
            warnings=warnings,
            summary={"files": 0},
        )

    # per-file checks
    for f in files_written:
        fname = f.get("file")
        size = f.get("bytes")
        sha = f.get("sha256")

        if not fname:
            errors.append("A written file entry is missing 'file' name.")
            continue

        fpath = out_dir / fname
        if not fpath.exists():
            errors.append(f"File missing on disk: {fname}")
            continue

        if size is None or size <= 0:
            errors.append(f"Zero/invalid bytes for file: {fname} (bytes={size})")

        if not sha:
            errors.append(f"Missing sha256 for file: {fname}")

        ext = fpath.suffix.lower()

        # read a small sample for content checks
        try:
            content = fpath.read_bytes()
        except Exception as e:
            errors.append(f"Cannot read file {fname}: {e}")
            continue

        if ext in TEXT_EXT:
            if not _looks_like_text(content):
                errors.append(f"Expected text-like file but looks binary: {fname}")

        if ext in CSV_EXT:
            if _starts_with_html(content):
                errors.append(f"CSV file appears to contain HTML (likely error page): {fname}")
            # warnings for tiny / single-line
            if len(content) < 200:
                warnings.append(f"Very small CSV/TSV file (<200B): {fname}")
            if b"\n" not in content:
                warnings.append(f"CSV/TSV file has no newline (single line): {fname}")

    ok = len(errors) == 0
    summary = {
        "files": len(files_written),
        "total_bytes": sum(int(f.get("bytes") or 0) for f in files_written),
    }
    return ValidationResult(ok=ok, errors=errors, warnings=warnings, summary=summary)


def write_raw_validation(out_dir: Path, result: ValidationResult) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "raw_validation.json"
    payload = {
        "ok": result.ok,
        "errors": result.errors,
        "warnings": result.warnings,
        "summary": result.summary,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path