"""Encoding sniffing utilities for CSV profiling."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

COMMON_ENCODINGS = ["utf-8", "latin-1", "windows-1252", "CP1252"]


def _try_decode(filepath: Path, enc: str) -> Optional[str]:
    try:
        with filepath.open("r", encoding=enc, errors="strict") as f:
            return f.read(200_000)
    except Exception:
        return None


def sniff_encoding(filepath: Path) -> Tuple[str, str]:
    for enc in COMMON_ENCODINGS:
        txt = _try_decode(filepath, enc)
        if txt is not None:
            return enc, txt
    with filepath.open("r", encoding="utf-8", errors="replace") as f:
        return "utf-8", f.read(200_000)
