"""Encoding sniffing utilities for CSV profiling."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

COMMON_ENCODINGS = ["utf-8", "latin-1", "windows-1252", "CP1252"]

# Magic bytes for binary file formats supported by the toolkit.
# XLSX (ZIP-based): PK (0x50 0x43) at byte 0
# XLS (OLE2/BIFF): D0 CF (0xD0 0xCF) at byte 0
_BINARY_MAGIC = {
    b"PK\x03\x04": "xlsx",
    b"\xd0\xcf\x11\xe0": "xls",
}


def is_binary_file(filepath: Path) -> Optional[str]:
    """Detect binary file format from magic bytes. Returns 'xlsx', 'xls', or None."""
    with filepath.open("rb") as fh:
        header = fh.read(8)
    for magic, fmt in _BINARY_MAGIC.items():
        if header.startswith(magic):
            return fmt
    return None


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
