"""Encoding sniffing utilities for CSV profiling."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

COMMON_ENCODINGS = ["utf-8", "latin-1", "windows-1252", "CP1252"]

# Magic bytes for binary file formats supported by the toolkit.
# XLS (OLE2/BIFF): D0 CF (0xD0 0xCF) at byte 0 — unambiguous
# XLSX (ZIP-based): PK (0x50 0x43) at byte 0 — also matches generic ZIP
# XLSM is also ZIP-based with macro flag, treated same as XLSX.
_XLS_MAGIC = b"\xd0\xcf\x11\xe0"
_XLSX_ZIP_MAGIC = b"PK\x03\x04"


def is_binary_file(filepath: Path) -> Optional[str]:
    """Detect binary file format from magic bytes.

    Returns 'xls', 'xlsx', 'zip', or None.
    - 'xls': OLE2/BIFF magic — unambiguous legacy Excel format
    - 'xlsx': ZIP magic + .xlsx/.xlsm suffix — ZIP-based Excel format
    - 'zip': ZIP magic with non-Excel suffix — generic ZIP, not profiled as Excel
    - None: not a binary format we handle specially
    """
    ext = filepath.suffix.lower()
    with filepath.open("rb") as fh:
        header = fh.read(8)
    if header.startswith(_XLS_MAGIC):
        return "xls"
    if header.startswith(_XLSX_ZIP_MAGIC):
        if ext in (".xlsx", ".xlsm"):
            return "xlsx"
        return "zip"
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
