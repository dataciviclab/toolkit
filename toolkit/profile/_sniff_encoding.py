"""Encoding sniffing utilities for CSV profiling."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

COMMON_ENCODINGS = ["utf-8", "latin-1", "windows-1252", "CP1252"]

# Recognised binary file magic bytes.
# XLSX/XLSM/XLTX/XLTM are ZIP archives (PK prefix).
KNOWN_BINARY_SIGNATURES = [
    b"PK\x03\x04",  # ZIP / XLSX / DOCX / ODSX
    b"\xd0\xcf\x11\xe0",  # OLE2 / XLS (pre-2007)
]


def _file_magic_bytes(filepath: Path, n: int = 4) -> bytes:
    """Read the first n bytes of a file, or empty bytes on read error."""
    try:
        with filepath.open("rb") as f:
            return f.read(n)
    except Exception:
        return b""


def is_binary_file(filepath: Path) -> str | None:
    """Check if a file is a known binary format by magic bytes.

    Returns the detected format name (e.g. "xlsx", "xls") or None if the
    file appears to be text.
    """
    magic = _file_magic_bytes(filepath)
    for sig in KNOWN_BINARY_SIGNATURES:
        if magic.startswith(sig):
            # Distinguish xlsx from older xls by second signature byte.
            if sig == b"PK\x03\x04":
                ext = filepath.suffix.lower()
                if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
                    return "xlsx"
                elif ext in (".zip",):
                    return "zip"
                # PK without a known Excel extension — treat generically
                return "zip"
            if sig == b"\xd0\xcf\x11\xe0":
                return "xls"
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
