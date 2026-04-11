from pathlib import Path

from toolkit.core.exceptions import DownloadError


class LocalFileSource:
    """Read a local file (offline, deterministic)."""

    def fetch(self, path: str) -> bytes:
        p = Path(path)
        if not p.exists():
            raise DownloadError(f"Local file not found: {p}")
        return p.read_bytes()
