from pathlib import Path

from toolkit.core.exceptions import DownloadError
from toolkit.core.registry import registry


class LocalFileSource:
    """Read a local file (offline, deterministic)."""

    def __init__(self):
        pass

    def fetch(self, path: str) -> bytes:
        p = Path(path)
        if not p.exists():
            raise DownloadError(f"Local file not found: {p}")
        return p.read_bytes()


registry.register("local_file", lambda **client: LocalFileSource())
