# toolkit/core/metadata.py
from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from toolkit.version import __version__


def _jsonable(x: Any) -> Any:
    if is_dataclass(x):
        return asdict(x)
    if isinstance(x, Path):
        return str(x)
    return x


def write_metadata(folder: Path, data: dict[str, Any], filename: str = "metadata.json") -> Path:
    folder.mkdir(parents=True, exist_ok=True)

    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "toolkit_version": __version__,
        **data,
    }

    out = folder / filename
    tmp = folder / f".{filename}.tmp"

    tmp.write_text(
        json.dumps(meta, indent=2, ensure_ascii=False, default=_jsonable),
        encoding="utf-8",
    )
    tmp.replace(out)
    return out