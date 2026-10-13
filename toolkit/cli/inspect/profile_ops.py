"""inspect profile — logica profiling RAW.

``csv_preview`` e' in ``toolkit.domain.profile``.
``run_profile`` (logica) resta qui: e' il core del profiling usato da
test e pipeline (il comando CLI Typer corrispondente e' deprecato a
favore di ``toolkit inspect config -m profile``).
"""

from __future__ import annotations

from logging import Logger
from typing import Any

from toolkit.core.config import ensure_dict
from toolkit.core.paths import layer_year_dir
from toolkit.core.config import ToolkitConfig
from toolkit.profile.raw import profile_raw, write_raw_profile


def run_profile(cfg: ToolkitConfig, years: list[int], logger: Logger) -> None:
    """Core logic: profiling RAW per ogni anno e scrittura su _profile/.

    Chiamabile da cmd_profile (deprecato) e dai test di integrazione.
    """
    clean_cfg: dict[str, Any] = ensure_dict(cfg.clean) or {}

    for y in years:
        raw_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, y)
        out_dir = raw_dir / "_profile"
        out_dir.mkdir(parents=True, exist_ok=True)

        prof = profile_raw(raw_dir, cfg.dataset, y, read_cfg=clean_cfg.get("read"))
        paths = write_raw_profile(out_dir, prof)
        written_paths = list(paths.values())

        if written_paths:
            logger.info("PROFILE RAW -> %s", " | ".join(str(path) for path in written_paths))
        else:
            logger.info("PROFILE RAW -> no optional artifacts written for current policy")
