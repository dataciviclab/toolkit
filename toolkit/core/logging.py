# toolkit/core/logging.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from rich.logging import RichHandler


class ContextAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: dict[str, Any]):
        ctx = " ".join([f"{k}={v}" for k, v in self.extra.items() if v is not None])
        return (f"{ctx} | {msg}" if ctx else msg), kwargs


def get_logger(name: str = "toolkit", level: str | int = "INFO", log_file: str | Path | None = None):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # reset controllato: evita duplicati ma permette reconfig
    logger.handlers = []
    logger.propagate = False

    logger.addHandler(RichHandler(rich_tracebacks=True))

    if log_file:
        p = Path(log_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(p, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(fh)

    return logger


def bind_logger(logger: logging.Logger, **context):
    return ContextAdapter(logger, extra=context)