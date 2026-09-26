# toolkit/core/logging.py
from __future__ import annotations

import logging
import sys
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any

from rich.logging import RichHandler

_ASCII_FALLBACKS = {
    "→": "->",
    "←": "<-",
    "—": "-",
    "–": "-",
    "…": "...",
}

# Chiavi che non servono nel prefisso log (rumore)
_CONTEXT_SKIP = {"run_id"}


def safe_console_text(text: str, encoding: str | None = None) -> str:
    target_encoding = encoding or sys.stdout.encoding or "utf-8"
    try:
        text.encode(target_encoding)
        return text
    except UnicodeEncodeError:
        normalized = text
        for source, replacement in _ASCII_FALLBACKS.items():
            normalized = normalized.replace(source, replacement)
        return normalized.encode(target_encoding, errors="replace").decode(target_encoding)


def short_path(path: Path | str, root: Path | str | None = None) -> str:
    """Mostra solo la parte finale di un path (ultime 2-3 cartelle).

    Se root e' fornito, mostra la parte relativa. Altrimenti le ultime 3 parti.
    """
    p = Path(path)
    if root:
        try:
            rel = p.relative_to(Path(root))
            parts = rel.parts
        except ValueError:
            parts = p.parts
    else:
        parts = p.parts

    if len(parts) <= 3:
        return str(p)
    return "/".join(parts[-3:])


class ContextAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: MutableMapping[str, Any]):
        base_extra = self.extra or {}
        extra = dict(kwargs.get("extra") or {})
        extra.update({k: v for k, v in base_extra.items() if v is not None})
        kwargs["extra"] = extra

        # Prefisso compatto: salta run_id e chiavi vuote
        ctx_parts = [
            f"{k}={v}" for k, v in extra.items() if v is not None and k not in _CONTEXT_SKIP
        ]
        ctx = " ".join(ctx_parts)
        message = f"{ctx} | {msg}" if ctx else msg
        return safe_console_text(message), kwargs


def get_logger(
    name: str = "toolkit", level: str | int = "INFO", log_file: str | Path | None = None
):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # reset controllato: evita duplicati ma permette reconfig
    logger.handlers = []
    logger.propagate = False

    logger.addHandler(
        RichHandler(
            rich_tracebacks=True,
            show_path=False,
            show_level=True,
            show_time=True,
            omit_repeated_times=True,
        )
    )

    if log_file:
        p = Path(log_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(p, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(fh)

    return logger


def bind_logger(logger: logging.Logger | logging.LoggerAdapter, **context):
    if isinstance(logger, logging.LoggerAdapter):
        base_logger = logger.logger
        extra = logger.extra or {}
        merged_context = {**extra, **context}
    else:
        base_logger = logger
        merged_context = context
    return ContextAdapter(base_logger, extra=merged_context)
