from __future__ import annotations

from typing import Any


def render_template(text: str, ctx: dict[str, Any]) -> str:
    """
    Minimal deterministic templating used by the runtime contract.

    Supports only plain placeholders such as `{year}` and `{dataset}`.
    This is intentionally not a general templating engine.
    """
    out = text
    for k, v in ctx.items():
        out = out.replace("{" + k + "}", str(v))
    return out
