from __future__ import annotations

from typing import Any


def render_template(text: str, ctx: dict[str, Any]) -> str:
    """
    Templating MINIMO e deterministico.
    Supporta placeholder stile: {year}, {dataset}.
    Non è Jinja: niente logica, niente espressioni.
    """
    out = text
    for k, v in ctx.items():
        out = out.replace("{" + k + "}", str(v))
    return out