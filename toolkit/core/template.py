from __future__ import annotations

import re
from pathlib import Path
from typing import Any

_UNRESOLVED_PLACEHOLDER_RE = re.compile(r"\{[A-Za-z_][A-Za-z0-9_]*\}")


def render_template(text: str, ctx: dict[str, Any]) -> str:
    """
    Minimal deterministic templating used by the runtime contract.

    Supports only plain placeholders such as `{year}` and `{dataset}`.
    This is intentionally not a general templating engine.
    """
    out = text
    for k, v in ctx.items():
        out = out.replace("{" + k + "}", str(v))
    unresolved = sorted(set(_UNRESOLVED_PLACEHOLDER_RE.findall(out)))
    if unresolved:
        raise ValueError(
            "Template contains unresolved placeholders after render: "
            + ", ".join(unresolved)
        )
    return out


def build_runtime_template_ctx(
    *,
    dataset: str,
    year: int,
    root: str | Path | None = None,
    base_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Build the minimal deterministic template context exposed to SQL runtime.

    Existing placeholders `{year}` and `{dataset}` remain stable; additional
    path placeholders are additive-only and let SQL bind to the effective root
    without depending on the current working directory.

    Path placeholders trust that `root` and `base_dir` are already canonical
    runtime paths. Callers should resolve them before building the context.
    """
    ctx: dict[str, Any] = {"year": year, "dataset": dataset}
    if root is not None:
        root_path = Path(root)
        ctx["root"] = str(root_path)
        ctx["root_posix"] = root_path.as_posix()
    if base_dir is not None:
        ctx["base_dir"] = str(base_dir)
        ctx["base_dir_posix"] = base_dir.as_posix()
    return ctx


def public_template_ctx(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    Return the stable public subset safe to persist in metadata.

    Runtime-only path helpers such as `root_posix` and `base_dir_posix` are
    intentionally excluded so metadata stays portable and does not leak
    absolute filesystem paths.
    """
    public_keys = ("year", "dataset")
    return {key: ctx[key] for key in public_keys if key in ctx}
