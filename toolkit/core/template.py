from __future__ import annotations

import re
from pathlib import Path
from typing import Any

_UNRESOLVED_PLACEHOLDER_RE = re.compile(r"\{[A-Za-z_][A-Za-z0-9_.]*\}")
_SQL_COMMENT_LINE_RE = re.compile(r"^\s*--.*$", re.MULTILINE)


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL single-line comments (-- ...) from the text.

    Used to avoid false positives when checking for unresolved template
    placeholders — DuckDB error messages embedded in SQL comments can
    contain ``{n}`` or similar patterns that are not actual placeholders.
    """
    return _SQL_COMMENT_LINE_RE.sub("", sql)


def render_template(text: str, ctx: dict[str, Any]) -> str:
    """
    Minimal deterministic templating used by the runtime contract.

    Supports only plain placeholders such as `{year}` and `{dataset}`.
    This is intentionally not a general templating engine.

    SQL comments (``-- ...``) are excluded from the unresolved-placeholder
    check so that DuckDB error messages embedded in comments do not trigger
    false positives.
    """
    out = text
    for k, v in sorted(ctx.items(), key=lambda item: len(item[0]), reverse=True):
        out = out.replace("{" + k + "}", str(v))
    # Strip comments before checking for unresolved placeholders, so
    # that {n} or other patterns in DuckDB error messages don't break.
    code_only = _strip_sql_comments(out)
    unresolved = sorted(set(_UNRESOLVED_PLACEHOLDER_RE.findall(code_only)))
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
    support: dict[str, Any] | None = None,
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
    if support:
        ctx.update(support)
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
