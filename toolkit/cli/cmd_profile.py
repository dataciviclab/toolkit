from __future__ import annotations

from pathlib import Path
from typing import Any

import typer

from toolkit.cli.common import iter_years, load_cfg_and_logger
from toolkit.core.artifacts import resolve_artifact_policy, should_write
from toolkit.core.paths import layer_year_dir
from toolkit.profile.raw import (
    build_suggested_read_cfg,
    profile_raw,
    write_raw_profile,
    write_suggested_read_yml,
)


def render_profile_md(profile: dict[str, Any]) -> str:
    ds = profile.get("dataset")
    year = profile.get("year")
    enc = profile.get("encoding_suggested")
    header = profile.get("header_line")
    file_used = profile.get("file_used")
    suggested_read = build_suggested_read_cfg(profile)

    columns_raw: list[str] = profile.get("columns_raw", [])
    miss = profile.get("missingness_top", [])
    warnings = profile.get("warnings", [])
    mapping = profile.get("mapping_suggestions", {}) or {}

    md: list[str] = []
    md.append(f"# RAW Profile - {ds} ({year})\n")
    if file_used:
        md.append(f"- file used: `{file_used}`\n")
    if enc:
        md.append(f"- encoding suggested: `{enc}`\n")

    md.append("## Suggested read options\n")
    md.append("```yml")
    md.append("clean:")
    md.append("  read:")
    for key, value in suggested_read.items():
        md.append(f"    {key}: {_yml_scalar(value)}")
    md.append("```\n")

    md.append("## Header (first line)\n")
    if header:
        md.append("```")
        md.append(header)
        md.append("```\n")
    else:
        md.append("_header not available_\n")

    md.append("## Columns (preview)\n")
    for c in columns_raw[:80]:
        md.append(f"- `{c}`")
    if len(columns_raw) > 80:
        md.append(f"- ... +{len(columns_raw) - 80} more")
    md.append("")

    md.append("## Missingness (top)\n")
    for m in miss[:20]:
        md.append(f"- `{m['column']}`: {m['missing_pct']:.1f}%")
    md.append("")

    md.append("## Mapping suggestions (first 15)\n")
    shown = 0
    for out_col, spec in mapping.items():
        md.append(
            f"- `{out_col}` -> type: `{spec.get('type')}`"
            + (f", parse: `{spec.get('parse', {}).get('kind')}`" if spec.get("parse") else "")
        )
        shown += 1
        if shown >= 15:
            break
    if len(mapping) > 15:
        md.append(f"- ... +{len(mapping) - 15} more")
    md.append("")

    if warnings:
        md.append("## Warnings\n")
        for w in warnings[:20]:
            md.append(f"- {w}")
        md.append("")

    return "\n".join(md)


def _yml_scalar(v: Any) -> str:
    if isinstance(v, str):
        v = v.replace('"', '\\"')
        return f'"{v}"'
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    return str(v)


def write_suggested_mapping_yml(out_dir: Path, profile: dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    mapping = profile.get("mapping_suggestions") or {}

    lines: list[str] = []
    lines.append("clean:")
    lines.append("  mapping:")

    for out_col, spec in mapping.items():
        lines.append(f"    {out_col}:")
        for k in ["from", "type"]:
            if k in spec:
                lines.append(f"      {k}: {_yml_scalar(spec[k])}")

        if "normalize" in spec:
            lines.append("      normalize:")
            for op in spec["normalize"]:
                lines.append(f"        - {_yml_scalar(op)}")

        if "nullify" in spec:
            lines.append("      nullify:")
            for tok in spec["nullify"]:
                lines.append(f"        - {_yml_scalar(tok)}")

        if "parse" in spec:
            lines.append("      parse:")
            for pk, pv in spec["parse"].items():
                lines.append(f"        {pk}: {_yml_scalar(pv)}")

    p = out_dir / "suggested_mapping.yml"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def profile(
    step: str = typer.Argument(..., help="raw"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Profiling (assist) per i layer. Per ora: raw.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_config_flag)

    if step != "raw":
        raise typer.BadParameter("step must be: raw")

    for year in iter_years(cfg, None):
        raw_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, year)
        out_dir = raw_dir / "_profile"
        out_dir.mkdir(parents=True, exist_ok=True)
        policy = resolve_artifact_policy(cfg.output)

        prof = profile_raw(raw_dir, cfg.dataset, year, read_cfg=(cfg.clean or {}).get("read"))
        paths = write_raw_profile(
            out_dir,
            prof,
            write_canonical=should_write("profile", "raw_profile", policy, cfg),
            write_legacy_alias=should_write("profile", "profile_alias", policy, cfg),
        )

        written_paths = list(paths.values())

        if should_write("profile", "profile_md", policy, cfg):
            md_path = out_dir / "profile.md"
            md_path.write_text(render_profile_md(prof.__dict__), encoding="utf-8")
            written_paths.append(md_path)

        if should_write("profile", "suggested_read", policy, cfg):
            written_paths.append(write_suggested_read_yml(out_dir, prof.__dict__))

        if should_write("profile", "suggested_mapping", policy, cfg):
            written_paths.append(write_suggested_mapping_yml(out_dir, prof.__dict__))

        if written_paths:
            logger.info("PROFILE RAW -> %s", " | ".join(str(path) for path in written_paths))
        else:
            logger.info("PROFILE RAW -> no optional artifacts written for current policy")


def register(app: typer.Typer) -> None:
    app.command("profile")(profile)
