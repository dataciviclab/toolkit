from __future__ import annotations

from typing import Any, Dict, List


def render_profile_md(profile: Dict[str, Any]) -> str:
    ds = profile.get("dataset")
    year = profile.get("year")
    enc = profile.get("encoding_suggested")
    delim = profile.get("delim_suggested")
    dec = profile.get("decimal_suggested")

    header = profile.get("header_line")
    file_used = profile.get("file_used")

    columns_raw: List[str] = profile.get("columns_raw", [])
    miss = profile.get("missingness_top", [])
    warnings = profile.get("warnings", [])
    mapping = profile.get("mapping_suggestions", {}) or {}

    md: list[str] = []
    md.append(f"# RAW Profile — {ds} ({year})\n")
    if file_used:
        md.append(f"- file used: `{file_used}`\n")

    md.append("## Suggested read options\n")
    md.append("```yml")
    md.append("clean:")
    md.append("  read:")
    if delim:
        md.append(f'    delim: "{delim}"')
    if dec:
        md.append(f'    decimal: "{dec}"')
    if enc:
        md.append(f'    encoding: "{enc}"')
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
        md.append(f"- ... +{len(columns_raw)-80} more")
    md.append("")

    md.append("## Missingness (top)\n")
    for m in miss[:20]:
        md.append(f"- `{m['column']}`: {m['missing_pct']:.1f}%")
    md.append("")

    md.append("## Mapping suggestions (first 15)\n")
    shown = 0
    for out_col, spec in mapping.items():
        md.append(f"- `{out_col}` → type: `{spec.get('type')}`"
                  + (f", parse: `{spec.get('parse',{}).get('kind')}`" if spec.get("parse") else ""))
        shown += 1
        if shown >= 15:
            break
    if len(mapping) > 15:
        md.append(f"- ... +{len(mapping)-15} more")
    md.append("")

    if warnings:
        md.append("## Warnings\n")
        for w in warnings[:20]:
            md.append(f"- {w}")
        md.append("")

    return "\n".join(md)