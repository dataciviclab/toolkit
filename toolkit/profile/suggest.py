from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List


def _yml_scalar(v: Any) -> str:
    if isinstance(v, str):
        # quote strings safely
        v = v.replace('"', '\\"')
        return f'"{v}"'
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    return str(v)


def write_suggested_read_yml(out_dir: Path, profile: Dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    delim = profile.get("delim_suggested")
    dec = profile.get("decimal_suggested")
    enc = profile.get("encoding_suggested")

    lines = ["clean:", "  read:"]
    if delim:
        lines.append(f'    delim: "{delim}"')
    if dec:
        lines.append(f'    decimal: "{dec}"')
    if enc:
        lines.append(f'    encoding: "{enc}"')

    p = out_dir / "suggested_read.yml"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def write_suggested_mapping_yml(out_dir: Path, profile: Dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    mapping = profile.get("mapping_suggestions") or {}

    lines: List[str] = []
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