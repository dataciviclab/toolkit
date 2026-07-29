"""toolkit contract — interroga i contratti di pipeline.

Leggibile per umani, machine-readable con --json.
Speculare al tool MCP toolkit_contract.
"""

from __future__ import annotations

import json
from typing import Any

import typer


def contract(
    layer: str = typer.Option(
        "all",
        "--layer",
        "-l",
        help="Layer del contratto: 'raw', 'clean', 'mart', o 'all' (default)",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Output JSON strutturato (machine-readable)",
    ),
) -> None:
    """Mostra i contratti di pipeline del toolkit.

    I contratti descrivono view names, macro SQL, regole di validazione
    e formati numerici che i file clean.sql e mart.sql devono rispettare.

    Chiama questo comando PRIMA di scrivere clean.sql o mart.sql.
    """
    from toolkit.contracts.pipeline import CONTRACTS

    # Risolvi layer
    data: dict[str, Any]
    if layer == "all":
        data = CONTRACTS
    elif layer == "raw":
        data = {"layer": layer, **CONTRACTS["raw"]}
    elif layer == "clean":
        data = {"layer": layer, **CONTRACTS["clean"]}
    elif layer == "mart":
        data = {"layer": layer, **CONTRACTS["mart"]}
    else:
        typer.echo(f"Layer sconosciuto: {layer}. Usa 'raw', 'clean', 'mart' o 'all'.", err=True)
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        return

    # ── Output leggibile per umani ───────────────────────────────────────
    typer.echo("╭─ Toolkit Pipeline Contracts ─────────────────────────────╮")
    typer.echo("│")
    typer.echo(f"│  Versione: {CONTRACTS['version']}")
    typer.echo("│")

    if layer in ("all", "raw"):
        _print_raw(data if layer == "raw" else CONTRACTS["raw"])

    if layer == "all":
        typer.echo("│")

    if layer in ("all", "clean"):
        _print_clean(data if layer == "clean" else CONTRACTS["clean"])

    if layer == "all":
        typer.echo("│")

    if layer in ("all", "mart"):
        _print_mart(data if layer == "mart" else CONTRACTS["mart"])

    if layer == "all":
        typer.echo("│")
        typer.echo("│  ── CLI ─────────────────────────────────────────────────")
        for cmd in CONTRACTS["cli"]:
            typer.echo(f"│    {cmd['command']}")
            typer.echo(f"│      {cmd['description']}")
        typer.echo("│")
        typer.echo("│  ── dataset.yml ──────────────────────────────────────────")
        typer.echo(
            f"│  Campi richiesti: {', '.join(CONTRACTS['config']['required_top_level_fields'][:3])}"
        )
        typer.echo(
            f"│  + {len(CONTRACTS['config']['common_optional_fields'])} campi opzionali comuni"
        )
        typer.echo("│")
        typer.echo(f"│  🔗 Docs: {CONTRACTS['pipeline']['config_docs']}")
        typer.echo(f"│  📖 Macro: {CONTRACTS['pipeline']['macros_docs']}")
        typer.echo(f"│  📐 Conventions: {CONTRACTS['pipeline']['conventions_docs']}")
        typer.echo("│")
        typer.echo(f"│  💡 {CONTRACTS['tldr']}")
        typer.echo("│")
        typer.echo("╰────────────────────────────────────────────────────────╯")


def _print_raw(raw: dict[str, Any]) -> None:
    typer.echo("│  ── LAYER RAW ────────────────────────────────────────────")
    typer.echo("│  Tipi fonte disponibili:")
    for src in raw["source_types"]:
        typer.echo(f"│    {src['type']:25s} {src['description'][:70]}")
    typer.echo("│")
    typer.echo("│  Extractor:")
    for ext in raw["extractors"]:
        typer.echo(f"│    {ext['type']:25s} {ext['description']}")
    typer.echo("│")
    v = raw.get("validation", {})
    if v.get("profile", {}).get("known_issue"):
        typer.echo(f"│  ⚠ Profilo: {v['profile']['known_issue']}")
    typer.echo("│")
    typer.echo(f"│  Output: {raw.get('output', {}).get('path', '-')}")


def _print_clean(clean: dict[str, Any]) -> None:
    typer.echo("│  ── LAYER CLEAN ──────────────────────────────────────────")
    typer.echo(f"│  View SQL:      {clean['sql_source']['view']}")
    typer.echo(f"│  Year token:    {clean['year_placeholder']['syntax']}")
    typer.echo(f"│  required_columns: {clean['validation']['required_columns']['scope']}")
    typer.echo("│")
    typer.echo("│  Macro disponibili:")
    for m in clean["macros"]:
        warning = ""
        if "warning" in m:
            warning = " ⚠"
        example = m.get("example", "")
        typer.echo(f"│    {m['name']:35s} {m['returns']:10s}  {m['description'][:60]}{warning}")
        typer.echo(f"│    {'':35s} {'':10s}  es: {example[:60]}")
    if clean.get("read_params", {}).get("decimal"):
        d = clean["read_params"]["decimal"]
        typer.echo("│")
        typer.echo(f"│  ⚠ decimal=',': {d['consequence']}")
        typer.echo(f"│    → {d['recommended_usage']}")


def _print_mart(mart: dict[str, Any]) -> None:
    typer.echo("│  ── LAYER MART ───────────────────────────────────────────")
    typer.echo(f"│  View SQL:      {mart['sql_source']['view']}")
    typer.echo(f"│  Multi-year:    {mart.get('multi_year', {}).get('description', '-')[:70]}")
    typer.echo(
        f"│  table_rules:   {mart.get('validation', {}).get('table_rules', {}).get('scope', '-')}"
    )


def register(app: typer.Typer) -> None:
    app.command("contract")(contract)
