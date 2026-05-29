"""CLI command: toolkit review-readiness

Usage:
    toolkit review-readiness --config candidates/terna-electricity-by-source/dataset.yml --year 2023
    toolkit review-readiness --config candidates/terna-electricity-by-source/dataset.yml --year 2023 --json
"""

from __future__ import annotations

from pathlib import Path

from toolkit.cli.inspect.readiness_ops import review_readiness as _review_readiness
from toolkit.core.config import load_config

import typer


def review_readiness(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Dataset year (default: last declared year)"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
) -> None:
    """Check di prontezza per review candidate: layer, output e coerenza run record.

    Classifica il candidate come:
    - ready: tutti i check passano — pronto per review
    - needs-review: qualche check fallito ma recuperabile
    - incomplete: troppi check falliti — non pronto

    Exit code:
        0 — readiness generata
        1 — config non trovato o errore nell'analisi
    """
    try:
        load_config(config, strict_config=False)
        config_path_resolved = str(Path(config).resolve())
        result = _review_readiness(config_path_resolved, year)
    except FileNotFoundError:
        typer.echo(f"error: config file not found: {config}", err=True)
        raise typer.Exit(code=1)
    except Exception as exc:
        typer.echo(f"error: {type(exc).__name__}: {exc}", err=True)
        raise typer.Exit(code=1)

    if as_json:
        import json
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return

    # Human-readable output
    dataset = result.get("dataset", "?")
    config_path = result.get("config_path", "?")
    year_val = result.get("year", "?")
    readiness = result.get("readiness", "?")
    ok_count = result.get("ok_count", 0)
    fail_count = result.get("fail_count", 0)

    readiness_icon = {"ready": "✅", "needs-review": "⚠️", "incomplete": "🔴"}.get(readiness, "?")
    typer.echo(f"dataset: {dataset}")
    typer.echo(f"config: {config_path}")
    typer.echo(f"year: {year_val}")
    typer.echo(f"readiness: {readiness_icon} {readiness}")
    typer.echo(f"checks: {ok_count}/{ok_count + fail_count} ok")
    typer.echo("")

    checks = result.get("checks", [])
    if not checks:
        typer.echo("nessun check disponibile")
        return

    typer.echo("checks:")
    for check in checks:
        name = check.get("check", "?")
        ok = check.get("ok", False)
        detail = check.get("detail", "")
        icon = "✅" if ok else "🔴"
        typer.echo(f"  {icon} [{name}]")
        if isinstance(detail, list):
            for item in detail:
                if isinstance(item, dict):
                    item_icon = "✅" if item.get("readable") else "🔴"
                    typer.echo(f"      {item_icon} {item.get('name', '?')}  ({item.get('rows', '?')} righe)")
                else:
                    typer.echo(f"      {item}")
        elif detail:
            typer.echo(f"      {detail}")

    # --- Validation details with messages ---
    layers = result.get("layers") or {}
    has_val = False
    for lname, label in [("raw", "raw"), ("clean", "clean"), ("mart", "mart")]:
        ln = layers.get(lname) or {}
        v = ln.get("validation") or {}
        msgs = ln.get("validation_msgs") or {}
        if v.get("ok") is None and not msgs.get("warnings") and not msgs.get("errors"):
            continue
        if not has_val:
            typer.echo("")
            typer.echo("validation:")
            has_val = True
        count_parts = []
        if v.get("errors_count"):
            count_parts.append(f"{v['errors_count']} errori")
        if v.get("warnings_count"):
            count_parts.append(f"{v['warnings_count']} warning")
        counts = f" ({', '.join(count_parts)})" if count_parts else ""
        status = "✅" if v.get("ok") else ("🔴" if v.get("ok") is False else "·")
        typer.echo(f"  {label}: {status}{counts}")
        for w in msgs.get("warnings", [])[:2]:
            typer.echo(f"    ⚠ {w[:120]}")
        for e in msgs.get("errors", [])[:2]:
            typer.echo(f"    🔴 {e[:120]}")

    # --- Raw profile warnings ---
    raw_warnings = (layers.get("raw") or {}).get("profile_warnings") or []
    if raw_warnings:
        typer.echo("raw profile warnings:")
        for w in raw_warnings[:5]:
            typer.echo(f"  ⚠ {w}")
        if len(raw_warnings) > 5:
            typer.echo(f"  ... e {len(raw_warnings) - 5} altro(i)")

    # --- Rich layer info ---
    typer.echo("")

    # Raw layer
    raw_layer = layers.get("raw") or {}
    raw_val = raw_layer.get("validation") or {}
    raw_profile = raw_layer.get("profile") or {}
    raw_warnings = raw_layer.get("profile_warnings") or []
    raw_status = "✅" if raw_val.get("ok") else ("🔴" if raw_val.get("ok") is False else "·")
    raw_cols = raw_val.get("col_count")
    raw_info_parts = []
    if raw_cols is not None:
        raw_info_parts.append(f"{raw_cols} colonne")
    if raw_profile.get("encoding"):
        raw_info_parts.append(f"encoding={raw_profile['encoding']}")
    if raw_profile.get("delim"):
        raw_info_parts.append(f"delim={raw_profile['delim']}")
    if raw_warnings:
        raw_info_parts.append(f"{len(raw_warnings)} warning")
    typer.echo(f"  raw:   {raw_status}  {'  '.join(raw_info_parts)}" if raw_info_parts else f"  raw:   {raw_status}")

    # Clean layer
    clean_layer = layers.get("clean") or {}
    clean_val = clean_layer.get("validation") or {}
    clean_status = "✅" if clean_val.get("ok") else ("🔴" if clean_val.get("ok") is False else "·")
    clean_rows = clean_val.get("row_count") or clean_layer.get("row_count")
    clean_cols = clean_val.get("col_count")
    trans = clean_layer.get("transition") or {}
    clean_info = []
    if clean_rows is not None:
        clean_info.append(f"{clean_rows} righe")
    if clean_cols is not None:
        clean_info.append(f"{clean_cols} colonne")
    if trans.get("row_drop_pct") is not None:
        clean_info.append(f"raw→clean: {trans['row_drop_pct']}% righe")
    if trans.get("col_drop") is not None and trans["col_drop"] != 0:
        clean_info.append(f"-{trans['col_drop']} colonne")
    typer.echo(f"  clean: {clean_status}  {'  '.join(clean_info)}" if clean_info else f"  clean: {clean_status}")

    # Mart layer
    mart_layer = layers.get("mart") or {}
    mart_val = mart_layer.get("validation") or {}
    mart_status = "✅" if mart_val.get("ok") else ("🔴" if mart_val.get("ok") is False else "·")
    mart_tables = mart_layer.get("tables") or []
    mart_ready = sum(1 for t in mart_tables if t.get("readable"))
    mart_total = len(mart_tables)
    typer.echo(f"  mart:  {mart_status}  {mart_ready}/{mart_total} tabelle" if mart_tables else f"  mart:  {mart_status}  (nessuna tabella)")

    typer.echo("")
    if readiness == "ready":
        typer.echo("✅ Pronto per review — tutti i check passano.")
    elif readiness == "needs-review":
        typer.echo(f"⚠️  {fail_count} check falliti — verificare prima del merge.")
    else:
        typer.echo(f"🔴 {fail_count} check falliti — candidate non pronto.")


def register(app: typer.Typer) -> None:
    app.command("review-readiness")(review_readiness)
