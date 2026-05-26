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

    typer.echo("")
    if readiness == "ready":
        typer.echo("✅ Pronto per review — tutti i check passano.")
    elif readiness == "needs-review":
        typer.echo(f"⚠️  {fail_count} check falliti — verificare prima del merge.")
    else:
        typer.echo(f"🔴 {fail_count} check falliti — candidate non pronto.")


def register(app: typer.Typer) -> None:
    app.command("review-readiness")(review_readiness)
