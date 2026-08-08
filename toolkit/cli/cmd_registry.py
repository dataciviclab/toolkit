"""toolkit registry — consulta gli artifact registry committati nel workspace.

Legge i cataloghi generati dal registry builder e committati nei repo
(``{repo}/registry/*.json``): clean_catalog, mart_catalog, pipeline_signals,
codelists. Cross-repo, senza riprogettare la discovery.

Speculare al tool MCP toolkit_registry.
"""

from __future__ import annotations

import json
from typing import Any

import typer


def _print_list(data: dict[str, Any]) -> None:
    repos = data.get("repos", [])
    typer.echo(f"Registry artifacts nel workspace ({data.get('total_repos', 0)} repo):")
    for repo_info in repos:
        typer.echo(f"  {repo_info['repo']}:")
        for art in repo_info["artifacts"]:
            size = art.get("size_bytes")
            size_txt = f"  [{size // 1024} KB]" if size is not None else ""
            sections = art.get("sections") or {}
            counts = ", ".join(f"{k}: {v}" for k, v in sections.items() if v is not None)
            legacy = " (legacy)" if art.get("legacy") else ""
            typer.echo(f"    registry.json{size_txt}{legacy}  {counts}")


def registry_list(json_output: bool = typer.Option(False, "--json", help="Output JSON")) -> None:
    """Elenca gli artifact registry committati nei repo del workspace."""
    from toolkit.registry.reader import list_registries

    data = list_registries()
    if json_output:
        typer.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        return
    _print_list(data)


def registry_show(
    repo: str = typer.Argument(..., help="Repo (es. 'eurostat')"),
    artifact: str = typer.Argument(..., help="Artifact (es. 'clean_catalog')"),
    slug: str = typer.Option(None, "--slug", help="Filtra un'entry (dataset/mart/signal/codelist)"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
) -> None:
    """Mostra un artifact registry di un repo del workspace."""
    from toolkit.registry.reader import show_registry

    try:
        data = show_registry(repo, artifact, slug=slug)
    except (FileNotFoundError, ValueError) as exc:
        typer.echo(f"ERRORE: {exc}", err=True)
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        return

    entry = data.get("entry")
    if entry is not None:
        typer.echo(json.dumps(entry, indent=2, ensure_ascii=False, default=str))
        return
    payload = data["data"]
    typer.echo(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def register(app: typer.Typer) -> None:
    registry = typer.Typer(
        no_args_is_help=True, help="Consulta gli artifact registry del workspace"
    )
    registry.command("list")(registry_list)
    registry.command("show")(registry_show)
    app.add_typer(registry, name="registry")
