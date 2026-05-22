"""inspect url command — DEPRECATO. Usa toolkit scout <URL>.

Mantenuto per retrocompatibilita'. Tutta la logica e' stata spostata
in toolkit scout.
"""

from __future__ import annotations

import json
import warnings
from typing import Any

import typer

from toolkit.scout.http import DEFAULT_TIMEOUT, DEFAULT_USER_AGENT
from toolkit.scout.probe import probe_url_routed


def url(
    url: str = typer.Argument(..., help="URL da ispezionare"),
    timeout: int = typer.Option(DEFAULT_TIMEOUT, "--timeout", min=1, help="Timeout HTTP in secondi"),
    user_agent: str = typer.Option(DEFAULT_USER_AGENT, "--user-agent"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
) -> None:
    """
    [DEPRECATO] Usa toolkit scout <URL>.

    Ispeziona un URL con probe + routing automatico.
    Sostituito da toolkit scout che offre le stesse funzionalita'
    piu' inferenze e scaffold opzionale.
    """
    warnings.warn(
        "toolkit inspect url e' deprecato. Usa: toolkit scout <URL>",
        DeprecationWarning,
        stacklevel=1,
    )
    typer.echo("⚠️  DEPRECATO: usa 'toolkit scout <URL>' invece.", err=True)
    typer.echo("")
    try:
        result = probe_url_routed(url, timeout=timeout, user_agent=user_agent)
    except RuntimeError as exc:
        typer.echo(f"error: {exc}")
        raise typer.Exit(code=1) from exc

    if as_json:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return

    _print_human(result)


def _print_human(result: dict[str, Any]) -> None:
    """Output human-readable."""
    typer.echo(f"requested_url: {result['requested_url']}")
    typer.echo(f"final_url: {result['final_url']}")
    typer.echo(f"status_code: {result['status_code']}")
    typer.echo(f"content_type: {result.get('content_type')}")
    typer.echo(f"content_disposition: {result.get('content_disposition')}")
    typer.echo(f"source_type: {result.get('source_type', '?')}")

    if result.get("resolved_format"):
        typer.echo(f"resolved_format: {result['resolved_format']}")

    if result.get("source_type") == "ckan":
        resources = result.get("ckan_resources") or []
        typer.echo(f"ckan_resources: {len(resources)} found")
        for res in resources[:5]:
            typer.echo(f"  - {res['name']} ({res['format']}): {res['url']}")
        if result.get("ckan_dataset_title"):
            typer.echo(f"ckan_title: {result['ckan_dataset_title']}")
        if result.get("ckan_tags"):
            typer.echo(f"ckan_tags: {', '.join(result['ckan_tags'][:10])}")

    elif result.get("source_type") == "sdmx":
        info = result.get("sdmx_info") or {}
        typer.echo(f"sdmx_flow: {info.get('flow_id', '?')}")
        if info.get("year_min") and info.get("year_max"):
            typer.echo(f"sdmx_years: {info['year_min']}-{info['year_max']}")

    elif result.get("candidate_links"):
        links = result["candidate_links"]
        typer.echo(f"candidate_links: {len(links)} found")
        for link in links[:20]:
            typer.echo(f"  - {link}")
        remaining = len(links) - 20
        if remaining > 0:
            typer.echo(f"candidate_links_more: {remaining}")
    else:
        links = result.get("candidate_links") or []
        typer.echo(f"candidate_links: {len(links)}")

    # Suggest init --url per scaffold
    if result.get("source_type") in ("file", "html", "ckan", "sdmx"):
        typer.echo("")
        typer.echo(f"Next: toolkit init --url \"{result['requested_url']}\"")
        if result["source_type"] == "file":
            typer.echo("      toolkit init --url <URL> --run  (include raw run)")
