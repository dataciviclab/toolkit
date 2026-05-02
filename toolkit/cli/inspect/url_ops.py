"""inspect url command — probe HTTP URL and optionally scaffold dataset.yml."""

from __future__ import annotations

import json
from typing import Any

import requests
import typer

from toolkit.cli.cmd_url_inspect import (
    _EXTENDED_EXTENSIONS,
    _DEFAULT_TIMEOUT,
    _DEFAULT_USER_AGENT,
    _detect_ckan,
    _discover_ckan_resources,
    _extract_ckan_dataset_id,
    _generate_yaml_scaffold,
    probe_url,
)


def url(
    url: str = typer.Argument(..., help="URL da ispezionare"),
    scaffold: bool = typer.Option(False, "--scaffold", "-s", help="Genera scaffold YAML (blocchi dataset + raw)"),
    run: bool = typer.Option(False, "--run", "-r", help="Bootstrap dal scaffold generato (implies --scaffold)"),
    timeout: int = typer.Option(_DEFAULT_TIMEOUT, "--timeout", min=1, help="Timeout HTTP in secondi"),
    user_agent: str = typer.Option(_DEFAULT_USER_AGENT, "--user-agent"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
) -> None:
    """
    Ispeziona un URL per dataset scouting: probe HTTP e generazione scaffold YAML.

    Con --run (alias -r): dopo il probe, scaffold YAML e avvia bootstrap init.
    Usa --run senza --scaffold per indicizzare URL e fare bootstrap in un solo comando.
    """
    try:
        result = probe_url(url, timeout=timeout, user_agent=user_agent, capture_html=scaffold or run)
    except requests.RequestException as exc:
        typer.echo(f"error: {type(exc).__name__}: {exc}")
        raise typer.Exit(code=1) from exc

    if scaffold or run:
        ckan_resources: list[dict[str, Any]] | None = None
        candidate_file_links: list[str] | None = None

        if result["kind"] == "html":
            html_content = result.get("html_content", b"")
            html_text = html_content.decode("utf-8", errors="replace") if html_content else ""
            dataset_id = _extract_ckan_dataset_id(result["final_url"], html_text)
            is_ckan = _detect_ckan(html_content) if html_content else False

            if dataset_id and html_content and is_ckan:
                ckan_resources = _discover_ckan_resources(
                    result["final_url"],
                    dataset_id,
                    timeout=timeout,
                    user_agent=user_agent,
                )

            if not ckan_resources and html_content:
                candidate_file_links = [
                    link for link in result.get("candidate_links", [])
                    if any(ext in link.lower() for ext in _EXTENDED_EXTENSIONS)
                ]

        yaml_scaffold = _generate_yaml_scaffold(result, ckan_resources, candidate_file_links)

        if run:
            import tempfile

            with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False, encoding="utf-8") as f:
                f.write(yaml_scaffold)
                scaffold_path = f.name
            typer.echo(f"[inspect] Scaffold generato: {scaffold_path}")
            typer.echo("[inspect] Avvio bootstrap...")
            typer.echo("")
            from toolkit.cli.cmd_run import run_init

            run_init(
                config=scaffold_path,
                years=None,
                dry_run=False,
                strict_config=False,
            )
            typer.echo("")
            typer.echo("[inspect] Bootstrap completato. Prossimi passi:")
            typer.echo(f"  toolkit run clean --config {scaffold_path}")
            typer.echo(f"  toolkit run mart --config {scaffold_path}")
        else:
            typer.echo(yaml_scaffold)
        return

    if as_json:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return

    typer.echo(f"requested_url: {result['requested_url']}")
    typer.echo(f"final_url: {result['final_url']}")
    typer.echo(f"status_code: {result['status_code']}")
    typer.echo(f"content_type: {result['content_type']}")
    typer.echo(f"content_disposition: {result['content_disposition']}")
    typer.echo(f"kind: {result['kind']}")

    if result["candidate_links"]:
        typer.echo("candidate_links:")
        for link in result["candidate_links"][:20]:
            typer.echo(f"  - {link}")
        remaining = len(result["candidate_links"]) - 20
        if remaining > 0:
            typer.echo(f"candidate_links_more: {remaining}")
    else:
        typer.echo("candidate_links: none")
