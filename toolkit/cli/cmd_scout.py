"""toolkit scout — esplora URL esterni con probe + routing.

`toolkit preview` profila un CSV/TSV remoto con la stessa implementazione
usata da MCP `toolkit_query(preview)` e da `run preflight`
(``toolkit.profile.preview.preview_url``).

Usage:
    toolkit scout <URL>            # probe + routing (CKAN, HTML, SDMX, file)
    toolkit scout <URL> --json     # probe in JSON
    toolkit preview <URL>          # profilo CSV/TSV remoto (encoding, colonne, anni)
"""

from __future__ import annotations

from typing import Any

import typer

from toolkit.scout.http import DEFAULT_TIMEOUT
from toolkit.scout.probe import probe_url_routed


# ---------------------------------------------------------------------------
# Helper: echo condizionale (silenizato in modalità JSON)
# ---------------------------------------------------------------------------


def _make_echoer(json_mode: bool):
    """Restituisce una funzione echo che stampa solo se non in modalità JSON."""

    def _echo(msg: str, *, err: bool = False) -> None:
        if not json_mode:
            typer.echo(msg, err=err)

    return _echo


# ---------------------------------------------------------------------------
# Scout orchestration — esportata per scout CLI e uso programmatico
# ---------------------------------------------------------------------------


def scout_url(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    json_output: bool = False,
) -> dict[str, Any] | None:
    """Probe arricchito con routing automatico sul tipo di fonte.

    Args:
        url: URL da esplorare.
        timeout: Timeout HTTP in secondi.
        json_output: Se True, restituisce dict invece di stampare.

    Returns:
        dict con risultato probe se json_output=True, None altrimenti.
    """
    _echo = _make_echoer(json_output)
    _echo(f"Probing {url}...")

    # Step 1: Probe arricchito
    probe = probe_url_routed(url, timeout=min(timeout, 30))
    source_type = probe["source_type"]
    result: dict[str, Any] = dict(probe)

    _echo(f"  Source type: {source_type}")
    _echo(f"  HTTP status: {probe['status_code']}")
    if probe.get("resolved_format"):
        _echo(f"  Format: {probe['resolved_format']}")

    # Step 2: Routing per tipo fonte
    if source_type == "ckan":
        is_portal = probe.get("ckan_portal", False)
        resources = probe.get("ckan_resources") or []
        if is_portal and not resources:
            _echo("  CKAN portal detected (homepage)")
            _echo("  Nessun dataset specifico — fornisci un URL diretto a un dataset:")
            _echo("    toolkit scout <URL_PORTALE>/dataset/<NOME_DATASET>")
            if json_output:
                result["ckan_portal"] = True
                return result
        elif not resources:
            _echo("error: CKAN portal detected but no downloadable resources found", err=True)
            raise typer.Exit(code=1)
        else:
            _echo(f"  CKAN resources: {len(resources)} found")
            for res in resources[:3]:
                _echo(f"    - {res['name']} ({res['format']})")
            if len(resources) > 3:
                _echo(f"    ... and {len(resources) - 3} more")

    elif source_type == "html":
        candidates = probe.get("candidate_links") or []
        if not candidates:
            _echo("error: HTML page with no downloadable data links", err=True)
            raise typer.Exit(code=1)
        _echo(f"  Candidate links: {len(candidates)} found")
        for link in candidates[:5]:
            _echo(f"    - {link}")
        if len(candidates) > 5:
            _echo(f"    ... and {len(candidates) - 5} more")

    elif source_type == "sparql":
        sparql_info = probe.get("sparql_info") or {}
        _echo(f"  SPARQL endpoint: {sparql_info.get('endpoint', '?')}")
        ds_count = sparql_info.get("dataset_count", 0)
        if ds_count:
            _echo(f"  Dataset DCAT trovati: {ds_count}")
            for ds in sparql_info.get("datasets", [])[:5]:
                _echo(f"    - {ds['title'][:70]}")
            if ds_count > 5:
                _echo(f"    ... e altri {ds_count - 5}")

    elif source_type == "sdmx":
        _echo(f"  SDMX flow: {probe.get('sdmx_info', {}).get('flow_id', '?')}")
        sdmx_info = probe.get("sdmx_info") or {}
        if sdmx_info.get("year_min"):
            _echo(f"  Year range: {sdmx_info['year_min']}-{sdmx_info.get('year_max', '?')}")

    elif source_type == "file":
        resolved_format = probe.get("resolved_format")
        if resolved_format:
            _echo(f"  Detected format: {resolved_format}")

    elif source_type == "opaque":
        _echo("error: URL returned opaque content", err=True)
        raise typer.Exit(code=1)

    else:
        _echo(f"error: unexpected source type '{source_type}'", err=True)
        raise typer.Exit(code=1)

    # Step 3: Suggerimento prossimo passo (solo in modalità umana)
    if not json_output:
        if source_type == "file":
            _echo("")
            _echo(f'Next: toolkit preview "{url}"   # profilo CSV/TSV (encoding, colonne, anni)')
        elif source_type == "html":
            _echo("")
            _echo('Next: toolkit preview "<link>"   # profila il file scelto')

    if json_output:
        return result
    return None


def scout(
    url: str = typer.Argument(..., help="URL da esplorare"),
    json_output: bool = typer.Option(False, "--json", help="Output in formato JSON"),
    timeout: int = typer.Option(
        DEFAULT_TIMEOUT, "--timeout", min=1, help="Timeout HTTP in secondi"
    ),
):
    """
    Esplora un URL esterno: probe HTTP e routing automatico.

    Rileva automaticamente se l'URL e' un file CSV/XLSX/JSON diretto,
    una pagina HTML con link a dati, un portale CKAN o un endpoint SDMX.

    Per il profilo di un CSV/TSV remoto (encoding, colonne, granularità, anni)
    usa `toolkit preview <URL>`.
    """
    result = scout_url(url, timeout=timeout, json_output=json_output)

    if json_output and result is not None:
        from toolkit.cli.common import echo_json

        echo_json(result)


# ── preview subcommand ──────────────────────────────────────────────────────


def preview(
    url: str = typer.Argument(..., help="URL del file dati remoto da profilare"),
    json_output: bool = typer.Option(False, "--json", help="Output in formato JSON"),
    known_encoding: str | None = typer.Option(
        None, "--encoding", help="Encoding noto (salta sniff)"
    ),
    known_delim: str | None = typer.Option(None, "--delim", help="Delimiter noto (salta sniff)"),
    known_skip: int | None = typer.Option(
        None, "--skip", min=0, help="Righe da saltare in testa (metadati prima dell'header)"
    ),
):
    """
    Preview remoto di un URL CSV/TSV: scarica un chunk, profila con DuckDB,
    e restituisce colonne, tipi, granularità e intervallo anni.

    Solo CSV/TSV per ora. Usa ``toolkit scout <URL>`` per probe generico.
    """
    from dataclasses import asdict

    from toolkit.profile.preview import preview_url as _preview_url

    result = _preview_url(
        url,
        known_encoding=known_encoding,
        known_delim=known_delim,
        known_skip=known_skip,
    )

    if json_output:
        from toolkit.cli.common import echo_json

        echo_json(asdict(result))
        return

    typer.echo(f"URL: {url}")
    typer.echo(f"  Status:          {result.status}")
    typer.echo(f"  Reachable:       {result.reachable}")
    typer.echo(f"  HTTP status:     {result.http_status}")
    typer.echo(f"  File size:       {result.file_size}")
    typer.echo(f"  Format:          {result.resource_format}")
    typer.echo(f"  Encoding:        {result.encoding_suggested}")
    typer.echo(f"  Delimiter:       {result.delim_suggested}")
    typer.echo(f"  Skip rows:       {result.skip_suggested}")
    typer.echo(f"  Granularity:     {result.granularity}")
    typer.echo(f"  Year range:      {result.year_min} - {result.year_max}")
    typer.echo(f"  Row count:       {result.preview_row_count}")
    if result.quality_score is not None:
        verdict_icon = {"buona": "✅", "accettabile": "⚠️", "scarsa": "🔴"}
        icon = verdict_icon.get(result.quality_verdict or "", "")
        typer.echo(
            f"  Quality score:   {result.quality_score}/100 ({icon} {result.quality_verdict})"
        )
        if result.quality_semantic_score is not None:
            typer.echo(f"  Semantic:        {result.quality_semantic_score}/100 (indicativo)")
        if result.quality_combined_score is not None:
            typer.echo(f"  Combined:        {result.quality_combined_score}/100")
        if result.quality_flags:
            typer.echo(f"  Quality flags:   {', '.join(result.quality_flags)}")
        if result.quality_ontologies:
            families = sorted(result.quality_ontologies.keys())
            typer.echo(f"  Ontologies:      {', '.join(families)}")
        if result.quality_note:
            typer.echo(f"  Quality note:    {result.quality_note}")
    if result.columns:
        cols = result.columns
        typer.echo(f"  Columns ({len(cols)}): {', '.join(str(c) for c in cols[:20])}")
        if len(cols) > 20:
            typer.echo(f"    ... and {len(cols) - 20} more")


def register(app: typer.Typer) -> None:
    app.command("scout")(scout)
    app.command("preview")(preview)
