"""toolkit scout — esplora URL esterni con probe + routing + inferenze.

Output leggibile o JSON. Con --scaffold genera anche i file candidate.
Sostituisce inspect url (deprecato) come comando di URL scouting.

Usage:
    toolkit scout <URL>                  # probe + info leggibile
    toolkit scout <URL> --json           # probe in JSON
    toolkit scout <URL> --scaffold       # probe + scaffold candidate
    toolkit scout <URL> --scaffold --run # probe + scaffold + raw run
"""

from __future__ import annotations

import json
import tempfile
import uuid
from pathlib import Path
from typing import Any

import typer

from toolkit.cli.cmd_run import run_init as _run_init
from toolkit.scout.http import DEFAULT_TIMEOUT, fetch_content
from toolkit.scaffold.full import (
    generate_full_scaffold,
    suggest_validation,
)
from toolkit.scaffold.sources import infer_ext, infer_filename, slugify
from toolkit.scout.infer import (
    infer_granularity_from_name_and_columns,
    infer_topics,
    suggest_years,
)
from toolkit.scout.probe import probe_url_routed

_SAMPLE_SIZE = 1024 * 1024  # 1MB


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
# Scout orchestration — esportata per init --url
# ---------------------------------------------------------------------------


def scout_url(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    scaffold: bool = False,
    run_raw: bool = False,
    json_output: bool = False,
) -> dict[str, Any] | None:
    """Probe arricchito + profiling + inferenze + scaffold opzionale.

    Args:
        url: URL da esplorare.
        timeout: Timeout HTTP in secondi.
        scaffold: Se True, genera anche i file candidate.
        run_raw: Se True, esegue run raw dopo scaffold.
        json_output: Se True, restituisce dict invece di stamapare.

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
        resources = probe.get("ckan_resources") or []
        if not resources:
            _echo("error: CKAN portal detected but no downloadable resources found", err=True)
            raise typer.Exit(code=1)
        _echo(f"  CKAN resources: {len(resources)} found")
        for res in resources[:3]:
            _echo(f"    - {res['name']} ({res['format']})")
        if len(resources) > 3:
            _echo(f"    ... and {len(resources) - 3} more")
        if scaffold and not json_output:
            _scaffold_ckan(url, probe, run_raw=run_raw)

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
        if scaffold and not json_output:
            _scaffold_html(url, probe, run_raw=run_raw)

    elif source_type == "sdmx":
        sdmx_info = probe.get("sdmx_info") or {}
        flow_id = sdmx_info.get("flow_id")
        year_min = sdmx_info.get("year_min")
        year_max = sdmx_info.get("year_max")
        _echo(f"  SDMX flow: {flow_id}")
        if year_min and year_max:
            _echo(f"  Years: {year_min}-{year_max}")
        if scaffold and not json_output:
            _scaffold_sdmx(url, probe, run_raw=run_raw)

    elif source_type == "file":
        resolved_format = probe.get("resolved_format")
        if resolved_format:
            _echo(f"  Detected format: {resolved_format}")
        if scaffold and not json_output:
            _scaffold_file(url, probe, run_raw=run_raw)

    elif source_type == "opaque":
        _echo(f"error: URL returned opaque content", err=True)
        raise typer.Exit(code=1)

    else:
        _echo(f"error: unexpected source type '{source_type}'", err=True)
        raise typer.Exit(code=1)

    # Step 3: Suggerimento prossimo passo (solo in modalità umana)
    if not scaffold and not json_output and source_type in ("file", "html", "ckan", "sdmx"):
        _echo("")
        _echo(f"Next: toolkit init --url \"{url}\"")
        if source_type == "file":
            _echo("      toolkit init --url <URL> --run  (include raw run)")

    if json_output:
        return result
    return None


# ---------------------------------------------------------------------------
# Scaffold per tipo fonte (usate da scout_url e da init --url)
# ---------------------------------------------------------------------------


def _scaffold_file(url: str, probe_result: dict[str, Any], *, run_raw: bool = False) -> None:
    """Scarica sample, profila, inferisce, genera scaffold."""
    slug = slugify(url)
    tmp_dir = Path(tempfile.gettempdir())
    tmp_name = f"scout_{slug}_{uuid.uuid4().hex[:8]}"

    # 1. Download sample
    typer.echo("Downloading sample...")
    try:
        fetched = fetch_content(url, max_bytes=_SAMPLE_SIZE, timeout=30)
    except RuntimeError as exc:
        typer.echo(f"error: failed to fetch {url}: {exc}", err=True)
        raise typer.Exit(code=1)

    content = fetched["content"]
    ct = fetched.get("content_type") or probe_result.get("content_type", "")
    ext = infer_ext(url, ct)
    sample_path = tmp_dir / f"{tmp_name}{ext}"
    sample_path.write_bytes(content)
    typer.echo(f"  Saved {len(content)} bytes to {sample_path}")

    # 2. Sniff + Profile
    from toolkit.profile.raw import profile_with_read_cfg, sniff_source_file

    sniff_hints = sniff_source_file(sample_path)
    typer.echo(f"  Encoding: {sniff_hints.get('encoding_suggested')}")
    typer.echo(f"  Delimiter: {sniff_hints.get('delim_suggested')}")
    typer.echo(f"  Columns: {sniff_hints.get('columns_preview')}")

    read_cfg: dict[str, Any] = {}
    if sniff_hints.get("encoding_suggested"):
        read_cfg["encoding"] = sniff_hints["encoding_suggested"]
    if sniff_hints.get("delim_suggested"):
        read_cfg["delim"] = sniff_hints["delim_suggested"]
    if sniff_hints.get("skip_suggested", 0) > 0:
        read_cfg["skip"] = sniff_hints["skip_suggested"]
    if sniff_hints.get("robust_read_suggested"):
        from toolkit.core.csv_read import robust_preset
        read_cfg = robust_preset(read_cfg)

    profile = profile_with_read_cfg(sample_path, sniff_hints, read_cfg)

    # 3. Retry skip se 0 colonne
    retry_skip = _resolve_columns(profile, sniff_hints, read_cfg, sample_path)
    if retry_skip is not None and retry_skip != sniff_hints.get("skip_suggested"):
        sniff_hints["skip_suggested"] = retry_skip
        read_cfg["skip"] = retry_skip
        profile = profile_with_read_cfg(sample_path, sniff_hints, read_cfg)

    # 4. Clean read via scaffold canonico
    from toolkit.scaffold.clean import propose_clean_read

    enriched = dict(profile)
    for k in (
        "encoding_suggested", "delim_suggested", "decimal_suggested",
        "skip_suggested", "header_line", "true_header_line", "robust_read_suggested",
    ):
        if sniff_hints.get(k) is not None:
            enriched[k] = sniff_hints[k]

    clean_read = propose_clean_read(enriched)

    # 5. Inferenze
    norm_cols = profile.get("columns_norm") or profile.get("columns_raw") or profile.get("columns") or []
    col_names = [str(c) for c in norm_cols]

    inferred_years = suggest_years(url=url, column_names=col_names, profile=profile)
    typer.echo(f"  Suggested years: {inferred_years}")

    granularity = infer_granularity_from_name_and_columns(slug, col_names)
    typer.echo(f"  Granularity: {granularity}")

    topics = infer_topics(f"{slug} {' '.join(col_names)}")
    if topics:
        top_topics = [t["topic"] for t in topics[:3]]
        typer.echo(f"  Topics: {', '.join(top_topics)}")

    validation = suggest_validation(profile)
    if validation:
        typer.echo(f"  Validation rules: suggested")

    # 6. Genera scaffold
    out_dir = Path(slug)
    out_dir.mkdir(parents=True, exist_ok=True)

    probe_result["inferred_granularity"] = granularity
    probe_result["inferred_topics"] = topics

    files = generate_full_scaffold(
        slug,
        probe_result,
        clean_read=clean_read,
        profile=profile,
        inferred_years=inferred_years,
        validation_suggestions=validation,
    )

    for rel_path, content in files.items():
        full_path = out_dir / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(content, encoding="utf-8")

    (out_dir / "notebooks").mkdir(exist_ok=True)

    columns_count = len(clean_read.get("columns") or profile.get("columns_raw") or profile.get("columns_norm") or [])
    typer.echo(f"\nDataset YAML generated: {out_dir / 'dataset.yml'}")
    typer.echo(f"  clean.read.columns: {columns_count} columns")
    typer.echo(f"  years: {inferred_years}")
    typer.echo(f"  source_type: {probe_result.get('source_type', 'file')}")
    typer.echo("  sql/clean.sql:      generated (with type casts)")
    typer.echo("  sql/mart.sql:       generated (with aggregation)")
    typer.echo("  README.md, notes.md, notebooks/: created")

    # 7. Opzionalmente raw run
    if run_raw:
        _run_bootstrap(str(out_dir / "dataset.yml"))

    # 8. Cleanup
    sample_path.unlink(missing_ok=True)

    if not run_raw:
        typer.echo(f"\nNext: toolkit run all --config {out_dir / 'dataset.yml'}")


def _scaffold_ckan(url: str, probe_result: dict[str, Any], *, run_raw: bool = False) -> None:
    """Scaffold per risorsa CKAN."""
    resources = probe_result.get("ckan_resources") or []
    if not resources:
        typer.echo("error: no CKAN resources available", err=True)
        raise typer.Exit(code=1)

    first_url = resources[0]["url"]
    try:
        _scaffold_file(first_url, probe_result, run_raw=run_raw)
        return
    except (typer.Exit, Exception):
        typer.echo(f"  Warning: profiling failed for resource, generating minimal scaffold")


def _scaffold_html(url: str, probe_result: dict[str, Any], *, run_raw: bool = False) -> None:
    """Scaffold per pagina HTML con link."""
    candidates = probe_result.get("candidate_links") or []
    if not candidates:
        typer.echo("error: no candidate links available", err=True)
        raise typer.Exit(code=1)

    if len(candidates) == 1:
        _scaffold_file(candidates[0], probe_result, run_raw=run_raw)
    else:
        _scaffold_file(candidates[0], probe_result, run_raw=run_raw)
        typer.echo("  (using first link — run init again with a direct URL for a different one)")


def _scaffold_sdmx(url: str, probe_result: dict[str, Any], *, run_raw: bool = False) -> None:
    """Scaffold per endpoint SDMX."""
    slug = slugify(url)
    from toolkit.scaffold.sources import block_sdmx as _generate_raw_sources_block_sdmx

    sdmx_info = probe_result.get("sdmx_info") or {}
    year_min = sdmx_info.get("year_min")
    year_max = sdmx_info.get("year_max")

    if year_min and year_max:
        inferred_years = list(range(year_min, year_max + 1))
    else:
        inferred_years = [2024]

    # Scaffold minimo per SDMX (no profiling CSV)
    files = generate_full_scaffold(
        slug,
        probe_result,
        clean_read=None,
        profile=None,
        inferred_years=inferred_years,
        validation_suggestions=None,
    )

    # Sovrascrivi dataset.yml con configurazione SDMX
    lines = [
        "# Auto-generated by toolkit init --url",
        "# Review and adjust before running",
        "",
        'root: "../../out"',
        "schema_version: 1",
        "",
        "dataset:",
        f'  name: "{slug}"',
        "  years: " + _fmt_years(inferred_years),
        "",
        "raw:",
        "  output_policy: overwrite",
        "  sources:",
    ]
    lines.extend(_generate_raw_sources_block_sdmx(sdmx_info, url))
    lines.append("")
    lines.append("clean:")
    lines.append('  sql: "sql/clean.sql"')
    lines.append("")
    lines.append("mart:")
    lines.append("  tables:")
    lines.append(f'    - name: "{slug}"')
    lines.append('      sql: "sql/mart.sql"')

    out_dir = Path(slug)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "dataset.yml").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # clean.sql generico per SDMX
    clean_sql_path = out_dir / "sql" / "clean.sql"
    clean_sql_path.parent.mkdir(parents=True, exist_ok=True)
    clean_sql_path.write_text(
        "-- SDMX flow: transform raw SDMX to clean tabular.\n"
        "-- Personalizza estrazione delle dimensioni e misure.\n"
        "SELECT * FROM raw_input\n",
        encoding="utf-8",
    )

    mart_sql_path = out_dir / "sql" / "mart.sql"
    if not mart_sql_path.exists():
        mart_sql_path.parent.mkdir(parents=True, exist_ok=True)
        mart_sql_path.write_text(
            "-- Default mart: SELECT * FROM clean.\n"
            "SELECT * FROM clean\n",
            encoding="utf-8",
        )

    (out_dir / "README.md").write_text(
        f"# {slug}\n\nFonte: {url}\n\n"
        "## Domanda\n\n-\n\n"
        "## Dataset\n\n-\n\n"
        "## Stato\n\n- intake\n",
        encoding="utf-8",
    )
    (out_dir / "notes.md").write_text(
        "## Tecnico\n\n- Fonte SDMX\n\n"
        "## Analitico\n\n-\n\n"
        "## Cautele\n\n- Verificare completezza serie storica\n",
        encoding="utf-8",
    )
    (out_dir / "notebooks").mkdir(exist_ok=True)

    typer.echo(f"\nDataset YAML generated: {out_dir / 'dataset.yml'}")
    typer.echo(f"  source_type: sdmx")
    typer.echo(f"  flow: {sdmx_info.get('flow_id', '?')}")
    if year_min and year_max:
        typer.echo(f"  years: {year_min}-{year_max}")

    if run_raw:
        _run_bootstrap(str(out_dir / "dataset.yml"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_columns(profile, sniff_hints, read_cfg, sample_path) -> int | None:
    """If profiling returned 0 columns, retry with skip 0..5."""
    from toolkit.profile.raw import profile_with_read_cfg

    raw_cols = profile.get("columns_raw") or profile.get("columns_norm") or []
    if raw_cols:
        return None

    for try_skip in range(6):
        if try_skip == sniff_hints.get("skip_suggested", 0):
            continue
        retry_cfg = dict(read_cfg)
        retry_cfg["skip"] = try_skip
        retry_profile = profile_with_read_cfg(sample_path, sniff_hints, retry_cfg)
        retry_cols = retry_profile.get("columns_raw") or retry_profile.get("columns_norm") or []
        if len(retry_cols) >= 2:
            typer.echo(f"  Retry with skip={try_skip}: {len(retry_cols)} columns found")
            return try_skip
    return None


def _run_bootstrap(config_path: str) -> None:
    """Esegue init --config dopo scaffold (run raw + scaffold clean.sql)."""
    typer.echo("")
    typer.echo("[scout] --run flag enabled: bootstrapping raw...")
    typer.echo("")
    _run_init(
        config=config_path,
        year=None,
        years=None,
        dry_run=False,
        strict_config=False,
    )
    typer.echo("")
    typer.echo("[scout] Raw run completed.")
    typer.echo(f"Next: toolkit run clean --config {config_path}")
    typer.echo(f"      toolkit run mart --config {config_path}")


def _fmt_years(years: list[int]) -> str:
    if len(years) <= 4:
        return "[" + ", ".join(str(y) for y in years) + "]"
    return f"[{years[0]}..{years[-1]}]"


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------


def scout(
    url: str = typer.Argument(..., help="URL da esplorare"),
    scaffold: bool = typer.Option(False, "--scaffold", "-s", help="Genera scaffold candidate dataset"),
    run: bool = typer.Option(False, "--run", "-r", help="Scaffold + raw run (implies --scaffold)"),
    json_output: bool = typer.Option(False, "--json", help="Output in formato JSON"),
    timeout: int = typer.Option(DEFAULT_TIMEOUT, "--timeout", min=1, help="Timeout HTTP in secondi"),
):
    """
    Esplora un URL esterno: probe HTTP, routing automatico e inferenze.

    Rileva automaticamente se l'URL e' un file CSV/XLSX/JSON diretto,
    una pagina HTML con link a dati, un portale CKAN o un endpoint SDMX.

    Con --scaffold (alias -s): genera anche i file candidate (dataset.yml,
    sql/clean.sql, sql/mart.sql, README.md, notes.md).

    Con --run (alias -r): dopo lo scaffold esegue anche il run raw.
    """
    if run:
        scaffold = True

    result = scout_url(
        url,
        timeout=timeout,
        scaffold=scaffold,
        run_raw=run,
        json_output=json_output,
    )

    if json_output and result is not None:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False))


def register(app: typer.Typer) -> None:
    app.command("scout")(scout)
