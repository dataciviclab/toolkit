"""Top-level `init` command — bootstrap a dataset from URL or config.

Usage:
    toolkit init --url <URL>                     # scout + generate dataset.yml
    toolkit init --url <URL> --run               # scout + run raw + scaffold
    toolkit init --config <dataset.yml>           # run raw + scaffold (existing)
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from pathlib import Path
from typing import Any

import typer

from lab_connectors.http import HttpClient

from toolkit.scout.http import DEFAULT_TIMEOUT, fetch_content, resolve_preview_kind
from toolkit.scout.infer import infer_granularity_from_name_and_columns, infer_topics, infer_years, suggest_years, suggest_clean_sql, suggest_mart_sql, suggest_validation
from toolkit.scout.probe import probe_url_routed
from toolkit.scout.scaffold import (
    generate_full_scaffold,
    infer_ext,
    infer_filename,
    slugify,
)
from toolkit.cli.cmd_run import run_init as _run_init

logger = logging.getLogger("toolkit.cli.init")

_SAMPLE_SIZE = 1024 * 1024  # 1MB sample


# ---------------------------------------------------------------------------
# Scout arricchito
# ---------------------------------------------------------------------------


def _scout(url: str, *, timeout: int = DEFAULT_TIMEOUT, run_raw: bool = False) -> None:
    """Probe arricchito + profiling + scaffold completo.

    Usa probe_url_routed() invece della probe_url() classica:
    - Rileva automaticamente CKAN, HTML, SDMX, file diretto
    - Per CKAN: scarica risorse e genera config appropriata
    - Per HTML con link: propone link candidati
    - Per file diretto: profiling + scaffold come prima ma con inferenze
    """
    typer.echo(f"Probing {url}...")

    # Step 1: Probe arricchito con routing
    probe = probe_url_routed(url, timeout=min(timeout, 30))
    source_type = probe["source_type"]

    typer.echo(f"  Source type: {source_type}")
    typer.echo(f"  HTTP status: {probe['status_code']}")
    if probe.get("resolved_format"):
        typer.echo(f"  Format: {probe['resolved_format']}")

    # Routing in base al tipo fonte
    if source_type == "ckan":
        resources = probe.get("ckan_resources") or []
        if not resources:
            typer.echo("error: CKAN portal detected but no downloadable resources found", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"  CKAN resources: {len(resources)} found")
        for res in resources[:3]:
            typer.echo(f"    - {res['name']} ({res['format']})")
        if len(resources) > 3:
            typer.echo(f"    ... and {len(resources) - 3} more")
        # Per CKAN facciamo comunque profiling su una risorsa
        _scout_ckan(url, probe, run_raw=run_raw)
        return

    elif source_type == "html":
        candidates = probe.get("candidate_links") or []
        if not candidates:
            typer.echo("error: HTML page with no downloadable data links", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"  Candidate links: {len(candidates)} found")
        for link in candidates[:5]:
            typer.echo(f"    - {link}")
        if len(candidates) > 5:
            typer.echo(f"    ... and {len(candidates) - 5} more")
        if len(candidates) == 1:
            # Un solo link candidato: usalo direttamente
            typer.echo("  (single link — proceeding with profiling)")
            _scout_file(candidates[0], probe, run_raw=run_raw)
        else:
            # Multipli link: chiedi interazione o usa primo
            _scout_file(candidates[0], probe, run_raw=run_raw)
            typer.echo("  (using first link — run init again with a direct URL for a different one)")
        return

    elif source_type == "sdmx":
        sdmx_info = probe.get("sdmx_info") or {}
        flow_id = sdmx_info.get("flow_id")
        year_min = sdmx_info.get("year_min")
        year_max = sdmx_info.get("year_max")
        typer.echo(f"  SDMX flow: {flow_id}")
        if year_min and year_max:
            typer.echo(f"  Years: {year_min}-{year_max}")
        _scout_sdmx(url, probe, run_raw=run_raw)
        return

    elif source_type == "file":
        resolved_format = probe.get("resolved_format")
        if resolved_format:
            typer.echo(f"  Detected format: {resolved_format}")
        _scout_file(url, probe, run_raw=run_raw)
        return

    elif source_type == "opaque":
        ct = probe.get("content_type", "unknown")
        typer.echo(f"error: URL returned opaque content (Content-Type: {ct})", err=True)
        typer.echo("  Impossibile determinare il tipo di contenuto.", err=True)
        raise typer.Exit(code=1)

    else:
        typer.echo(f"error: unexpected source type '{source_type}'", err=True)
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# Scout per file diretto
# ---------------------------------------------------------------------------


def _scout_file(url: str, probe_result: dict[str, Any], *, run_raw: bool = False) -> None:
    """Scarica sample, profila, genera scaffold."""
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
    from toolkit.profile.raw import sniff_source_file, profile_with_read_cfg

    sniff_hints = sniff_source_file(sample_path)
    typer.echo(f"  Encoding: {sniff_hints.get('encoding_suggested')}")
    typer.echo(f"  Delimiter: {sniff_hints.get('delim_suggested')}")
    typer.echo(f"  Columns: {sniff_hints.get('columns_preview')}")

    # 3. Build read config
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

    # 4. Retry skip se 0 colonne
    retry_skip = _resolve_columns(profile, sniff_hints, read_cfg, sample_path)
    if retry_skip is not None and retry_skip != sniff_hints.get("skip_suggested"):
        sniff_hints["skip_suggested"] = retry_skip
        read_cfg["skip"] = retry_skip
        profile = profile_with_read_cfg(sample_path, sniff_hints, read_cfg)

    # 5. Clean read via scaffold canonico
    from toolkit.scaffold.clean import propose_clean_read

    enriched = dict(profile)
    for k in ("encoding_suggested", "delim_suggested", "decimal_suggested",
              "skip_suggested", "header_line", "true_header_line", "robust_read_suggested"):
        if sniff_hints.get(k) is not None:
            enriched[k] = sniff_hints[k]

    clean_read = propose_clean_read(enriched)

    # 6. Inferenze
    norm_cols = profile.get("columns_norm") or profile.get("columns_raw") or profile.get("columns") or []
    col_names = [str(c) for c in norm_cols]

    # Anni
    inferred_years = suggest_years(url=url, column_names=col_names, profile=profile)
    typer.echo(f"  Suggested years: {inferred_years}")

    # Granularità
    granularity = infer_granularity_from_name_and_columns(slug, col_names)
    typer.echo(f"  Granularity: {granularity}")

    # Topic
    topics = infer_topics(f"{slug} {' '.join(col_names)}")
    if topics:
        top_topics = [t["topic"] for t in topics[:3]]
        typer.echo(f"  Topics: {', '.join(top_topics)}")

    # Validation rules suggerite
    validation = suggest_validation(profile)
    if validation:
        typer.echo(f"  Validation rules: suggested")

    # 7. Genera scaffold
    out_dir = Path(slug)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Aggiungi inferenze al probe_result per lo scaffold
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

    # 8. Opzionalmente esegui run raw
    if run_raw:
        _run_init_after_scaffold(str(out_dir / "dataset.yml"))

    # 9. Cleanup
    sample_path.unlink(missing_ok=True)

    typer.echo(f"\nNext: toolkit run all --config {out_dir / 'dataset.yml'}")


# ---------------------------------------------------------------------------
# Scout per CKAN
# ---------------------------------------------------------------------------


def _scout_ckan(url: str, probe_result: dict[str, Any], *, run_raw: bool = False) -> None:
    """Scaffold per risorsa CKAN."""
    slug = slugify(url)
    resources = probe_result.get("ckan_resources") or []
    if not resources:
        typer.echo("error: no CKAN resources available", err=True)
        raise typer.Exit(code=1)

    # Usa la prima risorsa per profilo (se raggiungibile)
    # Prova a fare profiling sulla prima risorsa
    first_url = resources[0]["url"]
    try:
        _scout_file(first_url, probe_result, run_raw=run_raw)
        return
    except typer.Exit as exc:
        if exc.args and exc.args[0] == 0:
            return
        # Se profiling fallisce, scaffold senza profilo
        typer.echo(f"  Warning: profiling failed for resource, generating minimal scaffold")
    except Exception:
        typer.echo(f"  Warning: profiling failed for resource, generating minimal scaffold")

    # Scaffold minimo anche senza profilo
    ckan_resources = probe_result.get("ckan_resources", [])
    if run_raw:
        _run_init_after_scaffold(str(Path(slug) / "dataset.yml"))


# ---------------------------------------------------------------------------
# Scout per SDMX
# ---------------------------------------------------------------------------


def _scout_sdmx(url: str, probe_result: dict[str, Any], *, run_raw: bool = False) -> None:
    """Scaffold per endpoint SDMX."""
    slug = slugify(url)
    from toolkit.scout.scaffold import _generate_raw_sources_block_sdmx

    sdmx_info = probe_result.get("sdmx_info") or {}
    year_min = sdmx_info.get("year_min")
    year_max = sdmx_info.get("year_max")

    # Anni
    if year_min and year_max:
        inferred_years = list(range(year_min, year_max + 1))
    else:
        inferred_years = [2024]

    # Scaffold minimo (SDMX non si profila con sniff CSV)
    files = generate_full_scaffold(
        slug,
        probe_result,
        clean_read=None,
        profile=None,
        inferred_years=inferred_years,
        validation_suggestions=None,
    )

    # Sovrascrivi dataset.yml con configurazione SDMX appropriata
    lines = [
        "# Auto-generated by toolkit init --url",
        "# Review and adjust before running",
        "",
        'root: "../../out"',
        "schema_version: 1",
        "",
        "dataset:",
        f'  name: "{slug}"',
        "  years: " + _format_years_simple(inferred_years),
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
            "-- Personalizza per aggregazioni SDMX.\n"
            "SELECT * FROM clean\n",
            encoding="utf-8",
        )

    (out_dir / "README.md").write_text(_generate_readme_simple(slug, url), encoding="utf-8")
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
        _run_init_after_scaffold(str(out_dir / "dataset.yml"))


def _format_years_simple(years: list[int]) -> str:
    if len(years) <= 4:
        return "[" + ", ".join(str(y) for y in years) + "]"
    return f"[{years[0]}..{years[-1]}]"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_columns(
    profile: dict[str, Any],
    sniff_hints: dict[str, Any],
    read_cfg: dict[str, Any],
    sample_path: Path,
) -> int | None:
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


def _run_init_after_scaffold(config_path: str) -> None:
    """Esegue init --config dopo lo scaffold (run raw + scaffold clean.sql)."""
    typer.echo("")
    typer.echo("[init] --run flag enabled: bootstrapping raw...")
    typer.echo("")
    _run_init(
        config=config_path,
        year=None,
        years=None,
        dry_run=False,
        strict_config=False,
    )
    typer.echo("")
    typer.echo("[init] Raw run completed.")
    typer.echo(f"Next: toolkit run clean --config {config_path}")
    typer.echo(f"      toolkit run mart --config {config_path}")


def _generate_readme_simple(slug: str, url: str) -> str:
    return (
        f"# {slug}\n\n"
        f"Fonte: {url}\n\n"
        "## Domanda\n\n-\n\n"
        "## Dataset\n\n-\n\n"
        "## Stato\n\n- intake\n\n"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def init(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to dataset.yml"),
    url: str | None = typer.Option(None, "--url", "-u", help="Download, profile and scaffold from URL"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year (for --config)"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years (for --config)"),
    run: bool = typer.Option(False, "--run", "-r", help="Also execute raw run after scaffold (only with --url)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print plan without executing"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Bootstrap candidate: prepara dataset.yml e scaffold SQL.

    Con --url: scarica un sample, profila encoding/delim/colonne, genera
    dataset.yml completo con sql/clean.sql e sql/mart.sql placeholder.
    Usa probe arricchito con rilevamento automatico di CKAN, SDMX, HTML.
    Usa --run per eseguire anche il run raw dopo lo scaffold.

    Con --config: run raw + scaffold clean.sql se assente.
    """
    if url and config:
        typer.echo("error: specificare --url o --config, non entrambi", err=True)
        raise typer.Exit(code=1)

    if url:
        _scout(url, run_raw=run)
        return

    if not config:
        typer.echo("error: specificare --url o --config", err=True)
        raise typer.Exit(code=1)

    _run_init(
        config=config,
        year=year,
        years=years,
        dry_run=dry_run,
        strict_config=strict_config,
    )


def register(app: typer.Typer) -> None:
    app.command("init")(init)
