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
from toolkit.scaffold.full import generate_full_scaffold
from toolkit.scaffold.sources import infer_ext, slugify
from toolkit.scout.http import DEFAULT_TIMEOUT, fetch_content
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
# Scout orchestration — esportata per scout CLI e uso programmatico
# ---------------------------------------------------------------------------


def scout_url(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    scaffold: bool = False,
    run_raw: bool = False,
    json_output: bool = False,
    slug: str | None = None,
) -> dict[str, Any] | None:
    """Probe arricchito + profiling + inferenze + scaffold opzionale.

    Args:
        url: URL da esplorare.
        timeout: Timeout HTTP in secondi.
        scaffold: Se True, genera anche i file candidate.
        run_raw: Se True, esegue run raw dopo scaffold.
        json_output: Se True, restituisce dict invece di stamapare.
        slug: Slug personalizzato (auto-generato da URL se None).

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
        if scaffold and not json_output:
            if resources:
                _scaffold_ckan(url, probe, run_raw=run_raw, slug=slug)

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
            _scaffold_html(url, probe, run_raw=run_raw, slug=slug)

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
        if scaffold and not json_output:
            _scaffold_sparql(url, probe, run_raw=run_raw, slug=slug)

    elif source_type == "sdmx":
        _echo(f"  SDMX flow: {probe.get('sdmx_info', {}).get('flow_id', '?')}")
        sdmx_info = probe.get("sdmx_info") or {}
        if sdmx_info.get("year_min"):
            _echo(f"  Year range: {sdmx_info['year_min']}-{sdmx_info.get('year_max', '?')}")
        if scaffold and not json_output:
            _scaffold_sdmx(url, probe, run_raw=run_raw, slug=slug)

    elif source_type == "file":
        resolved_format = probe.get("resolved_format")
        if resolved_format:
            _echo(f"  Detected format: {resolved_format}")
        if scaffold and not json_output:
            _scaffold_file(url, probe, run_raw=run_raw, slug=slug)

    elif source_type == "opaque":
        _echo("error: URL returned opaque content", err=True)
        raise typer.Exit(code=1)

    else:
        _echo(f"error: unexpected source type '{source_type}'", err=True)
        raise typer.Exit(code=1)

    # Step 3: Suggerimento prossimo passo (solo in modalità umana)
    if (
        not scaffold
        and not json_output
        and source_type in ("file", "html", "ckan", "sdmx", "sparql")
    ):
        _echo("")
        _echo(f'Next: toolkit scout "{url}" --scaffold')
        if source_type in ("file", "sparql"):
            _echo("      toolkit scout <URL> --scaffold --run  (include raw run)")

    if json_output:
        return result
    return None


# ---------------------------------------------------------------------------
# Scaffold per tipo fonte (usate da scout_url)
# ---------------------------------------------------------------------------


def _profile_sample(
    sample_path: Path,
    url: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Sniff + profile a downloaded sample file. Returns (profile, sniff_hints)."""
    from toolkit.profile.raw import profile_with_read_cfg, sniff_source_file

    sniff_hints = sniff_source_file(sample_path)
    typer.echo(f"  Encoding: {sniff_hints.get('encoding_suggested')}")
    typer.echo(f"  Delimiter: {sniff_hints.get('delim_suggested')}")
    typer.echo(f"  Columns: {sniff_hints.get('columns_preview')}")

    binary_fmt = sniff_hints.get("is_binary_file")
    if binary_fmt in ("xlsx", "xls"):
        from toolkit.profile.raw import profile_excel

        profile = profile_excel(sample_path, None)
        profile["is_binary_file"] = binary_fmt
    else:
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

        retry_skip = _resolve_columns(profile, sniff_hints, read_cfg, sample_path)
        if retry_skip is not None and retry_skip != sniff_hints.get("skip_suggested"):
            sniff_hints["skip_suggested"] = retry_skip
            read_cfg["skip"] = retry_skip
            profile = profile_with_read_cfg(sample_path, sniff_hints, read_cfg)

    return profile, sniff_hints


def _scaffold_file(
    url: str,
    probe_result: dict[str, Any],
    *,
    run_raw: bool = False,
    slug: str | None = None,
) -> None:
    """Download sample, profile, generate scaffold via orchestrator."""
    slug = slug or slugify(url)
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
    profile, sniff_hints = _profile_sample(sample_path, url)

    # 3. Propagate robust_read_suggested
    if sniff_hints.get("robust_read_suggested") or profile.get("robust_read_suggested"):
        profile["_robust_read_suggested"] = True
        probe_result["robust_read_suggested"] = True

    # 4. Infer years + granularity + topics
    year_values = _read_year_values_from_sample(sample_path, sniff_hints, profile)
    if year_values:
        typer.echo(f"  Year values in data: {sorted(year_values)}")

    norm_cols = (
        profile.get("columns_norm") or profile.get("columns_raw") or profile.get("columns") or []
    )
    col_names = [str(c) for c in norm_cols]

    inferred_years = suggest_years(
        column_names=col_names,
        profile=profile,
        year_values=year_values,
    )
    typer.echo(f"  Suggested years: {inferred_years}")

    granularity = infer_granularity_from_name_and_columns(slug, col_names)
    typer.echo(f"  Granularity: {granularity}")

    topics = infer_topics(f"{slug} {' '.join(col_names)}")
    if topics:
        typer.echo(f"  Topics: {', '.join(t['topic'] for t in topics[:3])}")

    # 5. Enrich probe_result for orchestrator
    probe_result["inferred_granularity"] = granularity
    probe_result["inferred_topics"] = topics
    for key in (
        "encoding_suggested",
        "delim_suggested",
        "decimal_suggested",
        "skip_suggested",
        "header_line",
        "true_header_line",
    ):
        if sniff_hints.get(key) is not None:
            probe_result[key] = sniff_hints[key]

    # 6. Generate scaffold (one call)
    files = generate_full_scaffold(
        slug,
        probe_result,
        profile=profile,
        inferred_years=inferred_years,
    )

    # 7. Write files
    out_dir = Path(slug)
    for rel_path, file_content in files.items():
        full_path = out_dir / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(file_content, encoding="utf-8")
    (out_dir / "notebooks").mkdir(exist_ok=True)

    typer.echo(f"\nDataset YAML generated: {out_dir / 'dataset.yml'}")
    typer.echo(f"  years: {inferred_years}")
    typer.echo(f"  source_type: {probe_result.get('source_type', 'file')}")
    typer.echo("  sql/clean.sql, sql/mart.sql: generated")

    # 8. Optional raw run
    if run_raw:
        _run_bootstrap(str(out_dir / "dataset.yml"))

    sample_path.unlink(missing_ok=True)
    if not run_raw:
        typer.echo(f"\nNext: toolkit run all --config {out_dir / 'dataset.yml'}")


def _write_scaffold_files(
    slug: str,
    scaffold_files: dict[str, str],
) -> Path:
    """Scrive su disco i file restituiti da ``generate_full_scaffold``."""
    out_dir = Path(slug)
    out_dir.mkdir(parents=True, exist_ok=True)
    for fname, content in scaffold_files.items():
        fpath = out_dir / fname
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_text(content, encoding="utf-8")
    return out_dir


def _scaffold_ckan(
    url: str,
    probe_result: dict[str, Any],
    *,
    run_raw: bool = False,
    slug: str | None = None,
) -> None:
    """Scaffold per risorsa CKAN. Tenta DataStore → CSV profiling → minimal."""
    resources = probe_result.get("ckan_resources") or []
    if not resources:
        typer.echo("error: no CKAN resources available", err=True)
        raise typer.Exit(code=1)

    slug = slug or slugify(url)

    # Try DataStore schema first
    if resources[0].get("datastore_active"):
        try:
            from toolkit.scout.http import fetch_ckan_datastore_schema
            from toolkit.scaffold.clean import profile_from_datastore

            fields = fetch_ckan_datastore_schema(url, resources[0]["id"])
            if fields:
                profile = profile_from_datastore(fields)
                files = generate_full_scaffold(
                    slug,
                    probe_result,
                    profile=profile,
                )
                out_dir = _write_scaffold_files(slug, files)
                typer.echo(f"\nCKAN DataStore scaffold: {out_dir / 'dataset.yml'}")
                typer.echo(f"  clean.sql with {len(fields)} columns")
                return
        except Exception:
            typer.echo("  DataStore schema fetch failed, trying CSV profiling...")

    # Fallback: download + profile first resource
    try:
        _scaffold_file(resources[0]["url"], probe_result, run_raw=run_raw, slug=slug)
    except (typer.Exit, Exception):
        typer.echo("  Warning: profiling failed, generating minimal scaffold")
        files = generate_full_scaffold(slug, probe_result)
        out_dir = _write_scaffold_files(slug, files)
        typer.echo(f"\nMinimal scaffold: {out_dir / 'dataset.yml'}")


def _scaffold_html(
    url: str,
    probe_result: dict[str, Any],
    *,
    run_raw: bool = False,
    slug: str | None = None,
) -> None:
    """Scaffold per pagina HTML con link."""
    candidates = probe_result.get("candidate_links") or []
    if not candidates:
        typer.echo("error: no candidate links available", err=True)
        raise typer.Exit(code=1)

    if len(candidates) == 1:
        _scaffold_file(candidates[0], probe_result, run_raw=run_raw, slug=slug)
    else:
        _scaffold_file(candidates[0], probe_result, run_raw=run_raw, slug=slug)
        typer.echo("  (using first link — run scout again with a direct URL for a different one)")


def _scaffold_sparql(
    url: str,
    probe_result: dict[str, Any],
    *,
    run_raw: bool = False,
    slug: str | None = None,
) -> None:
    """Scaffold per endpoint SPARQL — uses orchestrator."""
    slug = slug or slugify(url)
    files = generate_full_scaffold(slug, probe_result)
    _write_scaffold_files(slug, files)
    typer.echo(f"\nSPARQL scaffold generated: {Path(slug) / 'dataset.yml'}")


def _scaffold_sdmx(
    url: str,
    probe_result: dict[str, Any],
    *,
    run_raw: bool = False,
    slug: str | None = None,
) -> None:
    """Scaffold per endpoint SDMX — uses orchestrator."""
    slug = slug or slugify(url)
    sdmx_info = probe_result.get("sdmx_info") or {}
    year_min = sdmx_info.get("year_min")
    year_max = sdmx_info.get("year_max")
    years = list(range(year_min, year_max + 1)) if year_min and year_max else None

    files = generate_full_scaffold(
        slug,
        probe_result,
        inferred_years=years,
    )
    out_dir = _write_scaffold_files(slug, files)
    typer.echo(f"\nSDMX scaffold generated: {out_dir / 'dataset.yml'}")
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


def _read_year_values_from_sample(
    sample_path: Path,
    sniff_hints: dict[str, Any],
    profile: dict[str, Any],
) -> set[int]:
    """Legge i valori della colonna Anno dal sample CSV scaricato.

    Usa ``csv.DictReader`` con le stesse opzioni dello sniff per
    leggere le prime righe e trovare la colonna che sembra "anno"
    (normalizzata via ``_find_anno_raw_column``).
    Restituisce un set di anni interi (vuoto se non trovata).
    """
    from toolkit.scaffold.clean import _find_anno_raw_column

    anno_col = _find_anno_raw_column(profile)
    if not anno_col:
        return set()

    import csv

    delim = sniff_hints.get("delim_suggested", ",")
    encoding = sniff_hints.get("encoding_suggested", "utf-8")
    skip = sniff_hints.get("skip_suggested", 0)

    years: set[int] = set()
    try:
        with open(sample_path, encoding=encoding) as f:
            for _ in range(skip):
                next(f)
            reader = csv.DictReader(f, delimiter=delim)
            if anno_col not in (reader.fieldnames or []):
                return set()
            for i, row in enumerate(reader):
                if i >= 100:
                    break
                val = row.get(anno_col, "").strip()
                try:
                    # Gestisce "2023" e "2023.0" ma NON "2023.5" (non intero)
                    year_float = float(val)
                    if year_float.is_integer():
                        years.add(int(year_float))
                except (ValueError, TypeError):
                    pass
    except Exception:
        return set()
    return years


def _run_bootstrap(config_path: str) -> None:
    """Esegue run raw dopo scaffold (scaffold clean.sql se mancante)."""
    typer.echo("")
    typer.echo("[scout] --run flag enabled: bootstrapping raw...")
    typer.echo("")
    _run_init(
        config=config_path,
        year=None,
        years=None,
        dry_run=False,
    )
    typer.echo("")
    typer.echo("[scout] Raw run completed.")
    typer.echo(f"Next: toolkit run clean --config {config_path}")
    typer.echo(f"      toolkit run mart --config {config_path}")


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------


def scout(
    url: str = typer.Argument(..., help="URL da esplorare"),
    scaffold: bool = typer.Option(
        False, "--scaffold", "-s", help="Genera scaffold candidate dataset"
    ),
    run: bool = typer.Option(False, "--run", "-r", help="Scaffold + raw run (implies --scaffold)"),
    json_output: bool = typer.Option(False, "--json", help="Output in formato JSON"),
    slug: str | None = typer.Option(
        None, "--slug", help="Slug personalizzato per il candidate dataset (default: auto da URL)"
    ),
    timeout: int = typer.Option(
        DEFAULT_TIMEOUT, "--timeout", min=1, help="Timeout HTTP in secondi"
    ),
):
    """
    Esplora un URL esterno: probe HTTP, routing automatico e inferenze.

    Rileva automaticamente se l'URL e' un file CSV/XLSX/JSON diretto,
    una pagina HTML con link a dati, un portale CKAN o un endpoint SDMX.

    Con --scaffold (alias -s): genera anche i file candidate (dataset.yml,
    sql/clean.sql, sql/mart.sql, README.md, notes.md).

    Con --slug <nome>: fissa lo slug invece di usare l'auto-generazione dall'URL.

    Con --run (alias -r): dopo lo scaffold esegue anche il run raw.
    """
    if run:
        scaffold = True

    if slug is not None:
        import re

        if not re.match(r"^[a-z0-9-]+$", slug):
            typer.echo(
                "error: --slug deve contenere solo lettere minuscole, numeri e trattini",
                err=True,
            )
            raise typer.Exit(code=1)

    result = scout_url(
        url,
        timeout=timeout,
        scaffold=scaffold,
        run_raw=run,
        json_output=json_output,
        slug=slug,
    )

    if json_output and result is not None:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False))


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
        typer.echo(json.dumps(asdict(result), indent=2, ensure_ascii=False, default=str))
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
