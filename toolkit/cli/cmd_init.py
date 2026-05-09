"""Top-level `init` command — bootstrap a dataset from URL or config.

Usage:
    toolkit init --url <URL>                     # scout + generate dataset.yml
    toolkit init --config <dataset.yml>           # run raw + scaffold (existing)
    toolkit run init --config <dataset.yml>       # same as above
"""

from __future__ import annotations

import logging
import re
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import typer

from lab_connectors.http import HttpClient

from toolkit.cli.cmd_run import run_init as _run_init

logger = logging.getLogger("toolkit.cli.init")

_SAMPLE_SIZE = 1024 * 1024  # 1MB sample


def _infer_ext(url: str, content_type: str) -> str:
    """Infer file extension from URL or Content-Type."""
    url_ext = Path(urlparse(url).path).suffix.lower()
    if url_ext and url_ext not in (".php", ".asp", ".aspx", ".jsp"):
        return url_ext
    ct = content_type.lower()
    if "csv" in ct:
        return ".csv"
    if "json" in ct:
        return ".json"
    if "spreadsheetml" in ct or "excel" in ct:
        return ".xlsx"
    if "xml" in ct:
        return ".xml"
    return ".csv"  # fallback


def _slugify(url: str) -> str:
    """Generate a dataset slug from a URL."""
    parsed = urlparse(url)
    stem = Path(parsed.path).stem or "dataset"
    slug = re.sub(r"[^a-z0-9_]", "_", stem.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        slug = "dataset"
    # Add short hash from URL to reduce collision risk
    short_hash = uuid.uuid5(uuid.NAMESPACE_URL, url).hex[:6]
    return f"{slug}_{short_hash}"


def _scout(url: str, *, timeout: int = 60) -> None:
    """Download sample, profile, generate dataset.yml."""
    slug = _slugify(url)
    tmp_dir = Path("/tmp")
    tmp_name = f"scout_{slug}_{uuid.uuid4().hex[:8]}"

    # 1. Download sample
    typer.echo(f"Downloading sample from {url}...")
    client = HttpClient(timeout=timeout)
    result = client.get(url)
    if not result.is_ok or result.response is None:
        typer.echo(f"error: failed to fetch {url}: {result.err}", err=True)
        raise typer.Exit(code=1)

    resp = result.response
    if resp.status_code >= 400:
        typer.echo(f"error: HTTP {resp.status_code} for {url}", err=True)
        raise typer.Exit(code=1)

    # Validate content-type to avoid profiling HTML pages as CSV
    ct = (resp.headers.get("Content-Type") or "").lower()
    if "html" in ct:
        typer.echo(f"error: URL returned HTML (Content-Type: {ct}), not a data file", err=True)
        typer.echo(f"  Controlla che l'URL punti direttamente a un file CSV/XLSX/JSON.", err=True)
        raise typer.Exit(code=1)
    if ct and "csv" not in ct and "json" not in ct and "xml" not in ct and "octet-stream" not in ct and "text/plain" not in ct and not ct.startswith("application/"):
        typer.echo(f"warning: Content-Type inaspettato: {ct}", err=True)

    content = resp.content[:_SAMPLE_SIZE]
    # Infer extension from content-type if URL has none or has .php
    ext = _infer_ext(url, ct)
    sample_path = tmp_dir / f"{tmp_name}{ext}"
    sample_path.write_bytes(content)
    typer.echo(f"  Saved {len(content)} bytes to {sample_path}")

    # 2. Sniff
    from toolkit.profile.raw import sniff_source_file, profile_with_read_cfg

    sniff_hints = sniff_source_file(sample_path)
    typer.echo(f"  Encoding: {sniff_hints.get('encoding_suggested')}")
    typer.echo(f"  Delimiter: {sniff_hints.get('delim_suggested')}")
    typer.echo(f"  Columns: {sniff_hints.get('columns_preview')}")

    # 3. Profile
    read_cfg: dict[str, Any] = {}
    if sniff_hints.get("encoding_suggested"):
        read_cfg["encoding"] = sniff_hints["encoding_suggested"]
    if sniff_hints.get("delim_suggested"):
        read_cfg["delim"] = sniff_hints["delim_suggested"]
    if sniff_hints.get("decimal_suggested"):
        read_cfg["decimal"] = sniff_hints["decimal_suggested"]
    if sniff_hints.get("skip_suggested", 0) > 0:
        read_cfg["skip"] = sniff_hints["skip_suggested"]

    # Use robust preset: strict_mode=false, null_padding=true, ignore_errors=true
    from toolkit.core.csv_read import robust_preset
    read_cfg = robust_preset(read_cfg)

    profile = profile_with_read_cfg(sample_path, sniff_hints, read_cfg)

    # 4. Build columns spec for clean.read.
    #    Se profiling restituisce 0 colonne (skip sbagliato, CSV anomalo),
    #    riprova con skip incrementale (0..5) per trovare l'header reale.
    columns_spec, sniff_hints = _resolve_columns(profile, sniff_hints, read_cfg, sample_path)

    # 5. Generate dataset.yml
    dataset_yml = _generate_dataset_yml(url, slug, sniff_hints, read_cfg, columns_spec)
    out_dir = Path(slug)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "dataset.yml").write_text(dataset_yml, encoding="utf-8")

    # 6. Generate sql/clean.sql placeholder
    sql_dir = out_dir / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)
    _generate_clean_sql(sql_dir / "clean.sql", columns_spec)

    # 7. Generate sql/mart.sql placeholder (SELECT * FROM clean per run all)
    (sql_dir / "mart.sql").write_text("-- Default mart: SELECT * FROM clean.\n-- Personalizza per aggregazioni.\nSELECT * FROM clean\n")

    # 8. Generate candidate scaffold (README.md, notes.md, notebooks/)
    (out_dir / "notebooks").mkdir(exist_ok=True)
    (out_dir / "README.md").write_text(
        f"# {slug}\n\n"
        f"Fonte: {url}\n\n"
        "## Domanda\n\n-\n\n"
        "## Dataset\n\n-\n\n"
        "## Perche vale la pena testarlo\n\n-\n\n"
        "## Output minimo atteso\n\n-\n\n"
        "## Criterio di promozione\n\n-\n\n"
        "## Stato\n\n- intake\n\n"
        "## Prossimo passo\n\n- run init --url poi run all\n",
        encoding="utf-8",
    )
    (out_dir / "notes.md").write_text(
        "## Tecnico\n\n-\n\n## Analitico\n\n-\n\n## Cautele\n\n- La serie storica e omogenea su tutti gli anni?\n"
        "- Ci sono discontinuita dichiarate dalla fonte?\n- I valori nulli sono zero reale o dato mancante?\n",
        encoding="utf-8",
    )

    typer.echo(f"\nDataset YAML generated: {out_dir / 'dataset.yml'}")
    typer.echo(f"  sql/clean.sql:      generated ({len(columns_spec)} columns)")
    typer.echo(f"  sql/mart.sql:       generated (default: SELECT * FROM clean)")
    typer.echo(f"  README.md:          generated")
    typer.echo(f"  notes.md:           generated")
    typer.echo(f"  notebooks/:         created (empty)")
    typer.echo(f"Next: toolkit run all --config {out_dir / 'dataset.yml'}")

    # 9. Cleanup temp
    sample_path.unlink(missing_ok=True)


def _resolve_columns(
    profile: dict[str, Any],
    sniff_hints: dict[str, Any],
    read_cfg: dict[str, Any],
    sample_path: Path,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """Build columns spec, retrying with incremental skip if empty."""
    from toolkit.profile.raw import profile_with_read_cfg

    def _build(cols_raw: list[str], types: list[str]) -> list[dict[str, str]]:
        spec = []
        for i, col in enumerate(cols_raw):
            dtype = types[i] if i < len(types) else "VARCHAR"
            spec.append({"name": col, "type": _map_duckdb_to_clean(dtype)})
        return spec

    raw_cols = profile.get("columns_raw") or profile.get("columns_norm") or []
    duckdb_types = profile.get("duckdb_types") or []
    columns_spec = _build(raw_cols, duckdb_types)

    # Se 0 colonne, riprova con skip 0..5
    if not columns_spec:
        for try_skip in range(6):
            if try_skip == sniff_hints.get("skip_suggested", 0):
                continue
            retry_cfg = dict(read_cfg)
            retry_cfg["skip"] = try_skip
            retry_profile = profile_with_read_cfg(sample_path, sniff_hints, retry_cfg)
            retry_cols = retry_profile.get("columns_raw") or retry_profile.get("columns_norm") or []
            retry_types = retry_profile.get("duckdb_types") or []
            if len(retry_cols) >= 2:
                sniff_hints["skip_suggested"] = try_skip
                sniff_hints["columns_preview"] = retry_cols
                typer.echo(f"  Retry with skip={try_skip}: {len(retry_cols)} columns found")
                columns_spec = _build(retry_cols, retry_types)
                break

    return columns_spec, sniff_hints


def _generate_clean_sql(path: Path, columns_spec: list[dict[str, str]]) -> None:
    """Generate clean.sql placeholder with explicit column projections."""
    if not columns_spec:
        path.write_text(
            "-- ATTENZIONE: profiling non ha rilevato colonne.\n"
            "-- Possibili cause: file vuoto, formato non supportato, encoding errato.\n"
            "-- Rivedi il file e compila manualmente le colonne.\n",
            encoding="utf-8",
        )
        return
    col_names = [f'    "{col["name"]}"' for col in columns_spec]
    sql_lines = [
        "-- Auto-generated by toolkit init --url",
        "-- Personalizza le trasformazioni qui sotto.",
        "SELECT",
        ",\n".join(col_names),
        "FROM raw_input",
        "",
    ]
    path.write_text("\n".join(sql_lines) + "\n", encoding="utf-8")


def _map_duckdb_to_clean(dtype: str) -> str:
    """Map DuckDB type to clean schema type."""
    dtype_lower = dtype.lower()
    if dtype_lower in ("integer", "int", "int32", "int64", "bigint"):
        return "integer"
    if dtype_lower in ("float", "double", "real", "decimal"):
        return "float"
    if dtype_lower in ("date", "timestamp", "datetime"):
        return "date"
    return "text"


def _generate_dataset_yml(
    url: str,
    slug: str,
    sniff_hints: dict[str, Any],
    read_cfg: dict[str, Any],
    columns_spec: list[dict[str, str]],
) -> str:
    """Generate dataset.yml YAML content."""
    lines: list[str] = []
    lines.append("# Auto-generated by toolkit init --url")
    lines.append("# Review and adjust before running")
    lines.append("")
    lines.append(f'root: "../../out"')
    lines.append("schema_version: 1")
    lines.append("")
    lines.append("dataset:")
    lines.append(f'  name: "{slug}"')
    lines.append("  years: [2024]  # TODO: adjust")
    lines.append("")
    lines.append("raw:")
    lines.append("  output_policy: overwrite")
    lines.append("  sources:")
    lines.append(f'    - name: "{slug}_source"')
    lines.append('      type: "http_file"')
    lines.append("      args:")
    lines.append(f'        url: "{url}"')
    # Infer filename: use proper extension from sniffed content
    url_path = urlparse(url).path
    if url_path.endswith(".php"):
        fname = Path(url_path).stem + ".csv"
    else:
        fname = Path(url_path).name or f"{slug}.csv"
    lines.append(f'        filename: "{fname}"')
    lines.append("      primary: true")
    if read_cfg:
        lines.append("")
        lines.append("  read:")
        if read_cfg.get("encoding"):
            lines.append(f'    encoding: "{read_cfg["encoding"]}"')
        if read_cfg.get("delim"):
            lines.append(f'    delim: "{read_cfg["delim"]}"')
        if read_cfg.get("decimal"):
            lines.append(f'    decimal: "{read_cfg["decimal"]}"')
        if read_cfg.get("skip", 0) > 0:
            lines.append(f'    skip: {read_cfg["skip"]}')
        if read_cfg.get("strict_mode") is False:
            lines.append("    strict_mode: false")
        if read_cfg.get("ignore_errors") is True:
            lines.append("    ignore_errors: true")
        if read_cfg.get("null_padding") is True:
            lines.append("    null_padding: true")

    if columns_spec:
        lines.append("")
        lines.append("clean:")
        lines.append("  read:")
        lines.append("    columns:")
        for col in columns_spec:
            lines.append(f'      - name: "{col["name"]}"')
            if col["type"]:
                lines.append(f'        type: {col["type"]}')
        lines.append("")
        lines.append("  sql: sql/clean.sql")

    lines.append("")
    lines.append("mart:")
    lines.append("  tables:")
    lines.append(f'    - name: "{slug}"')
    lines.append("      sql: sql/mart.sql")

    lines.append("")
    return "\n".join(lines)


def init(
    config: str | None = typer.Option(None, "--config", "-c", help="Path to dataset.yml"),
    url: str | None = typer.Option(None, "--url", "-u", help="Download, profile and scaffold from URL"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single dataset year"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated dataset years"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print plan without executing"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Bootstrap candidate: prepara dataset.yml e scaffold SQL.

    Con --url: scarica un sample, profila encoding/delim/colonne, genera
    dataset.yml completo con sql/clean.sql e sql/mart.sql placeholder.
    Pronto per toolkit run all --config dataset.yml.

    Con --config: come run init (run raw + scaffold clean.sql).
    Nota: run init e' deprecato, usa toolkit init --config.
    """
    if url and config:
        typer.echo("error: specificare --url o --config, non entrambi", err=True)
        raise typer.Exit(code=1)

    if url:
        _scout(url)
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
