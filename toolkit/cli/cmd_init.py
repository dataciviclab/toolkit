"""Top-level `init` command — bootstrap a dataset from URL or config.

Usage:
    toolkit init --url <URL>                     # scout + generate dataset.yml
    toolkit init --config <dataset.yml>           # run raw + scaffold (existing)
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
    return ".csv"


def _slugify(url: str) -> str:
    """Generate a dataset slug from a URL."""
    parsed = urlparse(url)
    stem = Path(parsed.path).stem or "dataset"
    slug = re.sub(r"[^a-z0-9_]", "_", stem.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        slug = "dataset"
    short_hash = uuid.uuid5(uuid.NAMESPACE_URL, url).hex[:6]
    return f"{slug}_{short_hash}"


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


# ---------------------------------------------------------------------------
# Scout
# ---------------------------------------------------------------------------


def _scout(url: str, *, timeout: int = 60) -> None:
    """Download sample, profile, generate dataset.yml."""
    slug = _slugify(url)
    tmp_dir = Path("/tmp")
    tmp_name = f"scout_{slug}_{uuid.uuid4().hex[:8]}"

    # 1. Download sample (primi 1MB via Range header)
    typer.echo(f"Downloading sample from {url}...")
    client = HttpClient(timeout=timeout)
    result = client.get(url, headers={"Range": f"bytes=0-{_SAMPLE_SIZE - 1}"})
    if not result.is_ok or result.response is None:
        typer.echo(f"error: failed to fetch {url}: {result.err}", err=True)
        raise typer.Exit(code=1)

    resp = result.response
    if resp.status_code >= 400:
        typer.echo(f"error: HTTP {resp.status_code} for {url}", err=True)
        raise typer.Exit(code=1)

    ct = (resp.headers.get("Content-Type") or "").lower()
    if "html" in ct:
        typer.echo(f"error: URL returned HTML (Content-Type: {ct}), not a data file", err=True)
        typer.echo("  Controlla che l'URL punti direttamente a un file CSV/XLSX/JSON.", err=True)
        raise typer.Exit(code=1)

    # Cap difensivo: anche se server ignora Range e risponde 200, tronca
    content = resp.content[:_SAMPLE_SIZE]
    ext = _infer_ext(url, ct)
    sample_path = tmp_dir / f"{tmp_name}{ext}"
    sample_path.write_bytes(content)
    typer.echo(f"  Saved {len(content)} bytes to {sample_path}")

    # 2. Sniff + Profile
    from toolkit.profile.raw import sniff_source_file, profile_with_read_cfg

    sniff_hints = sniff_source_file(sample_path)
    typer.echo(f"  Encoding: {sniff_hints.get('encoding_suggested')}")
    typer.echo(f"  Delimiter: {sniff_hints.get('delim_suggested')}")
    typer.echo(f"  Columns: {sniff_hints.get('columns_preview')}")

    # 3. Build read config (robust solo se sniff lo suggerisce)
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

    # 5. Propose clean.read via scaffold canonico
    from toolkit.scaffold.clean import propose_clean_read

    # Merge sniff hints into profile for propose_clean_read
    enriched = dict(profile)
    for k in ("encoding_suggested", "delim_suggested", "decimal_suggested",
              "skip_suggested", "header_line", "true_header_line", "robust_read_suggested"):
        if sniff_hints.get(k) is not None:
            enriched[k] = sniff_hints[k]

    clean_read = propose_clean_read(enriched)
    columns_count = len(clean_read.get("columns") or profile.get("columns_raw") or profile.get("columns_norm") or [])

    # 6. Genera scaffold
    out_dir = Path(slug)
    out_dir.mkdir(parents=True, exist_ok=True)

    _generate_dataset_yml(url, slug, clean_read, out_dir)
    _generate_clean_sql(out_dir / "sql" / "clean.sql", profile)
    _generate_mart_sql(out_dir / "sql" / "mart.sql")
    _generate_readme(out_dir / "README.md", slug, url)
    _generate_notes(out_dir / "notes.md")
    (out_dir / "notebooks").mkdir(exist_ok=True)

    typer.echo(f"\nDataset YAML generated: {out_dir / 'dataset.yml'}")
    typer.echo(f"  clean.read.columns: {columns_count} columns")
    typer.echo("  sql/clean.sql:      generated")
    typer.echo("  sql/mart.sql:       generated (default)")
    typer.echo("  README.md, notes.md, notebooks/: created")
    typer.echo(f"Next: toolkit run all --config {out_dir / 'dataset.yml'}")

    # 7. Cleanup
    sample_path.unlink(missing_ok=True)


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


# ---------------------------------------------------------------------------
# Scaffold generators
# ---------------------------------------------------------------------------


def _generate_dataset_yml(
    url: str,
    slug: str,
    clean_read: dict[str, Any],
    out_dir: Path,
) -> None:
    """Generate dataset.yml YAML content."""
    fname = _infer_filename(url, slug)
    lines: list[str] = []
    lines.append("# Auto-generated by toolkit init --url")
    lines.append("# Review and adjust before running")
    lines.append("")
    lines.append("root: \"../../out\"")
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
    lines.append(f'        filename: "{fname}"')
    lines.append("      primary: true")
    lines.append("")
    lines.append("clean:")
    lines.append("  read:")

    # Serialize clean.read (from propose_clean_read)
    if "delim" in clean_read:
        lines.append(f'    delim: "{clean_read["delim"]}"')
    if "encoding" in clean_read:
        lines.append(f'    encoding: "{clean_read["encoding"]}"')
    if "decimal" in clean_read:
        lines.append(f'    decimal: "{clean_read["decimal"]}"')
    if "header" in clean_read:
        lines.append(f"    header: {str(clean_read['header']).lower()}")
    if clean_read.get("skip", 0) > 0:
        lines.append(f"    skip: {clean_read['skip']}")
    if clean_read.get("strict_mode") is False:
        lines.append("    strict_mode: false")
    if clean_read.get("null_padding") is True:
        lines.append("    null_padding: true")
    if clean_read.get("ignore_errors") is True:
        lines.append("    ignore_errors: true")

    columns = clean_read.get("columns")
    if columns:
        lines.append("    columns:")
        for col_name, col_type in columns.items():
            lines.append(f'      "{col_name}": "{col_type}"')
    else:
        lines.append("    # columns: auto-detected from header")

    lines.append("")
    lines.append('  sql: "sql/clean.sql"')
    lines.append("")
    lines.append("mart:")
    lines.append("  tables:")
    lines.append(f'    - name: "{slug}"')
    lines.append('      sql: "sql/mart.sql"')
    lines.append("")

    (out_dir / "dataset.yml").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _infer_filename(url: str, slug: str) -> str:
    """Infer filename from URL."""
    path = urlparse(url).path
    if path.endswith(".php"):
        return Path(path).stem + ".csv"
    name = Path(path).name
    return name or f"{slug}.csv"


def _generate_clean_sql(path: Path, profile: dict[str, Any]) -> None:
    """Generate clean.sql placeholder from profile."""
    raw_cols = profile.get("columns_raw") or profile.get("columns_norm") or []
    if not raw_cols:
        path.write_text(
            "-- ATTENZIONE: profiling non ha rilevato colonne.\n"
            "-- Possibili cause: file vuoto, formato non supportato, encoding errato.\n"
            "-- Rivedi il file e compila manualmente le colonne.\n",
            encoding="utf-8",
        )
        return
    col_names = [f'    "{col}"' for col in raw_cols]
    sql = (
        "-- Auto-generated by toolkit init --url\n"
        "-- Personalizza le trasformazioni qui sotto.\n"
        "SELECT\n"
        + ",\n".join(col_names)
        + "\nFROM raw_input\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(sql, encoding="utf-8")


def _generate_mart_sql(path: Path) -> None:
    """Generate mart.sql placeholder."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "-- Default mart: SELECT * FROM clean.\n"
        "-- Personalizza per aggregazioni.\n"
        "SELECT * FROM clean\n",
        encoding="utf-8",
    )


def _generate_readme(path: Path, slug: str, url: str) -> None:
    """Generate README.md placeholder."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
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


def _generate_notes(path: Path) -> None:
    """Generate notes.md placeholder."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "## Tecnico\n\n-\n\n"
        "## Analitico\n\n-\n\n"
        "## Cautele\n\n"
        "- La serie storica e omogenea su tutti gli anni?\n"
        "- Ci sono discontinuita dichiarate dalla fonte?\n"
        "- I valori nulli sono zero reale o dato mancante?\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


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
