"""Helper per URL scout — slug, estensione, filename, scaffold YAML.

Tutta logica pura (no HTTP, no I/O). Condivisa tra CLI, MCP tools e SO.

Schema a strati:
  toolkit.scout.http      → trasporto HTTP
  toolkit.scout.probe     → routing + discovery
  toolkit.scout.infer     → inferenze pure
  toolkit.scout.scaffold  → scaffold YAML (QUI)
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Slug, extension, filename helpers
# ---------------------------------------------------------------------------


def slugify(url: str) -> str:
    """Genera uno slug stabile e univoco per un URL.

    Usa uuid5 (namespace URL) per garantire unicità: stesso URL
    produce sempre lo stesso slug. Utile come dataset name/dir.
    """
    parsed = urlparse(url)
    stem = Path(parsed.path).stem or "dataset"
    slug = re.sub(r"[^a-z0-9_]", "_", stem.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        slug = "dataset"
    short_hash = uuid.uuid5(uuid.NAMESPACE_URL, url).hex[:6]
    return f"{slug}_{short_hash}"


def infer_ext(url: str, content_type: str) -> str:
    """Inferisce estensione file da URL o Content-Type."""
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


def infer_filename(url: str, slug: str) -> str:
    """Inferisce nome file dall'URL."""
    path = urlparse(url).path
    if path.endswith(".php"):
        return Path(path).stem + ".csv"
    name = Path(path).name
    return name or f"{slug}.csv"


# ---------------------------------------------------------------------------
# Source block generators (per tipo)
# ---------------------------------------------------------------------------


def _make_source_name(link: str) -> str:
    stem = Path(urlparse(link).path).stem
    return re.sub(r"[^a-z0-9_]", "_", (stem or "resource").lower())


def _generate_raw_sources_block_http_file(url: str, slug: str, fname: str | None = None) -> list[str]:
    """Genera il blocco YAML raw.sources per http_file (URL diretto)."""
    parsed = urlparse(url)
    if fname is None:
        fname = Path(parsed.path).name or f"{slug}.csv"
    return [
        f'    - name: "{slug}_source"',
        '      type: "http_file"',
        "      args:",
        f'        url: "{url}"',
        f'        filename: "{fname}"',
        "      primary: true",
    ]


def _generate_raw_sources_block_ckan(
    resources: list[dict[str, Any]],
    portal_url: str,
) -> list[str]:
    """Genera il blocco YAML raw.sources per risorse CKAN."""
    parsed = urlparse(portal_url)
    portal_base = f"{parsed.scheme}://{parsed.netloc}"
    lines: list[str] = []
    for res in resources:
        res_name = re.sub(r"[^a-z0-9_]", "_", (res["name"] or "resource").lower())
        res_url = res["url"]
        fmt = res["format"]
        fname = Path(urlparse(res_url).path).name or f"{res_name}.{fmt}"
        lines.append(f'    - name: "{res_name}"')
        lines.append('      type: "ckan"')
        lines.append("      args:")
        lines.append(f'        portal_url: "{portal_base}"')
        lines.append(f'        resource_id: "{res.get("id") or ""}"')
        lines.append(f'        filename: "{fname}"')
        lines.append("      primary: true")
    return lines


def _generate_raw_sources_block_links(links: list[str]) -> list[str]:
    """Genera il blocco YAML raw.sources per link candidati (HTML page)."""
    lines: list[str] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        link_name = _make_source_name(link)
        fname = Path(urlparse(link).path).name
        lines.append(f'    - name: "{link_name}"')
        lines.append('      type: "http_file"')
        lines.append("      args:")
        lines.append(f'        url: "{link}"')
        if fname:
            lines.append(f'        filename: "{fname}"')
        lines.append("      primary: true")
    return lines


def _generate_raw_sources_block_sdmx(sdmx_info: dict[str, Any] | None, url: str) -> list[str]:
    """Genera il blocco YAML raw.sources per endpoint SDMX."""
    if sdmx_info and sdmx_info.get("flow_id"):
        return [
            f'    - name: "sdmx_{sdmx_info["flow_id"]}"',
            '      type: "sdmx"',
            "      args:",
            f'        endpoint: "{url}"',
            f'        flow: "{sdmx_info["flow_id"]}"',
            "      primary: true",
        ]
    # Fallback: http_file con l'URL SDMX
    return _generate_raw_sources_block_http_file(url, "sdmx")


# ---------------------------------------------------------------------------
# Backward compat: legacy raw sources block
# ---------------------------------------------------------------------------


def _generate_raw_sources_block(url: str, slug: str, fname: str | None = None) -> list[str]:
    """Legacy: generava sempre http_file. Mantenuto per retrocompat."""
    return _generate_raw_sources_block_http_file(url, slug, fname)


# ---------------------------------------------------------------------------
# Deprecated: generate_yaml_scaffold (conservata per test, non usata dalla CLI)
# Usa generate_full_scaffold() o toolkit init --url per scaffold completi.


def generate_yaml_scaffold(
    probe_result: dict[str, Any],
    ckan_resources: list[dict[str, Any]] | None = None,
    candidate_links: list[str] | None = None,
    inferred_years: list[int] | None = None,
) -> str:
    """Genera scaffold YAML per un URL ispezionato.

    Args:
        probe_result: dict con almeno 'final_url'.
        ckan_resources: se presenti, genera source type ckan.
        candidate_links: se presenti (e no ckan), genera http_file per ogni link.
        inferred_years: lista anni suggeriti. Default [2024].
    """
    url = probe_result["final_url"]
    slug = slugify(url)
    years = inferred_years or [2024]

    lines = [
        "# Scaffold generato da scout-url --scaffold",
        "# Verifica e modifica prima di usare",
        "",
        'root: "../../out"',
        "schema_version: 1",
        "",
        "dataset:",
        f'  name: "{slug}"',
        "  years: " + _format_years(years),
        "",
        "raw:",
        "  output_policy: overwrite",
        "  sources:",
    ]

    if ckan_resources:
        parsed = urlparse(url)
        portal_base = f"{parsed.scheme}://{parsed.netloc}"
        lines.extend(_generate_raw_sources_block_ckan(ckan_resources, portal_base))
    elif candidate_links:
        lines.extend(_generate_raw_sources_block_links(candidate_links))
    else:
        lines.extend(_generate_raw_sources_block_http_file(url, slug))

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Nuovo: scaffold completo per init --url
# ---------------------------------------------------------------------------


def generate_full_scaffold(
    slug: str,
    probe_result: dict[str, Any],
    *,
    clean_read: dict[str, Any] | None = None,
    profile: dict[str, Any] | None = None,
    inferred_years: list[int] | None = None,
    validation_suggestions: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Genera scaffold completo: dataset.yml, sql/clean.sql, sql/mart.sql.

    Returns dict: {filename: content}
    """
    years = inferred_years or [2024]
    source_type = probe_result.get("source_type", "file")
    final_url = probe_result["final_url"]

    # --- dataset.yml ---
    yml_lines: list[str] = [
        "# Auto-generated by toolkit init --url",
        "# Review and adjust before running",
        "",
        'root: "../../out"',
        "schema_version: 1",
        "",
        "dataset:",
        f'  name: "{slug}"',
        "  years: " + _format_years(years),
        "",
        "raw:",
        "  output_policy: overwrite",
        "  sources:",
    ]

    if source_type == "ckan" and probe_result.get("ckan_resources"):
        parsed = urlparse(final_url)
        portal_base = f"{parsed.scheme}://{parsed.netloc}"
        yml_lines.extend(_generate_raw_sources_block_ckan(probe_result["ckan_resources"], portal_base))
    elif source_type == "sdmx":
        yml_lines.extend(_generate_raw_sources_block_sdmx(probe_result.get("sdmx_info"), final_url))
    elif source_type == "html" and probe_result.get("candidate_links"):
        yml_lines.extend(_generate_raw_sources_block_links(probe_result["candidate_links"]))
    else:
        fname = infer_filename(final_url, slug)
        yml_lines.extend(_generate_raw_sources_block_http_file(final_url, slug, fname))

    # clean section (read + sql + validate)
    yml_lines.append("")
    yml_lines.append("clean:")
    if clean_read:
        yml_lines.extend(_serialize_clean_read(clean_read))
    yml_lines.append('  sql: "sql/clean.sql"')

    # clean.validate (merged inside clean:)
    if validation_suggestions:
        cv = validation_suggestions.get("clean")
        if cv:
            validate_block = cv.get("validate", cv)
            if validate_block:
                yml_lines.append("  validate:")
                for key, val in validate_block.items():
                    if isinstance(val, list):
                        items = ", ".join(f'"{v}"' for v in val)
                        yml_lines.append(f'    {key}: [{items}]')
                    elif isinstance(val, bool):
                        yml_lines.append(f"    {key}: {str(val).lower()}")
                    else:
                        yml_lines.append(f"    {key}: {val}")

    # mart section (tables + validate)
    yml_lines.append("")
    yml_lines.append("mart:")
    yml_lines.append("  tables:")
    yml_lines.append(f'    - name: "{slug}"')
    yml_lines.append('      sql: "sql/mart.sql"')

    # mart.validate (merged inside mart:)
    if validation_suggestions:
        mv = validation_suggestions.get("mart")
        if mv:
            validate_block = mv.get("validate", mv)
            if validate_block:
                yml_lines.append("  validate:")
                for key, val in validate_block.items():
                    if isinstance(val, list):
                        items = ", ".join(f'"{v}"' for v in val)
                        yml_lines.append(f'    {key}: [{items}]')
                    elif isinstance(val, bool):
                        yml_lines.append(f"    {key}: {str(val).lower()}")
                    else:
                        yml_lines.append(f"    {key}: {val}")

    if profile:
        topics = probe_result.get("inferred_topics")
        granularity = probe_result.get("inferred_granularity")
        notes = _generate_notes(granularity, topics)
    else:
        notes = _generate_notes(None, None)

    return {
        "dataset.yml": "\n".join(yml_lines) + "\n",
        "sql/clean.sql": _generate_clean_sql(profile) if profile else _generate_clean_sql_fallback(),
        "sql/mart.sql": _generate_mart_sql(profile) if profile else _generate_mart_sql_fallback(),
        "README.md": _generate_readme(slug, final_url),
        "notes.md": notes,
    }


def _format_years(years: list[int]) -> str:
    if len(years) <= 4:
        return "[" + ", ".join(str(y) for y in years) + "]"
    return f"[{years[0]}..{years[-1]}]  # {len(years)} years"


def _serialize_clean_read(clean_read: dict[str, Any]) -> list[str]:
    """Serializza clean.read SENZA l'intestazione 'clean:' (aggiunta dopo)."""
    lines: list[str] = []
    lines.append("  read:")
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
    return lines


# ---------------------------------------------------------------------------
# SQL generators
# ---------------------------------------------------------------------------


def _generate_clean_sql(profile: dict[str, Any]) -> str:
    """Genera clean.sql da profilo raw."""
    from toolkit.scout.infer import suggest_clean_sql
    norm_cols = profile.get("columns_norm") or profile.get("columns_raw") or profile.get("columns") or []
    return suggest_clean_sql(norm_cols, profile)


def _generate_clean_sql_fallback() -> str:
    return (
        "-- ATTENZIONE: profiling non ha rilevato colonne.\n"
        "-- Possibili cause: file vuoto, formato non supportato, encoding errato.\n"
        "-- Rivedi il file e compila manualmente le colonne.\n"
        "SELECT 1 AS placeholder FROM raw_input\n"
    )


def _generate_mart_sql(profile: dict[str, Any]) -> str:
    """Genera mart.sql da profilo raw."""
    from toolkit.scout.infer import suggest_mart_sql
    norm_cols = profile.get("columns_norm") or profile.get("columns_raw") or profile.get("columns") or []
    return suggest_mart_sql(norm_cols, profile)


def _generate_mart_sql_fallback() -> str:
    return (
        "-- Default mart: SELECT * FROM clean.\n"
        "-- Personalizza per aggregazioni.\n"
        "SELECT * FROM clean\n"
    )


# ---------------------------------------------------------------------------
# README / notes generators
# ---------------------------------------------------------------------------


def _generate_readme(slug: str, url: str) -> str:
    return (
        f"# {slug}\n\n"
        f"Fonte: {url}\n\n"
        "## Domanda\n\n-\n\n"
        "## Dataset\n\n-\n\n"
        "## Perche vale la pena testarlo\n\n-\n\n"
        "## Output minimo atteso\n\n-\n\n"
        "## Criterio di promozione\n\n-\n\n"
        "## Stato\n\n- intake\n\n"
        "## Prossimo passo\n\n- run init --url poi run all\n"
    )


def _generate_notes(granularity: str | None, topics: list[dict[str, Any]] | None) -> str:
    lines: list[str] = ["## Tecnico\n\n-\n"]
    if granularity:
        lines.append(f"- Granularita rilevata: {granularity}\n")
    if topics:
        topic_names = ", ".join(t["topic"] for t in topics[:3])
        lines.append(f"- Topic suggeriti: {topic_names}\n")
    lines.append("\n## Analitico\n\n-\n\n")
    lines.append("## Cautele\n\n")
    lines.append("- La serie storica e omogenea su tutti gli anni?\n")
    lines.append("- Ci sono discontinuita dichiarate dalla fonte?\n")
    lines.append("- I valori nulli sono zero reale o dato mancante?\n")
    return "".join(lines)
