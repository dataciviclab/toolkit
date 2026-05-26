"""Generazione completa di un candidate dataset: YAML, SQL, README, notes.

Dipende da:
  scaffold/clean.py   → propose_clean_read (serializza clean.read)
  scaffold/sources.py → slugify, infer_filename, block_*
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from toolkit.scaffold.clean import generate_clean_sql
from toolkit.scaffold.sources import block_ckan, block_http_file, block_links, block_sdmx, infer_filename


def _format_years(years: list[int]) -> str:
    if len(years) <= 4:
        return "[" + ", ".join(str(y) for y in years) + "]"
    return f"[{years[0]}..{years[-1]}]"


def _serialize_clean_read(clean_read: dict[str, Any]) -> list[str]:
    """Serializza clean.read come righe YAML (senza intestazione clean:)."""
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


# ---------------------------------------------------------------------------
# SQL generation
# ---------------------------------------------------------------------------


def suggest_validation(profile: dict[str, Any]) -> dict[str, Any]:
    """Suggerisce validation rules da inserire in dataset.yml."""
    norm_cols = profile.get("columns_norm") or profile.get("columns_raw") or []
    row_count = profile.get("row_count", 0)
    validation: dict[str, Any] = {}
    clean_val: dict[str, Any] = {}
    if row_count:
        clean_val["min_rows"] = max(1, int(row_count * 0.5))
    if norm_cols:
        clean_val["required_columns"] = norm_cols[:5]
    if clean_val:
        validation["clean"] = {"validate": clean_val}
    mart_val: dict[str, Any] = {}
    if row_count:
        mart_val["min_rows"] = max(1, int(row_count * 0.5))
    if mart_val:
        validation["mart"] = {"validate": mart_val}
    return validation


def _has_year_column(columns: list[dict[str, Any]] | list[str]) -> bool:
    year_keywords = ["anno", "year", "periodo", "period", "data", "date", "mese", "month"]
    for col in columns:
        name = col if isinstance(col, str) else col.get("name", "")
        if any(kw in name.lower() for kw in year_keywords):
            return True
    return False


def _has_region_column(columns: list[dict[str, Any]] | list[str]) -> bool:
    region_keywords = ["regione", "region", "provincia", "province", "comune", "municip", "area", "territorio"]
    for col in columns:
        name = col if isinstance(col, str) else col.get("name", "")
        if any(kw in name.lower() for kw in region_keywords):
            return True
    return False


def _has_numeric_column(columns: list[dict[str, Any]] | list[str], profile: dict[str, Any]) -> bool:
    mapping = profile.get("mapping_suggestions") or {}
    for col in columns:
        name = col if isinstance(col, str) else col.get("name", "")
        spec = mapping.get(name) or {}
        if spec.get("type") in ("integer", "float", "double", "bigint", "decimal", "int"):
            return True
        if isinstance(col, dict) and col.get("type") in ("integer", "float", "double", "int"):
            return True
    return False


def suggest_clean_sql(columns: list[dict[str, Any]] | list[str], profile: dict[str, Any]) -> str:
    """Genera clean.sql con TRY_CAST suggeriti basati sul profilo."""
    if columns and isinstance(columns[0], dict):
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(columns)]
    else:
        col_names = list(columns) if columns else []
    if not col_names:
        return "-- ATTENZIONE: profiling non ha rilevato colonne.\nSELECT 1 AS placeholder FROM raw_input\n"
    mapping = profile.get("mapping_suggestions") or {}
    lines = ["-- Auto-generated. Personalizza le trasformazioni.", "SELECT"]
    select_parts = []
    for name in col_names:
        spec = mapping.get(name) or {}
        raw_type = spec.get("type", "text") if isinstance(spec, dict) else "text"
        if raw_type in ("integer", "bigint", "int"):
            select_parts.append(f'  TRY_CAST("{name}" AS BIGINT) AS "{name}"')
        elif raw_type in ("float", "double", "decimal"):
            select_parts.append(f'  TRY_CAST("{name}" AS DOUBLE) AS "{name}"')
        elif raw_type in ("date",):
            select_parts.append(f'  TRY_CAST("{name}" AS DATE) AS "{name}"')
        else:
            select_parts.append(f'  trim("{name}")')
    lines.append(",\n".join(select_parts))
    lines.append("FROM raw_input")
    return "\n".join(lines) + "\n"


def _find_matching_column(col_names: list[str], keywords: list[str]) -> str | None:
    """Trova la prima colonna in col_names che contiene uno dei keywords (case-insensitive)."""
    for col in col_names:
        if any(kw in col.lower() for kw in keywords):
            return col
    return None


def suggest_mart_sql(columns: list[dict[str, Any]] | list[str], profile: dict[str, Any]) -> str:
    """Genera mart.sql con aggregazione di base."""
    if columns and isinstance(columns[0], dict):
        col_names = [c.get("name", f"col{i}") for i, c in enumerate(columns)]
    else:
        col_names = list(columns) if columns else []
    if not col_names:
        return "-- Default mart: SELECT * FROM clean.\nSELECT * FROM clean\n"
    has_year = _has_year_column(col_names)
    has_region = _has_region_column(col_names)
    has_numeric = _has_numeric_column(col_names, profile)
    if has_year and has_numeric:
        year_keywords = ["anno", "year", "periodo", "period"]
        measure_keywords = ["importo", "ammontare", "valore", "costo", "spesa", "gettito", "reddito", "canone", "prezzo", "tariffa"]
        mapping = profile.get("mapping_suggestions") or {}
        numeric_col = None

        # 1. Cerca colonna con keyword misura (evita ID)
        for name in col_names:
            is_year_col = any(kw in name.lower() for kw in year_keywords)
            spec = mapping.get(name) or {}
            if is_year_col:
                continue
            if spec.get("type") in ("integer", "float", "double", "bigint", "decimal", "int"):
                if any(kw in name.lower() for kw in measure_keywords):
                    numeric_col = name
                    break

        # 2. Fallback: primo numerico non-anno
        if numeric_col is None:
            for name in col_names:
                is_year_col = any(kw in name.lower() for kw in year_keywords)
                spec = mapping.get(name) or {}
                if is_year_col:
                    continue
                if spec.get("type") in ("integer", "float", "double", "bigint", "decimal", "int"):
                    numeric_col = name
                    break

        # 3. Fallback estremo: primo numerico
        if numeric_col is None:
            for name in col_names:
                spec = mapping.get(name) or {}
                if spec.get("type") in ("integer", "float", "double", "bigint", "decimal", "int"):
                    numeric_col = name
                    break
        if numeric_col:
            group_cols = [c for c in col_names if c != numeric_col]
            group_expr = ", ".join(f'"{c}"' for c in group_cols) if group_cols else ""
            if group_expr:
                group_list = ", ".join(group_cols)
                return (
                    f"-- Aggregazione su {group_list}\n"
                    f"SELECT\n"
                    f"  {group_expr},\n"
                    f'  SUM("{numeric_col}") AS totale_{numeric_col}\n'
                    f"FROM clean\n"
                    f"GROUP BY {group_expr}\n"
                    f"ORDER BY {group_expr}\n"
                )
    if has_year:
        year_col = _find_matching_column(col_names, ["anno", "year", "periodo", "period"])
        if year_col is None:
            year_col = "anno"
        return (
            f'-- Conteggio record per anno\n'
            f'SELECT\n'
            f'  "{year_col}" AS year,\n'
            f'  COUNT(*) AS record_count\n'
            f'FROM clean\n'
            f'GROUP BY "{year_col}"\n'
            f'ORDER BY "{year_col}"\n'
        )
    if has_region:
        region_col = _find_matching_column(col_names, ["regione", "region", "provincia", "province", "comune"])
        if region_col is None:
            region_col = "regione"
        return (
            f'-- Conteggio record per regione\n'
            f'SELECT\n'
            f'  "{region_col}" AS {region_col},\n'
            f'  COUNT(*) AS record_count\n'
            f'FROM clean\n'
            f'GROUP BY "{region_col}"\n'
            f'ORDER BY "{region_col}"\n'
        )
    return "-- Default mart: SELECT * FROM clean.\n-- Personalizza per aggregazioni.\nSELECT * FROM clean\n"


# ---------------------------------------------------------------------------
# Full scaffold orchestration
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
    """Genera tutti i file di un candidate dataset.

    Returns dict: {filename: content} con dataset.yml, sql/clean.sql,
                  sql/mart.sql, README.md, notes.md.
    """
    years = inferred_years or [2024]
    source_type = probe_result.get("source_type", "file")
    final_url = probe_result["final_url"]

    yml_lines: list[str] = [
        "# Auto-generated by toolkit",
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
        yml_lines.extend(block_ckan(probe_result["ckan_resources"], portal_base))
    elif source_type == "sdmx":
        yml_lines.extend(block_sdmx(probe_result.get("sdmx_info"), final_url))
    elif source_type == "html" and probe_result.get("candidate_links"):
        yml_lines.extend(block_links(probe_result["candidate_links"]))
    else:
        fname = infer_filename(final_url, slug)
        yml_lines.extend(block_http_file(final_url, slug, fname))

    # clean section (read + sql + validate)
    yml_lines.append("")
    yml_lines.append("clean:")
    if clean_read:
        yml_lines.extend(_serialize_clean_read(clean_read))
    yml_lines.append('  sql: "sql/clean.sql"')

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

    yml_lines.append("")
    yml_lines.append("mart:")
    yml_lines.append("  tables:")
    yml_lines.append(f'    - name: "{slug}"')
    yml_lines.append('      sql: "sql/mart.sql"')

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
        first_year = years[0]
        clean_sql = generate_clean_sql(profile, slug, first_year)
        norm_cols = profile.get("columns_norm") or profile.get("columns_raw") or profile.get("columns") or []
        mart_sql = suggest_mart_sql(norm_cols, profile)
    else:
        mart_sql = "-- Default mart: SELECT * FROM clean.\nSELECT * FROM clean\n"
        clean_sql = "-- ATTENZIONE: profiling non ha rilevato colonne.\nSELECT 1 AS placeholder FROM raw_input\n"

    if profile:
        topics = probe_result.get("inferred_topics")
        granularity = probe_result.get("inferred_granularity")
        notes = _generate_notes(granularity, topics)
    else:
        notes = _generate_notes(None, None)

    result: dict[str, str] = {
        "dataset.yml": "\n".join(yml_lines) + "\n",
        "sql/clean.sql": clean_sql,
        "sql/mart.sql": mart_sql,
        "README.md": _generate_readme(slug, final_url),
        "notes.md": notes,
    }

    return result
