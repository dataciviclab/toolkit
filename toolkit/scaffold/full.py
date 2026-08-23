"""Generazione di un candidate dataset: YAML, SQL, README, notes.

Thin orchestrator: builds config dict → yaml_dumps(),
delegates SQL to clean.generate_clean_sql().
"""

from __future__ import annotations

from typing import Any

from toolkit.core.io import yaml_dumps
from toolkit.scaffold.clean import generate_clean_sql, propose_clean_read


def generate_full_scaffold(
    slug: str,
    probe_result: dict[str, Any],
    *,
    clean_read: dict[str, Any] | None = None,
    profile: dict[str, Any] | None = None,
    inferred_years: list[int] | None = None,
    validation_suggestions: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Generate all files for a candidate dataset.

    Returns {filename: content} with dataset.yml, sql/clean.sql,
    sql/mart.sql, README.md, notes.md.
    """
    safe_name = slug.replace("-", "_")
    source_type = probe_result.get("source_type", "file")
    years = inferred_years or [2024]
    final_url = probe_result.get("final_url", "")

    # Infer years from profile's anno column if available
    if inferred_years is None and profile:
        anno_col = _find_anno_col(profile)
        if anno_col:
            anno_values = profile.get("date_raw_values", {}).get(anno_col, [])
            if not anno_values:
                # Try reading from column values in mapping
                anno_values = profile.get("column_values", {}).get(anno_col, [])
            if anno_values:
                unique_years = sorted(
                    {
                        int(v)
                        for v in anno_values
                        if v and str(v).isdigit() and 1900 <= int(v) <= 2100
                    }
                )
                if unique_years:
                    years = unique_years

    config = _build_config_dict(
        safe_name,
        years,
        source_type,
        probe_result,
        profile,
        clean_read=clean_read,
        validation_suggestions=validation_suggestions,
    )

    if profile:
        clean_sql = generate_clean_sql(profile, slug, years[0])
        norm_cols = (
            profile.get("columns_norm")
            or profile.get("columns_raw")
            or profile.get("columns")
            or []
        )
        mart_sql = suggest_mart_sql(norm_cols, profile)
    else:
        clean_sql = (
            "-- ATTENZIONE: profiling non ha rilevato colonne.\n"
            "SELECT 1 AS placeholder FROM raw_input\n"
        )
        mart_sql = (
            "-- mart placeholder — sostituisci con la tua aggregazione.\n"
            "SELECT * FROM clean_input\n"
        )

    topics = probe_result.get("inferred_topics")
    granularity = probe_result.get("inferred_granularity")

    return {
        "dataset.yml": yaml_dumps(config),
        "sql/clean.sql": clean_sql,
        "sql/mart.sql": mart_sql,
        "README.md": _generate_readme(slug, final_url),
        "notes.md": _generate_notes(granularity, topics),
    }


def _build_config_dict(
    name: str,
    years: list[int],
    source_type: str,
    probe_result: dict[str, Any],
    profile: dict[str, Any] | None,
    *,
    clean_read: dict[str, Any] | None = None,
    validation_suggestions: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config: dict[str, Any] = {
        "root": "../../out",
        "schema_version": 1,
        "dataset": {"name": name, "years": years},
        "raw": {
            "output_policy": "overwrite",
            "sources": _build_sources(source_type, probe_result, name),
        },
        "clean": {"sql": "sql/clean.sql"},
        "mart": {"tables": [{"name": name, "sql": "sql/mart.sql"}]},
    }

    if clean_read:
        config["clean"]["read"] = clean_read
    elif profile:
        enriched = _enrich_profile(profile, probe_result)
        suggested = propose_clean_read(enriched)
        if suggested:
            config["clean"]["read"] = suggested
        # Propose robust mode when profiling detected CSV errors
        if profile.get("robust_read_suggested") or profile.get("_robust_read_suggested"):
            config["clean"]["read_mode"] = "robust"

    vs = validation_suggestions or suggest_validation(profile)
    if vs:
        req = vs.get("required_columns")
        if req:
            config["clean"]["required_columns"] = req
        min_rows = vs.get("min_rows")
        if min_rows is not None:
            config["clean"].setdefault("validate", {})["min_rows"] = min_rows
        mart_min = vs.get("mart_min_rows")
        if mart_min is not None:
            config["mart"]["validate"] = {
                "table_rules": {name: {"min_rows": mart_min}},
            }

    return config


def _build_sources(
    source_type: str,
    probe_result: dict[str, Any],
    slug: str,
) -> list[dict[str, Any]]:
    final_url = probe_result.get("final_url", "")
    if source_type == "ckan" and probe_result.get("ckan_resources"):
        return _ckan_sources(probe_result)
    if source_type == "sdmx":
        return _sdmx_sources(probe_result, final_url)
    if source_type == "sparql":
        info = probe_result.get("sparql_info") or {}
        return [
            {
                "name": "sparql",
                "type": "sparql",
                "args": {
                    "endpoint": info.get("endpoint", final_url),
                    "query": "SELECT * WHERE { ?s ?p ?o } LIMIT 1000",
                },
                "primary": True,
            }
        ]
    if source_type == "html":
        links = probe_result.get("candidate_links") or []
        return [_http_file_dict(links[0] if links else final_url, slug)]
    return [_http_file_dict(final_url, slug)]


def _ckan_sources(probe_result: dict[str, Any]) -> list[dict[str, Any]]:
    import re
    from pathlib import Path as _P
    from urllib.parse import urlparse as _up

    resources = probe_result.get("ckan_resources") or []
    parsed = _up(probe_result.get("final_url", ""))
    portal = f"{parsed.scheme}://{parsed.netloc}"
    out = []
    for r in resources:
        name = re.sub(r"[^a-z0-9_]", "_", (r.get("name") or "resource").lower())
        url = r.get("url", "")
        fmt = r.get("format", "csv")
        fname = _P(_up(url).path).name or f"{name}.{fmt}"
        out.append(
            {
                "name": name,
                "type": "ckan",
                "args": {"portal_url": portal, "resource_id": r.get("id") or "", "filename": fname},
                "primary": True,
            }
        )
    return out


def _sdmx_sources(probe_result: dict[str, Any], url: str) -> list[dict[str, Any]]:
    info = probe_result.get("sdmx_info") or {}
    flow = info.get("flow_id")
    if not flow:
        return [_http_file_dict(url, "sdmx")]
    s: dict[str, Any] = {
        "name": f"sdmx_{flow}",
        "type": "sdmx",
        "args": {"flow": flow},
        "primary": True,
    }
    agency = info.get("agency")
    if agency and str(agency).upper() == "ESTAT":
        s["args"]["agency"] = "ESTAT"
    else:
        s["args"]["endpoint"] = url
    return [s]


def _http_file_dict(url: str, slug: str) -> dict[str, Any]:
    from pathlib import Path as _P
    from urllib.parse import urlparse as _up

    fname = _P(_up(url).path).name or f"{slug}.csv"
    return {
        "name": f"{slug}_source",
        "type": "http_file",
        "args": {"url": url, "filename": fname},
        "primary": True,
    }


def _find_anno_col(profile: dict[str, Any]) -> str | None:
    """Find a year/anno column name in the profile."""
    from toolkit.scaffold.clean import _find_anno_raw_column

    return _find_anno_raw_column(profile)


def _enrich_profile(profile: dict[str, Any], probe: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(profile)
    for k in (
        "encoding_suggested",
        "delim_suggested",
        "decimal_suggested",
        "skip_suggested",
        "header_line",
        "true_header_line",
        "robust_read_suggested",
    ):
        if probe.get(k) is not None:
            enriched[k] = probe[k]
    return enriched


def suggest_validation(profile: dict[str, Any] | None) -> dict[str, Any]:
    """Suggest validation rules from profiling results."""
    if not profile:
        return {}
    rules: dict[str, Any] = {}
    cols = profile.get("columns_norm") or profile.get("columns_raw") or []
    rc = profile.get("row_count", 0)
    if cols:
        from toolkit.scaffold.clean import _snake_case

        rules["required_columns"] = [_snake_case(c) for c in cols]
    if rc:
        rules["min_rows"] = max(1, int(rc * 0.5))
        rules["mart_min_rows"] = 1
    return rules


def suggest_mart_sql(
    columns: list[dict[str, Any]] | list[str],
    profile: dict[str, Any],
) -> str:
    """Generate mart.sql skeleton with column type hints."""
    if columns and isinstance(columns[0], dict):
        names = [c.get("name", f"col{i}") for i, c in enumerate(columns)]
    else:
        names = list(columns) if columns else []
    if not names:
        return (
            "-- mart placeholder — sostituisci con la tua aggregazione.\n"
            "SELECT * FROM clean_input\n"
        )
    mapping = profile.get("mapping_suggestions") or {}
    hints = [f"{n}: {(mapping.get(n) or {}).get('type', '?')}" for n in names]
    return (
        f"-- Colonne clean_input ({len(names)}): {', '.join(hints[:8])}"
        f"{' ...' if len(hints) > 8 else ''}\n"
        "-- Sostituisci con la tua aggregazione (es. SUM, COUNT, AVG).\n"
        "SELECT * FROM clean_input\n"
    )


def _generate_readme(slug: str, url: str) -> str:
    return (
        f"# {slug}\n\nFonte: {url}\n\n"
        "## Domanda\n\n-\n\n## Dataset\n\n-\n\n"
        "## Perche vale la pena testarlo\n\n-\n\n"
        "## Output minimo atteso\n\n-\n\n"
        "## Criterio di promozione\n\n-\n\n"
        "## Stato\n\n- intake\n\n"
        "## Prossimo passo\n\n- scout URL poi run all\n"
    )


def _generate_notes(granularity: str | None, topics: list[dict[str, Any]] | None) -> str:
    lines: list[str] = ["## Tecnico\n\n-\n"]
    if granularity:
        lines.append(f"- Granularita rilevata: {granularity}\n")
    if topics:
        names = ", ".join(t["topic"] for t in topics[:3])
        lines.append(f"- Topic suggeriti: {names}\n")
    lines.append("\n## Analitico\n\n-\n\n## Cautele\n\n")
    lines.append("- La serie storica e omogenea su tutti gli anni?\n")
    lines.append("- Ci sono discontinuita dichiarate dalla fonte?\n")
    lines.append("- I valori nulli sono zero reale o dato mancante?\n")
    return "".join(lines)
