"""Backend condiviso per query su layer RAW/CLEAN/MART.

Usato da:
- CLI ``toolkit inspect config`` (via ``config_ops.py``)
- MCP ``toolkit_layer`` (via ``aggregate_ops.py``)

Supporta due modalita' di indirizzamento:
- ``config_path``: dataset locale (pipeline mode)
- ``datasets``: lista slug risolti via CatalogResolver (catalog mode)

Le funzioni qui NON gestiscono errori MCP (ToolkitClientError) — quelle
vanno aggiunte nei wrapper MCP.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from lab_connectors.duckdb import safe_connect
from toolkit.core.config import load_config
from toolkit.core.duckdb_shape import parquet_preview
from toolkit.core.io import read_json_or_none, read_yaml
from toolkit.core.paths import RAW_PROFILE, RAW_SUGGESTED_READ
from toolkit.cli.inspect._helpers import _payload_for_year

# ---------------------------------------------------------------------------
# Costanti
# ---------------------------------------------------------------------------

VALID_LAYERS: set[str] = {"raw", "clean", "mart"}
VALID_MODES: set[str] = {"schema", "preview", "profile", "sql"}
SQL_SCOPE_BLOCKED_KEYWORDS = {
    "ALTER",
    "ATTACH",
    "CALL",
    "COPY",
    "CREATE",
    "DELETE",
    "DETACH",
    "DROP",
    "EXPORT",
    "IMPORT",
    "INSERT",
    "INSTALL",
    "LOAD",
    "MERGE",
    "REPLACE",
    "TRUNCATE",
    "UPDATE",
    "VACUUM",
}
MAX_ROWS_HARD_CAP = 500


# ---------------------------------------------------------------------------
# Helper path (pipeline mode)
# ---------------------------------------------------------------------------


def _resolve_clean_path(cfg: Any, year: int) -> Path:
    paths = _payload_for_year(cfg, year)
    parquet_str = paths["paths"]["clean"].get("output")
    if not parquet_str:
        raise FileNotFoundError("Nessun output clean configurato")
    return Path(parquet_str)


def _resolve_mart_path(cfg: Any, year: int, mart_index: int = 0) -> Path:
    paths = _payload_for_year(cfg, year)
    outputs = paths["paths"]["mart"].get("outputs") or []
    if not outputs:
        raise FileNotFoundError("Nessun output mart configurato")
    if mart_index < 0 or mart_index >= len(outputs):
        raise ValueError(f"Indice mart {mart_index} non valido: {len(outputs)} output disponibili")
    return Path(outputs[mart_index])


def _resolve_raw_dir(cfg: Any, year: int) -> tuple[Path, dict[str, Any]]:
    paths = _payload_for_year(cfg, year)
    raw_dir = Path(paths["paths"]["raw"]["dir"])
    return raw_dir, paths


# ---------------------------------------------------------------------------
# Scope validation (catalog mode)
# ---------------------------------------------------------------------------


def _validate_sql_scope(sql: str, allowed_tables: set[str]) -> str:
    """Valida che l'SQL utente sia safe e referenzi solo tabelle consentite.

    Args:
        sql: SQL da validare.
        allowed_tables: Nomi CTE/tabella consentiti (es ``{"data"}``
            o ``{"anac_bandi_gara", "popolazione_istat"}``).

    Returns:
        SQL pulito (stripped).

    Raises:
        ValueError: se la validazione fallisce.
    """
    sql = sql.strip()
    if not sql:
        raise ValueError("SQL vuoto")

    # Solo SELECT o WITH consentiti
    upper = sql.upper().strip()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise ValueError("Solo query SELECT o WITH sono consentite")

    # Blocca keyword DDL/DML
    stripped = _strip_sql_comments_and_strings(sql)
    for kw in SQL_SCOPE_BLOCKED_KEYWORDS:
        pattern = re.compile(rf"\b{kw}\b", re.IGNORECASE)
        if pattern.search(stripped):
            raise ValueError(f"Keyword non consentita nell'SQL: {kw}")

    # Blocca read_parquet/read_csv nell'SQL utente (bypass CTE)
    blocked_funcs = {"read_parquet", "read_csv", "read_csv_auto"}
    for func in blocked_funcs:
        pattern = re.compile(rf"\b{func}\s*\(", re.IGNORECASE)
        if pattern.search(stripped):
            raise ValueError(f"Funzione non consentita nell'SQL: {func}")

    # Blocca riferimenti a tabelle non consentite
    from_pattern = re.compile(
        r"""
        \bFROM\b           # FROM keyword
        \s+                # spazio
        (?:                # inizio gruppo tabella
          (?:"|')?         # quoting opzionale
          (?P<table>       # nome tabella
            [a-zA-Z_]\w*  # identifier
          )
          (?:"|')?         # quoting opzionale
          (?:\s+(?:AS\s+)?\w+)?  # alias opzionale
        )
        """,
        re.IGNORECASE | re.VERBOSE,
    )
    # Cerca anche nei JOIN
    join_pattern = re.compile(
        r"""
        \b(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+|FULL\s+)?JOIN\s+
        (?:"|')?(?P<table>[a-zA-Z_]\w*)(?:"|')?
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    for pattern in (from_pattern, join_pattern):
        for match in pattern.finditer(stripped):
            table = match.group("table")
            if table.upper() not in {t.upper() for t in allowed_tables}:
                raise ValueError(
                    f"Riferimento a tabella non consentita: '{table}'. "
                    f"Tabelle consentite: {', '.join(sorted(allowed_tables))}"
                )

    return sql


def _strip_sql_comments_and_strings(sql: str) -> str:
    """Rimuove commenti SQL e stringhe letterali per ispezione."""
    # Commenti -- e /* */
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    # Stringhe (sostituisce con spazio)
    sql = re.sub(r"'[^']*'", " ", sql)
    sql = re.sub(r'"[^"]*"', " ", sql)
    return sql


# ---------------------------------------------------------------------------
# Schema mode
# ---------------------------------------------------------------------------


def show_schema(config_path: str, layer: str = "clean", year: int | None = None) -> dict[str, Any]:
    """Mostra lo schema (colonne + tipi) di raw, clean o mart."""
    from toolkit.cli.inspect.schema_ops import show_schema as _cli_show_schema

    return _cli_show_schema(config_path, layer=layer, year=year)


# ---------------------------------------------------------------------------
# Profile mode (raw only)
# ---------------------------------------------------------------------------


def raw_profile(config_path: str, year: int | None = None) -> dict[str, Any]:
    """Legge il profilo raw (raw_profile.json o suggested_read.yml)."""
    from toolkit.cli.inspect._helpers import _payload_for_year

    cfg = load_config(config_path)
    if year is None:
        year = max(cfg.years) if cfg.years else 0
    paths = _payload_for_year(cfg, year)
    raw_dir = Path(paths["paths"]["raw"]["dir"])
    profile_path = raw_dir / "_profile"
    raw_profile_json = profile_path / RAW_PROFILE
    suggested_read_yml = profile_path / RAW_SUGGESTED_READ

    if raw_profile_json.exists():
        profile = read_json_or_none(raw_profile_json)
    elif suggested_read_yml.exists():
        raw_yaml = read_yaml(suggested_read_yml)
        clean_section = raw_yaml.get("clean", {}) if isinstance(raw_yaml, dict) else {}
        read_section = clean_section.get("read", {}) if isinstance(clean_section, dict) else {}
        profile = {
            "dataset": None,
            "year": None,
            "encoding_suggested": read_section.get("encoding"),
            "delim_suggested": read_section.get("delim"),
            "decimal_suggested": read_section.get("decimal"),
            "skip_suggested": read_section.get("skip"),
            "robust_read_suggested": None,
            "columns_raw": None,
            "columns_norm": None,
            "missingness_top": [],
            "mapping_suggestions": {},
            "warnings": [],
        }
    else:
        raise FileNotFoundError(
            f"Profilo raw non trovato in {profile_path}. "
            "Nessun file raw_profile.json ne suggested_read.yml."
        )

    return {
        "dataset": profile.get("dataset"),
        "year": profile.get("year"),
        "config_path": str(config_path),
        "profile_path": str(profile_path),
        "file_used": profile.get("file_used"),
        "read_hints": {
            "encoding": profile.get("encoding_suggested"),
            "delimiter": profile.get("delim_suggested"),
            "decimal": profile.get("decimal_suggested"),
            "skip": profile.get("skip_suggested"),
            "robust": profile.get("robust_read_suggested"),
        },
        "header_line": profile.get("header_line"),
        "columns": {
            "raw": profile.get("columns_raw") or [],
            "normalized": profile.get("columns_norm") or [],
            "count": len(profile.get("columns_raw") or []),
        },
        "missingness_top": profile.get("missingness_top", []),
        "mapping_suggestions": profile.get("mapping_suggestions", {}),
        "warnings": profile.get("warnings", []),
        "profile_exists": True,
    }


# ---------------------------------------------------------------------------
# Preview mode
# ---------------------------------------------------------------------------


def _read_parquet_preview(parquet_path: Path, limit: int = 10) -> dict[str, Any]:
    """Legge schema + prime N righe da un parquet."""
    from toolkit.core.duckdb_shape import parquet_preview

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet non trovato: {parquet_path}")
    if parquet_path.suffix not in (".parquet",):
        raise ValueError(f"Formato non supportato: {parquet_path.suffix}. Solo .parquet.")
    result = parquet_preview(parquet_path, limit=limit)
    result.pop("path", None)
    result.pop("sql", None)
    return result


def clean_preview(
    config_path: str,
    layer: str = "clean",
    mart_index: int = 0,
    year: int | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    """Preview dati da un parquet clean o mart."""
    from toolkit.cli.inspect._helpers import _payload_for_year

    cfg = load_config(config_path)
    if year is None:
        year = max(cfg.years) if cfg.years else 0
    paths = _payload_for_year(cfg, year)

    if layer == "clean":
        parquet_path = _resolve_clean_path(cfg, year)
    elif layer == "mart":
        parquet_path = _resolve_mart_path(cfg, year, mart_index)
    else:
        raise ValueError(f"layer deve essere 'clean' o 'mart', non '{layer}'")

    result = _read_parquet_preview(parquet_path, limit=limit)
    result.update(
        {
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
            "layer": layer,
            "config_path": str(config_path),
            "mart_name": parquet_path.stem if layer == "mart" else None,
        }
    )
    return result


def raw_preview(config_path: str, year: int | None = None, limit: int = 20) -> dict[str, Any]:
    """Preview del raw file primario di un dataset."""
    cfg = load_config(config_path)
    if year is None:
        year = max(cfg.years) if cfg.years else 0
    raw_dir, paths = _resolve_raw_dir(cfg, year)

    primary_file = (paths.get("raw_hints") or {}).get("primary_output_file")
    if not primary_file:
        raise FileNotFoundError("Nessun primary_output_file nel manifest raw")
    raw_file = raw_dir / primary_file
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file non trovato: {raw_file}")

    suffix = raw_file.suffix.lower()
    if suffix in (".csv", ".tsv", ".txt"):
        from toolkit.cli.inspect.profile_ops import csv_preview as _csv_preview

        return _csv_preview(str(raw_file), limit=limit)
    elif suffix in (".xlsx", ".xls"):
        return {
            "path": str(raw_file),
            "format": "xlsx",
            "note": "File binario XLSX. Usa mode='schema' per lo schema colonne.",
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
        }
    else:
        return {
            "path": str(raw_file),
            "format": suffix.lstrip("."),
            "note": f"Formato '{suffix}' non supportato per preview raw.",
            "dataset": paths.get("dataset"),
            "year": paths.get("year"),
        }


# ---------------------------------------------------------------------------
# SQL mode
# ---------------------------------------------------------------------------


def _resolve_datasets(
    datasets: list[str],
    layer: str,
    year: int | None,
    table: str | None = None,
) -> dict[str, str]:
    """Risolve lista slug → dict {slug: parquet_url} via CatalogResolver.

    Converte URL ``s3://`` in ``https://storage.googleapis.com/``
    per evitare dipendenza dall'estensione httpfs di DuckDB.

    Args:
        datasets: Lista slug.
        layer: ``"clean"`` o ``"mart"``.
        year: Anno filtro.
        table: Nome tabella mart (es. ``"mart_top_sa"``). Match sul filename.

    Returns:
        Dict slug → url HTTPS del parquet.
    """
    from toolkit.cli.catalog_ops import CatalogResolver

    _HTTPS_STORAGE = "https://storage.googleapis.com/"

    resolver = CatalogResolver()
    resolved: dict[str, str] = {}
    for slug in datasets:
        files = resolver.resolve_slug(slug, layer=layer, year=year, table=table)
        if not files:
            raise FileNotFoundError(f"Slug '{slug}' non trovato (layer={layer})")
        url = files[0]["url"]
        if url.startswith("s3://"):
            url = _HTTPS_STORAGE + url[5:]  # s3:// < 5 len
        resolved[slug] = url
    return resolved


def layer_sql(
    config_path: str | None = None,
    datasets: list[str] | None = None,
    layer: str = "clean",
    year: int | None = None,
    limit: int = 20,
    sql: str | None = None,
    mart_index: int = 0,
    table: str | None = None,
) -> dict[str, Any]:
    """Esegue SQL arbitrario su uno o piu' dataset.

    Args:
        config_path: Path a dataset.yml (pipeline mode).
            Mutuamente esclusivo con ``datasets``.
        datasets: Lista slug (catalog mode).
            Mutuamente esclusivo con ``config_path``.
        table: Nome tabella mart (es ``"mart_top_sa"``).
            Solo per catalog mode, layer=mart.
        layer: ``"raw"``, ``"clean"`` (default) o ``"mart"``.
        year: Anno filtro.
        limit: Max righe.
        sql: Query SQL. I dati sono disponibili come tabella ``data``
            (singolo dataset) o CTE col nome slug (multi dataset).
        mart_index: Indice tabella mart (solo pipeline mode).

    Returns:
        Risultato della query SQL.

    Raises:
        ValueError: se parametri invalidi o SQL non valido.
        FileNotFoundError: se dataset non trovato.
    """
    if not sql:
        raise ValueError("mode=sql richiede il parametro sql")
    if config_path and datasets:
        raise ValueError("Specificare solo uno tra config_path e datasets")
    if not config_path and not datasets:
        raise ValueError("Specificare config_path (pipeline) o datasets (catalogo)")

    limit = min(limit, MAX_ROWS_HARD_CAP)

    # ---- Catalog mode: risolvi slug → URL parquet ----
    if datasets:
        resolved = _resolve_datasets(datasets, layer=layer, year=year, table=table)
        # Scope validation: solo i CTE dichiarati
        allowed = set(resolved.keys())
        validated_sql = _validate_sql_scope(sql, allowed)

        # Costruisci CTE per ogni dataset
        cte_defs = []
        for slug, url in resolved.items():
            cte_defs.append(f"{slug} AS (SELECT * FROM read_parquet(['{url}']))")
        cte = ", ".join(cte_defs)
        wrapped_sql = f"WITH {cte} {validated_sql}"

        with safe_connect() as conn:
            result = conn.execute(wrapped_sql).fetchall()
            columns = [d[0] for d in conn.description]

        return {
            "columns": columns,
            "rows": [dict(zip(columns, row)) for row in result[:limit]],
            "total_count": len(result),
            "truncated": len(result) > limit,
            "datasets": datasets,
            "layer": layer,
            "year": year,
            "mode": "sql",
        }

    # ---- Pipeline mode: config_path ----
    assert config_path is not None  # mypy: gia' verificato in layer_query
    cfg = load_config(config_path)
    if year is None:
        year = max(cfg.years) if cfg.years else 0

    if layer == "raw":
        return _layer_sql_raw(config_path, cfg, year, sql, limit)

    if layer == "clean":
        parquet_path = _resolve_clean_path(cfg, year)
    elif layer == "mart":
        parquet_path = _resolve_mart_path(cfg, year, mart_index)
    else:
        raise ValueError(f"layer deve essere 'raw', 'clean' o 'mart', non '{layer}'")

    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet {layer} non trovato: {parquet_path}. "
            f"Esegui 'toolkit run all -c {config_path}' per generarlo."
        )

    preview = parquet_preview(parquet_path, limit=limit, sql=sql)
    preview.update(
        {
            "dataset": cfg.dataset,
            "year": year,
            "layer": layer,
            "config_path": str(config_path),
            "mode": "sql",
        }
    )
    return preview


def _layer_sql_raw(
    config_path: str,
    cfg: Any,
    year: int,
    sql: str,
    limit: int,
) -> dict[str, Any]:
    """Esegue SQL su layer raw (CSV). Estratta per chiarezza tipi."""
    raw_dir, paths = _resolve_raw_dir(cfg, year)
    primary_file = (paths.get("raw_hints") or {}).get("primary_output_file")
    if not primary_file:
        raise FileNotFoundError("Nessun primary_output_file nel manifest raw")
    raw_file = raw_dir / primary_file
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file non trovato: {raw_file}")

    from toolkit.core.sql_utils import sql_literal as _sq

    source = f"read_csv_auto('{_sq(str(raw_file))}')"
    with safe_connect() as con:
        con.execute(f"CREATE OR REPLACE VIEW data AS SELECT * FROM {source}")
        rows = con.execute(f"SELECT * FROM ({sql}) AS q LIMIT {limit + 1}").fetchall()
        # connection.description riflette le colonne dell'SQL eseguito,
        # non quelle raw — corretto anche per aggregazioni/alias
        col_names = [d[0] for d in con.description]
        columns_info = [{"name": str(d[0]), "type": str(d[1])} for d in con.description]
        preview = [dict(zip(col_names, row)) for row in rows[:limit]]

    return {
        "columns": columns_info,
        "column_count": len(columns_info),
        "row_count": len(rows),
        "preview": preview,
        "truncated": len(rows) > limit,
        "dataset": cfg.dataset,
        "year": year,
        "layer": "raw",
        "config_path": str(config_path),
        "mode": "sql",
    }


# ---------------------------------------------------------------------------
# Router principale
# ---------------------------------------------------------------------------


def layer_query(
    config_path: str | None = None,
    datasets: list[str] | None = None,
    layer: str = "clean",
    mode: str = "schema",
    year: int | None = None,
    limit: int = 20,
    sql: str | None = None,
    mart_index: int = 0,
    table: str | None = None,
) -> dict[str, Any]:
    """Query unificata su layer RAW/CLEAN/MART.

    Args:
        config_path: Path a dataset.yml (pipeline mode).
        datasets: Lista slug (catalog mode, mut. esclusivo con config_path).
        layer: ``"raw"``, ``"clean"`` (default) o ``"mart"``.
        mode: ``"schema"``, ``"preview"``, ``"profile"``, ``"sql"``.
        year: Anno filtro.
        limit: Max righe in preview/sql.
        sql: Query SQL per mode=sql.
        mart_index: Indice tabella mart (solo pipeline mode).

    Raises:
        ValueError: se layer/mode non validi.
    """
    if config_path and datasets:
        raise ValueError("Specificare solo uno tra config_path e datasets")
    use_catalog = datasets is not None

    if not use_catalog and not config_path:
        raise ValueError("Specificare config_path (pipeline) o datasets (catalogo)")

    safe_layer = layer.strip().lower()
    safe_mode = mode.strip().lower() if isinstance(mode, str) else mode

    if safe_layer not in VALID_LAYERS:
        raise ValueError(
            f"layer deve essere uno tra: {', '.join(sorted(VALID_LAYERS))} (ricevuto: {layer})"
        )
    if safe_mode not in VALID_MODES:
        raise ValueError(
            f"mode deve essere uno tra: {', '.join(sorted(VALID_MODES))} (ricevuto: {mode})"
        )
    if safe_mode == "sql" and not sql:
        raise ValueError("mode=sql richiede il parametro sql (es. sql='SELECT * FROM data')")

    # --- Catalog mode: solo SQL supportato ---
    if use_catalog:
        if safe_mode != "sql":
            raise ValueError(
                f"Catalog mode (datasets) supporta solo mode='sql'. "
                f"Usa config_path per mode='{safe_mode}'."
            )
        return layer_sql(
            config_path=None,
            datasets=datasets,
            layer=safe_layer,
            year=year,
            limit=limit,
            sql=sql,
            table=table,
        )

    if safe_mode == "profile" and safe_layer != "raw":
        raise ValueError(f"mode=profile e' valido solo per layer=raw (ricevuto: layer={layer})")

    # --- Pipeline mode ---
    assert config_path is not None  # garantito dal guard sopra
    if safe_mode == "schema":
        return show_schema(config_path, layer=safe_layer, year=year)
    if safe_mode == "profile":
        return raw_profile(config_path, year=year)
    if safe_mode == "preview":
        if safe_layer == "raw":
            return raw_preview(config_path, year=year, limit=limit)
        return clean_preview(
            config_path, layer=safe_layer, mart_index=mart_index, year=year, limit=limit
        )
    if safe_mode == "sql":
        return layer_sql(
            config_path=config_path,
            datasets=None,
            layer=safe_layer,
            year=year,
            limit=limit,
            sql=sql,
            mart_index=mart_index,
        )

    raise RuntimeError(f"mode non gestito: {safe_mode}")
