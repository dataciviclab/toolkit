"""Schema colonne per gli artifact registry.

Lo schema dei parquet clean è letto da ``toolkit.core.duckdb_shape.parquet_schema``
(il reader runtime del toolkit); qui solo l'arricchimento di catalogo:
mappatura tipo DuckDB→catalogo, role (dimension/metric) e semantic_type.

La mappatura tipi replica quella di ``dataset-incubator/scripts/build_clean_catalog.py``:
il parquet locale è lo stesso file che verrà pushato su GCS.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from toolkit.core.duckdb_shape import parquet_schema
from toolkit.domain.path_resolver import payload_for_year

from toolkit.registry.layout import DatasetManifest

# Vocabolario condiviso dei tipi semantici (contratto Lab, package-data).
DEFAULT_SEMANTIC_TYPES = Path(__file__).resolve().parent / "semantic_types.yaml"

# Mappa tipi DuckDB → tipo catalogo (preserva BIGINT vs INTEGER)
DUCKDB_TO_CATALOG: dict[str, str] = {
    "integer": "INTEGER",
    "int32": "INTEGER",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "int64": "BIGINT",
    "smallint": "INTEGER",
    "tinyint": "INTEGER",
    "hugeint": "BIGINT",
    "float": "DOUBLE",
    "real": "DOUBLE",
    "double": "DOUBLE",
    "decimal": "DOUBLE",
    "numeric": "DOUBLE",
    "varchar": "VARCHAR",
    "text": "VARCHAR",
    "char": "VARCHAR",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamp_s": "TIMESTAMP",
    "timestamp_ms": "TIMESTAMP",
    "timestamp_ns": "TIMESTAMP",
    "time": "TIME",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
}

_DIMENSION_TYPES = {"VARCHAR", "DATE", "BOOLEAN"}


def load_semantic_types(path: Path | None = None) -> dict[str, str]:
    """Carica alias_map dal vocabolario semantic_types (alias.lower → semantic_type).

    Nessun partial/substring match — solo match esatto.
    Default: il vocabolario condiviso del toolkit (package-data); un path
    esplicito fa da override (es. vocabolario esteso di un repo).
    """
    if path is None:
        path = DEFAULT_SEMANTIC_TYPES
    if not path.is_file():
        return {}
    import yaml

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    alias_map: dict[str, str] = {}
    for stype, info in (data.get("types") or {}).items():
        for alias in info.get("aliases", []) if isinstance(info, dict) else []:
            alias_lower = str(alias).lower()
            if alias_lower not in alias_map:
                alias_map[alias_lower] = stype
    return alias_map


def load_valid_types(path: Path | None = None) -> set[str]:
    """Carica i nomi validi di semantic_type dal vocabolario (chiavi di ``types``)."""
    if path is None:
        path = DEFAULT_SEMANTIC_TYPES
    if not path.is_file():
        return set()
    import yaml

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return set((data.get("types") or {}).keys())


def normalize_semantic_type(
    value: str,
    alias_map: dict[str, str],
    valid_types: set[str],
) -> str | None:
    """Normalizza un semantic_type potenzialmente errato.

    Se ``value`` è un tipo valido → lo restituisce.
    Se è un alias → risolve al tipo corretto.
    Altrimenti → None (tipo irrecuperabile).
    """
    if not value:
        return None
    if value in valid_types:
        return value
    return alias_map.get(value.lower())


def _assign_semantic_type(col_name: str, alias_map: dict[str, str]) -> str | None:
    return alias_map.get(col_name.lower())


def clean_parquet_path(cfg: Any, year: int) -> Path | None:
    """Path del parquet clean locale per anno (path resolver del toolkit).

    Returns:
        Path se il file esiste, altrimenti None.
    """
    try:
        payload = payload_for_year(cfg, year)
        output = (payload.get("paths") or {}).get("clean", {}).get("output")
    except Exception:
        return None
    if not output:
        return None
    path = Path(output)
    return path if path.is_file() else None


def parquet_columns(
    parquet_path: Path | None,
    alias_map: dict[str, str] | None = None,
) -> list[dict[str, Any]] | None:
    """Schema del parquet (via reader runtime) → colonne del catalogo.

    ``role`` è derivato — mai dal tipo DuckDB da solo:
    - colonna con ``semantic_type`` → dimension (i tipi semantici del
      vocabolario sono tutti entity/dimension: codici, anni, id, ...);
    - altrimenti dal tipo DuckDB (VARCHAR/DATE/BOOLEAN → dimension).
    """
    if parquet_path is None:
        return None
    alias_map = alias_map or {}
    try:
        rows = parquet_schema(parquet_path)
    except Exception:
        return None
    if not rows:
        return None

    columns: list[dict[str, Any]] = []
    for row in rows:
        col_name = str(row.get("name", ""))
        raw_type = str(row.get("type", "")).lower()
        bq_type = DUCKDB_TO_CATALOG.get(raw_type, "VARCHAR")
        semantic_type = _assign_semantic_type(col_name, alias_map)
        if semantic_type:
            role = "dimension"
        else:
            role = "dimension" if bq_type in _DIMENSION_TYPES else "metric"
        col_entry: dict[str, Any] = {
            "name": col_name,
            "type": bq_type,
            "role": role,
            "description": "",
        }
        if semantic_type:
            col_entry["semantic_type"] = semantic_type
        columns.append(col_entry)
    return columns


def latest_clean_columns(
    manifest: DatasetManifest,
    alias_map: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]] | None, int | None]:
    """Schema del parquet clean più recente presente in locale.

    Returns:
        Tuple (columns, latest_year) — (None, None) se nessun parquet locale.
    """
    if not manifest.years:
        return None, None
    for year in sorted(manifest.years, reverse=True):
        parquet = clean_parquet_path(manifest.cfg, year)
        if parquet is not None:
            return parquet_columns(parquet, alias_map), year
    return None, None


# Pattern civic "forti" per colonne che spesso non hanno alias espliciti.
# Ordinati dal più specifico al più generico: regione/ipa/provincia prima
# di municipality, che resta legato al contesto "comune" (mai un generico
# "codice_istat" che catturerebbe ipa/regione/provincia).
_CIVIC_KEY_PATTERNS: tuple[tuple[str, str], ...] = (
    # Specifici territoriali (PRIMA del generic comune)
    (r"codice_istat_regione|regione_codice_istat|codice_regione_istat", "region_code"),
    (r"codice_ipa|codice_ente_ipa|codice_istat_ipa", "ipa_code"),
    (r"provincia_cm_codice|codice_provincia_cm", "province_code"),
    (r"codice_catastale|codi_catastale|cod_catastale", "cadastral_code"),
    # Chiavi denaro/progetto
    (r"(^|_)cf$|_cf$|^cf_|codice_fiscale|partita_iva|^piva", "fiscal_code"),
    (r"^cup$|^codice_cup$|codice_cup_", "cup_code"),
    (r"^cig$|^codice_cig$|codice_cig_", "cig_code"),
    (r"codice_ente_siope|codice_ente_bdap", "siope_code"),
    (r"codice_missione|pnrr_missione|^missione$", "programma_code"),
    (r"codice_indicatore|COD_INDICATORE", "indicatore_code"),
    (r"codice_miur|codice_ente_miur", "miur_code"),
    (r"^COD_ATECO|codice_ateco", "ateco_code"),
    (r"nuts_parent_code", "nuts_code"),
    # Municipality: solo contesto comune — non "username" generico
    (
        r"^codice_istat$|^cod_comune$|^codice_comune$|^comune_istat$"
        r"|comune_codice_istat|codice_comune_",
        "municipality_code",
    ),
)

# Colonne che sono label/descrizione di una chiave, non la chiave stessa.
# Non suggerire semantic_type civico: il join forte resta sul codice.
_DESC_LABEL_RE = re.compile(
    r"(^descr_|^desc_|_descrizione$|^descrizione_|_label$|^label_)",
    re.IGNORECASE,
)


def suggest_semantic_type(
    col_name: str,
    alias_map: dict[str, str],
    valid_types: set[str],
) -> str | None:
    """Suggerisce un semantic_type per una colonna civic-like.

    1. Alias esatto (case-insensitive) dal vocabolario.
    2. Pattern civic forti, ordinati specifico→generico (solo se il tipo
       esiste nel vocabolario).

    Le colonne di descrizione/label (descr_*, descrizione_*, *_label) non
    vengono tipizzate come chiavi: sono testo legato alla chiave.

    Returns:
        Tipo suggerito o None se non riconosciuto.
    """
    if not col_name:
        return None
    if _DESC_LABEL_RE.search(col_name):
        return None
    hit = alias_map.get(col_name.lower())
    if hit and hit in valid_types:
        return hit
    for pattern, suggested in _CIVIC_KEY_PATTERNS:
        if suggested in valid_types and re.search(pattern, col_name, re.IGNORECASE):
            return suggested
    return None


def find_untyped_civic_keys(
    catalog: dict[str, Any],
    semantic_types_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Trova colonne civic senza semantic_type in un clean_catalog/registry.

    Non blocca: è un warning di qualità per il registry build. Le colonne
    senza tipo che matchano alias/pattern civic meritano un alias in
    ``semantic_types.yaml`` (copertura centralizzata nel toolkit).
    """
    alias_map = load_semantic_types(semantic_types_path)
    valid_types = load_valid_types(semantic_types_path)
    if not alias_map and not valid_types:
        return []

    findings: list[dict[str, Any]] = []
    for ds in catalog.get("datasets") or []:
        slug = ds.get("slug") or ""
        for col in ds.get("columns") or []:
            name = col.get("name") or ""
            if not name or col.get("semantic_type"):
                continue
            suggested = suggest_semantic_type(name, alias_map, valid_types)
            if not suggested:
                continue
            via = "alias" if alias_map.get(name.lower()) == suggested else "pattern"
            findings.append(
                {
                    "slug": slug,
                    "column": name,
                    "suggested": suggested,
                    "via": via,
                }
            )
    return findings
