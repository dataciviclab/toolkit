"""Contratti di pipeline del toolkit — struttura leggibile da agenti AI.

Il contratto delle **macro SQL** è AUTO-GENERATO da ``toolkit/sql/macros.sql``
tramite ``toolkit.core.macro_reader.read_macros()``. Le annotazioni
``@contract`` nei commenti di macros.sql arricchiscono ogni macro con
metadati machine-readable (returns, warning, example, see...).

Sezioni curate manualmente: validation rules, read_params, pipeline layers.
Queste descrivono comportamenti del runtime che non sono auto-estraibili.
"""

from __future__ import annotations

from typing import Any

from toolkit.core.constants import (
    CLEAN_INPUT_VIEW,
    RAW_INPUT_DF_VIEW,
    RAW_INPUT_SOURCE_VIEW,
    RAW_INPUT_VIEW,
    SOURCE_INPUT_VIEW,
    YEAR_PLACEHOLDER,
)
from toolkit.core.macro_reader import read_macros


# ── Tipi fonte RAW disponibili ────────────────────────────────────────────
# Corrispondono ai file in toolkit/plugins/.
_SOURCE_TYPES: list[dict[str, Any]] = [
    {
        "type": "http_file",
        "description": "Scarica un file da URL HTTP/HTTPS. Il tipo piu' comune.",
        "args": {
            "url": "URL del file (puo' contenere {year})",
            "filename": "nome locale (puo' contenere {year})",
        },
    },
    {
        "type": "http_post_file",
        "description": "Come http_file ma con POST (per API che richiedono body).",
        "args": {
            "url": "URL dell'endpoint",
            "filename": "nome locale",
            "data": "body della POST (opzionale)",
        },
    },
    {
        "type": "local_file",
        "description": "File gia' presente su disco. Usato per test e smoke.",
        "args": {
            "path": "path al file o directory",
            "filename": "pattern glob (opzionale)",
        },
    },
    {
        "type": "ckan",
        "description": "Scarica risorse da un portale CKAN via API.",
        "args": {
            "portal_url": "URL del portale CKAN (es. https://dati.gov.it)",
            "package_id": "ID del dataset CKAN",
            "resource_id": "ID specifico della risorsa (opzionale)",
            "filename": "nome locale (opzionale)",
        },
    },
    {
        "type": "sdmx",
        "description": "Scarica dati da API SDMX 2.1 (es. ISTAT, Eurostat).",
        "args": {
            "endpoint": "URL dell'endpoint SDMX",
            "resource": "ID del dataflow/risorsa",
            "filename": "nome locale (opzionale)",
        },
    },
    {
        "type": "sparql",
        "description": "Esegue query SPARQL SELECT su endpoint pubblico.",
        "args": {
            "endpoint": "URL dello SPARQL endpoint",
            "query": "Query SPARQL SELECT",
            "filename": "nome locale (opzionale)",
        },
    },
]

# ── Tipi extractor RAW ───────────────────────────────────────────────────
_EXTRACTOR_TYPES: list[dict[str, Any]] = [
    {
        "type": "identity",
        "description": "Nessuna estrazione: il file e' gia' nel formato giusto.",
    },
    {
        "type": "unzip_all",
        "description": "Estrae tutti i file da uno ZIP.",
    },
    {
        "type": "unzip_first",
        "description": "Estrae solo il primo file da uno ZIP.",
    },
    {
        "type": "unzip_first_csv",
        "description": "Estrae il primo file CSV da uno ZIP.",
    },
]


# ── Contratto layer RAW ──────────────────────────────────────────────────
_RAW_CONTRACT: dict[str, Any] = {
    "source_types": _SOURCE_TYPES,
    "extractors": _EXTRACTOR_TYPES,
    "validation": {
        "profile": {
            "description": "Il profilo raw (raw_profile.json) rileva automaticamente encoding, delim, decimal, skip e colonne del CSV.",
            "known_issue": "La profilazione potrebbe suggerire decimal='.' anche se il CSV usa ','. Va sovrascritto in clean.read.decimal.",
        },
    },
    "output": {
        "path": "data/raw/<dataset>/<anno>/<filename>",
        "profile": "data/raw/<dataset>/<anno>/_profile/raw_profile.json",
    },
}

# ── Macros: AUTO-GENERATE da macros.sql ───────────────────────────────────
# Ogni macro in macros.sql con un header "-- ── nome ──" e annotazioni
# @contract viene parsificata e inclusa qui. Nessuna manutenzione manuale.
_MACROS: list[dict[str, Any]] = read_macros()

# ── Contratto layer CLEAN ────────────────────────────────────────────────
_CLEAN_CONTRACT: dict[str, Any] = {
    "sql_source": {
        "view": RAW_INPUT_VIEW,
        "how_to_use": (
            f"clean.sql deve fare SELECT ... FROM {RAW_INPUT_VIEW}. "
            f"Il toolkit crea {RAW_INPUT_VIEW} come view DuckDB leggendo "
            f"i file raw (CSV/parquet/Excel) con i parametri configurati "
            f"in clean.read."
        ),
        "also_available": [
            f"{RAW_INPUT_SOURCE_VIEW} (view intermedia con colonne raw)",
            f"{RAW_INPUT_DF_VIEW} (DataFrame pandas intermedio, reader normalizzato/Excel)",
        ],
    },
    "year_placeholder": {
        "syntax": YEAR_PLACEHOLDER,
        "description": (
            "Il toolkit sostituisce {year} con l'anno del run prima di "
            "eseguire clean.sql. Esempio: CAST({year} AS INTEGER) AS anno."
        ),
    },
    "macros": _MACROS,
    "validation": {
        "required_columns": {
            "scope": "nomi colonna OUTPUT del clean.sql (dopo AS alias)",
            "note": (
                "clean.required_columns in dataset.yml verifica i nomi "
                "delle colonne come appaiono nel risultato di clean.sql, "
                "non i nomi raw del CSV. "
                "Esempio: se raw ha ANNO e clean fa CAST(ANNO AS INTEGER) AS anno, "
                "usa required_columns: [anno]."
            ),
        },
        "transition": {
            "description": (
                "Il monitor di transizione confronta colonne raw vs clean. "
                "Avvisa se colonne raw spariscono dal clean senza commento "
                "-- DROP: <motivo>."
            ),
        },
    },
    "read_params": {
        "decimal": {
            "description": "Se read.decimal=',' in dataset.yml, DuckDB usa la virgola come separatore decimale nel CSV.",
            "consequence": "I numeri in formato italiano sono già parsati correttamente. NON serve normalize_italian_number.",
            "recommended_usage": "CAST(colonna AS DOUBLE) — il cast diretto preserva il valore già convertito da DuckDB.",
        },
    },
    "example_file": "project-example/sql/clean.sql",
    "dataset_dir": "datasets/",
}

# ── Contratto layer MART ─────────────────────────────────────────────────
_MART_CONTRACT: dict[str, Any] = {
    "sql_source": {
        "view": CLEAN_INPUT_VIEW,
        "how_to_use": (
            f"mart.sql deve fare SELECT ... FROM {CLEAN_INPUT_VIEW}. "
            f"Il runner MART crea {CLEAN_INPUT_VIEW} come view DuckDB "
            f"sul parquet clean dell'anno corrente."
        ),
    },
    "multi_year": {
        "description": (
            "Per tabelle che aggregano più anni, specifica years in "
            "dataset.yml mart.tables[].years. Il toolkit unisce i parquet "
            f"degli anni indicati e li espone come view {CLEAN_INPUT_VIEW}."
        ),
    },
    "validation": {
        "table_rules": {
            "scope": "primary_key, not_null, ranges, min_rows vanno dentro mart.validate.table_rules.<nome_tabella>",
        },
        "required_tables": {
            "scope": "mart.required_tables verifica che le tabelle dichiarate siano state prodotte.",
        },
    },
    "example_file": "project-example/sql/mart/mart_regione_anno.sql",
}

# ── Contratto generale pipeline ──────────────────────────────────────────
_PIPELINE_CONTRACT: dict[str, Any] = {
    "layers": [
        {
            "name": "RAW",
            "description": "Download file originale dalla fonte. Profilo: encoding, delim, decimal, colonne.",
            "output": "CSV/parquet in data/raw/<dataset>/<anno>/",
            "validation": "raw_validation.json",
        },
        {
            "name": "CLEAN",
            "description": "Trasformazione SQL (clean.sql) su raw_input. Output parquet normalizzato.",
            "output": "Parquet in data/clean/<dataset>/<anno>/",
            "validation": "_validate/clean_validation.json",
            "view": RAW_INPUT_VIEW,
        },
        {
            "name": "MART",
            "description": "Aggregazione SQL (mart.sql) su clean_input. Output parquet per data-explorer/notebook.",
            "output": "Parquet in data/mart/<dataset>/<anno>/",
            "validation": "_validate/mart_validation.json",
            "view": CLEAN_INPUT_VIEW,
        },
    ],
    "config_file": "dataset.yml",
    "config_docs": "docs/config-schema.md",
    "conventions_docs": "docs/conventions.md",
    "macros_docs": "docs/standard-macros.md",
}

# ── Comandi CLI disponibili ──────────────────────────────────────────────
_CLI_COMMANDS: list[dict[str, str]] = [
    {
        "command": "toolkit run raw --config dataset.yml",
        "description": "Scarica i file raw dalla fonte e produce il profilo.",
        "when": "Prima esecuzione o quando cambi fonte.",
    },
    {
        "command": "toolkit run init --config dataset.yml",
        "description": "Bootstrap: run raw + scaffold clean.sql se assente.",
        "when": "Primo avvio di un nuovo dataset.",
    },
    {
        "command": "toolkit run clean --config dataset.yml",
        "description": "Applica clean.sql ai dati raw e produce parquet CLEAN.",
        "when": "Dopo aver scritto/modificato clean.sql.",
    },
    {
        "command": "toolkit run mart --config dataset.yml",
        "description": "Genera tabelle MART dai dati clean.",
        "when": "Dopo clean, per produrre dataset pubblici.",
    },
    {
        "command": "toolkit run all --config dataset.yml",
        "description": "Pipeline completa: raw -> clean -> mart in un comando.",
        "when": "Esecuzione standard dopo aver configurato tutto.",
    },
    {
        "command": "toolkit run preflight --config dataset.yml",
        "description": "Diagnostica: valida config, verifica fonti, quality score CSV.",
        "when": "Prima di run all, per scoprire problemi.",
    },
    {
        "command": "toolkit scout <URL>",
        "description": "Esplora una fonte esterna: probe + routing + infer schema.",
        "when": "Quando devi capire se una fonte e' utile.",
    },
    {
        "command": "toolkit inspect summary --config dataset.yml",
        "description": "Stato ultimo run del dataset.",
        "when": "Per debug o verifica.",
    },
    {
        "command": "toolkit contract --layer <raw|clean|mart|all>",
        "description": "Mostra i contratti di pipeline (questo comando).",
        "when": "PRIMA di scrivere SQL o dataset.yml.",
    },
]

# ── Struttura dataset.yml (quick reference) ─────────────────────────────
_CONFIG_QUICKREF: dict[str, Any] = {
    "required_top_level_fields": [
        "dataset.name (nome del dataset)",
        "dataset.years (lista anni, es: [2024])",
        "raw.sources (almeno una fonte)",
    ],
    "common_optional_fields": [
        "clean.sql (path al file SQL di pulizia)",
        "clean.read.delim (delimitatore CSV, default auto-detect)",
        "clean.read.encoding (encoding CSV, default utf-8, PA spesso cp1252)",
        "clean.read.decimal (separatore decimale, default '.'; usa ',' per italiano)",
        "clean.required_columns (lista colonne OUTPUT attese nel clean)",
        "mart.tables (lista tabelle MART con nome e path SQL)",
        "mart.tables[].years (per tabelle multi-anno)",
        "mart.validate.table_rules (regole per tabella: primary_key, not_null, ranges)",
        "raw.sources[].type (http_file, ckan, sdmx, sparql, local_file)",
        "raw.sources[].extractor (identity, unzip_all, unzip_first, unzip_first_csv)",
        "support (lista dataset di supporto, eseguiti prima del candidate)",
    ],
    "minimal_example": """dataset:
  name: mio_dataset
  years: [2024]
raw:
  sources:
    - type: http_file
      args:
        url: "https://esempio.it/dati.csv"
        filename: dati_{year}.csv
clean:
  sql: sql/clean.sql
mart:
  tables:
    - name: riepilogo
      sql: sql/mart/riepilogo.sql
""",
}


# ── Contratto completo ───────────────────────────────────────────────────
# Struttura stabile: nuove chiavi sono additive e backward-compatibili.
CONTRACTS: dict[str, Any] = {
    "version": "1",
    "pipeline": _PIPELINE_CONTRACT,
    "raw": _RAW_CONTRACT,
    "clean": _CLEAN_CONTRACT,
    "mart": _MART_CONTRACT,
    "cli": _CLI_COMMANDS,
    "config": _CONFIG_QUICKREF,
    "constants": {
        "RAW_INPUT_VIEW": RAW_INPUT_VIEW,
        "CLEAN_INPUT_VIEW": CLEAN_INPUT_VIEW,
        "SOURCE_INPUT_VIEW": SOURCE_INPUT_VIEW,
        "YEAR_PLACEHOLDER": YEAR_PLACEHOLDER,
    },
    "tldr": (
        "PRIMA: toolkit contract --layer raw|clean|mart  |  "
        "dataset.yml: name + years + raw.sources + clean.sql + mart.tables  |  "
        "raw: http_file/ckan/sdmx/sparql/local_file  |  "
        "clean.sql: SELECT ... FROM raw_input  |  {year} per l'anno  |  "
        "usa le macro (normalize_string, cast_int, ...)  |  "
        "required_columns = nomi OUTPUT del clean, non raw  |  "
        "se decimal=',' basta CAST(x AS DOUBLE)  |  "
        "mart.sql: SELECT ... FROM clean_input  |  "
        "comandi: toolkit run init / preflight / all / scout / inspect"
    ),
}
