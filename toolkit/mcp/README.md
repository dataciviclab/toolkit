# Toolkit MCP

Server MCP locale, read-only, per ispezionare la pipeline e il catalogo
dataset del DataCivicLab.

## Tool esposti (15)

### Catalogo (slug-based — GCS + workspace)

- `toolkit_find(query="", source="all", layer=None, limit=15, stage="all", status_filter=None)` —
  cerca dataset per slug, source, layer. `source="gcs"` = pubblicati,
  `source="workspace"` = in sviluppo (con run status), `source="all"` (default) = unione.
  Restituisce `{datasets, total_count, truncated}`.
- `toolkit_dataset_overview(slug, layer="clean", year=None, source="all")` —
  schema colonne (DuckDB DESCRIBE) + row count + preview per slug.
  `source="gcs"` | `"workspace"` | `"all"` (default).

### Tool aggregati (raccomandati per pipeline)

- `toolkit_layer(config_path=None, datasets=None, layer="clean", mode="schema", year=0, limit=20, sql=None, mart_index=0, table=None)` —
  query unificata RAW/CLEAN/MART. Due modalita':
  - `config_path` (pipeline): dataset locale da dataset.yml
  - `datasets` (catalogo): lista slug, risolti via GCS manifest + workspace
  - `mode`: `schema`, `preview`, `profile` (solo raw), `sql` (SQL su vista `data`)
  - `layer=raw` con `mode=sql`: legge CSV via DuckDB `read_csv_auto`
  - `layer=mart` e `table` (es. `"mart_top_sa"`): seleziona tabella mart specifica
- `toolkit_status(config_path, year=0)` —
  stato completo dataset: paths + summary + readiness + run_stats + info

### Tool granulari (ispezione pipeline)

- `toolkit_inspect_paths(config_path, year=0)` — path contract + run metadata
- `toolkit_inspect_schema(config_path, layer="clean", year=0)`
- `toolkit_inspect_profile(config_path, year=0)` — profilo raw (encoding, delim, colonne)
- `toolkit_list_runs(config_path, year=0, since=None, until=None, status=None, limit=20, cross_year=False)`
- `toolkit_list_candidates` — **RIMOSSO**: usa `toolkit_find(source="workspace")`
- `toolkit_schema_diff(config_path)` — confronto segnali schema raw cross-year
- `toolkit_preflight(config_path, years=None)` — pre-flight check: valida config, verifica fonti, quality score PA

### Scout fonti

- `toolkit_probe_url(url, timeout=15, routed=False)` — probe HTTP.
  `routed=True` attiva routing automatico (CKAN, SDMX, HTML, file diretto)
- `toolkit_ckan_package_show(endpoint, package_id, timeout=30)` — fetch dataset CKAN
- `toolkit_html_extract_links(url, timeout=20)` — estrae link dati da HTML
- `toolkit_sparql_query(endpoint, query, timeout=60, max_rows=500)` — SPARQL SELECT
- `toolkit_preview_url(url, known_encoding=None, known_delim=None, known_decimal=None, known_skip=None)` —
  preview remoto CSV/TSV (HEAD + Range + sniff + DuckDB)

## Config workspace

Esempio `.mcp.json`:

```json
"toolkit": {
  "command": "/path/to/python",
  "args": ["-m", "toolkit.mcp.server"]
}
```

Sostituire il path del `command` con il Python reale del clone locale.

## Note tecniche

- I tool catalogo usano `gcs_manifest.json` (auto-generato dalla CI,
  pubblicato su GCS) come fonte di verità per i dataset pubblicati
- `toolkit_find` unifica GCS + workspace locale (clean parquet + dataset.yml)
- `list_candidates` è deprecato: l'unificazione è in `toolkit_find(source="workspace")`
- `toolkit_layer(mode=sql, datasets=[...])` usa DuckDB con CTE multipli
  e scope validation (blocca DDL, read_parquet, tabelle non consentite)
- `toolkit_csv_preview` riusa la stessa pipeline di `profile_raw`
- `toolkit_inspect_paths` usa `toolkit inspect paths --json` internamente
