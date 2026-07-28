# Toolkit MCP

Server MCP locale, read-only, per ispezionare la pipeline e il catalogo
dataset del DataCivicLab.

## Tool esposti (12)

### Catalogo (slug-based — GCS + workspace)

- `toolkit_find` — cerca dataset per slug, source, layer, stage, run status
- `toolkit_dataset_overview` — schema colonne + row count + preview per slug

### Pipeline (config-based — dataset.yml locale)

- `toolkit_layer` — query unificata RAW/CLEAN/MART: schema, preview, profile, SQL. Due modalita':
  `config_path` (pipeline locale) o `datasets` (catalogo GCS/workspace)
- `toolkit_status` — stato completo dataset: paths, summary, readiness, run_stats, info
- `toolkit_schema_diff` — confronto segnali schema raw cross-year
- `toolkit_preflight` — pre-flight check: valida config, verifica fonti, quality score
- `toolkit_list_runs` — run record con filtri (status, data, limit)

### Scout fonti

- `toolkit_probe_url(url, routed=False)` — probe HTTP con routing automatico opzionale
- `toolkit_ckan_package_show` — fetch metadati dataset CKAN
- `toolkit_html_extract_links` — estrae link dati da pagina HTML
- `toolkit_sparql_query` — SPARQL SELECT su endpoint pubblico
- `toolkit_preview_url` — preview remoto CSV/TSV (HEAD + Range + sniff + DuckDB)

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
- `list_candidates` è stato rimosso: usa `toolkit_find(source="workspace")`
- `toolkit_layer(mode=sql, datasets=[...])` usa DuckDB con CTE multipli
  e scope validation (blocca DDL, read_parquet, tabelle non consentite)
- `inspect_paths/schema/profile` sono stati rimossi come tool MCP —
  coperti da `toolkit_status` e `toolkit_layer(mode="schema")`
