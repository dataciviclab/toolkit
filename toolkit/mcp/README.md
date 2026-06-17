# Toolkit MCP

Server MCP locale, read-only, per ispezionare rapidamente path risolti, schemi e stato run del `toolkit`.

## Tool esposti

### Tool aggregati (raccomandati)

- `toolkit_layer(config_path, layer="clean", mode="schema", year=0, limit=20, sql=None, mart_index=0)` — query unificata RAW/CLEAN/MART. Mode: `schema` (colonne+tipi), `preview` (anteprima righe), `profile` (diagnostica raw), `sql` (SQL arbitrario su vista `data`)
- `toolkit_status(config_path, year=0)` — stato completo dataset: paths + summary + readiness + run_stats + info in una chiamata

### Tool granulari (ispezione pipeline)

- `toolkit_inspect_paths(config_path, year=0)` — path contract + run metadata (run_file_count, years_seen, latest_run)
- `toolkit_inspect_schema(config_path, layer="clean", year=0)`
- `toolkit_inspect_profile(config_path, year=0)` — profilo raw (encoding, delim, colonne, missingness)
- `toolkit_run_summary(config_path, year=0)` — statistiche aggregate (totali, successi, durata media)
- `toolkit_summary(config_path, year=0)` — dashboard diagnostico (layer + run + warnings)
- `toolkit_review_readiness(config_path, year=0)` — check di prontezza per review candidate
- `toolkit_list_runs(config_path, year=0, since=None, until=None, status=None, limit=20, cross_year=False)`
- `toolkit_list_candidates(stage="all", status_filter=None)` — elenca dataset disponibili in workspace
- `toolkit_schema_diff(config_path)` — confronto segnali schema raw cross-year (encoding, colonne, ecc.)
- `toolkit_csv_preview(csv_path, limit=20)` — schema + preview CSV via profiler pipeline

### Tool aggregati (raccomandati)

- `toolkit_status(config_path, year=0)` — stato completo: paths + summary + readiness + run_stats + info
- `toolkit_layer(config_path, layer="clean", mode="schema")` — query unificata: schema/preview/profile/sql

### Scout fonti

- `toolkit_probe_url(url, timeout=15)` — probe HTTP leggero (HEAD + Range): reachability, status code, content-type
- `toolkit_probe_url_routed(url, timeout=15)` — probe arricchito con routing automatico (rileva CKAN, SDMX, HTML, file diretto)
- `toolkit_ckan_package_show(endpoint, package_id, timeout=30)` — fetch dataset CKAN via API `package_show`
- `toolkit_html_extract_links(url, timeout=20)` — estrae link a file dati (CSV, JSON, XLSX, ZIP, XML) da pagina HTML
- `toolkit_sparql_query(endpoint, query, timeout=60, max_rows=500)` — esegue query SPARQL SELECT su endpoint pubblico

## Boundary

Questo MCP resta nel repo `toolkit` perche' espone solo introspezione tecnica del contract del motore e servizi di base per scouting fonti:

- path contract risolto
- schema `raw`, `clean`, `mart`
- stato minimo dei run
- readiness check per review candidate
- probe URL, inferenza topic, fetch CKAN/SPARQL/HTML

Non espone:

- `support_resolve`
- `list_candidates`
- logica di workspace state o catalogo

## Config workspace

Esempio `.mcp.json`:

```json
"toolkit": {
  "command": "C:\\path\\to\\toolkit\\.venv\\Scripts\\python.exe",
  "args": [
    "-m",
    "toolkit.mcp.server"
  ]
}
```

Sostituire il path del `command` con il Python reale del clone locale che usera' il server.

## Note tecniche

- `toolkit_inspect_paths` usa `toolkit inspect paths --json`; arricchito con run_file_count e years_seen dalla CLI
- `toolkit_inspect_schema`
  - `raw`: usa `toolkit inspect schema-diff --json`
  - `clean` / `mart`: legge schema reale dei parquet risolti via `inspect paths`
- `toolkit_schema_diff` confronta segnali schema raw (encoding, delim, colonne) tra tutti gli anni configurati per il dataset; riutilizza la stessa logica di `toolkit inspect schema-diff` ma esposto come tool MCP
- `toolkit_csv_preview` legge un CSV usando la stessa pipeline di `profile_raw` (`sniff_source_file` + `profile_with_read_cfg`); restituisce schema + prime N righe + mapping_suggestions — utile per ispezionare file raw senza runnare la pipeline
- `toolkit_run_summary` aggrega tutti i run record per dataset/year
- `toolkit_summary` include `run.latest_run_record` (payload completo dell'ultimo run)
- `toolkit_review_readiness` esegue check di readiness per review candidate: config valida, layer presenti, output leggibili, coerenza run record
