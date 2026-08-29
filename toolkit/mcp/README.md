# Toolkit MCP

Server MCP locale, read-only, per ispezionare la pipeline e il catalogo
dataset del DataCivicLab.

## Tool esposti (5)

Ogni tool ha un parametro `action` che fa da dispatch.

### `toolkit_dataset` — ispezione dataset

| Action | Params chiave | Cosa fa |
|---|---|---|
| `find` | query, layer, limit, source, stage, status_filter | Cerca dataset per slug/testo/source |
| `overview` | slug, layer, year, source, profile | Schema colonne + row count + preview |
| `status` | config_path, year, since, until | Stato completo: paths, readiness, run_stats |
| `preflight` | config_path, years | Diagnostica pre-run: valida config, verifica fonti |
| `schema-diff` | config_path | Confronto segnali schema raw cross-year |

### `toolkit_query` — query dati

| Action | Params chiave | Cosa fa |
|---|---|---|
| `run` | datasets, sql, layer, mode, year, limit, dry_run | SQL su raw/clean/mart (catalog o pipeline mode) |
| `preview` | url, known_encoding, known_delim, known_decimal, known_skip | Preview remoto CSV/TSV |

### `toolkit_pipeline` — contratti, run history, registry

| Action | Params chiave | Cosa fa |
|---|---|---|
| `contract` | layer | Contratti pipeline (raw/clean/mart/all) |
| `runs` | config_path, year, since, until, status, limit | Run record con filtri |
| `registry_list` | — | Elenca artifact registry committati |
| `registry_show` | repo, artifact, slug | Mostra artifact registry |
| `graph` | by_key, by_dataset, by_registry, by_domain | Mappa relazioni cross-dataset |

### `toolkit_source` — fonti dati esterne

| Action | Params chiave | Cosa fa |
|---|---|---|
| `probe` | url, timeout | Probe HTTP: reachability + routing auto |
| `ckan` | endpoint, package_id, timeout | Fetch metadati dataset CKAN |
| `links` | url, timeout | Estrae link dati da pagina HTML |
| `sparql` | endpoint, query, timeout, max_rows | SPARQL SELECT su endpoint pubblico |

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

- I tool usano i `registry.json` committati nei repo del workspace
  (fusion ADR — path GCS esatti per repo) come fonte per i dataset pubblicati
- `toolkit_dataset(action="find")` unifica GCS + workspace locale (clean parquet + dataset.yml)
- `toolkit_query(action="run", mode=sql, datasets=[...])` usa DuckDB con CTE multipli
  e scope validation (blocca DDL, read_parquet, tabelle non consentite)
- Le implementazioni `*_ops.py` non sono cambiate — solo il layer MCP in `server.py`
