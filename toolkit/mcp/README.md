# Toolkit MCP

Server MCP locale, read-only, per ispezionare rapidamente path risolti, schemi e stato run del `toolkit`.

## Tool esposti

- `toolkit_inspect_paths(config_path, year=0)` — path contract + run metadata (run_file_count, years_seen, latest_run)
- `toolkit_show_schema(config_path, layer="clean", year=0)`
- `toolkit_run_summary(config_path, year=0)` — statistiche aggregate (totali, successi, durata media)
- `toolkit_summary(config_path, year=0)` — dashboard diagnostico (layer + run + warnings)
- `toolkit_blocker_hints(config_path, year=0)`
- `toolkit_review_readiness(config_path, year=0)`
- `toolkit_list_runs(config_path, year=0, since=None, until=None, status=None, limit=20, cross_year=False)`

## Boundary

Questo MCP resta nel repo `toolkit` perche' espone solo introspezione tecnica del contract del motore:

- path contract risolto
- schema `raw`, `clean`, `mart`
- stato minimo dei run
- readiness check per review candidate

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
- `toolkit_show_schema`
  - `raw`: usa `toolkit inspect schema-diff --json`
  - `clean` / `mart`: legge schema reale dei parquet risolti via `inspect paths`
- `toolkit_run_summary` aggrega tutti i run record per dataset/year
- `toolkit_summary` include `run.latest_run_record` (payload completo dell'ultimo run)
- `toolkit_blocker_hints` evidenzia mismatch pratici tra output risolti e stato run
- `toolkit_review_readiness` esegue check di readiness per review candidate: config valida, layer presenti, output leggibili, coerenza run record
