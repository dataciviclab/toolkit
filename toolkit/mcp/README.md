# Toolkit MCP

Server MCP locale, read-only, per ispezionare rapidamente path risolti, schemi e stato run del `toolkit`.

## Tool esposti

- `toolkit_inspect_paths(config_path, year=0)`
- `toolkit_show_schema(config_path, layer="clean", year=0)`
- `toolkit_run_state(config_path, year=0)`
- `toolkit_summary(config_path, year=0)`

## Boundary

Questo MCP resta nel repo `toolkit` perche' espone solo introspezione tecnica del contract del motore:

- path contract risolto
- schema `raw`, `clean`, `mart`
- stato minimo dei run

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

- `toolkit_inspect_paths` usa `toolkit inspect paths --json`
- `toolkit_show_schema`
  - `raw`: usa `toolkit inspect schema-diff --json`
  - `clean` / `mart`: legge schema reale dei parquet risolti via `inspect paths`
- `toolkit_run_state` legge `latest_run` e il relativo record JSON se presente
