# Registry schema — contratto condiviso Lab

Schema canonico dell'artifact registry generato dal builder condiviso
(`toolkit.registry`). Ogni repo (dataset-incubator, eurostat, dcl-bologna, ...)
genera il proprio `registry.json` con questo contratto e lo committa.

## Il modello unico (fusion ADR)

Un solo file `registry.json` con cinque sezioni — stesso stato (dataset.yml +
run records + parquet locali + GCS):

| Sezione | Risponde a | Contenuto | Consumatori |
|---|---|---|---|
| `datasets` | "cosa esiste e com'è fatto" (clean) | slug, columns (role/semantic_type), period, location, stage, run | clean-query, data-explorer, resolver |
| `marts` | "cosa esiste a livello analitico" (mart) | tabelle, PK, required_columns, min_rows, location, run | clean-query, data-explorer |
| `signals` | "come è andato e cosa manca" (operativo) | status ok/warn/error, years, run, detail, action | ACB (bootstrap/triage) |
| `codelists` | "quali valori hanno le dimensioni" | mappa nome-codelist → righe (code, label_en, ...) | agenti MCP (risoluzione codici) |
| `entities` | "con chi si collega ogni dataset" | grafo entità → dataset, bridges | lab-dashboard, clean-query (dataset_graph) |

Le definizioni condivise (dataset, column, location, run, mart, signal) sono
inline come `$defs` — nessun `$ref` esterno.

## Compatibilità

Reader e resolver hanno fallback legacy: i repo con i vecchi file separati
(`clean_catalog.json`, `mart_catalog.json`, `pipeline_signals.json`,
`codelists.json`, `entity_graph.json`) continuano a funzionare finché non
migrano a `registry.json`.
