# Registry schemas — contratto condiviso Lab

Schemi canonici degli artifact registry generati dal builder condiviso
(`lab_connectors.registry`, Fase 1). Ogni repo (dataset-incubator, eurostat,
dcl-bologna, ...) genera i propri JSON con questi contratti e li committa.

## Il modello a 4 viste

Stesso stato (dataset.yml + run records + parquet locali + `--check-gcs`),
quattro proiezioni:

| Artifact | Risponde a | Contenuto | Consumatori |
|---|---|---|---|
| `clean_catalog.json` | "cosa esiste e com'è fatto" (clean) | slug, columns (role/semantic_type), period, location, stage | clean-query, data-explorer, resolver |
| `mart_catalog.json` | "cosa esiste a livello analitico" (mart) | tabelle, PK, required_columns, min_rows, location, run | clean-query, data-explorer |
| `pipeline_signals.json` | "come è andato e cosa manca" (operativo) | status ok/warn/error, years, run, detail, action | ACB (bootstrap/triage) |
| `codelists.json` | "quali valori hanno le dimensioni" | mappa nome-codelist → righe (code, label_en, ...) | agenti MCP (risoluzione codici) |

Il codelists è opzionale: assente se il repo non ha una dir `codelists/`.

Regole di coerenza:

- **Namespace id**: slug pulito (clean) → `{dataset}__{mart}` (mart, pattern
  validato) → eventuale prefisso `compose:` (signals). 1 clean → N mart.
- **Due dimensioni ortogonali**: `stage` (pubblicazione, nei cataloghi) vs
  `status` (salute strutturale, nei signals). `status=ok` NON significa
  pubblicato.
- **`period` vs `years`**: `period` = copertura dati (da `time_coverage`);
  `years` = anni di run (da `years` del dataset.yml).
- **Blocco `run`**: vedi `run.schema.json` — inclusa inline in ogni schema
  (`$defs.run`) perché i validatori standalone non risolvono `$ref` esterni.

## Versionamento

- `clean_catalog.schema.json`: v1 identico a dataset-incubator + aggiunte
  opzionali (`years`, `mart_refs`, `run`) — backward compatible.
- `pipeline_signals.schema.json`: v1 identico a dataset-incubator + aggiunte
  opzionali (`years`, `run`) — backward compatible. `latest_run` (sample run
  GHA) resta distinto da `run` (run record toolkit).
- `mart_catalog.schema.json`: v1 nuovo. `columns` opzionale in v1.
- `codelists.schema.json`: v1 nuovo (mappa nome-codelist → righe CSV).
- `run.schema.json`: blocco condiviso, fonte canonica.

## Migrazione

- dataset-incubator mantiene copie locali in `registry/` fino alla Fase 2
  (CLI wrapper che generano con gli schemi condivisi, output identico).
