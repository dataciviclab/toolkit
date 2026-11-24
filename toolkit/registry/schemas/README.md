# Registry schema — contratto condiviso Lab

Schema canonico dell'artifact registry generato dal builder condiviso
(`toolkit.registry`). Ogni repo (dataset-incubator, eurostat, dcl-bologna, ...)
genera il proprio `registry.json` con questo contratto e lo committa.

**Versione corrente: `schema_version: 2`.** Il reader accetta anche v1: la
compatibilità è centralizzata in `lab_connectors.registry.models.Registry.from_dict`,
che sintetizza le `location` v2 dal contratto `gcs`.

## Il modello unico (fusion ADR)

Un solo file `registry.json` con quattro sezioni — stesso stato (dataset.yml +
run records + parquet locali):

| Sezione | Risponde a | Contenuto | Consumatori |
|---|---|---|---|
| `datasets` | "cosa esiste e com'è fatto" | slug, columns (role/semantic_type), period, **partitions** (partizione → righe clean), **last_run**, mart_refs | ACB → dcl-mcp, data-explorer, resolver, dashboards |
| `marts` | "cosa esiste a livello analitico" | tabelle, **layout** (flat/year/glob), year, **primary_key** | dcl-mcp, data-explorer, `support external` |
| `checks` | "cosa non va" | anomalie riproducibili dal config (`warn`/`error`), vuoto = ok | ACB (triage/bootstrap) |
| `codelists` | quali dimensioni hanno un codelist | nomi dei CSV in `codelists/` (contenuto letto on-demand) | agenti MCP |

Il contratto di pubblicazione vive nel blocco `gcs`:

```json
"gcs": {"prefix": "open-politica/", "clean": "year"}
```

I path **non** sono scritti voce per voce: `lab_connectors.registry.locations`
li deriva da `gcs` + `partitions` + `marts[].layout`. Verificato su tutto il
workspace: 544/544 clean e 1.197/1.197 mart identici al builder precedente.

## Regole di derivazione (lab-connectors)

| Layer | Condizione | Path |
|---|---|---|
| clean | `gcs.clean=year`, 1 partizione | `{prefix}{slug}/{year}/{slug}_{year}_clean.parquet` |
| clean | `gcs.clean=year`, >1 partizioni | `{prefix}{slug}/*/{slug}_*_clean.parquet` |
| clean | `gcs.clean=flat`, 1 partizione | `{prefix}{slug}/{slug}_{year}_clean.parquet` |
| clean | `gcs.clean=flat`, >1 partizioni | `{prefix}{slug}/{slug}_*_clean.parquet` |
| clean | nessuna partizione (mart-only) | nessuna location |
| mart | `layout=flat` | `{prefix}{dataset}/{table}.parquet` |
| mart | `layout=year` | `{prefix}{dataset}/{year}/{table}.parquet` |
| mart | `layout=glob` | `{prefix}{dataset}/*/{table}.parquet` |

Regole del builder:
- `partitions`: partizioni con parquet locale; le partizioni non rieseguite
  (CI per-dataset) sono preservate dal registry committato (il locale vince).
- `last_run`: record con `started_at` massimo su **tutte** le partizioni
  (non l'anno più alto: per i dataset previsionali l'anno massimo è nel futuro).
- `clean_rows` non sta nel file: è `sum(partitions.values())`, calcolata a valle.
- `period`: copertura dichiarata (`time_coverage`), non le partizioni.
- Campi vuoti non scritti (regola `_omit_empty`).
- `marts[].layout`: tabella con `years` esplicite → flat; repo `--flat` → flat;
  dataset multi-anno → glob; mono-anno → year.

## Compatibilità

Il reader accetta esclusivamente `registry.json` (fusion ADR). Il fallback
legacy sui vecchi file separati (`clean_catalog.json`, `mart_catalog.json`,
`pipeline_signals.json`, `codelists.json`, `entity_graph.json`) è stato
rimosso (PR #468): tutti i repo del workspace sono migrati.

Regole di transizione v1 → v2:
- un repo non ancora rigenerato resta leggibile (v1: `location` per voce,
  `signals`, `stage`); ACB ne deriva `last_run`/`clean_rows`;
- un consumer che vede `schema_version: 1` non deve assumere `partitions`.
