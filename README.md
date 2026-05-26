# DataCivicLab Toolkit

Motore dati riproducibile per dataset pubblici italiani (e non).

Prende dati grezzi da fonti eterogenee (HTTP, CKAN, SDMX, SPARQL, file locali),
li normalizza e li aggrega in parquet pronti per analisi — con un contratto
chiaro tra ogni layer.

---

## Per chi è questo toolkit

| Se sei… | toolkit fa per te perché… |
|---|---|
| **Autore di dataset** nel Lab | scrivi `dataset.yml` + SQL, toolkit esegue e produce parquet RAW → CLEAN → MART |
| **Analista / civic data person** | consumi i parquet già prodotti via `data-explorer` o notebook — qui trovi come sono stati generati |
| **Sviluppatore del motore** | contribuisci a `raw/`, `clean/`, `mart/`, `plugins/` — questo è il repo |

**Toolkit** fa parte dell'ecosistema [DataCivicLab](https://github.com/dataciviclab):
è il motore della pipeline. I dataset reali e le loro config vivono in
[dataset-incubator](https://github.com/dataciviclab/dataset-incubator).

---

## Primi passi in 3 comandi

```bash
pip install -e .[dev]

toolkit run all -c project-example/dataset.yml
toolkit validate all -c project-example/dataset.yml
```

Output prodotto in `project-example/_smoke_out/data/clean/project_example/2022/`. Trova il percorso esatto con `toolkit inspect paths --config project-example/dataset.yml`.

Se `toolkit` non è nel PATH:

```bash
python -m toolkit.cli.app run all -c project-example/dataset.yml
```

---

## Pipeline: tre livelli di dati

Il toolkit trasforma i dati attraverso tre layer progressivi.
Ogni layer è una directory separata con proprietà distinte:

```
  RAW ──→ CLEAN ──→ MART
```

| Layer | Cosa contiene | Chi lo usa |
|---|---|---|
| **RAW** | Il file originale scaricato, esattamente come dalla fonte, senza modifiche. Manifest e metadata registrano provenienza e hash. | Audit, verifica fonte, debug |
| **CLEAN** | Dato normalizzato: nomi colonna coerenti, tipi fissi, valori puliti, schema stabile tra anni. Formato: parquet. | Notebook, analisi SQL, data-explorer |
| **MART** | Il dato aggregato, raggruppato per le dimensioni rilevanti, con metriche pronte. Un dataset può avere più tabelle MART. | Report, dashboard, insight rapidi |

Ogni run produce anche `metadata.json` e `validation.json` per audit trail,
e un run record in `_runs/` per tracciabilità e resume.

---

## Come si inserisce nell'ecosistema

```
source-observatory ──→ dataset-incubator ──→ [toolkit] ──→ GCS ──→ data-explorer
                                                    ↑
                                               MCP server
                                           (agenti AI, IDE)
```

| Componente | Ruolo |
|---|---|
| **source-observatory** | Scouting e health check delle fonti pubbliche |
| **dataset-incubator** | Incubazione dei candidati: `dataset.yml`, SQL, notebook — triggera i run del toolkit in CI |
| **toolkit** | **Qui.** Esegue RAW → CLEAN → MART, produce parquet e metadata |
| **GCS** | Storage degli artifact validati (bucket `dataciviclab-clean`) |
| **data-explorer** | Frontend pubblico sui parquet puliti |
| **MCP server** | Integrazione AI: ispezione read-only di path, schema, run (vedi sotto) |

Il toolkit non gestisce il deployment: scrive nella directory configurata via
`root` o `DCL_ROOT`. La CI di `dataset-incubator` carica su GCS dopo ogni run validato.

---

## CLI — panoramica

### Esecuzione

| Caso d'uso | Comando |
|---|---|
| Prima esecuzione del dataset | `toolkit run all --config dataset.yml` |
| Cambiato solo SQL di clean | `toolkit run clean --config dataset.yml` + `toolkit run mart` |
| Cambiato solo SQL di mart | `toolkit run mart --config dataset.yml` |
| Run interrotto (artefatti coerenti) | `toolkit resume --dataset <name> --year <year> --config dataset.yml` |
| Aggiunto/modificato tabella multi-anno | Aggiungere `years: [2022, 2023]` alla tabella in `mart.tables[]` |

### Diagnostica

| Comando | Cosa fa |
|---|---|
| `toolkit inspect paths --config dataset.yml --year 2023` | Mostra path assoluti degli output (utile nei notebook). Esempio: `toolkit inspect paths --config project-example/dataset.yml --json` |
| `toolkit inspect schema-diff --config dataset.yml` | Confronta schema RAW tra anni configurati |
| `toolkit review-readiness --config dataset.yml` | Check di prontezza per review candidate (raccomandato) |
| `toolkit status --dataset <name> --year <year> --latest --config dataset.yml` | Ultimo run completato |
| `toolkit inspect profile --config dataset.yml` | Profilo diagnostico del RAW (encoding, delimitatore, colonne) — scrive `raw_profile.json` e `suggested_read.yml` |

### Altri comandi

| Comando | Cosa fa |
|---|---|
| `toolkit scout <URL>` | Esplora URL esterno (HTTP/CKAN/SDMX/HTML) — probe + routing + inferenze |
| `toolkit scout <URL> --scaffold` | Probe + scaffold candidato completo (dataset.yml, SQL, README) |
| `toolkit scout <URL> --run` | Probe + scaffold + raw run |
| `toolkit scaffold <slug>` | Genera scheletro `dataset.yml` + SQL da un template |
| `toolkit batch --file jobs.yml` | Esegue più dataset in sequenza |

---

## Configurazione (`dataset.yml`)

Il cuore del toolkit è un file YAML che descrive il dataset:

```yaml
dataset:
  name: mio_dataset
  years: [2023]

raw:
  sources:
    - type: http_file
      url: https://example.com/dati.csv

clean:
  sql: sql/clean.sql

mart:
  tables:
    - name: basic
      sql: sql/mart/basic.sql
```

Il toolkit risolve i path relativi rispetto alla directory del `dataset.yml`,
esegue le trasformazioni SQL su DuckDB e produce output in `root/data/`.

**Documenti di riferimento:**

| Documento | Contenuto |
|---|---|
| [config-schema.md](docs/config-schema.md) | Specifica completa del YAML (475 righe) |
| [conventions.md](docs/conventions.md) | Convenzioni su path, metadata, manifest, artifact policy |
| [advanced-workflows.md](docs/advanced-workflows.md) | Resume, run parziali, profile, debug |
| [notebook-contract.md](docs/notebook-contract.md) | Come leggere gli output nei notebook |
| [feature-stability.md](docs/feature-stability.md) | Cosa è stabile, cosa sperimentale, cosa deprecated |

### Plugin sorgente supportati

| `raw.sources[].type` | Fonte |
|---|---|
| `http_file` | File da URL HTTP(S), anche zippato |
| `http_post_file` | File da URL HTTP(S) via POST con form-encoded body |
| `local_file` | File sul filesystem locale |
| `ckan` | Dataset da portali CKAN (via API) |
| `sdmx` | Flussi SDMX (es. ISTAT) |
| `sparql` | Query SPARQL |

---

## MCP Server

Il toolkit espone un server **MCP (Model Context Protocol)** per integrazione con agenti AI e IDE.
Espone 9 tool read-only per ispezione rapida:

| Tool | Cosa fa |
|---|---|
| `toolkit_inspect_paths` | Path contract risolto + metadati run |
| `toolkit_show_schema` | Schema di raw / clean / mart |
| `toolkit_run_summary` | Statistiche aggregate dei run |
| `toolkit_summary` | Dashboard diagnostico per dataset |
| `toolkit_review_readiness` | Check di prontezza per review |
| `toolkit_list_runs` | Run records con filtri |
| `toolkit_schema_diff` | Confronto schema raw cross-year |
| `toolkit_csv_preview` | Schema + preview CSV via profiler pipeline |

Config esempio per IDE (`.mcp.json`):

```json
{
  "toolkit": {
    "command": "/path/to/python",
    "args": ["-m", "toolkit.mcp.server"]
  }
}
```

Dettaglio: [toolkit/mcp/README.md](toolkit/mcp/README.md).

---

## Sviluppo

Python 3.10+. Installa con dev extras:

```bash
pip install -e .[dev]
```

### Eseguire i test

```bash
pytest -m core                    # contratto pubblico e workflow canonico
pytest -m "core or advanced"      # tutto tranne compat legacy
pytest                            # tutto (55 test file)
ruff check .                      # lint
```

I test sono stratificati con marker:

| Marker | Cosa copre |
|---|---|
| `core` | Contratto pubblico e percorso canonico — **deve sempre passare** |
| `advanced` | Comportamenti secondari, run parziali, profile |
| `compat` | Solo compatibilità legacy e shim |

### Contribuire

- [CONTRIBUTING.md](CONTRIBUTING.md) — test suite tiers, pre-commit hook
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)

### CI

`.github/workflows/ci.yml` — test su Python 3.10–3.12, ruff, coverage ≥70%, build pacchetto.

---

## Struttura del repo

```
toolkit/
  .github/workflows/     # CI (ci.yml)
  toolkit/                # sorgente del package Python
    cli/                  # comandi CLI (typer)
    core/                 # engine condiviso: config, path, run record, manifest, multi_year_source
    core/config_models/   # modello tipizzato Pydantic di dataset.yml
    raw/                  # layer RAW: estrazione, run, validazione
    clean/                # layer CLEAN: lettura CSV/Excel, DuckDB, validazione
    mart/                 # layer MART: aggregazione SQL, validazione
    plugins/              # plugin sorgente (http_file, ckan, sdmx, sparql, local_file)
    profile/              # profiling RAW: encoding, delimitatore, colonne
    mcp/                  # server MCP per agenti AI
    scaffold/             # generazione scheletri dataset
  tests/                  # pytest (55 file)
  smoke/                  # smoke test su scenari reali
  docs/                   # documentazione tecnica (5 file)
  scripts/                # script di supporto (pre-commit, build)
  project-example/        # dataset.yml e SQL di esempio
  examples/               # esempi d'uso
```

---

## FAQ — problemi comuni

| Problema | Soluzione |
|---|---|
| `toolkit: command not found` | Usa `python -m toolkit.cli.app` al posto di `toolkit` |
| `run all` fallisce | `toolkit review-readiness --config dataset.yml` + controlla che la fonte sia raggiungibile |
| "dove sono i parquet prodotti?" | `toolkit inspect paths --config dataset.yml --year <anno>` o cerca in `root/data/` |
| "errore schema tra anni diversi" | `toolkit inspect schema-diff --config dataset.yml` per vedere il drift RAW |
| Voglio solo un layer, non tutto | `toolkit run clean` o `toolkit run mart` — skippa i layer upstream se già presenti |
| Il run si è interrotto a metà | `toolkit resume --dataset <name> --year <year> --config dataset.yml` (se i run record sono coerenti) |
| Devo cancellare output precedenti? | No — il toolkit sovrascrive in-place. Usa `output_policy: versioned` se vuoi tenere cronologia |
| Come si legge l'output in un notebook? | Vedi [notebook-contract.md](docs/notebook-contract.md) |

---

## Riferimenti

| Documento | Contenuto |
|---|---|
| [config-schema.md](docs/config-schema.md) | Specifica completa YAML di `dataset.yml` |
| [conventions.md](docs/conventions.md) | Path, manifest, artifact policy, CLEAN reader logic |
| [advanced-workflows.md](docs/advanced-workflows.md) | Resume, run parziali, profile, debug |
| [notebook-contract.md](docs/notebook-contract.md) | Come leggere gli output del toolkit nei notebook |
| [feature-stability.md](docs/feature-stability.md) | Matrice stabilità: canonico, advanced, compat, deprecated |
| [toolkit/mcp/README.md](toolkit/mcp/README.md) | Documentazione MCP server |
| [CHANGELOG.md](CHANGELOG.md) | Cronologia delle versioni (v1.2.0) |

---

<sub>DataCivicLab Toolkit v1.2.0 — MIT license</sub>
