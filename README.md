# DataCivicLab Toolkit

Motore di pipeline dati del DataCivicLab — da fonti pubbliche a dataset
pronti per l'analisi.

Prende dati grezzi da fonti eterogenee (HTTP, CKAN, SDMX, SPARQL, file locali),
li normalizza e li produce in parquet pronti per l'analisi, con un contratto
chiaro tra ogni layer.

## Per chi è

| Se sei... | toolkit fa per te perché... |
|---|---|
| **Autore di dataset** del Lab | scrivi `dataset.yml` + SQL, toolkit esegue e produce parquet RAW → CLEAN → MART |
| **Analista** | consumi i parquet già prodotti via `data-explorer` o notebook — qui trovi come sono stati generati |
| **Sviluppatore del motore** | contribuisci a `raw/`, `clean/`, `mart/`, `plugins/` — questo è il repo |

## Primi passi in 3 comandi

```bash
pip install -e .[dev]
toolkit run -c dataset.yml
toolkit inspect -c dataset.yml
```

Se `toolkit` non è nel PATH: `python -m toolkit.cli.app run -c dataset.yml`

## Pipeline: tre livelli

```
RAW ──→ CLEAN ──→ MART
```

| Layer | Contenuto | Destinazione |
|---|---|---|
| **RAW** | File originale dalla fonte, senza modifiche | Audit, verifica, debug |
| **CLEAN** | Dato normalizzato: nomi colonna coerenti, tipi fissi, schema stabile | Notebook, analisi, data-explorer |
| **MART** | Dato aggregato per report e dashboard | Report, insight rapidi |

Ogni run produce `metadata.json` e `validation.json` per audit trail.

## Ecosistema

```
source-observatory → dataset-incubator → [toolkit] → GCS → data-explorer
                                              ↑
                                         MCP server
```

Il toolkit non gestisce il deployment: scrive nella directory configurata.
La CI di `dataset-incubator` carica su GCS dopo ogni run validato.

## CLI — comandi essenziali

| Comando | Cosa fa |
|---|---|
| `toolkit run` | Esecuzione completa RAW→CLEAN→MART (default) |
| `toolkit run --batch <file>` | Esegue piú dataset in sequenza |
| `toolkit run --refresh-support` | Forza la rigenerazione dei support (di default i support con output già presenti vengono riusati) |
| `toolkit run raw` | Solo layer RAW |
| `toolkit run clean` | Solo layer CLEAN |
| `toolkit run mart` | Solo layer MART |
| `toolkit inspect` | Stato ultimo run (riassunto) |
| `toolkit inspect config --diff` | Schema-diff RAW tra anni |
| `toolkit inspect runs --resume` | Riprendi run interrotto |
| `toolkit scout <URL>` | Esplora fonte esterna (HTTP/CKAN/SDMX) |

`--config` è opzionale: se omesso, toolkit cerca `dataset.yml` nella directory corrente.
Se passi uno slug (es. `terna-electricity-by-source`), lo risolve nel workspace.

📖 **Reference completo**: `toolkit --help`

## Configurazione (`dataset.yml`)

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

Il toolkit risolve i path relativi, esegue SQL su DuckDB e produce output in `root/data/`.

**Plugin sorgente supportati**: `http_file`, `http_post_file`, `local_file`, `ckan`, `sdmx`, `sparql`.

📖 **Documenti di riferimento**:

| Documento | Contenuto |
|---|---|
| [config-schema.md](docs/config-schema.md) | Specifica completa YAML |
| [standard-macros.md](docs/standard-macros.md) | Macro SQL predefinite per clean.sql |
| [conventions.md](docs/conventions.md) | Path, metadata, manifest |
| [advanced-workflows.md](docs/advanced-workflows.md) | Resume, run parziali, debug |
| [notebook-contract.md](docs/notebook-contract.md) | Come leggere output nei notebook |
| [feature-stability.md](docs/feature-stability.md) | Cosa è stabile, sperimentale, deprecated |

## Integrazione AI (MCP)

Il toolkit espone **12 tool** MCP per agenti AI e IDE:

| Categoria | Tool |
|---|---|
| **Catalogo** | `toolkit_find`, `toolkit_dataset_overview` |
| **Pipeline** | `toolkit_layer`, `toolkit_status`, `toolkit_schema_diff`, `toolkit_preflight`, `toolkit_list_runs` |
| **Scout** | `toolkit_probe_url` (con `routed=True`), `toolkit_ckan_package_show`, `toolkit_html_extract_links`, `toolkit_sparql_query`, `toolkit_preview_url` |

Config IDE (`.mcp.json`):
```json
{
  "toolkit": {
    "command": "/path/to/python",
    "args": ["-m", "toolkit.mcp.server"]
  }
}
```

📖 **Dettaglio**: [toolkit/mcp/README.md](toolkit/mcp/README.md)

## FAQ — problemi comuni

| Problema | Soluzione |
|---|---|---|
| `toolkit: command not found` | Usa `python -m toolkit.cli.app` |
| Run interrotto | `toolkit inspect runs --resume -c dataset.yml` |
| Schema diverso tra anni | `toolkit inspect config -c dataset.yml --diff` |
| Dove sono i parquet? | `toolkit inspect -c dataset.yml` (mostra path nel riassunto) |

## Sviluppo

```bash
pip install -e .[dev]
pytest -m core                    # contratto pubblico
ruff check .                      # lint
```

**Test**: 85+ file, marker `core` (deve sempre passare), `advanced`, `compat`.
**CI**: `.github/workflows/ci.yml` — Python 3.10–3.12, ruff, coverage ≥70%.

## Struttura del repo

```
toolkit/
  toolkit/            # package Python
    cli/              # comandi CLI (typer, re-export)
    core/             # engine condiviso (infrastruttura)
    domain/           # logica di dominio (orchestrazione)
    raw/ clean/ mart/ # layer pipeline
    plugins/          # plugin sorgente
    mcp/              # server MCP
    profile/          # profiling RAW
  tests/              # pytest (85+ file)
  docs/               # documentazione tecnica
  project-example/    # esempio funzionante
```

## Licenza

MIT — [CONTRIBUTING.md](CONTRIBUTING.md) · [CHANGELOG.md](CHANGELOG.md)
