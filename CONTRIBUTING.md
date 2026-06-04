# Contributing to toolkit

Questa guida vale per la repo `toolkit`.

Per le regole GitHub condivise dell'organizzazione, parti prima da
[`.github`](https://github.com/dataciviclab/.github).

## A cosa serve questa repo

`toolkit` è il motore dati riproducibile del DataCivicLab. Prende dati
grezzi da fonti eterogenee (HTTP, CKAN, SDMX, SPARQL, file locali),
li normalizza e li aggrega in parquet pronti per analisi.

Pipeline: `RAW → CLEAN → MART`

Qui stanno:

- il motore della pipeline (`toolkit/core/`, `toolkit/raw/`, `toolkit/clean/`, `toolkit/mart/`)
- i plugin sorgente (`toolkit/plugins/`: http_file, ckan, sdmx, sparql, local_file)
- la CLI (`toolkit/cli/`)
- il profiler RAW (`toolkit/profile/`)
- il server MCP (`toolkit/mcp/`)
- `docs/` — documentazione tecnica del motore

Qui non stanno:

- i dataset reali con le loro config (`dataset.yml`, SQL) — vanno in `dataset-incubator`
- package condivisi di infrastruttura — vanno in `lab-connectors`
- scouting e monitoraggio fonti — va in `source-observatory`
- analisi pubbliche o notebook — vanno in `dataciviclab/analisi/`
- policy GitHub comuni — vanno in `.github`

## Setup locale

```bash
pip install -e .[dev]
```

Dipende da `lab-connectors` per HTTP client, GCS e MCP core:

```bash
pip install -e ../lab-connectors
```

### Eseguire i test

```bash
pytest -m core                    # contratto pubblico e workflow canonico
pytest -m "core or advanced"      # tutto tranne compat legacy
pytest                            # tutto (~55 test file)
ruff check .
mypy toolkit/
```

### Comandi utili per sviluppo

```bash
# Run completo su dataset di esempio
python -m toolkit.cli.app run all --config project-example/dataset.yml

# Validate
python -m toolkit.cli.app validate all --config project-example/dataset.yml

# Inspect paths
python -m toolkit.cli.app inspect paths --config project-example/dataset.yml

# Profile RAW
python -m toolkit.cli.app inspect profile --config project-example/dataset.yml
```

## Test Suite Tiers

I test sono stratificati con marker pytest per tenere visibile la copertura
critica per il rilascio:

| Marker | Cosa copre | Deve sempre passare |
|---|---|---|
| `core` | Contratto pubblico e percorso canonico — config, path contract, `run all`, `validate all`, end-to-end RAW→CLEAN→MART, run records, resume | ✅ Sì |
| `advanced` | Comportamenti secondari — read modes, extractors, plugin registry, profiling, artifact policy | ✅ Su release |
| `compat` | Solo compatibilità legacy e shim di import deprecati | No (può decadere) |

```bash
pytest -m core
pytest -m "core or advanced"
pytest -m compat
```

## Git Hook

Prima di contribuire, installa il pre-commit guardrail leggero:

```bash
cp scripts/pre-commit .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

Il hook blocca commit che includono artifact generati o cache:
`_smoke_out/`, `_test_out/`, `.pytest_cache/`, `.ruff_cache/`, `*.egg-info/`.

## Quando aprire una issue

Apri una issue in `toolkit` se il lavoro riguarda:

- bug nella pipeline o in un plugin sorgente
- nuova funzionalità del motore (es. nuovo tipo di plugin)
- miglioramento della CLI o del MCP server
- cambio di contratto in `dataset.yml` che impatta i consumer
- performance o affidabilità

Per dubbi su come usare il toolkit con un dataset specifico, apri prima
una issue in `dataset-incubator`.

## Prima di aprire una PR

- verifica se esiste già una issue collegata
- tieni il perimetro stretto: una PR = un layer o un fix mirato
- se cambi un contratto pubblico (struttura `dataset.yml`, path output, schema
  parquet), aggiorna anche:
  - i test `core` che lo proteggono
  - la documentazione in `docs/`
  - i consumer in `dataset-incubator`
- se aggiungi un plugin sorgente, includi test e documentazione
- controlla che `pytest -m core` passi (deve sempre passare)
- verifica con `ruff check .` e `mypy toolkit/`

## Riferimenti

- [README.md](README.md) — documentazione completa del toolkit
- [docs/config-schema.md](docs/config-schema.md) — specifica completa YAML di `dataset.yml`
- [docs/conventions.md](docs/conventions.md) — path, manifest, artifact policy
- [docs/notebook-contract.md](docs/notebook-contract.md) — come leggere gli output nei notebook
- [docs/feature-stability.md](docs/feature-stability.md) — matrice stabilità
- [`dataset-incubator`](https://github.com/dataciviclab/dataset-incubator) — downstream: qui vivono i dataset reali
- [`lab-connectors`](https://github.com/dataciviclab/lab-connectors) — dipendenza condivisa
- [`.github`](https://github.com/dataciviclab/.github) — policy condivise

## Deprecation policy

Prima di rimuovere un modulo, una funzione o una classe che fa parte dell'API pubblica:

1. **Cerca consumer** — `rg "from toolkit\.core\.X import|import toolkit\.core\.X" --include "*.py"` in tutta l'org.
2. Se ci sono consumer esterni, lascia uno **shim backward compat** che importi dal nuovo posto e emetta `DeprecationWarning`.
3. Se non ci sono consumer, **puoi rimuovere direttamente** ma annota nel commit message che è stata verificata l'assenza di import.
4. Se la rimozione è breaking (nessuno shim possibile), dichiara **esplicitamente** nel PR template la rottura e perché è accettabile.

Regola pratica: se qualcuno fa `from toolkit.core.X import Y`, deve continuare a funzionare per almeno una release dopo la deprecazione.

## Regole del codice: path artifact

Ogni path di file prodotto da un layer (validation, profile, metadata)
deve essere referenziato tramite la costante in `toolkit/core/paths.py`.
Non usare stringhe letterali.

Se aggiungi un nuovo artifact, aggiungi la costante in `core/paths.py`
e importala dove serve.

Costanti definite:
- `RAW_VALIDATION`, `CLEAN_VALIDATION`, `MART_VALIDATION`
- `RAW_PROFILE`, `RAW_PROFILE_DIR`, `RAW_SUGGESTED_READ`
- `METADATA`

Path di directory: usa `layer_year_dir()`, `dataset_dir()`, `resolve_root()`
da `core/paths.py` invece di costruirli a mano.
