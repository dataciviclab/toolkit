# DataCivicLab Toolkit

Motore tecnico per trasformazioni riproducibili su dati pubblici tramite DuckDB. Gestisce i layer `raw`, `clean`, `mart` e `validation`.

## 1. Core Workflow

1. `toolkit run all --config dataset.yml`: Scarica, pulisce e aggrega.
2. `toolkit validate all --config dataset.yml`: Esegue gate di qualità (PK, null, custom).
3. `toolkit status --dataset <name> --year <year> --latest --config dataset.yml`: Mostra l'ultimo run.

Se `toolkit` non è nel `PATH`, usa il modulo:

```bash
python -m toolkit.cli.app run all --config dataset.yml
```

## 2. Installazione Rapida

Richiede Python 3.10+.
```bash
pip install -e .[dev]
# Smoke test (Windows/Linux)
toolkit run all -c project-example/dataset.yml
toolkit validate all -c project-example/dataset.yml
toolkit status --dataset project_example --year 2022 --latest --config project-example/dataset.yml
```

## 3. Struttura del Contratto (dataset.yml)

Il toolkit risolve `dataset.yml` + `sql/` per produrre output auditabili.
- **`root`**: Directory di output (fallback su cartella config).
- **`raw`**: Sorgenti (local, generic_https, ckan, sdmx) e strategie di download.
- **`clean`**: Normalizzazione tramite `sql/clean.sql`.
- **`mart`**: Tabelle analitiche aggregate via `sql/mart/*.sql`.
- **`validation`**: Quality check per layer (`fail_on_error: true`).

## 4. CLI Helper Operativi

- `toolkit inspect paths --config dataset.yml --year 2024 --json`: Contratto path per notebook e script.
- `toolkit inspect schema-diff`: Analisi diagnostica di drift tra anni nel RAW.
- `toolkit run all --config dataset.yml --dry-run --strict-config`: Valida config/SQL in modo stretto.
- `toolkit status --dataset <name> --year <year> --latest --config dataset.yml`: Recupera l'ultimo run.
- `toolkit batch`: esecuzione batch quando devi orchestrare più config.
- `toolkit resume`: Riprende un run interrotto senza ricaricare i layer già validi.

Contratti correlati:
- notebook e path runtime: [notebook-contract.md](docs/notebook-contract.md)
- workflow avanzati, `--latest`, `--strict-config`, artifact policy: [advanced-workflows.md](docs/advanced-workflows.md)
- schema config e legacy support: [config-schema.md](docs/config-schema.md)

## 5. Riferimenti Tecnici

| Documento | Contenuto |
| --- | --- |
| [config-schema.md](docs/config-schema.md) | Specifica completa YAML e legacy support |
| [conventions.md](docs/conventions.md) | Policy su percorsi, metadata e manifest |
| [advanced-workflows.md](docs/advanced-workflows.md) | Focus su `resume`, profile e run parziali |
| [notebook-contract.md](docs/notebook-contract.md) | Come leggere gli output dal codice cliente |

## 6. Sviluppo e QA

- **Test**: `pytest` (unit, CLI, E2E su `project-example`).
- **Lint**: `ruff check .`
- **Hygiene**: Gli output in `out/` o `_smoke_out/` sono ignorati da git. Non versionare dati.

---
**Boundary**: Questa repo è il motore. Per i dataset reali, vedi `dataset-incubator`.
