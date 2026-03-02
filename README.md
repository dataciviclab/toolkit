# DataCivicLab Toolkit

Toolkit Python per pipeline dati riproducibili `RAW -> CLEAN -> MART`, con approccio SQL-first, audit degli artefatti e run tracking persistente.

## Obiettivi

- mantenere una struttura progetto semplice: `dataset.yml` + `sql/`
- eseguire trasformazioni riproducibili con DuckDB
- tenere separati i layer `raw`, `clean`, `mart`
- produrre output auditabili con metadata, manifest e validation report
- rendere la CLI scriptabile senza introdurre logica dataset-specifica nel core

## Stato attuale

Il toolkit include:

- pipeline `raw`, `clean`, `mart`
- validation gate post-layer integrato in `run`
- run tracking persistente in `data/_runs/...`
- comandi CLI `run`, `resume`, `status`, `validate`, `profile`, `gen-sql`
- `project-example/` offline per smoke test locale

## Installazione

```bash
git clone https://github.com/dataciviclab/toolkit.git
cd toolkit
pip install -e .[dev]
```

Richiede Python 3.10+.

## CLI Naming Note

Il comando CLI canonico del progetto e' `toolkit`.

Se nel tuo ambiente c'e' una collisione di nome o il console script non e' nel `PATH`, puoi usare direttamente il modulo Python:

```bash
python -m toolkit.cli.app run all --config dataset.yml
```

## Quickstart

Il percorso canonico per i repo dataset clonati dal template e':

1. `toolkit run all --config dataset.yml`
2. `toolkit validate all --config dataset.yml`
3. `toolkit status --dataset <dataset> --year <year> --latest --config dataset.yml`
4. notebook locali che leggono gli output reali sotto `root/data/...`

Giro offline completo con il progetto di esempio, eseguibile in pochi minuti su una macchina pulita.

Windows PowerShell:

```powershell
$env:TOOLKIT_OUTDIR = Join-Path $env:TEMP "dataciviclab-toolkit-quickstart"
py -m pip install -e ".[dev]"
py -m toolkit.cli.app run all -c project-example/dataset.yml
py -m toolkit.cli.app validate all -c project-example/dataset.yml
py -m toolkit.cli.app status --dataset project_example --year 2022 --config project-example/dataset.yml
```

Linux/macOS:

```bash
export TOOLKIT_OUTDIR="$(mktemp -d)/dataciviclab-toolkit-quickstart"
python -m pip install -e ".[dev]"
python -m toolkit.cli.app run all -c project-example/dataset.yml
python -m toolkit.cli.app validate all -c project-example/dataset.yml
python -m toolkit.cli.app status --dataset project_example --year 2022 --config project-example/dataset.yml
```

Validazione rapida della config prima di eseguire la pipeline:

```bash
toolkit run all --config dataset.yml --dry-run
```

Interpretazione errori config:

- `Config validation failed: output.<campo>: Extra inputs are not permitted` -> campo non supportato
- `Config validation failed: raw.sources: Input should be a valid list` -> tipo YAML sbagliato
- `Config validation failed: clean.validate.primary_key: ... string or a list of strings` -> forma del valore non valida
- warning di deprecazione -> config ancora accettata, ma in forma legacy da migrare

Schema completo e legacy supportato: [docs/config-schema.md](docs/config-schema.md)
Flow avanzati e tooling secondario: [docs/advanced-workflows.md](docs/advanced-workflows.md)
Matrice di stabilita`: [docs/feature-stability.md](docs/feature-stability.md)
Contratto notebook/output: [docs/notebook-contract.md](docs/notebook-contract.md)

Artefatti attesi:

- `$TOOLKIT_OUTDIR/data/raw/project_example/2022/manifest.json`
- `$TOOLKIT_OUTDIR/data/clean/project_example/2022/project_example_2022_clean.parquet`
- `$TOOLKIT_OUTDIR/data/mart/project_example/2022/rd_by_regione.parquet`
- `$TOOLKIT_OUTDIR/data/_runs/project_example/2022/<run_id>.json`

## Struttura progetto

Configurazione minima:

```yaml
root: "./_smoke_out"

dataset:
  name: "project_example"
  years: [2022]

raw:
  sources:
    - name: "local_csv"
      type: "local_file"
      args:
        path: "data/raw_sample.csv"
        filename: "ispra_dettaglio_comunale_{year}.csv"

clean:
  sql: "sql/clean.sql"
  read_mode: "fallback"
  read:
    mode: "explicit"
    include: ["ispra_dettaglio_comunale_*.csv"]
    delim: ";"
    encoding: "utf-8"

mart:
  tables:
    - name: "rd_by_regione"
      sql: "sql/mart/mart_regione_anno.sql"

validation:
  fail_on_error: true
```

I path relativi in `dataset.yml` sono risolti rispetto alla directory del `dataset.yml`, non rispetto al `cwd`.
La directory di output effettiva segue questa precedenza:

- `root` dichiarato in `dataset.yml`
- `DCL_ROOT`
- fallback sulla directory che contiene `dataset.yml`

Convenzioni:

- `dataset.yml` descrive dataset, anni, sorgenti, SQL e validazioni
- `sql/clean.sql` definisce il layer CLEAN
- `sql/mart/*.sql` definisce le tabelle MART
- per esempi pronti, vedi [examples/dataset_min.yml](examples/dataset_min.yml) e [examples/dataset_full.yml](examples/dataset_full.yml)

## CLI

Workflow canonico:

```bash
toolkit run all --config dataset.yml
toolkit validate all --config dataset.yml
```

Per il percorso base:

- `run all` esegue RAW -> CLEAN -> MART
- `validate all` esegue i quality checks su CLEAN e MART
- `status` legge il run record e mostra lo stato piu` recente
- `inspect paths` espone i path stabili per notebook e script locali
- `--dry-run` valida config e SQL senza eseguire la pipeline

Esempi:

```bash
toolkit run all --config dataset.yml --strict-config
toolkit validate all --config dataset.yml --strict-config
toolkit status --dataset my_dataset --year 2024 --latest --config dataset.yml
toolkit inspect paths --config dataset.yml --year 2024 --json
toolkit run all --config dataset.yml --dry-run --strict-config
```

`resume`, `profile raw`, `run raw|clean|mart`, `gen-sql` e la policy completa degli artifacts restano disponibili, ma sono tooling avanzato: vedi [docs/advanced-workflows.md](docs/advanced-workflows.md).

## Notebook locali

Nei repo dataset clonati dal template, i notebook dovrebbero leggere gli output reali gia` scritti dal toolkit, non ricostruire logica di path.

In pratica:

- RAW: `root/data/raw/<dataset>/<year>/`
- CLEAN: `root/data/clean/<dataset>/<year>/`
- MART: `root/data/mart/<dataset>/<year>/`
- run records: `root/data/_runs/<dataset>/<year>/`

Helper ufficiale per evitare path logic duplicata nei notebook:

```bash
toolkit inspect paths --config dataset.yml --year 2024 --json
```

Questo mantiene il contratto semplice tra toolkit e repo dataset:

- il toolkit produce artefatti e metadata stabili
- i notebook li ispezionano localmente
- `dataset.yml` resta la fonte di verita` per dataset, anni e path relativi

## Run Tracking

Ogni comando `toolkit run ...` o `toolkit resume ...` scrive un record JSON in:

```text
data/_runs/<dataset>/<year>/<run_id>.json
```

Il record contiene almeno:

- `status`: `RUNNING`, `SUCCESS`, `FAILED`, `SUCCESS_WITH_WARNINGS`, `DRY_RUN`
- `started_at`, `finished_at`
- `layers.raw|clean|mart.status`
- `validations.raw|clean|mart`
- `error` se presente
- `resumed_from` se il run deriva da una ripresa

Questo file e` la fonte per i comandi `status` e `resume`.

## Validation Gate

`toolkit run ...` esegue automaticamente la validazione dopo ogni layer completato con successo.

Comportamento:

- se la validazione passa, il run prosegue
- se la validazione fallisce e `validation.fail_on_error: true`, la pipeline si interrompe
- se la validazione fallisce e `validation.fail_on_error: false`, la pipeline continua e il run termina come `SUCCESS_WITH_WARNINGS`

La CLI `validate` resta disponibile per eseguire i check separatamente.

## Layer

### RAW

Responsabilita`:

- legge o scarica il payload da plugin sorgente
- applica extractor opzionale
- scrive file normalizzati nel layer RAW
- produce metadata, manifest e validation report

Output tipici:

- file sorgente normalizzati
- `metadata.json`
- `raw_validation.json`
- `manifest.json`

`manifest.json` nel RAW dichiara sempre il file primario da usare a valle.
Campi minimi: `dataset`, `year`, `run_id`, `created_at`, `sources`, `primary_output_file`.
`primary_output_file` e gli `output_file` delle source sono path relativi al RAW year-dir, in formato posix.
`raw.output_policy` supporta `versioned` (default, suffix `_1/_2`) e `overwrite` (stesso filename sovrascritto).
Con piu` source, si puo` fissare il primario con `primary: true`; altrimenti il toolkit usa la prima source e logga un warning.

### CLEAN

Responsabilita`:

- seleziona gli input dal RAW year-dir
- renderizza `clean.sql`
- esegue SQL in DuckDB
- esporta un parquet clean

Output tipici:

- `<dataset>_<year>_clean.parquet`
- `_run/clean_rendered.sql`
- `metadata.json`
- `manifest.json`
- `_validate/clean_validation.json`

### MART

Responsabilita`:

- legge parquet CLEAN
- renderizza le SQL delle tabelle finali
- esporta un parquet per tabella

Output tipici:

- `<table>.parquet`
- `_run/*_rendered.sql`
- `metadata.json`
- `manifest.json`
- `_validate/mart_validation.json`

## Validazioni

### CLEAN

- `required_columns`
- `min_rows`
- `not_null`
- `primary_key`
- `ranges`
- `max_null_pct`

### MART

- `min_rows`
- `required_columns`
- `not_null`
- `primary_key`
- `ranges`

Le validazioni vengono eseguite automaticamente da `toolkit run ...` dopo ogni layer completato con successo.
La CLI `toolkit validate ...` resta disponibile per eseguirle separatamente.

## Plugin sorgente

Plugin registrati:

- `local_file`
- `http_file`
- `api_json_paged`
- `html_table`

Stabilita`:

- core pipeline `raw`, `clean`, `mart`: stable
- plugin `local_file`, `http_file`: stable
- plugin `api_json_paged`, `html_table`: experimental

Come aggiungere un plugin:

- definisci una classe plugin in `toolkit/plugins/<nome>.py`
- contratto minimo:
  - `__init__(**client)` per ricevere configurazione client
  - `fetch(...) -> bytes` per restituire il payload RAW
- registra il plugin in modo esplicito in `toolkit.core.registry.register_builtin_plugins()`
- se il plugin dipende da librerie opzionali, il fallimento di import deve essere trattato come plugin opzionale non disponibile:
  - warning `DCLPLUGIN001` in non-strict
  - errore in strict mode

## Smoke locale

`project-example/` e` pensato per un giro completo locale, senza rete:

```bash
cd project-example
py -m toolkit.cli.app run all --config dataset.yml
py -m toolkit.cli.app status --dataset project_example --year 2022 --latest --config dataset.yml
```

Artefatti attesi:

- `project-example/_smoke_out/data/raw/project_example/2022/raw_validation.json`
- `project-example/_smoke_out/data/clean/project_example/2022/project_example_2022_clean.parquet`
- `project-example/_smoke_out/data/mart/project_example/2022/rd_by_regione.parquet`
- `project-example/_smoke_out/data/_runs/project_example/2022/<run_id>.json`

## Conventions

Vedi [docs/conventions.md](docs/conventions.md) per:

- policy RAW
- policy di selezione input CLEAN
- precedence del read config
- metadata, manifest e validation contracts
- workflow avanzati e tooling secondario: [docs/advanced-workflows.md](docs/advanced-workflows.md)

## Testing

Suite rapida:

```bash
py -m pytest
```

Lint:

```bash
py -m ruff check .
```

La suite include:

- test unitari su config, registry, extractors e validazioni
- test su CLI `run`, `status`, `resume`, `dry-run`
- golden path E2E su `project-example`

## Repository Hygiene

Gli output generati non devono mai essere committati.

- tieni artefatti e cache solo in directory locali gia` ignorate come `_smoke_out/`, `_test_out/`, `.pytest_cache/`, `.ruff_cache/` e `*.egg-info/`
- usa un `root` esplicito nel `dataset.yml` o `DCL_ROOT` per tenere gli output fuori dal codice sorgente quando lavori su smoke test o progetti locali
- se un output e` gia` tracciato da git, rimuovilo dall'index con `git rm -r --cached <path>` senza cancellarlo dal filesystem

## Cosa non fa

Questo repository non contiene dataset reali di produzione.

Contiene:

- il motore
- le convenzioni
- un progetto esempio minimo
