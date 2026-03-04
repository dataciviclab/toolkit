# DataCivicLab Toolkit

Toolkit Python per pipeline dati riproducibili `RAW -> CLEAN -> MART`, con approccio SQL-first, audit degli artefatti e run tracking persistente.

## Ruolo Nell'Ecosistema

Questa repo e' il motore tecnico della pipeline dati di DataCivicLab.

Ruoli delle repo correlate:

- `.github`: policy condivise, community health, template issue/PR, onboarding GitHub
- `dataciviclab`: hub pubblico e minimale dell'organizzazione
- `toolkit`: runtime, CLI, contract di config/path/output, documentazione tecnica del motore
- `project-template`: template operativo dei repo dataset
- repo dataset: progetti concreti che usano il toolkit

Questa repo non e' l'hub dell'organizzazione e non replica la documentazione org-wide: resta focalizzata sul motore e sul suo contratto tecnico.

## Confini Del Toolkit

Il toolkit espone un perimetro volutamente stretto:

- core runtime: `raw`, `clean`, `mart`, `run`, `validate`, `status`, `inspect`
- advanced tooling: `resume`, `profile raw`, run parziali per layer
- compatibility only: alias legacy e shim deprecati

Regola pratica:

- nuovi repo dataset: resta nel workflow canonico
- recovery o diagnostica: usa gli strumenti advanced
- bootstrap o compatibilita': non trattarli come parte del contratto stabile

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
- comandi CLI `run`, `resume`, `status`, `validate`, `profile`, `inspect`
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
Confini runtime e superfici non-core: [docs/runtime-boundaries.md](docs/runtime-boundaries.md)
Per policy condivise e community health organizzativa, fai riferimento alla repo `.github` dell'organizzazione.

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

Caso utile per CSV pubblici multi-anno quasi stabili ma non identici:

```yaml
clean:
  sql: "sql/clean.sql"
  read:
    source: config_only
    header: false
    skip: 1
    delim: ";"
    encoding: "utf-8"
    columns:
      column00: "VARCHAR"
      column01: "VARCHAR"
      column02: "VARCHAR"
    normalize_rows_to_columns: true
```

Usa `normalize_rows_to_columns: true` quando:

- vuoi un layout posizionale canonico
- alcuni file o anni hanno righe piu corte del numero di colonne atteso
- vuoi evitare che una colonna aggiunta o mancante faccia saltare il `clean`

Non usarlo come default. Ha senso quando stai deliberatamente gestendo una fonte instabile con schema posizionale dichiarato.

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
- `inspect paths` espone i path stabili per notebook e script locali, insieme ai principali hints del RAW
- `inspect schema-diff` confronta i principali segnali di schema RAW tra gli anni configurati
- `--dry-run` valida config e SQL senza eseguire la pipeline

Esempi:

```bash
toolkit run all --config dataset.yml --strict-config
toolkit validate all --config dataset.yml --strict-config
toolkit status --dataset my_dataset --year 2024 --latest --config dataset.yml
toolkit inspect paths --config dataset.yml --year 2024 --json
toolkit inspect schema-diff --config dataset.yml --json
toolkit run all --config dataset.yml --dry-run --strict-config
```

`resume`, `profile raw`, `run raw|clean|mart` e la policy completa degli artifacts restano disponibili, ma sono tooling avanzato: vedi [docs/advanced-workflows.md](docs/advanced-workflows.md).

## Notebook locali

Nei repo dataset clonati dal template, i notebook dovrebbero leggere gli output reali gia` scritti dal toolkit, non ricostruire logica di path.

In pratica:

- RAW: `root/data/raw/<dataset>/<year>/`
- CLEAN: `root/data/clean/<dataset>/<year>/`
- MART: `root/data/mart/<dataset>/<year>/`
- run records: `root/data/_runs/<dataset>/<year>/`
- hints RAW: `root/data/raw/<dataset>/<year>/_profile/suggested_read.yml`

Helper ufficiale per evitare path logic duplicata nei notebook:

```bash
toolkit inspect paths --config dataset.yml --year 2024 --json
```

Ruoli stabili degli output:

- `metadata.json`: payload ricco del layer
- `manifest.json`: summary stabile del layer con puntatori a metadata e validation
- `data/_runs/.../<run_id>.json`: stato del run usato da `status` e `resume`
- `inspect paths --json`: discovery helper read-only per notebook e script locali, con blocco `raw_hints`

Questo mantiene il contratto semplice tra toolkit e repo dataset:

- il toolkit produce artefatti e metadata stabili
- i notebook li ispezionano localmente
- `dataset.yml` resta la fonte di verita` per dataset, anni e path relativi

## Operative Notes

Run tracking:

- ogni `run` e `resume` scrive un record in `data/_runs/<dataset>/<year>/<run_id>.json`
- `status` legge questi record
- `inspect paths` espone i path stabili da usare in notebook e script

Validation gate:

- `toolkit run ...` valida automaticamente dopo ogni layer completato
- con `validation.fail_on_error: true` la pipeline si ferma
- con `validation.fail_on_error: false` il run puo` terminare come `SUCCESS_WITH_WARNINGS`

Per dettagli completi su layer, validazioni, plugin, artifact policy e flow avanzati, vedi:

- [docs/conventions.md](docs/conventions.md)
- [docs/config-schema.md](docs/config-schema.md)
- [docs/feature-stability.md](docs/feature-stability.md)
- [docs/advanced-workflows.md](docs/advanced-workflows.md)

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
