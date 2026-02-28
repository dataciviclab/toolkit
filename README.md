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

## Quickstart

Giro offline completo con il progetto di esempio, eseguibile in pochi minuti su una macchina pulita.

Windows PowerShell:

```powershell
$env:TOOLKIT_OUTDIR = Join-Path $env:TEMP "dataciviclab-toolkit-quickstart"
py -m pip install -e ".[dev]"
py -m toolkit.cli.app run all -c project-example/dataset.yml
py -m toolkit.cli.app status --dataset project_example --year 2022 --config project-example/dataset.yml
```

Linux/macOS:

```bash
export TOOLKIT_OUTDIR="$(mktemp -d)/dataciviclab-toolkit-quickstart"
python -m pip install -e ".[dev]"
python -m toolkit.cli.app run all -c project-example/dataset.yml
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
  source:
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

Esecuzione step singolo:

```bash
toolkit run raw --config dataset.yml
toolkit run clean --config dataset.yml
toolkit run mart --config dataset.yml
```

Esecuzione end-to-end:

```bash
toolkit run all --config dataset.yml
```

Dry-run:

```bash
toolkit run all --config dataset.yml --dry-run
```

Il dry-run:

- valida config e path SQL richiesti
- stampa l'execution plan per dataset/year
- crea solo il run record in `data/_runs/...`
- non scarica RAW, non esegue DuckDB, non scrive artefatti nei layer

Resume:

```bash
toolkit resume --dataset project_example --year 2022 --config dataset.yml
toolkit resume --dataset project_example --year 2022 --run-id <old_run_id> --config dataset.yml
```

`resume`:

- legge un run record esistente
- trova il primo layer non `SUCCESS`
- crea un nuovo `run_id`
- salva `resumed_from=<old_run_id>` nel nuovo record

Status:

```bash
toolkit status --dataset project_example --year 2022 --latest --config dataset.yml
toolkit status --dataset project_example --year 2022 --run-id <run_id> --config dataset.yml
```

Validazione separata:

```bash
toolkit validate clean --config dataset.yml
toolkit validate mart --config dataset.yml
toolkit validate all --config dataset.yml
```

Profilazione RAW:

```bash
toolkit profile raw --config dataset.yml
```

`toolkit profile raw` scrive sempre hint utilizzabili anche se il parsing DuckDB fallisce.
Tutti gli artefatti di profiling vivono in `raw/<dataset>/<year>/_profile/`.
Il nome canonico del profilo JSON e` `raw_profile.json`; `profile.json` resta un alias di compatibilita` opzionale.
Gli output effettivi dipendono dalla policy `output.artifacts`.
`suggested_read.yml` usa le stesse chiavi che CLEAN passa a `clean.read`, senza mapping extra.
Se DuckDB non riesce a sniffare il file, il profiler usa un fallback Python leggero per `header`, `delim`, `decimal`, `encoding` e aggiunge warning espliciti.
L'output resta quindi consumabile da CLEAN anche su CSV sporchi o irregolari.

Artifacts policy:

```yaml
output:
  artifacts: standard   # minimal | standard | debug
  legacy_aliases: true  # abilita l'alias legacy profile.json
```

`standard` resta il default compatibile. `minimal` tiene solo gli artefatti di pipeline e salta report/debug SQL. `debug` tiene tutto.

Generazione SQL CLEAN da mapping dichiarativo:

```bash
toolkit gen-sql --config dataset.yml
```

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

## CLEAN Input Selection

La selezione degli input RAW per CLEAN e` configurabile via `clean.read`.

Opzioni supportate:

- `mode: explicit`
- `mode: latest`
- `mode: largest`
- `mode: all`
- `glob: "*"`
- `include: [...]`
- `prefer_from_raw_run: true`
- `allow_ambiguous: false`

Note operative:

- `explicit` richiede `include`
- `latest` seleziona il file con `mtime` piu` recente
- `largest` seleziona il file piu` grande
- `all` passa tutti i candidati a DuckDB in ordine deterministico
- se `mode` non e` specificato, il toolkit usa il fallback legacy su `largest` e logga un warning di deprecazione

CSV read mode:

- `clean.read_mode: strict` usa solo i parametri dichiarati
- `clean.read_mode: fallback` prova strict e, se fallisce, riprova con preset robusto loggando il fallback
- `clean.read_mode: robust` usa direttamente il preset robusto
- il preset robusto mantiene `delim`/`decimal`/`encoding` noti e aggiunge poche opzioni conservative come `ignore_errors`, `null_padding`, `strict_mode: false`, `sample_size: -1`
- forma canonica:

```yaml
clean:
  read:
    source: auto  # oppure config_only
```

- `clean.read.source: auto` usa anche i format hints di `raw/<dataset>/<year>/_profile/suggested_read.yml`; `config_only` li ignora
- da `suggested_read.yml` vengono applicate solo chiavi di formato come `delim`, `decimal`, `encoding`, `header`, `skip`, `quote`, `escape`, `comment`, `nullstr`, `trim_whitespace`, `columns`
- le opzioni di robustezza presenti nel file suggerito non cambiano la policy di lettura: restano governate da `clean.read_mode` e dal preset robusto
- il metadata CLEAN salva anche `read_source_used` (`strict` / `robust` / `parquet`)
- il metadata CLEAN salva `read_params_used` con i parametri finali effettivamente usati dal reader
- il metadata CLEAN salva `read_params_source` con le sorgenti del merge (`defaults`, `suggested`, `config_overrides`)
- ogni `metadata.json` include `metadata_schema_version: 1`

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
