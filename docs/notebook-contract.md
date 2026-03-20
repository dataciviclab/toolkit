# Notebook Contract

Nei repo dataset clonati dal template, i notebook non dovrebbero ricostruire la logica della pipeline. Dovrebbero leggere gli output reali e i metadata stabili prodotti dal toolkit.

Contratto stabile:

- RAW: `root/data/raw/<dataset>/<year>/`
- CLEAN: `root/data/clean/<dataset>/<year>/`
- MART: `root/data/mart/<dataset>/<year>/`
- run records: `root/data/_runs/<dataset>/<year>/`

File utili:

- RAW: `manifest.json`, `metadata.json`, `raw_validation.json`
- CLEAN: `<dataset>_<year>_clean.parquet`, `manifest.json`, `metadata.json`
- MART: `<table>.parquet`, `manifest.json`, `metadata.json`

Ruoli dei file:

- `metadata.json`: payload ricco del layer. Contiene input, output, `config_hash` e campi specifici del layer.
- `manifest.json`: summary stabile del layer. Punta a metadata e validation e riassume `ok/errors_count/warnings_count`.
- run record in `data/_runs/...`: stato del run (`run_id`, layer, validations, status), utile per `status` e `resume`.
- `inspect paths --json`: helper read-only per notebook e script locali; restituisce i path assoluti utili del runtime, incluso `latest_run`.

Per evitare duplicazione di path logic nei notebook:

- leggi `dataset.yml`
- usa `toolkit inspect paths --config dataset.yml --year <year> --json`
- poi apri parquet, metadata, manifest, validation e run record dai path restituiti

Nota pratica:

- `inspect paths` restituisce path assoluti della macchina locale: e' pensato per notebook e script nello stesso ambiente, non come formato portabile tra macchine diverse.

## Contratto operativo di `inspect paths`

`inspect paths` e' il comando da usare quando il problema e':

- trovare i path runtime gia` risolti dal toolkit
- evitare di ricostruire a mano `root/data/...`
- leggere l'ultimo run disponibile per un anno
- recuperare i principali hint RAW senza aprire a mano i metadata

Input minimo:

- `--config dataset.yml`
- opzionale `--year` per restringere il payload a un solo anno

Output garantito in `--json`:

- `dataset`, `year`, `config_path`, `root`
- `paths.raw` con `dir`, `manifest`, `metadata`, `validation`
- `paths.clean` con `dir`, `output`, `manifest`, `metadata`, `validation`
- `paths.mart` con `dir`, `outputs`, `manifest`, `metadata`, `validation`
- `paths.run_dir`
- `raw_hints` con:
  - `primary_output_file`
  - `suggested_read_path`
  - `suggested_read_exists`
  - `encoding`
  - `delim`
  - `decimal`
  - `skip`
  - `warnings`
- `latest_run`

Regola pratica:

- notebook e script locali: usa sempre `inspect paths --json`
- CI che deve validare `effective_root` o path contract: usa `inspect paths --json`
- se non passi `--year`, il payload puo` essere una lista multi-anno

## Differenza rispetto a `inspect schema-diff`

`inspect schema-diff` non e' un helper per notebook.

Serve invece quando vuoi:

- confrontare schema e hints RAW tra anni configurati
- capire se una fonte multi-anno ha drift di colonne o profile hints
- decidere se `clean.read` o una nota metodologica devono esplicitare un caveat

In breve:

- `inspect paths`: "dove sono gli artefatti e quale runtime path contract posso usare?"
- `inspect schema-diff`: "il RAW cambia tra anni e quanto cambia?"

Regola pratica:

- il toolkit produce
- i notebook ispezionano
- `dataset.yml` resta la fonte di verita` per root, dataset, anni e path relativi
