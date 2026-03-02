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

Per evitare duplicazione di path logic nei notebook:

- leggi `dataset.yml`
- usa `toolkit inspect paths --config dataset.yml --year <year> --json`
- poi apri parquet, metadata e run record dai path restituiti

Regola pratica:

- il toolkit produce
- i notebook ispezionano
- `dataset.yml` resta la fonte di verita` per root, dataset, anni e path relativi
