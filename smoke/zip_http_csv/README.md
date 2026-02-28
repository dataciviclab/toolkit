# zip_http_csv

Smoke manuale per `http_file` con ZIP locale servito via HTTP e extractor `unzip_first_csv`.

## Setup

Avvia un server HTTP dalla directory di questo progetto:

```bash
py -m http.server 8000
```

In un secondo terminale:

```bash
toolkit run raw --config dataset.yml
toolkit profile raw --config dataset.yml
toolkit run clean --config dataset.yml
toolkit run mart --config dataset.yml
toolkit status --dataset zip_http_csv --year 2022 --latest --config dataset.yml
```

## Verifiche attese

- `./_smoke_out/data/raw/zip_http_csv/2022/manifest.json`
- `./_smoke_out/data/raw/zip_http_csv/2022/raw_validation.json`
- `./_smoke_out/data/raw/zip_http_csv/2022/_profile/raw_profile.json`
- `./_smoke_out/data/raw/zip_http_csv/2022/_profile/suggested_read.yml`
- `./_smoke_out/data/clean/zip_http_csv/2022/metadata.json` con `read_params_source`, `read_source_used`, `read_params_used`
- `./_smoke_out/data/mart/zip_http_csv/2022/mart_ok.parquet`

Controlli contratto:

- RAW `manifest.json` contiene `primary_output_file`
- il file primario estratto dallo ZIP e` quello che CLEAN vede a valle
