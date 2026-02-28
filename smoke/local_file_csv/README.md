# local_file_csv

Smoke manuale offline per `local_file`.

## Comandi

```bash
toolkit run raw --config dataset.yml
toolkit profile raw --config dataset.yml
toolkit run clean --config dataset.yml
toolkit run mart --config dataset.yml
toolkit status --dataset local_file_csv --year 2022 --latest --config dataset.yml
```

## Verifiche attese

- `./_smoke_out/data/raw/local_file_csv/2022/manifest.json`
- `./_smoke_out/data/raw/local_file_csv/2022/_profile/raw_profile.json`
- `./_smoke_out/data/raw/local_file_csv/2022/_profile/suggested_read.yml`
- `./_smoke_out/data/clean/local_file_csv/2022/local_file_csv_2022_clean.parquet`
- `./_smoke_out/data/clean/local_file_csv/2022/metadata.json` con `read_params_source`, `read_source_used`, `read_params_used`
- `./_smoke_out/data/mart/local_file_csv/2022/mart_ok.parquet`
