# local_file_csv

Smoke offline per `local_file` — golden path di riferimento per i test contract.

## Feature coperte

- Source type: `local_file`
- Anni multipli: `[2022, 2023]`
- `read_mode: fallback`
- `read`: config esplicita (delim, encoding, header, mode: largest)
- `required_columns`: anno, comune, regione, valore
- `validate`: min_rows, not_null su comune e anno
- `mart`: 2 tabelle (mart_regione, mart_categoria) con required_tables e validate
- `output.artifacts: standard`
- `source_id`, `time_coverage`

## Comandi

```bash
toolkit run raw --config dataset.yml
toolkit profile raw --config dataset.yml
toolkit run clean --config dataset.yml
toolkit run mart --config dataset.yml
toolkit run all --config dataset.yml
toolkit run full --config dataset.yml
toolkit inspect summary -c dataset.yml --year 2022
```

## Verifiche attese

- `./_smoke_out/data/raw/local_file_csv/2022/manifest.json`
- `./_smoke_out/data/raw/local_file_csv/2023/local_file_sample_2023.csv`
- `./_smoke_out/data/raw/local_file_csv/2022/_profile/raw_profile.json`
- `./_smoke_out/data/clean/local_file_csv/2022/local_file_csv_2022_clean.parquet`
- `./_smoke_out/data/clean/local_file_csv/2022/metadata.json`
- `./_smoke_out/data/mart/local_file_csv/2022/mart_regione.parquet`
- `./_smoke_out/data/mart/local_file_csv/2022/mart_categoria.parquet`
- `./_smoke_out/data/mart/local_file_csv/2022/_validate/mart_validation.json`
