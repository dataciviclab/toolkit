# finanze_http_zip_2023

Smoke manuale per `http_file` su ZIP pubblico del MEF con extractor `unzip_first_csv`.

## Comandi

```bash
py -m toolkit.cli.app run all -c smoke/finanze_http_zip_2023/dataset.yml
py -m toolkit.cli.app status --dataset finanze_http_zip_2023 --year 2023 --config smoke/finanze_http_zip_2023/dataset.yml
```

## Verifiche attese

- `./_smoke_out/data/raw/finanze_http_zip_2023/2023/manifest.json`
- `./_smoke_out/data/raw/finanze_http_zip_2023/2023/raw_validation.json`
- `./_smoke_out/data/clean/finanze_http_zip_2023/2023/finanze_http_zip_2023_2023_clean.parquet`
- `./_smoke_out/data/mart/finanze_http_zip_2023/2023/irpef_by_regione.parquet`

## Nota

Questo smoke usa una sorgente pubblica reale. Se il file ZIP cambia nome interno, struttura CSV o disponibilita`, lo smoke puo` fallire anche senza regressioni nel toolkit.
