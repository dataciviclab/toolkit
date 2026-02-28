# bdap_http_csv

Smoke manuale per `http_file` su CSV pubblico BDAP.

## Comandi

```bash
toolkit run raw --config dataset.yml
toolkit profile raw --config dataset.yml
toolkit run clean --config dataset.yml
toolkit run mart --config dataset.yml
toolkit status --dataset bdap_http_csv --year 2022 --latest --config dataset.yml
```

## Verifiche attese

- `./_smoke_out/data/raw/bdap_http_csv/2022/manifest.json`
- `./_smoke_out/data/raw/bdap_http_csv/2022/raw_validation.json`
- `./_smoke_out/data/raw/bdap_http_csv/2022/_profile/raw_profile.json`
- `./_smoke_out/data/raw/bdap_http_csv/2022/_profile/suggested_read.yml`
- `./_smoke_out/data/clean/bdap_http_csv/2022/metadata.json` con `read_params_source`, `read_source_used`, `read_params_used`
- `./_smoke_out/data/mart/bdap_http_csv/2022/mart_ok.parquet`

## Nota

Questo smoke usa una sorgente HTTP pubblica reale. Se il CSV cambia formato o diventa non disponibile, il progetto smette di essere deterministico.
