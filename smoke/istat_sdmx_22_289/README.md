# istat_sdmx_22_289

Smoke manuale per `sdmx` su dataflow ISTAT `22_289`.

## Comandi

```bash
toolkit run raw --config dataset.yml
toolkit profile raw --config dataset.yml
toolkit run clean --config dataset.yml
toolkit run mart --config dataset.yml
toolkit status --dataset istat_sdmx_22_289 --year 2024 --latest --config dataset.yml
```

## Verifiche attese

- `./_smoke_out/data/raw/istat_sdmx_22_289/2024/manifest.json`
- `./_smoke_out/data/raw/istat_sdmx_22_289/2024/raw_validation.json`
- `./_smoke_out/data/clean/istat_sdmx_22_289/2024/metadata.json`
- `./_smoke_out/data/mart/istat_sdmx_22_289/2024/mart_ok.parquet`

## Nota

Questo smoke usa un flow ISTAT reale e richiede che la versione `1.5` sia ancora quella esposta dal dataflow `22_289`.
