# bdap_ckan_csv

Smoke manuale per `ckan` contro OpenBDAP, con fallback `resource_show -> package_show` e force `https`.

## Comandi

```bash
toolkit run raw --config dataset.yml
toolkit profile raw --config dataset.yml
toolkit run clean --config dataset.yml
toolkit run mart --config dataset.yml
toolkit status --dataset bdap_ckan_csv --year 2022 --latest --config dataset.yml
```

## Verifiche attese

- `./_smoke_out/data/raw/bdap_ckan_csv/2022/manifest.json`
- `./_smoke_out/data/raw/bdap_ckan_csv/2022/raw_validation.json`
- `./_smoke_out/data/raw/bdap_ckan_csv/2022/_profile/raw_profile.json`
- `./_smoke_out/data/raw/bdap_ckan_csv/2022/_profile/suggested_read.yml`
- `./_smoke_out/data/clean/bdap_ckan_csv/2022/metadata.json` con `read_params_source`, `read_source_used`, `read_params_used`
- `./_smoke_out/data/mart/bdap_ckan_csv/2022/mart_ok.parquet`

## Note

- questo smoke usa un dataset OpenBDAP reale
- il portale espone `package_show`, ma `resource_show` non risolve: il caso serve proprio a verificare il fallback del plugin
- l'URL file restituito dal portale puo' arrivare in `http://`: il toolkit lo forza a `https://` prima del download
