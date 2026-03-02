# Smoke Suite

Suite manuale di mini-progetti per verificare il toolkit end-to-end senza pytest.

Progetti inclusi:

- `smoke/ispra_http_csv`: `http_file` contro server locale `http.server`
- `smoke/local_file_csv`: `local_file` completamente offline
- `smoke/zip_http_csv`: `http_file` + extractor ZIP (`unzip_first_csv`) contro server locale
- `smoke/bdap_http_csv`: `http_file` contro CSV pubblico BDAP
- `smoke/finanze_http_zip_2023`: `http_file` contro ZIP pubblico reale, best-effort

Ogni progetto include:

- `dataset.yml` minimo
- `sql/clean.sql`
- `sql/mart/mart_ok.sql`
- `README.md` con i comandi del caso smoke, incluso un `--dry-run --strict-config` iniziale

Prerequisito:

- toolkit installato in ambiente locale
- per i casi HTTP, avviare un server dalla directory del progetto con:

```bash
py -m http.server 8000
```
