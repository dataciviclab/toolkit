# Smoke Suite

Suite manuale di mini-progetti per verificare il toolkit end-to-end senza pytest.

Questa cartella non definisce il core smoke contract del repository:

- lo smoke offline canonico resta `project-example`
- gli smoke tecnici deterministici restano in `tests/test_smoke_tiny_e2e.py`
- i casi sotto `smoke/` sono playbook manuali, utili per controlli operativi su sorgenti locali o pubbliche

Di conseguenza:

- non sono CI gate
- alcune sorgenti pubbliche possono cambiare formato o disponibilita`
- eventuali output locali sotto `_smoke_out/` non vanno tenuti nella working tree

Progetti inclusi:

- `smoke/local_file_csv`: `local_file` completamente offline
- `smoke/zip_http_csv`: `http_file` + extractor ZIP (`unzip_first_csv`) contro server locale
- `smoke/bdap_http_csv`: `http_file` contro CSV pubblico BDAP
- `smoke/bdap_ckan_csv`: `ckan` contro OpenBDAP, con fallback `package_show` e force `https`
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
