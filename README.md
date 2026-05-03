# DataCivicLab Toolkit

Motore tecnico per trasformazioni riproducibili su dati pubblici.
Scarica file da internet (o da sorgenti locali), li normalizza e li aggrega in tabelle pronte per l'analisi.

## 1. Core Workflow

```bash
# Scarica, pulisce e aggrega
toolkit run all --config dataset.yml

# Verifica che l'output sia valido
toolkit validate all --config dataset.yml

# Mostra l'ultimo run completato
toolkit status --dataset <name> --year <year> --latest --config dataset.yml
```

Se `toolkit` non è nel `PATH`:

```bash
python -m toolkit.cli.app run all --config dataset.yml
```

## 2. Installazione

Richiede Python 3.10+.

```bash
pip install -e .[dev]

# Smoke test
toolkit run all -c project-example/dataset.yml
toolkit validate all -c project-example/dataset.yml
toolkit status --dataset project_example --year 2022 --latest --config project-example/dataset.yml
```

## 3. Come funziona

Il toolkit prende un file di configurazione (`dataset.yml`) e una serie di trasformazioni SQL,
e produce tre livelli di output:

| Layer | Cosa contiene | Chi lo legge |
|---|---|---|
| **RAW** | Il dato grezzo scaricato, senza modifiche | chi vuole verificare la fonte |
| **CLEAN** | Il dato normalizzato e pulito | chi costruisce analisi |
| **MART** | Il dato aggregato, pronto per l'analisi | chi legge i risultati |

Il file `dataset.yml` dice al toolkit: dove trovare i dati sorgente,
come chiamare i file SQL di trasformazione, e dove salvare l'output.

**Esempio minimo di `dataset.yml`:**

```yaml
dataset:
  name: mio_dataset
  years: [2023]
raw:
  sources:
    - type: http_file
      url: https://example.com/dati.csv
clean:
  sql: sql/clean.sql
mart:
  sql: sql/mart.sql
```

Per la specifica completa: [config-schema.md](docs/config-schema.md).

## 4. CLI — quale comando usare

- **`run all`** — prima esecuzione di un dataset, o dopo aver cambiato fonte, anni o config
- **`run clean` + `run mart`** — dopo aver modificato solo la logica SQL (più veloce di `run all`)
- **`run mart`** — dopo aver modificato solo le aggregazioni finali
- **`resume`** — riprende un run interrotto a metà senza rifare i layer già completati
- **`inspect paths`** — mostra dove sono gli output generati (utile nei notebook)
- **`inspect schema-diff`** — confronta la struttura del dato grezzo tra anni diversi

```bash
# Riprende da dove si era interrotto
toolkit resume --dataset mio_dataset --year 2023 --latest --config dataset.yml

# Trova i path degli output
toolkit inspect paths --config dataset.yml --year 2023 --json

# Confronta il dato grezzo tra anni
toolkit inspect schema-diff --config dataset.yml
```

## 5. Struttura del Contratto (dataset.yml)

Il toolkit risolve `dataset.yml` + `sql/` per produrre output auditabili.

- **`root`**: Directory di output (default: directory del file config)
- **`raw`**: Sorgenti — `http_file`, `local_file`, `ckan`, `sdmx`, `sparql`
- **`clean`**: Normalizzazione tramite `sql/clean.sql`
- **`mart`**: Aggregazione tramite `sql/mart/*.sql`
- **`validation`**: Gate di qualità — fallisce al primo errore se `fail_on_error: true`

Contratti correlati:
- [notebook-contract.md](docs/notebook-contract.md) — come leggere gli output nei notebook
- [advanced-workflows.md](docs/advanced-workflows.md) — resume, profile, run parziali

## 6. Riferimenti

| Documento | Contenuto |
|---|---|
| [config-schema.md](docs/config-schema.md) | Specifica completa YAML |
| [conventions.md](docs/conventions.md) | Convenzioni su path, metadata, manifest |
| [advanced-workflows.md](docs/advanced-workflows.md) | Resume, profile, run parziali |
| [notebook-contract.md](docs/notebook-contract.md) | Come leggere gli output |
| [feature-stability.md](docs/feature-stability.md) | Cosa è stabile, cosa è sperimentale |

## 7. Sviluppo e QA

- **Test**: `pytest`
- **Lint**: `ruff check .`
- **Output runtime**: vivono in `out/` o `_smoke_out/` — non versionare mai dati

---

## 8. Ruolo nell'ecosistema

```
dataset-incubator  →  toolkit  →  GCS (parquet clean/mart)  →  data-explorer
```

Il toolkit non gestisce il deployment: scrive nella directory configurata via `root` o `DCL_ROOT`.
La CI di `dataset-incubator` carica gli output su GCS dopo ogni run validato.
`data-explorer` li legge via DuckDB per la visualizzazione pubblica.

**Boundary**: questa repo è il motore. I contratti dataset reali vivono in `dataset-incubator`.
