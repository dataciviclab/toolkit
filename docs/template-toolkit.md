# 🧰 1️⃣ TOOLKIT (motore centrale)

## 🎯 Ruolo

È il **motore standardizzato** del Lab.

Fa:

* RAW → CLEAN → MART
* Logging
* Metadata
* Validazione
* (Futuro) Load BigQuery

Non contiene logica specifica di un dataset.

---

## 📁 Struttura finale toolkit

```text
toolkit/
│
├── pyproject.toml
├── README.md
├── .github/workflows/ci.yml
│
├── toolkit/
│   ├── __init__.py
│   ├── version.py
│
│   ├── core/
│   │   ├── config.py
│   │   ├── logging.py
│   │   ├── paths.py
│   │   ├── metadata.py
│   │   ├── registry.py
│   │   ├── exceptions.py
│   │   └── validators.py
│   │
│   ├── plugins/
│   │   ├── http_file.py
│   │   ├── api_json_paged.py
│   │   └── html_table.py
│   │
│   ├── raw/
│   │   ├── extractors.py
│   │   └── run.py
│   │
│   ├── clean/
│   │   ├── run.py
│   │   ├── validate.py
│   │   └── sql_runner.py
│   │
│   ├── mart/
│   │   ├── run.py
│   │   └── validate.py
│   │
│   └── cli/
│       └── app.py
│
└── tests/
```

---

## 🧠 Responsabilità toolkit

### RAW

* Download (plugin-based)
* Extract (zip/identity)
* Save su Drive
* Metadata + checksum

### CLEAN

* Legge RAW
* Esegue SQL DuckDB
* Esporta parquet
* Validazione chiavi/required

### MART

* Legge CLEAN parquet
* Esegue SQL aggregazioni
* Esporta parquet
* (opzionale) load BigQuery

---

## 🔁 Esecuzione

```bash
toolkit run raw   -c dataset.yml
toolkit run clean -c dataset.yml
toolkit run mart  -c dataset.yml
toolkit validate clean -c dataset.yml
```

---

# 📦 2️⃣ PROJECT-TEMPLATE (repo progetto)

## 🎯 Ruolo

Contiene solo:

* configurazione dataset
* SQL di trasformazione
* documentazione
* notebook esplorativi

Non contiene ETL generico.

---

## 📁 Struttura definitiva project-template

```text
project-template/
│
├── README.md
├── dataset.yml
│
├── sql/
│   ├── clean.sql
│   └── mart/
│       ├── mart_regione_anno.sql
│       └── mart_provincia_anno.sql
│
├── notebooks/
│   ├── 00_exploration.ipynb
│   └── 01_run_pipeline.ipynb
│
├── docs/
│   ├── metodo.md
│   ├── assunzioni.md
│   ├── limiti.md
│   └── replicabilità.md
│
└── dashboard/
    └── definition.md
```

---

# 📄 `dataset.yml` (cuore del progetto)

Esempio standard:

```yaml
root: null

dataset:
  name: ispra_catasto_rifiuti
  years: [2022]

raw:
  source:
    type: http_file
    client:
      timeout: 60
      retries: 2
    args:
      url: "https://...aa={year}"
  extractor:
    type: identity

clean:
  sql: "sql/clean.sql"
  validate:
    required: ["anno", "regione", "provincia", "comune"]
    keys: ["anno", "regione", "provincia", "comune"]

mart:
  tables:
    - name: mart_regione_anno
      sql: "sql/mart/mart_regione_anno.sql"
    - name: mart_provincia_anno
      sql: "sql/mart/mart_provincia_anno.sql"

bq:
  enabled: false
  project_id: "dataciviclab"
  dataset_id: "mart"
```

---

# 🧠 Architettura mentale completa

```text
Project Repo
   │
   │ dataset.yml + sql
   ▼
Toolkit (motore)
   │
   ├── RAW  → Drive/raw
   ├── CLEAN → Drive/clean
   ├── MART → Drive/mart
   └── (opz.) → BigQuery
```

---

# 🔥 Perché questa separazione è potente

## Toolkit

= infrastruttura

## Project Repo

= logica analitica

---

# 🎯 Regole d’oro del Lab

1. Nessuna logica ETL nei notebook
2. Nessuna duplicazione codice tra progetti
3. SQL sempre esterno
4. Metadata sempre generato
5. dataset.yml = contratto del progetto

---

# 🏗 Stato maturità

Con questa struttura:

* M1–M3 toolkit = chiari
* Project-template = replicabile
* Drive now
* BigQuery ready
* Metodo documentabile

Tu adesso vuoi consolidare o stress-testare?
