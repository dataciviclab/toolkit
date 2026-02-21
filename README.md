# 🧰 DataCivicLab Toolkit

Motore standardizzato per pipeline dati:

```
RAW → CLEAN → MART
```

Progettato per:

* 📂 Storage su Drive (fase attuale)
* 🦆 Trasformazioni con DuckDB SQL
* 📊 Output Parquet per dashboard
* ☁️ BigQuery-ready (futuro)

---

# 🎯 Filosofia

Il toolkit **non contiene logica specifica di un dataset**.

Ogni progetto definisce:

* `dataset.yml`
* file SQL (`clean.sql`, `mart_*.sql`)
* documentazione

Il toolkit esegue in modo coerente e replicabile.

---

# 🏗 Architettura

```
Project Repo
   │
   │ dataset.yml + sql
   ▼
Toolkit
   ├── RAW
   ├── CLEAN
   ├── MART
   └── (optional) BigQuery Load
```

---

# 📦 Installazione

### Sviluppo locale

```bash
pip install -e .
```

### Da GitHub

```bash
pip install git+https://github.com/dataciviclab/toolkit.git
```

---

# 🚀 Esecuzione Pipeline

Dal repo progetto:

```bash
toolkit run raw   -c dataset.yml
toolkit run clean -c dataset.yml
toolkit run mart  -c dataset.yml
```

Validazione CLEAN:

```bash
toolkit validate clean -c dataset.yml
```

---

# 📁 Output Standard

I dati vengono salvati in:

```
DataCivicLab/
  data/
    raw/<dataset>/<year>/
    clean/<dataset>/<year>/
    mart/<dataset>/<year>/
```

Ogni cartella contiene:

* file dati
* `metadata.json`
* checksum

---

# 🦆 CLEAN & MART con DuckDB

Il toolkit usa DuckDB per:

* leggere CSV e Parquet
* eseguire trasformazioni SQL
* aggregare KPI
* esportare Parquet

## Regola

Nei file SQL si scrive **solo una SELECT**.

Esempio CLEAN:

```sql
SELECT
  CAST("Anno" AS INTEGER) AS anno,
  "Regione" AS regione
FROM raw_input;
```

Esempio MART:

```sql
SELECT
  anno,
  regione,
  AVG(pct_rd) AS pct_rd_avg
FROM clean
GROUP BY 1,2;
```

Il toolkit si occupa di:

* creare le view (`raw_input`, `clean`)
* creare le tabelle
* esportare parquet

---

# ⚙️ dataset.yml (contratto progetto)

Esempio minimale:

```yaml
dataset:
  name: ispra_catasto_rifiuti
  years: [2022]

raw:
  source:
    type: http_file
    args:
      url: "https://...aa={year}"
  extractor:
    type: identity

clean:
  sql: "sql/clean.sql"

mart:
  tables:
    - name: mart_regione_anno
      sql: "sql/mart/mart_regione_anno.sql"
```

---

# 🔌 Plugin Supportati (RAW)

* `http_file`
* `api_json_paged`
* `html_table` (estendibile)

Nuove fonti si aggiungono come plugin.

---

# ☁️ BigQuery (Futuro)

Configurabile in `dataset.yml`:

```yaml
bq:
  enabled: true
  project_id: "dataciviclab"
  dataset_id: "mart"
```

Quando attivato, il toolkit carica i parquet MART su BigQuery.

---

# 🧪 Test

```bash
pytest
```

CI attiva su PR.

---

# 📏 Regole del Toolkit

1. Nessuna logica dataset-specific nel core
2. SQL sempre esterno
3. Metadata sempre generato
4. Logging coerente
5. Nessuna duplicazione codice tra progetti

---

# 🎯 Roadmap

* [ ] RAW Engine
* [ ] CLEAN Engine
* [ ] MART Engine
* [ ] BigQuery Loader
* [ ] Config YAML avanzata
* [ ] CLI completa (`run all`)
* [ ] Test coverage 80%

---

# 🧠 Perché esiste

DataCivicLab vuole:

* trasformare open data pubblici in dataset puliti
* rendere replicabile l’analisi civica
* evitare notebook caotici
* costruire una mini data platform open

Questo toolkit è il motore.
