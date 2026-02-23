# 📦 DataCivicLab Toolkit

Framework modulare per costruire pipeline dati **RAW → CLEAN → MART** replicabili, validate e versionate.

Progettato per progetti civici, open data e dataset pubblici.

---

## 🎯 Obiettivo

Standardizzare la costruzione di pipeline dati nei progetti DataCivicLab:

* Ingestione RAW controllata
* Validazione automatica
* Trasformazioni SQL riproducibili
* Profilazione dataset
* Output MART pronti per dashboard

Il toolkit separa:

```
Progetto = dataset specifico
Toolkit  = motore standardizzato
```

---

## 🧠 Architettura

```
toolkit/
│
├── raw/        → estrazione + validazione RAW
├── clean/      → trasformazioni SQL + validazione CLEAN
├── mart/       → aggregazioni finali + validazione MART
├── profile/    → profiling dataset RAW
├── plugins/    → connettori sorgenti (HTTP, API, HTML, local)
├── core/       → config, registry, logging, metadata, paths
├── cli/        → interfaccia a linea di comando
```

Pipeline standard:

```
Fonte → RAW → CLEAN → MART
```

Ogni layer è:

* Validato
* Testato
* Riproducibile

---

## ⚙️ Installazione

```bash
git clone https://github.com/dataciviclab/toolkit.git
cd toolkit
pip install -e .
```

Richiede Python 3.10+

---

## 🚀 Uso base (CLI)

### 1️⃣ Eseguire layer RAW

```bash
toolkit run raw --config dataset.yml
```

---

### 2️⃣ Profilare un dataset RAW

```bash
toolkit profile --config dataset.yml
```

Output:

* report colonne
* suggerimenti tipo dati
* anomalie

---

### 3️⃣ Eseguire layer CLEAN

```bash
toolkit run clean --config dataset.yml
```

---

### 4️⃣ Eseguire layer MART

```bash
toolkit run mart --config dataset.yml
```

---

### 5️⃣ Validare un layer

```bash
toolkit validate clean --config dataset.yml
```

---

## 🗂️ Struttura di un progetto

Vedi `project-example/`

```
project/
│
├── dataset.yml
├── sql/
│   ├── clean.sql
│   └── mart/
│       ├── mart_regione_anno.sql
│       └── mart_provincia_anno.sql
```

Il progetto contiene:

* Config
* SQL
* Nessuna logica Python custom

Il motore resta nel toolkit.

---

## 🔌 Plugin sorgenti supportati

Nel modulo `plugins/`:

* `local_file`
* `http_file`
* `api_json_paged`
* `html_table`

Estendibili via registry.

---

## 🧪 Testing

Test automatici inclusi:

* config
* registry
* validazione layer
* rules
* profile
* extractors

Eseguire:

```bash
pytest
```

CI attiva via GitHub Actions (`.github/workflows/ci.yml`).

---

## 📐 Filosofia progettuale

Il toolkit impone:

* RAW intoccabile
* CLEAN deterministico
* MART leggibile
* Config dichiarativa
* SQL separato dal motore
* Validazione a ogni layer

Obiettivo:
Costruire pipeline civiche replicabili tra progetti diversi.

---

## 🧩 Come scalare

Ogni nuovo dataset:

1. Creare nuovo repo da `project-template`
2. Scrivere `dataset.yml`
3. Scrivere SQL
4. Usare il toolkit come motore

Il toolkit non contiene dataset.
Contiene metodo.

---

## 🤝 Contribuire

1. Fork
2. Branch feature
3. PR con test
4. Validazione CI obbligatoria

---

## 📜 Licenza

Da definire (MIT consigliata per massima adozione open civic).