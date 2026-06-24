# ADR-004: Parquet come formato di output

**Status:** implemented (2026-02)
**Rivisitazione:** 2026-05 — pyarrow reso opzionale

## Contesto

Il toolkit produce dati pronti per analisi. Il formato di output doveva:

- essere leggibile da strumenti di analisi (DuckDB, Pandas, R, Python)
- supportare colonnare (efficiente per query selettive)
- comprimere bene (dati pubblici italiani = molte stringhe, codici ISTAT)
- essere autocontenuto (schema incluso nel file)
- supportare dataset di dimensioni variabili (da pochi KB a centinaia di MB)

Alternative valutate:

| Formato | Pro | Contro |
|---|---|---|
| **CSV** | universale, leggibile | nessuno schema, lento, grande, no tipi |
| **Parquet** | colonnare, compresso, schema, DuckDB nativo | non human-readable |
| **JSON** | human-readable | 5-10x più grande, no schema forte |

## Decisione

Parquet come formato unico di output per CLEAN e MART.

- Scrittura via DuckDB `COPY TO ... (FORMAT PARQUET)` — nessuna dipendenza extra
- Lettura via DuckDB `read_parquet()` — nativa, zero-copia
- `pyarrow` reso opzionale (v1.2) — chi serve pyarrow direttamente installa `[parquet]`
- RAW conserva il formato originale (CSV, Excel, ZIP, JSON) — è l'artefatto sorgente

## Conseguenze

**Positive:**
- DuckDB legge Parquet senza conversioni
- Compressione automatica (snappy di default): riduzione 5-10x rispetto a CSV
- Schema embedded: ogni parquet contiene i tipi delle colonne
- Lettura selettiva: DuckDB legge solo le colonne richieste
- Pyarrow non è più dipendenza diretta (riduce spazio installazione)

**Negative:**
- Non human-readable: per ispezionare serve `toolkit inspect summary`, DuckDB o `pandas.read_parquet()`
- Strumenti legacy (Excel) non leggono Parquet direttamente
- Dimensione minima: file Parquet piccoli (< 100 righe) hanno overhead
