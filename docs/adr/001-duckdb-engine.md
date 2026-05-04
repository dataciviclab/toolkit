# ADR-001: DuckDB come motore di trasformazione

**Status:** implemented (2026-02)
**Rivisitazione:** 2026-05 — confermata

## Contesto

Il toolkit doveva trasformare dati tabellari (CSV, Excel, Parquet) attraverso
fasi successive (RAW → CLEAN → MART). Serviva un motore che:

- leggesse CSV con delimiteri, encoding e quoting eterogenei
- eseguisse SQL di trasformazione con JOIN, GROUP BY, window function
- scrivesse output in formato colonnare (Parquet)
- funzionasse in-process, senza server esterno
- gestisse dataset da poche righe a centinaia di migliaia

Alternative valutate:

| Opzione | Vantaggi | Svantaggi |
|---|---|---|
| **Pandas** | diffuso, flessibile | memoria doubling, tipi deboli, no SQL nativo |
| **SQLite** | SQL standard, file-based | no Parquet, poor CSV encoding handling |
| **DuckDB** | SQL completo, Parquet nativo, zero-copy CSV, embedded | ecosistema più giovane |

## Decisione

Usare DuckDB come motore unico di trasformazione.

- Lettura CSV via `read_csv_auto` con controllo esplicito dei parametri
- Trasformazioni via SQL puro (`SELECT`, `CREATE TABLE AS`)
- Output in Parquet via `COPY TO ... (FORMAT PARQUET)`
- DuckDB在用 come motore embedded (`duckdb.connect(":memory:")`)
- Pandas mantenuto solo per lettura Excel (.xls/.xlsx) — DuckDB non la supporta nativamente

## Conseguenze

**Positive:**
- SQL dichiarativo invece di DataFrame manipulation — più facile da revisionare
- Parquet nativo senza dipendenza da pyarrow (ora extra opzionale)
- Zero-copy CSV: non carica tutto in memoria
- DuckDB 0.10+ maturo e stabile

**Negative:**
- DuckDB ha un type system meno flessibile di Pandas per edge case (date malformate, tipi misti in colonna)
- `read_csv_auto` a volte inferisce tipi sbagliati — richiede override espliciti tramite `columns` in clean.read
- Excel richiede ancora Pandas come ponte (openpyxl/xlrd)
