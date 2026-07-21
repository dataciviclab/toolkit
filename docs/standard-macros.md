# Standard SQL Macros

Le **macro SQL** sono funzioni DuckDB precaricate automaticamente in ogni esecuzione del layer CLEAN. Servono a eliminare il boilerplate che oggi è riscritto da zero in ogni `clean.sql` — `TRY_CAST`, `REPLACE` per numeri italiani, `CASE` per flag booleani, `TRIM` per stringhe.

## Come funzionano

Le macro sono definite in `toolkit/sql/macros.sql` (dentro il pacchetto, distribuite via wheel). Quando il toolkit esegue un `clean.sql`, carica automaticamente tutte le macro nella connessione DuckDB prima di eseguire la query. Ogni macro è `CREATE OR REPLACE` — sicura da eseguire più volte.

**Non serve** importare nulla, includere file o modificare `dataset.yml`. Le macro sono sempre disponibili.

## Elenco macro

### `normalize_string(val)`

`TRIM` + stringa vuota → `NULL`. Per colonne testuali.

```sql
-- PRIMA
TRIM(CAST("Denominazione" AS VARCHAR)) AS denominazione,
NULLIF(TRIM("Codice"::VARCHAR), '') AS codice,

-- DOPO
normalize_string("Denominazione") AS denominazione,
normalize_string("Codice") AS codice,
```

### `cast_int(val)`

`TRY_CAST(val AS INTEGER)`. Per colonne numeriche intere (32-bit).

```sql
-- PRIMA
TRY_CAST("Prog" AS INTEGER) AS progressivo,
CAST("Anno" AS INTEGER) AS anno,

-- DOPO
cast_int("Prog") AS progressivo,
cast_int("Anno") AS anno,
```

### `cast_bigint(val)`

`TRY_CAST(val AS BIGINT)`. Per colonne numeriche grandi (64-bit). Usato dallo scaffold per colonne mappate come `int`/`bigint`.

```sql
-- PRIMA
TRY_CAST("Numero contribuenti" AS BIGINT) AS numero_contribuenti,

-- DOPO
cast_bigint("Numero contribuenti") AS numero_contribuenti,
```

### `cast_double(val)`

`TRY_CAST(val AS DOUBLE)`. Per colonne numeriche con decimali.

```sql
-- PRIMA
TRY_CAST(TRIM(CAST("Importo" AS VARCHAR)) AS DOUBLE) AS importo,

-- DOPO
cast_double("Importo") AS importo,
```

### `normalize_italian_number(val)`

Converte un numero in formato italiano (`1.234,56` → `1234.56`). Rimuove punti migliaia, converte virgola decimale in punto. `TRY_CAST` restituisce `NULL` se la conversione fallisce.

```sql
-- PRIMA (15 righe per 4 colonne)
TRY_CAST(REPLACE(REPLACE("Importo"::VARCHAR, '.', ''), ',', '.') AS DOUBLE) AS importo,
TRY_CAST(REPLACE(REPLACE("Numero scelte"::VARCHAR, '.', ''), ',', '.') AS INTEGER) AS numero_scelte,

-- DOPO (2 righe)
normalize_italian_number("Importo") AS importo,
normalize_italian_integer("Numero scelte") AS numero_scelte,
```

### `normalize_italian_integer(val)`

Come `normalize_italian_number` ma restituisce `INTEGER`. DuckDB `CAST(DOUBLE AS INTEGER)` **arrotonda** (non tronca): `5.432,90` → `5433`.

### `decode_flag(val, yes_value)`

Decodifica un flag testuale in `BOOLEAN`. Il secondo argomento è il valore che rappresenta `TRUE`.

```sql
-- PRIMA
CASE WHEN TRIM("ETS") = 'X' THEN TRUE ELSE FALSE END AS flag_ets,

-- DOPO
decode_flag("ETS", 'X') AS flag_ets,
```

### `remove_dot_thousands(val)`

Rimuove punti migliaia da **numeri interi**. Attenzione: rimuove **tutti** i punti, incluso un eventuale separatore decimale standard. Usa solo su interi con punti migliaia. Per numeri con decimali usa `normalize_italian_number` o `cast_double`.

```sql
-- SOLO PER INTERI
remove_dot_thousands("Popolazione") AS popolazione,  -- "1.234" → 1234.0

-- NON USARE SU DECIMALI — usa invece:
normalize_italian_number("Importo") AS importo,        -- "1.234,56" → 1234.56
cast_double("Valore") AS valore,                       -- "1234.56" → 1234.56
```

## Esempio completo

Prima (`ade-cinque-per-mille/sql/clean.sql`, 21 righe):

```sql
SELECT
    {year}::INTEGER AS anno,
    TRY_CAST("Prog" AS INTEGER) AS progressivo,
    TRIM("Codice fiscale") AS codice_fiscale,
    TRIM("Denominazione") AS denominazione,
    TRIM("Regione") AS regione,
    TRIM("PR") AS sigla_provincia,
    TRIM("Comune") AS comune,
    CASE WHEN TRIM("ETS") = 'X' THEN TRUE ELSE FALSE END AS flag_ets_onlus,
    CASE WHEN TRIM("ASD") = 'X' THEN TRUE ELSE FALSE END AS flag_asd,
    -- ... altri CASE WHEN identici ...
    TRY_CAST(REPLACE(REPLACE("Numero scelte"::VARCHAR, '.', ''), ',', '.') AS INTEGER) AS numero_scelte,
    TRY_CAST(REPLACE(REPLACE("Importo delle scelte espresse"::VARCHAR, '.', ''), ',', '.') AS DOUBLE) AS importo_scelte_espresse,
FROM raw_input
```

Dopo (18 righe, zero boilerplate — solo logica di dominio):

```sql
SELECT
    {year}::INTEGER AS anno,
    cast_int("Prog") AS progressivo,
    normalize_string("Codice fiscale") AS codice_fiscale,
    normalize_string("Denominazione") AS denominazione,
    normalize_string("Regione") AS regione,
    normalize_string("PR") AS sigla_provincia,
    normalize_string("Comune") AS comune,
    decode_flag("ETS", 'X') AS flag_ets_onlus,
    decode_flag("ASD", 'X') AS flag_asd,
    -- ... altri decode_flag identici ...
    normalize_italian_integer("Numero scelte") AS numero_scelte,
    normalize_italian_number("Importo delle scelte espresse") AS importo_scelte_espresse,
FROM raw_input
```

## Perché macro SQL invece di Python preprocessing?

| Approccio | Dove opera | Pro |
|---|---|---|
| **Macro DuckDB** | Dentro `clean.sql` | Puro SQL, testabile in DuckDB CLI, zero dipendenze Python |
| `normalize.py` (Python) | Prima di DuckDB (script/extractor) | Logica più complessa, gestione encoding, regex rename |

Le macro DuckDB e le funzioni Python in `toolkit/core/normalize.py` sono complementari: `normalize.py` prepara i dati prima che entrino in DuckDB, le macro lavorano dentro DuckDB.

## Verifica

Per testare una macro direttamente:

```bash
python -c "
import duckdb
con = duckdb.connect()
con.execute(open('toolkit/sql/macros.sql').read())
print(con.execute(\"SELECT normalize_italian_number('1.234,56')\").fetchone())
"
```

I test automatici: `pytest tests/test_macros_sql.py -v` (35 test).
