-- =============================================================================
-- Standard Macros SQL per il layer CLEAN del toolkit DataCivicLab
--
-- Queste macro DuckDB sono automaticamente caricate in ogni esecuzione
-- clean.sql. Possono essere usate liberamente nei file SQL dei dataset
-- senza dover riscrivere il boilerplate.
--
-- Uso: SELECT normalize_italian_number("colonna") AS importo FROM raw_input
--
-- Tutte le macro sono CREATE OR REPLACE — sicure da eseguire più volte.
--
-- 📌 Il contratto macro (toolkit/contracts/) e' AUTO-GENERATO da questo file.
--    Quando aggiungi una macro, aggiungi un commento "-- @contract <chiave>: <valore>"
--    per ogni informazione che deve finire nel contratto (returns, warning, example, see...).
--    I test contract verificano che ogni macro abbia un contratto.
-- =============================================================================

-- ── normalize_italian_number ───────────────────────────────────────────────
-- Converte un numero in formato italiano (1.234,56 → 1234.56).
-- Rimuove punti migliaia, converte virgola decimale in punto.
-- TRY_CAST restituisce NULL se la conversione fallisce.
-- L'input DEVE essere VARCHAR: se il reader ha gia' tipizzato la colonna
-- (DOUBLE/FLOAT/DECIMAL con decimali) l'informazione di formato italiano e'
-- persa, e la macro solleva un errore invece di corrompere il valore.
-- @contract returns: DOUBLE
-- @contract warning: NON usare se read.decimal=',' e' configurato in dataset.yml.
--   DuckDB ha gia' parsato il numero con la virgola decimale: basta CAST(x AS DOUBLE).
--   NON usare su colonne tipizzate dal reader: con clean.read.source=auto lo
--   sniffing puo' tipizzare DOUBLE una colonna di numeri italiani (es. "121.330")
--   e la macro corromperebbe il valore (×10 / ÷10). In quel caso usa
--   clean.read.all_varchar=true oppure dichiara la colonna VARCHAR in clean.read.columns.
-- @contract see: remove_dot_thousands per interi con punti migliaia (senza virgola decimale).
CREATE OR REPLACE MACRO normalize_italian_number(val) AS
   CASE
      WHEN typeof(val) IN ('DOUBLE', 'FLOAT', 'REAL')
           OR (typeof(val) LIKE 'DECIMAL%' AND typeof(val) NOT LIKE '%,0)')
         THEN error(
            'normalize_italian_number() ha ricevuto ' || typeof(val) ||
            ' invece di VARCHAR: il valore ha gia perso il formato italiano. ' ||
            'Usa clean.read.all_varchar: true oppure dichiara la colonna VARCHAR.')
      ELSE TRY_CAST(REPLACE(REPLACE(val::VARCHAR, '.', ''), ',', '.') AS DOUBLE)
   END;

-- ── normalize_italian_integer ──────────────────────────────────────────────
-- Come normalize_italian_number ma restituisce INTEGER.
-- DuckDB CAST(DOUBLE AS INTEGER) arrotonda (5432.90 → 5433).
-- "1.234" → 1234, "5.432,10" → 5432, "5.432,90" → 5433.
-- L'input DEVE essere VARCHAR (stessa guardia di normalize_italian_number).
-- @contract returns: INTEGER
-- @contract warning: NON usare su colonne tipizzate dal reader: con
--   clean.read.source=auto lo sniffing puo' tipizzare DOUBLE una colonna di
--   numeri italiani (es. "121.330") e la macro corromperebbe il valore (×10 / ÷10).
--   In quel caso usa clean.read.all_varchar=true oppure dichiara la colonna VARCHAR.
CREATE OR REPLACE MACRO normalize_italian_integer(val) AS
   CASE
      WHEN typeof(val) IN ('DOUBLE', 'FLOAT', 'REAL')
           OR (typeof(val) LIKE 'DECIMAL%' AND typeof(val) NOT LIKE '%,0)')
         THEN error(
            'normalize_italian_integer() ha ricevuto ' || typeof(val) ||
            ' invece di VARCHAR: il valore ha gia perso il formato italiano. ' ||
            'Usa clean.read.all_varchar: true oppure dichiara la colonna VARCHAR.')
      ELSE TRY_CAST(REPLACE(REPLACE(val::VARCHAR, '.', ''), ',', '.') AS INTEGER)
   END;

-- ── decode_flag ────────────────────────────────────────────────────────────
-- Decodifica un flag testuale in BOOLEAN.
-- decode_flag("ETS", 'X') → TRUE se il valore è 'X', FALSE altrimenti.
-- Lista di valori ammessi: passare come secondo argomento il valore che
-- rappresenta TRUE (es. 'X', 'S', '1', 'TRUE').
-- @contract returns: BOOLEAN
-- @contract example: decode_flag("ETS", 'X') AS is_ets
CREATE OR REPLACE MACRO decode_flag(val, yes_value) AS
   CASE WHEN TRIM(val::VARCHAR) = yes_value::VARCHAR THEN TRUE ELSE FALSE END;

-- ── normalize_string ───────────────────────────────────────────────────────
-- Normalizza una stringa: TRIM + converte stringhe vuote in NULL.
-- @contract returns: VARCHAR
-- @contract example: normalize_string("Regione") AS regione
CREATE OR REPLACE MACRO normalize_string(val) AS
   NULLIF(TRIM(val::VARCHAR), '');

-- ── cast_int ───────────────────────────────────────────────────────────────
-- TRY_CAST a INTEGER (32-bit). Per numeri grandi usa cast_bigint.
-- @contract returns: INTEGER
-- @contract example: cast_int("Eta") AS eta
CREATE OR REPLACE MACRO cast_int(val) AS
   TRY_CAST(val AS INTEGER);

-- ── cast_bigint ────────────────────────────────────────────────────────────
-- TRY_CAST a BIGINT (64-bit). Usato dallo scaffold per colonne numeriche.
-- @contract returns: BIGINT
-- @contract example: cast_bigint("CODICE_FISCALE") AS codice_fiscale
CREATE OR REPLACE MACRO cast_bigint(val) AS
   TRY_CAST(val AS BIGINT);

-- ── cast_double ────────────────────────────────────────────────────────────
-- TRY_CAST a DOUBLE con gestione NULL sicura.
-- @contract returns: DOUBLE
-- @contract example: cast_double("IMPORTO") AS importo
CREATE OR REPLACE MACRO cast_double(val) AS
   TRY_CAST(val AS DOUBLE);

-- ── remove_dot_thousands ───────────────────────────────────────────────────
-- Rimuove punti migliaia da numeri interi (senza virgola decimale).
-- ATTENZIONE: rimuove TUTTI i punti, incluso l'eventuale separatore
-- decimale standard. Usa SOLO su interi con punti migliaia.
-- Per numeri con decimali (italiani o standard) usa:
--   normalize_italian_number()  — formato italiano (virgola)
--   cast_double()               — formato internazionale (punto)
-- "1.234" → 1234.0, "1.234.567" → 1234567.0
-- L'input DEVE essere VARCHAR: come normalize_italian_*, se il reader ha gia'
-- tipizzato la colonna (DOUBLE/FLOAT/DECIMAL con decimali) la macro solleva un
-- errore invece di corrompere il valore.
-- @contract returns: DOUBLE
-- @contract warning: l'input deve essere VARCHAR. Su una colonna tipizzata
--   DOUBLE dal reader (clean.read.source=auto) rimuove il punto decimale e
--   corrompe il valore (es. 1234.56 → "1234.56" → "123456"): la macro fallisce
--   con errore esplicito. Usa clean.read.all_varchar=true o dichiara VARCHAR.
-- @contract example: remove_dot_thousands("POPOLAZIONE") AS popolazione
CREATE OR REPLACE MACRO remove_dot_thousands(val) AS
   CASE
      WHEN typeof(val) IN ('DOUBLE', 'FLOAT', 'REAL')
           OR (typeof(val) LIKE 'DECIMAL%' AND typeof(val) NOT LIKE '%,0)')
         THEN error(
            'remove_dot_thousands() ha ricevuto ' || typeof(val) ||
            ' invece di VARCHAR: il punto decimale verrebbe rimosso e il valore corrotto. ' ||
            'Usa clean.read.all_varchar: true oppure dichiara la colonna VARCHAR.')
      ELSE TRY_CAST(REPLACE(val::VARCHAR, '.', '') AS DOUBLE)
   END;
