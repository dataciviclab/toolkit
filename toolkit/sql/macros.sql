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
-- =============================================================================

-- ── normalize_italian_number ───────────────────────────────────────────────
-- Converte un numero in formato italiano (1.234,56 → 1234.56).
-- Rimuove punti migliaia, converte virgola decimale in punto.
-- TRY_CAST restituisce NULL se la conversione fallisce.
CREATE OR REPLACE MACRO normalize_italian_number(val) AS
   TRY_CAST(REPLACE(REPLACE(val::VARCHAR, '.', ''), ',', '.') AS DOUBLE);

-- ── normalize_italian_integer ──────────────────────────────────────────────
-- Come normalize_italian_number ma restituisce INTEGER.
-- "1.234" → 1234, "5.432,10" → 5432 (troncato).
CREATE OR REPLACE MACRO normalize_italian_integer(val) AS
   TRY_CAST(REPLACE(REPLACE(val::VARCHAR, '.', ''), ',', '.') AS INTEGER);

-- ── decode_flag ────────────────────────────────────────────────────────────
-- Decodifica un flag testuale in BOOLEAN.
-- decode_flag("ETS", 'X') → TRUE se il valore è 'X', FALSE altrimenti.
-- Lista di valori ammessi: passare come secondo argomento il valore che
-- rappresenta TRUE (es. 'X', 'S', '1', 'TRUE').
CREATE OR REPLACE MACRO decode_flag(val, yes_value) AS
   CASE WHEN TRIM(val::VARCHAR) = yes_value::VARCHAR THEN TRUE ELSE FALSE END;

-- ── normalize_string ───────────────────────────────────────────────────────
-- Normalizza una stringa: TRIM + converte stringhe vuote in NULL.
CREATE OR REPLACE MACRO normalize_string(val) AS
   NULLIF(TRIM(val::VARCHAR), '');

-- ── cast_int ───────────────────────────────────────────────────────────────
-- TRY_CAST a INTEGER con gestione NULL sicura.
CREATE OR REPLACE MACRO cast_int(val) AS
   TRY_CAST(val AS INTEGER);

-- ── cast_double ────────────────────────────────────────────────────────────
-- TRY_CAST a DOUBLE con gestione NULL sicura.
CREATE OR REPLACE MACRO cast_double(val) AS
   TRY_CAST(val AS DOUBLE);

-- ── remove_dot_thousands ───────────────────────────────────────────────────
-- Rimuove solo i punti migliaia (senza virgola decimale).
-- Utile per numeri con punto migliaia ma punto decimale standard:
-- "1.234" → 1234.0, "1.234.567" → 1234567.0
CREATE OR REPLACE MACRO remove_dot_thousands(val) AS
   TRY_CAST(REPLACE(val::VARCHAR, '.', '') AS DOUBLE);
