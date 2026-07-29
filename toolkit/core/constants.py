"""Costanti condivise del toolkit — view names, token, contratti di pipeline.

Queste costanti sono il **punto canonico** per i nomi delle view DuckDB
create dal runtime. Tutti i reader (CSV, parquet, Excel) devono riferirsi
a queste costanti, non a stringhe hardcoded.

Quando un agente AI chiede "come si chiama la view clean?", la risposta
deve venire da qui.
"""

# ── View name: layer CLEAN ────────────────────────────────────────────────
# Il reader CSV/parquet/Excel crea questa view nella connessione DuckDB.
# clean.sql deve fare SELECT ... FROM raw_input.
RAW_INPUT_VIEW = "raw_input"

# View intermedia usata dal reader CSV con colonne esplicite
# (source_columns). raw_input fa SELECT da raw_input_source.
RAW_INPUT_SOURCE_VIEW = "raw_input_source"

# Nome intermedio DataFrame registrato in DuckDB prima di rinominare
# in RAW_INPUT_VIEW. Usato da reader CSV normalizzato e Excel.
RAW_INPUT_DF_VIEW = "raw_input_df"

# ── View name: layer MART ─────────────────────────────────────────────────
# Il runner MART crea questa view dal parquet CLEAN dell'anno corrente.
# mart.sql deve fare SELECT ... FROM clean_input.
CLEAN_INPUT_VIEW = "clean_input"

# ── View name: multi-year source ──────────────────────────────────────────
# Usato da multi_year_source.py per unire parquet di più anni.
SOURCE_INPUT_VIEW = "source_input"

# ── Token ──────────────────────────────────────────────────────────────────
# Placeholder {year} nei file SQL viene sostituito dal runner con l'anno.
YEAR_PLACEHOLDER = "{year}"

# ─── Nome tabella output CLEAN ──────────────────────────────────────────────
# Tabella DuckDB usata per esportare il risultato di clean.sql in parquet.
CLEAN_OUT_TABLE = "clean_out"
