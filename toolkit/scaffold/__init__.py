"""Scaffold del layer CLEAN: generazione di ``sql/clean.sql`` dal profilo RAW."""

from toolkit.scaffold.clean import generate_clean_sql, scaffold_clean_if_missing

__all__ = [
    "generate_clean_sql",
    "scaffold_clean_if_missing",
]
