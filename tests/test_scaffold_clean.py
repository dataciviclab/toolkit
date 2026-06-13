"""Tests per toolkit/scaffold/clean.py — generazione clean.sql.

pure_unit: _select_expr, _columns_spec, generate_clean_sql, _find_anno_raw_column
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from toolkit.scaffold.clean import (
    _columns_spec,
    _find_anno_raw_column,
    _has_anno_column,
    _select_expr,
    _suggest_dateformat,
    generate_clean_sql,
)


# ---------------------------------------------------------------------------
# pure_unit: _select_expr
# ---------------------------------------------------------------------------


class TestSelectExpr:
    """pure_unit: _select_expr sceglie TRIM / REPLACE / TRY_CAST per tipo."""

    @pytest.mark.pure_unit
    def test_varchar_gets_trim(self) -> None:
        """VARCHAR columns use TRIM instead of unnecessary TRY_CAST."""
        result = _select_expr("Nome", "VARCHAR", "nome")
        assert result == 'trim(CAST("Nome" AS VARCHAR)) AS nome'

    @pytest.mark.pure_unit
    def test_integer_gets_try_cast(self) -> None:
        """Integer columns get TRY_CAST to BIGINT."""
        result = _select_expr("Anno", "BIGINT", "anno")
        assert result == 'TRY_CAST("Anno" AS BIGINT) AS anno'

    @pytest.mark.pure_unit
    def test_double_gets_try_cast(self) -> None:
        """Double columns get TRY_CAST to DOUBLE."""
        result = _select_expr("Valore", "DOUBLE", "valore")
        assert result == 'TRY_CAST("Valore" AS DOUBLE) AS valore'

    @pytest.mark.pure_unit
    def test_double_gets_plain_try_cast(self) -> None:
        """Double columns always get plain TRY_CAST (no REPLACE — handled by clean.read)."""
        result = _select_expr("Importo", "DOUBLE", "importo")
        assert result == 'TRY_CAST("Importo" AS DOUBLE) AS importo'
        assert "REPLACE" not in result

    @pytest.mark.pure_unit
    def test_bigint_gets_plain_try_cast(self) -> None:
        """BIGINT columns always get plain TRY_CAST."""
        result = _select_expr("Anno", "BIGINT", "anno")
        assert result == 'TRY_CAST("Anno" AS BIGINT) AS anno'
        assert "REPLACE" not in result

    @pytest.mark.pure_unit
    def test_date_gets_try_cast(self) -> None:
        """DATE columns get TRY_CAST."""
        result = _select_expr("Data", "DATE", "data")
        assert 'TRY_CAST("Data" AS DATE)' in result

    @pytest.mark.pure_unit
    def test_boolean_gets_try_cast(self) -> None:
        """BOOLEAN columns get TRY_CAST."""
        result = _select_expr("Attivo", "BOOLEAN", "attivo")
        assert 'TRY_CAST("Attivo" AS BOOLEAN)' in result


# ---------------------------------------------------------------------------
# pure_unit: _suggest_dateformat
# ---------------------------------------------------------------------------


class TestSuggestDateformat:
    """pure_unit: _suggest_dateformat rileva formati data non ISO.

    I test usano CSV reali processati da ``profile_raw`` per verificare
    che la funzione funzioni su profili reali (dove DuckDB converte le
    date in Timestamp, perdendo il formato originale).
    """

    @pytest.mark.policy
    def test_dd_mm_YYYY_with_slash(self, tmp_path: Path) -> None:
        """dd/mm/YYYY con slash → %d/%m/%Y."""
        from dataclasses import asdict
        from toolkit.profile.raw import profile_raw

        csv_file = tmp_path / "date_slash.csv"
        csv_file.write_text(
            "data;valore\n15/03/2024;100\n01/02/2023;200\n31/12/2025;300\n",
            encoding="utf-8",
        )
        profile = profile_raw(tmp_path, "test", 2024, primary_file=csv_file)
        fmt = _suggest_dateformat(asdict(profile))
        assert fmt == "%d/%m/%Y"

    @pytest.mark.policy
    def test_dd_mm_YYYY_with_dash(self, tmp_path: Path) -> None:
        """dd-mm-YYYY con trattino → %d-%m-%Y."""
        from dataclasses import asdict
        from toolkit.profile.raw import profile_raw

        csv_file = tmp_path / "date_dash.csv"
        csv_file.write_text(
            "data;valore\n15-03-2024;100\n01-02-2023;200\n",
            encoding="utf-8",
        )
        profile = profile_raw(tmp_path, "test", 2024, primary_file=csv_file)
        fmt = _suggest_dateformat(asdict(profile))
        assert fmt == "%d-%m-%Y"

    @pytest.mark.policy
    def test_iso_date_returns_none(self, tmp_path: Path) -> None:
        """Date ISO (YYYY-MM-DD) non attivano dateformat."""
        from dataclasses import asdict
        from toolkit.profile.raw import profile_raw

        csv_file = tmp_path / "date_iso.csv"
        csv_file.write_text(
            "data;valore\n2024-03-15;100\n2023-02-01;200\n",
            encoding="utf-8",
        )
        profile = profile_raw(tmp_path, "test", 2024, primary_file=csv_file)
        fmt = _suggest_dateformat(asdict(profile))
        assert fmt is None

    @pytest.mark.policy
    def test_multiple_date_cols_agree_on_format(self, tmp_path: Path) -> None:
        """Due colonne date con stesso formato → dateformat suggerito."""
        from dataclasses import asdict
        from toolkit.profile.raw import profile_raw

        csv_file = tmp_path / "multi_date.csv"
        csv_file.write_text(
            "inizio;fine;valore\n15/03/2024;20/04/2024;100\n01/02/2023;10/03/2023;200\n",
            encoding="utf-8",
        )
        profile = profile_raw(tmp_path, "test", 2024, primary_file=csv_file)
        fmt = _suggest_dateformat(asdict(profile))
        assert fmt == "%d/%m/%Y", f"got {fmt}"

    @pytest.mark.policy
    def test_no_date_columns_returns_none(self, tmp_path: Path) -> None:
        """Senza colonne date → None."""
        from dataclasses import asdict
        from toolkit.profile.raw import profile_raw

        csv_file = tmp_path / "no_date.csv"
        csv_file.write_text("nome;valore\nfoo;100\nbar;200\n", encoding="utf-8")
        profile = profile_raw(tmp_path, "test", 2024, primary_file=csv_file)
        fmt = _suggest_dateformat(asdict(profile))
        assert fmt is None

    @pytest.mark.policy
    def test_mixed_date_cols_different_format_returns_none(self, tmp_path: Path) -> None:
        """Se due colonne date hanno formati diversi, non suggerisce
        (dateformat e' globale per read_csv, non puo' essere diverso per colonna)."""
        from dataclasses import asdict
        from toolkit.profile.raw import profile_raw

        csv_file = tmp_path / "mixed_fmt.csv"
        csv_file.write_text(
            "data_it;data_us;valore\n15/03/2024;2024/03/15;100\n01/02/2023;2023/02/01;200\n",
            encoding="utf-8",
        )
        profile = profile_raw(tmp_path, "test", 2024, primary_file=csv_file)
        fmt = _suggest_dateformat(asdict(profile))
        # 4 values in dd/mm/YYYY, 4 in YYYY/mm/dd → 50% each → < 60% → None
        assert fmt is None, f"got {fmt}"

    @pytest.mark.policy
    def test_empty_profile_returns_none(self) -> None:
        """Profilo vuoto o senza date_raw_values → None."""
        assert _suggest_dateformat({}) is None
        assert _suggest_dateformat({"date_raw_values": {}}) is None

    @pytest.mark.policy
    def test_date_raw_values_capped_at_30_rows(self, tmp_path: Path) -> None:
        """date_raw_values contiene al massimo 30 valori per colonna."""
        from dataclasses import asdict
        from toolkit.profile.raw import profile_raw

        csv_file = tmp_path / "large.csv"
        lines = ["data;valore"]
        for i in range(1, 101):
            day = (i % 28) + 1
            month = ((i // 28) % 12) + 1
            lines.append(f"{day:02d}/{month:02d}/2024;{i}")
        csv_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

        profile = profile_raw(tmp_path, "test", 2024, primary_file=csv_file)
        d = asdict(profile)
        raw_vals = d.get("date_raw_values", {})
        for col, vals in raw_vals.items():
            assert len(vals) <= 30, f"Colonna {col} ha {len(vals)} valori, atteso max 30"


# ---------------------------------------------------------------------------
# pure_unit: _find_anno_raw_column / _has_anno_column
# ---------------------------------------------------------------------------


class TestFindAnnoColumn:
    """pure_unit: rilevamento colonna anno nel profilo."""

    @pytest.mark.pure_unit
    def test_finds_anno_in_mapping(self) -> None:
        """Trova 'Anno' nel mapping_suggestions."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "Anno": {"type": "int"},
                "Valore": {"type": "float"},
            },
        }
        assert _find_anno_raw_column(profile) == "Anno"
        assert _has_anno_column(profile) is True

    @pytest.mark.pure_unit
    def test_finds_anno_in_columns_raw(self) -> None:
        """Trova 'Anno' in columns_raw (fallback senza mapping)."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {},
            "columns_raw": ["Anno", "Nome", "Valore"],
        }
        assert _find_anno_raw_column(profile) == "Anno"
        assert _has_anno_column(profile) is True

    @pytest.mark.pure_unit
    def test_recognizes_year_variants(self) -> None:
        """Riconosce varianti: anno_di_imposta, YEAR, tax_year."""
        for col_name in ["anno_di_imposta", "YEAR", "Anno Imposta", "Tax_Year"]:
            profile: dict[str, Any] = {
                "mapping_suggestions": {col_name: {"type": "int"}},
            }
            assert _find_anno_raw_column(profile) == col_name, f"Failed for {col_name}"

    @pytest.mark.pure_unit
    def test_no_anno_column(self) -> None:
        """Nessuna colonna anno → None / False."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "Nome": {"type": "str"},
                "Valore": {"type": "float"},
            },
        }
        assert _find_anno_raw_column(profile) is None
        assert _has_anno_column(profile) is False

    @pytest.mark.pure_unit
    def test_empty_profile(self) -> None:
        """Profilo vuoto → None / False."""
        assert _find_anno_raw_column({}) is None
        assert _has_anno_column({}) is False


# ---------------------------------------------------------------------------
# pure_unit: _columns_spec
# ---------------------------------------------------------------------------


class TestColumnsSpec:
    """pure_unit: _columns_spec produce espressioni SELECT corrette."""

    @pytest.mark.pure_unit
    def test_mixed_types(self) -> None:
        """Mapping misto: VARCHAR → TRIM, numerici → TRY_CAST."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "Nome": {"type": "str"},
                "Anno": {"type": "int"},
                "Valore": {"type": "float"},
            },
        }
        exprs, spec = _columns_spec(profile, 2024)
        assert 'trim(CAST("Nome" AS VARCHAR)) AS nome' in exprs
        assert 'TRY_CAST("Anno" AS BIGINT) AS anno' in exprs
        assert 'TRY_CAST("Valore" AS DOUBLE) AS valore' in exprs
        assert spec["Nome"] == "VARCHAR"
        assert spec["Anno"] == "BIGINT"
        assert spec["Valore"] == "DOUBLE"

    @pytest.mark.pure_unit
    def test_comma_decimal(self) -> None:
        """Con decimal_suggested=',', colonne DOUBLE usano TRY_CAST normale
        (REPLACE non serve: clean.read.decimal gestito da DuckDB)."""
        profile: dict[str, Any] = {
            "decimal_suggested": ",",
            "mapping_suggestions": {
                "Nome": {"type": "str"},
                "Importo": {"type": "float"},
            },
        }
        exprs, _ = _columns_spec(profile, 2024)
        joined = "\n".join(exprs)
        assert 'trim(CAST("Nome" AS VARCHAR)) AS nome' in joined
        assert 'TRY_CAST("Importo" AS DOUBLE)' in joined
        assert "REPLACE" not in joined

    @pytest.mark.pure_unit
    def test_no_mapping_fallback(self) -> None:
        """Senza mapping: CAST(... AS VARCHAR) + TRIM per safety su tipi misti."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {},
            "columns_raw": ["Col1", "Col2"],
        }
        exprs, spec = _columns_spec(profile, 2024)
        assert 'trim(CAST("Col1" AS VARCHAR)) AS col1' in exprs
        assert 'trim(CAST("Col2" AS VARCHAR)) AS col2' in exprs
        assert spec == {"Col1": "VARCHAR", "Col2": "VARCHAR"}

    @pytest.mark.pure_unit
    def test_no_mapping_no_columns(self) -> None:
        """Senza mapping né columns_raw: wildcard."""
        profile: dict[str, Any] = {}
        exprs, _ = _columns_spec(profile, 2024)
        assert exprs == ["*"]


# ---------------------------------------------------------------------------
# pure_unit: generate_clean_sql
# ---------------------------------------------------------------------------


class TestGenerateCleanSql:
    """pure_unit: generate_clean_sql produce clean.sql completo."""

    @pytest.mark.pure_unit
    def test_basic_without_anno_column(self) -> None:
        """Senza colonna anno: inject {year}, nessun WHERE."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "Nome": {"type": "str"},
                "Valore": {"type": "float"},
            },
        }
        sql = generate_clean_sql(profile, "test_dataset", 2024)
        assert "{year}::INTEGER AS anno" in sql
        assert "FROM raw_input" in sql
        assert "WHERE" not in sql
        assert 'trim(CAST("Nome" AS VARCHAR))' in sql
        assert 'TRY_CAST("Valore"' in sql

    @pytest.mark.pure_unit
    def test_with_real_anno_column_adds_where(self) -> None:
        """Con colonna Anno reale: nessun inject, WHERE aggiunto."""
        profile: dict[str, Any] = {
            "file_used": "data_2024.csv",
            "mapping_suggestions": {
                "Anno": {"type": "int"},
                "Regione": {"type": "str"},
                "Valore": {"type": "float"},
            },
        }
        sql = generate_clean_sql(profile, "test_dataset", 2024)
        assert "{year}::INTEGER" not in sql  # non injectato
        assert 'TRY_CAST("Anno" AS BIGINT) AS anno' in sql
        assert 'WHERE try_cast("Anno" AS INTEGER) IS NOT NULL' in sql
        assert 'trim(CAST("Regione" AS VARCHAR))' in sql

    @pytest.mark.pure_unit
    def test_with_anno_di_imposta_adds_where(self) -> None:
        """Con anno_di_imposta: WHERE sulla colonna corretta."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "Anno di imposta": {"type": "int"},
                "Reddito": {"type": "float"},
            },
        }
        sql = generate_clean_sql(profile, "irpef", 2024)
        assert 'WHERE try_cast("Anno di imposta" AS INTEGER) IS NOT NULL' in sql
        assert "{year}::INTEGER" not in sql

    @pytest.mark.pure_unit
    def test_comma_decimal_in_header(self) -> None:
        """Con decimal_suggested=',': info nel commento, non REPLACE nel SQL."""
        profile: dict[str, Any] = {
            "decimal_suggested": ",",
            "encoding_suggested": "utf-8",
            "delim_suggested": ";",
            "mapping_suggestions": {
                "Anno": {"type": "int"},
                "Importo": {"type": "float"},
            },
        }
        sql = generate_clean_sql(profile, "test", 2024)
        assert "REPLACE" not in sql
        assert "Decimal: ," in sql  # nel commento header
        assert "Encoding: utf-8" in sql
        assert "Delimiter: ;" in sql

    @pytest.mark.pure_unit
    def test_warnings_in_comment(self) -> None:
        """Warning del profilo appaiono come commento."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {"A": {"type": "str"}},
            "warnings": ["header_preamble_detected: ...", "encoding_fallback: latin-1"],
        }
        sql = generate_clean_sql(profile, "test", 2024)
        assert "Warnings from profiling:" in sql
        assert "header_preamble_detected" in sql
        assert "encoding_fallback" in sql

    @pytest.mark.pure_unit
    def test_header_comments(self) -> None:
        """Commenti intestazione: generazione, source, meta, run hint."""
        profile: dict[str, Any] = {
            "file_used": "dati.csv",
            "mapping_suggestions": {"X": {"type": "str"}},
        }
        sql = generate_clean_sql(profile, "mio_dataset", 2024)
        assert "Generated by toolkit scaffold clean" in sql
        assert "Source: data/raw/mio_dataset/2024/dati.csv" in sql
        assert "toolkit run clean" in sql

    @pytest.mark.pure_unit
    def test_empty_mapping(self) -> None:
        """Senza mapping ne' columns_raw: SELECT * FROM raw_input."""
        profile: dict[str, Any] = {}
        sql = generate_clean_sql(profile, "test", 2024)
        assert "SELECT" in sql
        assert "{year}::INTEGER AS anno" in sql  # injected
        assert "FROM raw_input" in sql

    @pytest.mark.pure_unit
    def test_where_uses_try_cast_for_safety(self) -> None:
        """WHERE usa try_cast (non CAST) per gestire valori non interi."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "Anno": {"type": "int"},
            },
        }
        sql = generate_clean_sql(profile, "test", 2024)
        assert "try_cast" in sql
        assert "CAST" not in sql.split("WHERE")[1] if "WHERE" in sql else ""

    @pytest.mark.pure_unit
    def test_sql_ends_with_newline(self) -> None:
        """Il SQL generato termina con newline."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {"A": {"type": "str"}},
        }
        sql = generate_clean_sql(profile, "test", 2024)
        assert sql.endswith("\n")


# ---------------------------------------------------------------------------
# pure_unit: integrazione con suggest_clean_sql (full.py)
# ---------------------------------------------------------------------------


class TestSuggestCleanSqlIntegration:
    """pure_unit: suggest_clean_sql aggiornato coerentemente."""

    @pytest.mark.pure_unit
    def test_varchar_gets_trim(self) -> None:
        """suggest_clean_sql applica TRIM alle colonne VARCHAR."""
        from toolkit.scaffold.full import suggest_clean_sql

        cols = ["nome", "categoria", "valore"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "nome": {"type": "str"},
                "categoria": {"type": "str"},
                "valore": {"type": "float"},
            },
        }
        sql = suggest_clean_sql(cols, profile)
        assert 'trim(CAST("nome" AS VARCHAR))' in sql
        assert 'trim(CAST("categoria" AS VARCHAR))' in sql
        assert 'TRY_CAST("valore" AS DOUBLE)' in sql
