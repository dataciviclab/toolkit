"""Test del suggerimento/censimento colonne civic senza semantic_type.

Marker: pure_unit (logica pura sul vocabolario e sul catalogo in memoria).
"""

from __future__ import annotations

import pytest

from toolkit.registry.schema_reader import (
    find_untyped_civic_keys,
    load_semantic_types,
    load_valid_types,
    suggest_semantic_type,
)


class TestSuggestSemanticType:
    @pytest.fixture(autouse=True)
    def _vocab(self) -> None:
        self.alias_map = load_semantic_types()
        self.valid_types = load_valid_types()

    @pytest.mark.pure_unit
    def test_alias_missione_is_programma(self) -> None:
        assert (
            suggest_semantic_type("missione", self.alias_map, self.valid_types) == "programma_code"
        )

    @pytest.mark.pure_unit
    def test_alias_codice_missione_bdap(self) -> None:
        assert (
            suggest_semantic_type("codice_missione", self.alias_map, self.valid_types)
            == "programma_code"
        )

    @pytest.mark.pure_unit
    def test_alias_cf_variants(self) -> None:
        assert (
            suggest_semantic_type("soggetto_ricevente_cf", self.alias_map, self.valid_types)
            == "fiscal_code"
        )
        assert (
            suggest_semantic_type("OC_CODICE_FISCALE_SOGG", self.alias_map, self.valid_types)
            == "fiscal_code"
        )

    @pytest.mark.pure_unit
    def test_alias_catastale_comune(self) -> None:
        assert (
            suggest_semantic_type("codice_catastale_comune", self.alias_map, self.valid_types)
            == "cadastral_code"
        )

    @pytest.mark.pure_unit
    def test_pattern_municipality(self) -> None:
        # pattern: non nel vocabolario ma civic-like
        assert (
            suggest_semantic_type("codice_comune_sogg_titolare", self.alias_map, self.valid_types)
            == "municipality_code"
        )

    @pytest.mark.pure_unit
    def test_unknown_column_returns_none(self) -> None:
        assert suggest_semantic_type("descrizione_libera", self.alias_map, self.valid_types) is None
        assert suggest_semantic_type("stato_cup", self.alias_map, self.valid_types) is None

    @pytest.mark.pure_unit
    def test_description_columns_not_suggested_as_keys(self) -> None:
        # descrizioni di chiavi: non sono join key
        assert suggest_semantic_type("descr_codice_ateco", self.alias_map, self.valid_types) is None
        assert (
            suggest_semantic_type("descrizione_submisura", self.alias_map, self.valid_types) is None
        )
        assert (
            suggest_semantic_type("desc_codice_catastale", self.alias_map, self.valid_types) is None
        )

    @pytest.mark.pure_unit
    def test_provincia_cm_is_province_not_municipality(self) -> None:
        assert (
            suggest_semantic_type("provincia_cm_codice_istat", self.alias_map, self.valid_types)
            == "province_code"
        )

    @pytest.mark.pure_unit
    def test_piva_beneficiario_is_fiscal(self) -> None:
        assert (
            suggest_semantic_type("beneficiario_partita_iva", self.alias_map, self.valid_types)
            == "fiscal_code"
        )

    @pytest.mark.pure_unit
    def test_empty_name_returns_none(self) -> None:
        assert suggest_semantic_type("", self.alias_map, self.valid_types) is None


class TestFindUntypedCivicKeys:
    @pytest.mark.pure_unit
    def test_finds_untyped_and_skips_typed(self) -> None:
        catalog = {
            "datasets": [
                {
                    "slug": "demo",
                    "columns": [
                        {"name": "missione", "type": "varchar", "role": "dimension"},
                        {
                            "name": "cup",
                            "type": "varchar",
                            "role": "dimension",
                            "semantic_type": "cup_code",
                        },
                        {"name": "valore", "type": "double", "role": "metric"},
                    ],
                }
            ]
        }
        findings = find_untyped_civic_keys(catalog)
        assert len(findings) == 1
        assert findings[0]["slug"] == "demo"
        assert findings[0]["column"] == "missione"
        assert findings[0]["suggested"] == "programma_code"

    @pytest.mark.pure_unit
    def test_empty_catalog(self) -> None:
        assert find_untyped_civic_keys({"datasets": []}) == []
        assert find_untyped_civic_keys({}) == []

    @pytest.mark.pure_unit
    def test_missing_vocab_returns_empty(self, tmp_path) -> None:
        catalog = {
            "datasets": [
                {"slug": "x", "columns": [{"name": "cf", "type": "varchar", "role": "dimension"}]}
            ]
        }
        assert find_untyped_civic_keys(catalog, tmp_path / "nope.yaml") == []
