"""Test delle funzioni di schema_reader: vocabolario e normalizzazione semantic_type.

Marker: pure_unit (logica pura, no rete, no parquet).
"""

from __future__ import annotations

import pytest

from toolkit.registry.schema_reader import (
    load_semantic_types,
    load_valid_types,
    normalize_semantic_type,
)


class TestLoadValidTypes:
    @pytest.mark.pure_unit
    def test_returns_type_names_from_vocab(self) -> None:
        valid = load_valid_types()
        assert isinstance(valid, set)
        assert "ddl_id" in valid
        assert "municipality_code" in valid
        assert "fiscal_code" in valid

    @pytest.mark.pure_unit
    def test_empty_set_for_missing_file(self, tmp_path) -> None:
        assert load_valid_types(tmp_path / "nonexistent.yaml") == set()


class TestNormalizeSemanticType:
    @pytest.fixture(autouse=True)
    def _vocab(self) -> None:
        self.alias_map = load_semantic_types()
        self.valid_types = load_valid_types()

    @pytest.mark.pure_unit
    def test_valid_type_passthrough(self) -> None:
        assert normalize_semantic_type("ddl_id", self.alias_map, self.valid_types) == "ddl_id"

    @pytest.mark.pure_unit
    def test_alias_resolves_to_type(self) -> None:
        assert normalize_semantic_type("atto_num", self.alias_map, self.valid_types) == "ddl_id"

    @pytest.mark.pure_unit
    def test_case_insensitive_alias(self) -> None:
        assert normalize_semantic_type("ATTO_NUM", self.alias_map, self.valid_types) == "ddl_id"

    @pytest.mark.pure_unit
    def test_unknown_type_returns_none(self) -> None:
        assert normalize_semantic_type("fake_type_xyz", self.alias_map, self.valid_types) is None

    @pytest.mark.pure_unit
    def test_none_returns_none(self) -> None:
        assert normalize_semantic_type(None, self.alias_map, self.valid_types) is None

    @pytest.mark.pure_unit
    def test_empty_string_returns_none(self) -> None:
        assert normalize_semantic_type("", self.alias_map, self.valid_types) is None
