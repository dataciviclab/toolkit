"""Tests for refactored scaffold/full.py — dict-based YAML generation."""

from __future__ import annotations
import pytest
import yaml
from toolkit.scaffold.full import generate_full_scaffold, suggest_mart_sql, suggest_validation

pytestmark = pytest.mark.pure_unit


class TestSuggestValidation:
    def test_none(self) -> None:
        assert suggest_validation(None) == {}

    def test_with_columns(self) -> None:
        v = suggest_validation({"columns_norm": ["Nome", "Importo"], "row_count": 100})
        assert v["required_columns"] == ["nome", "importo"]
        assert v["min_rows"] == 50


class TestSuggestMartSql:
    def test_empty(self) -> None:
        assert "placeholder" in suggest_mart_sql([], {})

    def test_with_columns(self) -> None:
        p = {"mapping_suggestions": {"nome": {"type": "VARCHAR"}}}
        sql = suggest_mart_sql(["nome"], p)
        assert "nome: VARCHAR" in sql


@pytest.mark.policy
class TestGenerateFullScaffold:
    def test_file_no_profile(self) -> None:
        probe = {"final_url": "https://example.com/data.csv", "source_type": "file"}
        files = generate_full_scaffold("my-ds", probe)
        assert "dataset.yml" in files
        cfg = yaml.safe_load(files["dataset.yml"])
        assert cfg["raw"]["sources"][0]["type"] == "http_file"

    def test_ckan(self) -> None:
        probe = {
            "final_url": "https://data.example.org/dataset/x",
            "source_type": "ckan",
            "ckan_resources": [
                {
                    "name": "r1",
                    "url": "https://data.example.org/x.csv",
                    "format": "CSV",
                    "id": "123",
                }
            ],
        }
        files = generate_full_scaffold("my-ds", probe)
        cfg = yaml.safe_load(files["dataset.yml"])
        assert cfg["raw"]["sources"][0]["type"] == "ckan"

    def test_sdmx_estat(self) -> None:
        probe = {
            "final_url": "https://ec.europa.eu/.../NAMA",
            "source_type": "sdmx",
            "sdmx_info": {"flow_id": "NAMA", "agency": "ESTAT"},
        }
        files = generate_full_scaffold("my-ds", probe, inferred_years=list(range(2010, 2024)))
        cfg = yaml.safe_load(files["dataset.yml"])
        assert cfg["raw"]["sources"][0]["args"]["agency"] == "ESTAT"

    def test_sparql(self) -> None:
        probe = {
            "final_url": "https://ld.istat.it/sparql",
            "source_type": "sparql",
            "sparql_info": {"endpoint": "https://ld.istat.it/sparql"},
        }
        files = generate_full_scaffold("my-ds", probe)
        cfg = yaml.safe_load(files["dataset.yml"])
        assert cfg["raw"]["sources"][0]["type"] == "sparql"

    def test_with_profile(self) -> None:
        profile = {
            "columns_norm": ["nome", "importo"],
            "mapping_suggestions": {"nome": {"type": "VARCHAR"}, "importo": {"type": "DOUBLE"}},
            "row_count": 200,
            "encoding_suggested": "utf-8",
            "delim_suggested": ";",
        }
        probe = {
            "final_url": "https://example.com/data.csv",
            "source_type": "file",
            "encoding_suggested": "utf-8",
            "delim_suggested": ";",
        }
        files = generate_full_scaffold("my-ds", probe, profile=profile, inferred_years=[2020])
        cfg = yaml.safe_load(files["dataset.yml"])
        assert "read" in cfg["clean"]
        assert "normalize_string" in files["sql/clean.sql"]

    def test_all_files(self) -> None:
        probe = {"final_url": "https://example.com/data.csv", "source_type": "file"}
        files = generate_full_scaffold("my-ds", probe)
        assert set(files.keys()) == {
            "dataset.yml",
            "sql/clean.sql",
            "sql/mart.sql",
            "README.md",
            "notes.md",
        }
