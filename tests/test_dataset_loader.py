"""Tests for toolkit.core.dataset_loader — contratto di lettura dataset.yml.

Protegge il contratto pubblico: load_dataset_manifest, has_mart_sql.
I test usano dataset.yml minimi per verificare ogni campo.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from toolkit.core.dataset_loader import (
    has_mart_sql,
    load_dataset_manifest,
)

pytestmark = pytest.mark.pure_unit


def _write_dataset(tmp_path: Path, data: dict, filename: str = "dataset.yml") -> Path:
    path = tmp_path / filename
    path.write_text(yaml.dump(data), encoding="utf-8")
    return path


class TestLoadDatasetManifest:
    """Contract: load_dataset_manifest legge i campi comuni da dataset.yml."""

    def test_minimal(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {"dataset": {"name": "test", "years": [2023]}})
        result = load_dataset_manifest(tmp_path)
        assert result["name"] == "test"
        assert result["years"] == [2023]
        assert result["slug"] == tmp_path.name

    def test_slug_from_config(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {"slug": "custom-slug", "dataset": {"name": "test"}})
        result = load_dataset_manifest(tmp_path)
        assert result["slug"] == "custom-slug"

    def test_missing_file(self, tmp_path: Path) -> None:
        missing = tmp_path / "nonexistent.yml"
        result = load_dataset_manifest(missing)
        assert "error" in result
        assert result["slug"] == missing.parent.name

    def test_sources(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {
            "dataset": {"name": "test"},
            "raw": {"sources": [{"name": "src1"}, {"name": "src2"}]},
        })
        result = load_dataset_manifest(tmp_path)
        assert len(result["sources"]) == 2
        assert result["sources"][0]["name"] == "src1"

    def test_extra_ca_cert_urls_single(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {
            "dataset": {"name": "test"},
            "raw": {"sources": [{"args": {"extra_ca_cert_url": "https://certs.example.com/cert.pem"}}]},
        })
        result = load_dataset_manifest(tmp_path)
        assert result["extra_ca_cert_urls"] == ["https://certs.example.com/cert.pem"]

    def test_extra_ca_cert_urls_multiple(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {
            "dataset": {"name": "test"},
            "raw": {"sources": [{"args": {"extra_ca_cert_urls": ["a.pem", "b.pem"]}}]},
        })
        result = load_dataset_manifest(tmp_path)
        assert result["extra_ca_cert_urls"] == ["a.pem", "b.pem"]

    def test_support_root_level(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {
            "dataset": {"name": "test"},
            "support": [{"name": "sup1", "config": "path/to/config.yml"}],
        })
        result = load_dataset_manifest(tmp_path)
        assert len(result["support"]) == 1
        assert result["support"][0]["name"] == "sup1"

    def test_support_in_dataset(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {
            "dataset": {"name": "test", "support": [{"name": "sup1"}]},
        })
        result = load_dataset_manifest(tmp_path)
        assert len(result["support"]) == 1

    def test_time_coverage(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {
            "dataset": {"name": "test", "time_coverage": {"start_year": 2020, "end_year": 2024}},
        })
        result = load_dataset_manifest(tmp_path)
        assert result["time_coverage"]["start_year"] == 2020
        assert result["time_coverage"]["end_year"] == 2024

    def test_source_id(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {"dataset": {"source_id": "istat_sdmx"}})
        result = load_dataset_manifest(tmp_path)
        assert result["source_id"] == "istat_sdmx"

    def test_empty_years_default(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {"dataset": {"name": "test"}})
        result = load_dataset_manifest(tmp_path)
        assert result["years"] == []

    def test_accepts_filepath(self, tmp_path: Path) -> None:
        filepath = _write_dataset(tmp_path, {"dataset": {"name": "test"}})
        result = load_dataset_manifest(filepath)
        assert result["name"] == "test"

    def test_dataset_key_present(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {"dataset": {"name": "test"}})
        result = load_dataset_manifest(tmp_path)
        assert result["dataset"] is True

    def test_dataset_key_absent(self, tmp_path: Path) -> None:
        _write_dataset(tmp_path, {"slug": "no-dataset-section"})
        result = load_dataset_manifest(tmp_path)
        assert result["dataset"] is False

    def test_yaml_parse_error(self, tmp_path: Path) -> None:
        (tmp_path / "dataset.yml").write_text("invalid: [yaml: bad", encoding="utf-8")
        result = load_dataset_manifest(tmp_path)
        assert "error" in result
        assert "YAML parse error" in result["error"]


class TestHasMartSql:
    """Contract: has_mart_sql verifica esistenza di SQL mart nei formati DI."""

    def test_mart_sql_exact(self, tmp_path: Path) -> None:
        (tmp_path / "sql").mkdir(parents=True)
        (tmp_path / "sql" / "mart.sql").write_text("SELECT 1")
        assert has_mart_sql(tmp_path) is True

    def test_mart_star_sql(self, tmp_path: Path) -> None:
        (tmp_path / "sql").mkdir(parents=True)
        (tmp_path / "sql" / "mart_indicatori.sql").write_text("SELECT 1")
        assert has_mart_sql(tmp_path) is True

    def test_mart_subdir(self, tmp_path: Path) -> None:
        (tmp_path / "sql" / "mart").mkdir(parents=True)
        (tmp_path / "sql" / "mart" / "table1.sql").write_text("SELECT 1")
        assert has_mart_sql(tmp_path) is True

    def test_missing(self, tmp_path: Path) -> None:
        assert has_mart_sql(tmp_path) is False

    def test_no_sql_dir(self, tmp_path: Path) -> None:
        assert has_mart_sql(tmp_path) is False

    def test_sql_dir_no_mart_files(self, tmp_path: Path) -> None:
        (tmp_path / "sql").mkdir(parents=True)
        (tmp_path / "sql" / "clean.sql").write_text("SELECT 1")
        assert has_mart_sql(tmp_path) is False
