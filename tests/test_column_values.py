"""Test per ``toolkit column-values`` — profilo valori delle colonne dimensionali.

Testa la logica di dominio (build_column_values_profile) e il layer CLI
(comando ``column-values``). Fixture: parquet creato via DuckDB in tmp_path,
nessuna rete.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from toolkit.cli.app import app
from toolkit.domain.column_values import (
    build_column_values_profile,
    generate_workspace_column_values,
)

pytest.importorskip("duckdb")

runner = CliRunner()


def _write_parquet(path: Path) -> None:
    import duckdb

    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.execute(
            "CREATE TABLE t (regione VARCHAR, provincia VARCHAR, "
            "anno INTEGER, valore DOUBLE, attivo BOOLEAN)"
        )
        con.execute(
            "INSERT INTO t VALUES "
            "('Lombardia', 'MI', 2024, 10.5, true), "
            "('Lombardia', 'MI', 2024, 12.0, true), "
            "('Lombardia', 'BG', 2024, 9.5, false), "
            "('Piemonte', 'TO', 2024, 7.0, true), "
            "(NULL, NULL, 2024, 5.0, NULL)"
        )
        con.execute(f"COPY t TO '{path}' (FORMAT parquet)")


def _columns_fixture() -> list[dict]:
    return [
        {"name": "regione", "type": "VARCHAR", "role": "dimension", "semantic_type": "region_code"},
        {"name": "provincia", "type": "VARCHAR", "role": "dimension"},
        {"name": "anno", "type": "INTEGER", "role": "dimension"},
        {"name": "valore", "type": "DOUBLE", "role": "metric"},
        {"name": "attivo", "type": "BOOLEAN", "role": "dimension"},
    ]


class TestColumnValuesProfile:
    @pytest.mark.contract
    def test_happy_path_dimensions_only(self, tmp_path: Path) -> None:
        parquet = tmp_path / "clean.parquet"
        _write_parquet(parquet)

        profile = build_column_values_profile(parquet, _columns_fixture(), top_n=10)

        assert profile["n_rows"] == 5
        assert profile["count_distinct_mode"] == "approx"
        assert profile["n_columns_profiled"] == 4  # regione, provincia, anno, attivo

        # metriche escluse
        assert "valore" not in profile["columns"]

        # regione: 4 non-null, ~2 distinct (approx)
        regione = profile["columns"]["regione"]
        assert regione["role"] == "dimension"
        assert regione["semantic_type"] == "region_code"
        assert regione["n_null"] == 1
        assert 2 <= regione["n_distinct"] <= 3
        assert regione["top_values"][0] == {"value": "Lombardia", "n": 3, "pct": 60.0}
        assert regione["top_truncated"] is False

    @pytest.mark.contract
    def test_boolean_and_date_values_json_safe(self, tmp_path: Path) -> None:
        parquet = tmp_path / "clean.parquet"
        _write_parquet(parquet)

        profile = build_column_values_profile(parquet, _columns_fixture(), top_n=10)
        # valore booleano serializzabile in JSON
        attivo = profile["columns"]["attivo"]
        assert attivo["n_distinct"] == 2
        json.dumps(profile)  # non deve sollevare TypeError

    @pytest.mark.contract
    def test_top_truncated(self, tmp_path: Path) -> None:
        parquet = tmp_path / "clean.parquet"
        _write_parquet(parquet)

        profile = build_column_values_profile(parquet, _columns_fixture(), top_n=1)
        assert profile["columns"]["regione"]["top_truncated"] is True
        assert len(profile["columns"]["regione"]["top_values"]) == 1

    @pytest.mark.contract
    def test_no_dimensions(self, tmp_path: Path) -> None:
        parquet = tmp_path / "clean.parquet"
        _write_parquet(parquet)

        profile = build_column_values_profile(
            parquet, [{"name": "valore", "type": "DOUBLE", "role": "metric"}]
        )
        assert profile["n_rows"] is None
        assert profile["n_columns_profiled"] == 0
        assert profile["columns"] == {}


class TestColumnValuesBatch:
    @pytest.mark.contract
    def test_batch_generates_profiles_for_repo(self, tmp_path: Path) -> None:
        # workspace/repo/section/slug/dataset.yml + parquet clean
        repo_root = tmp_path / "repo-a"
        parquet = repo_root / "out" / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
        _write_parquet(parquet)
        ds_dir = repo_root / "datasets" / "demo"
        ds_dir.mkdir(parents=True)
        yml = """
root: "../../out"
schema_version: 1
dataset:
  name: "demo"
  source_id: "src1"
  tags: [test]
  category: demo
  years: [2024]
"""
        (ds_dir / "dataset.yml").write_text(yml, encoding="utf-8")

        out_dir = tmp_path / "_local" / "generated" / "column_values"
        result = generate_workspace_column_values(tmp_path, out_dir, top_n=10)

        assert result.processed == 1
        assert result.written == 1
        assert result.errors == []
        assert (out_dir / "demo__column_values.json").is_file()

    @pytest.mark.contract
    def test_batch_dedup_slug_across_repos(self, tmp_path: Path) -> None:
        # stesso slug in due repo: solo il primo viene scritto, il secondo skip
        def _make_repo(name: str) -> None:
            repo_root = tmp_path / name
            parquet = (
                repo_root / "out" / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
            )
            _write_parquet(parquet)
            ds_dir = repo_root / "datasets" / "demo"
            ds_dir.mkdir(parents=True)
            yml = """
root: "../../out"
schema_version: 1
dataset:
  name: "demo"
  source_id: "src1"
  tags: [test]
  category: demo
  years: [2024]
"""
            (ds_dir / "dataset.yml").write_text(yml, encoding="utf-8")

        _make_repo("aaa-first")
        _make_repo("zzz-second")

        out_dir = tmp_path / "_local" / "generated" / "column_values"
        result = generate_workspace_column_values(tmp_path, out_dir, top_n=10)

        assert result.written == 1
        assert len(result.skipped) == 1
        assert "duplicato" in result.skipped[0]

    @pytest.mark.contract
    def test_batch_skips_no_clean(self, tmp_path: Path) -> None:
        # dataset senza parquet clean: skip, nessun errore
        repo_root = tmp_path / "repo-a"
        ds_dir = repo_root / "datasets" / "demo"
        ds_dir.mkdir(parents=True)
        yml = """
root: "../../out"
schema_version: 1
dataset:
  name: "demo"
  source_id: "src1"
  tags: [test]
  category: demo
  years: [2024]
"""
        (ds_dir / "dataset.yml").write_text(yml, encoding="utf-8")

        out_dir = tmp_path / "_local" / "generated" / "column_values"
        result = generate_workspace_column_values(tmp_path, out_dir, top_n=10)

        assert result.processed == 1
        assert result.written == 0
        assert len(result.skipped) == 1
        assert result.errors == []


class TestColumnValuesCli:
    @pytest.mark.contract
    def test_requires_config(self) -> None:
        result = runner.invoke(app, ["column-values"])
        assert result.exit_code != 0
        assert "config" in (result.stdout + result.stderr).lower()

    @pytest.mark.contract
    def test_json_output(self, tmp_path: Path) -> None:
        parquet = tmp_path / "out" / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
        _write_parquet(parquet)

        # mini-repo: dataset.yml con root relativo che punta a out/
        ds_dir = tmp_path / "datasets" / "demo"
        ds_dir.mkdir(parents=True)
        yml = """
root: "../../out"
schema_version: 1
dataset:
  name: "demo"
  source_id: "src1"
  tags: [test]
  category: demo
  years: [2024]
"""
        config_path = ds_dir / "dataset.yml"
        config_path.write_text(yml, encoding="utf-8")

        result = runner.invoke(
            app,
            ["column-values", "-c", str(config_path), "--json", "--top", "5"],
        )
        assert result.exit_code == 0, result.output

        data = json.loads(result.stdout)
        assert data["dataset"] == "demo"
        assert data["year"] == 2024
        assert data["n_rows"] == 5
        assert "regione" in data["columns"]

    @pytest.mark.contract
    def test_writes_json_file(self, tmp_path: Path) -> None:
        parquet = tmp_path / "out" / "data" / "clean" / "demo" / "2024" / "demo_2024_clean.parquet"
        _write_parquet(parquet)

        ds_dir = tmp_path / "datasets" / "demo"
        ds_dir.mkdir(parents=True)
        yml = """
root: "../../out"
schema_version: 1
dataset:
  name: "demo"
  source_id: "src1"
  tags: [test]
  category: demo
  years: [2024]
"""
        config_path = ds_dir / "dataset.yml"
        config_path.write_text(yml, encoding="utf-8")

        out_dir = tmp_path / "_local" / "generated" / "column_values"
        result = runner.invoke(
            app,
            ["column-values", "-c", str(config_path), "--out", str(out_dir)],
        )
        assert result.exit_code == 0, result.output

        out_path = out_dir / "demo__column_values.json"
        assert out_path.is_file()
        data = json.loads(out_path.read_text(encoding="utf-8"))
        assert data["dataset"] == "demo"

    @pytest.mark.contract
    def test_missing_parquet_errors(self, tmp_path: Path) -> None:
        ds_dir = tmp_path / "datasets" / "demo"
        ds_dir.mkdir(parents=True)
        yml = """
root: "../../out"
schema_version: 1
dataset:
  name: "demo"
  source_id: "src1"
  tags: [test]
  category: demo
  years: [2024]
"""
        config_path = ds_dir / "dataset.yml"
        config_path.write_text(yml, encoding="utf-8")

        result = runner.invoke(app, ["column-values", "-c", str(config_path), "--json"])
        assert result.exit_code != 0
        assert "non trovato" in (result.stdout + result.stderr).lower()
