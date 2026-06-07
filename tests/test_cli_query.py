"""Test per CLI toolkit query.

Contract: copre path diretto e modalita' --config (con e senza artifact).
Policy: missing file, no args, --config senza artifact.
"""

from __future__ import annotations

import json

import pytest
import yaml
from typer.testing import CliRunner

from tests.helpers import write_parquet


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def sample_parquet(tmp_path):
    """Crea un parquet di esempio per i test."""
    pq = tmp_path / "data.parquet"
    write_parquet(
        pq,
        "CREATE TABLE t AS "
        "SELECT 'a' AS cat, 10 AS val UNION ALL "
        "SELECT 'b', 20 UNION ALL "
        "SELECT 'a', 30",
    )
    return pq


@pytest.fixture
def dataset_with_clean_parquet(tmp_path):
    """Crea un dataset.yml minimale con un parquet clean nel path atteso.

    Crea la struttura:
        {tmp_path}/_smoke_out/data/clean/mio_test/2024/mio_test_2024_clean.parquet
        {tmp_path}/dataset.yml
    """
    root = tmp_path / "_smoke_out"
    slug = "mio_test"
    year = 2024
    clean_dir = root / "data" / "clean" / slug / str(year)
    clean_dir.mkdir(parents=True, exist_ok=True)
    pq = clean_dir / f"{slug}_{year}_clean.parquet"
    write_parquet(pq, "CREATE TABLE t AS SELECT 1 AS id, 'x' AS val")

    # dataset.yml
    config = {
        "root": str(root),
        "dataset": {"name": slug, "years": [year]},
        "raw": {"sources": [{"type": "local_file", "args": {"path": "dummy.csv"}}]},
        "clean": {"sql": "sql/clean.sql"},
        "mart": {"tables": []},
    }
    cfg_path = tmp_path / "dataset.yml"
    cfg_path.write_text(yaml.dump(config), encoding="utf-8")

    # sql stub (evita errori di validazione)
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir(exist_ok=True)
    (sql_dir / "clean.sql").write_text("SELECT 1", encoding="utf-8")

    return {
        "config_path": str(cfg_path),
        "layer": "clean",
        "year": year,
        "slug": slug,
        "parquet_path": pq,
    }


@pytest.mark.contract
def test_query_default_no_sql(runner, sample_parquet):
    """query senza --sql: SELECT * LIMIT 20 (default)."""
    from toolkit.cli.app import app

    result = runner.invoke(app, ["query", str(sample_parquet)])
    assert result.exit_code == 0
    assert "cat" in result.stdout
    assert "val" in result.stdout


@pytest.mark.contract
def test_query_with_sql(runner, sample_parquet):
    """query con --sql: esegue SQL arbitrario."""
    from toolkit.cli.app import app

    result = runner.invoke(
        app,
        [
            "query",
            str(sample_parquet),
            "--sql",
            "SELECT cat, SUM(val) AS total FROM data GROUP BY cat ORDER BY cat",
        ],
    )
    assert result.exit_code == 0
    assert "a" in result.stdout
    assert "b" in result.stdout
    assert "total" in result.stdout
    assert "40" in result.stdout  # SUM(a: 10+30)
    assert "20" in result.stdout


@pytest.mark.contract
def test_query_with_where(runner, sample_parquet):
    """query con WHERE: filtra correttamente."""
    from toolkit.cli.app import app

    result = runner.invoke(
        app,
        [
            "query",
            str(sample_parquet),
            "--sql",
            "SELECT * FROM data WHERE cat = 'a'",
        ],
    )
    assert result.exit_code == 0
    assert result.stdout.count("a") >= 2  # header + 2 righe
    assert "30" in result.stdout


@pytest.mark.contract
def test_query_json_output(runner, sample_parquet):
    """query --json: output JSON valido."""
    from toolkit.cli.app import app

    result = runner.invoke(app, ["query", str(sample_parquet), "--json"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["column_count"] == 2
    assert data["row_count"] == 3
    assert len(data["preview"]) == 3


@pytest.mark.policy
def test_query_missing_file(runner, tmp_path):
    """query su file inesistente: exit code 1."""
    from toolkit.cli.app import app

    result = runner.invoke(app, ["query", str(tmp_path / "nonexistent.parquet")])
    assert result.exit_code == 1
    assert "error" in result.stdout.lower() or "error" in result.stderr.lower()


@pytest.mark.policy
def test_query_no_path_no_config(runner):
    """query senza path ne --config: exit code 1."""
    from toolkit.cli.app import app

    result = runner.invoke(app, ["query"])
    assert result.exit_code != 0


# --- Modalita' --config ---


@pytest.mark.contract
def test_query_config_with_artifact(runner, dataset_with_clean_parquet):
    """query -c dataset.yml -l clean con artifact presente: successo."""
    from toolkit.cli.app import app

    info = dataset_with_clean_parquet
    result = runner.invoke(
        app,
        [
            "query",
            "--config",
            info["config_path"],
            "--layer",
            info["layer"],
            "--year",
            str(info["year"]),
        ],
    )
    assert result.exit_code == 0
    assert "id" in result.stdout
    assert "val" in result.stdout
    assert "x" in result.stdout


@pytest.mark.contract
def test_query_config_json_output(runner, dataset_with_clean_parquet):
    """query -c dataset.yml -l clean --json con artifact: JSON valido."""
    from toolkit.cli.app import app

    info = dataset_with_clean_parquet
    result = runner.invoke(
        app,
        [
            "query",
            "--config",
            info["config_path"],
            "--layer",
            info["layer"],
            "--json",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["column_count"] == 2
    assert data["row_count"] == 1


@pytest.mark.policy
def test_query_config_missing_artifact(runner, tmp_path):
    """query -c dataset.yml -l clean SENZA artifact: exit code 1.

    Crea un dataset.yml ma non genera il parquet. Il comando deve fallire
    con messaggio chiaro, non restituire preview vuota.
    """
    # dataset.yml senza parquet generato
    root = tmp_path / "_smoke_out"
    slug = "mai_run"
    year = 2024
    config = {
        "root": str(root),
        "dataset": {"name": slug, "years": [year]},
        "raw": {"sources": [{"type": "local_file", "args": {"path": "dummy.csv"}}]},
        "clean": {"sql": "sql/clean.sql"},
        "mart": {"tables": []},
    }
    cfg_path = tmp_path / "dataset.yml"
    cfg_path.write_text(yaml.dump(config), encoding="utf-8")

    # sql stub
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir(exist_ok=True)
    (sql_dir / "clean.sql").write_text("SELECT 1", encoding="utf-8")

    from toolkit.cli.app import app

    result = runner.invoke(
        app,
        [
            "query",
            "--config",
            str(cfg_path),
            "--layer",
            "clean",
            "--year",
            str(year),
        ],
    )
    assert result.exit_code == 1
    assert "non trovato" in result.stdout or "non trovato" in result.stderr
