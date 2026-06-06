"""Test per CLI toolkit query."""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from tests.helpers import write_parquet


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def sample_parquet(tmp_path):
    """Crea un parquet di esempio per i test."""
    pq = tmp_path / "data.parquet"
    write_parquet(pq, "CREATE TABLE t AS "
                       "SELECT 'a' AS cat, 10 AS val UNION ALL "
                       "SELECT 'b', 20 UNION ALL "
                       "SELECT 'a', 30")
    return pq


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

    result = runner.invoke(app, [
        "query", str(sample_parquet),
        "--sql", "SELECT cat, SUM(val) AS total FROM data GROUP BY cat ORDER BY cat",
    ])
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

    result = runner.invoke(app, [
        "query", str(sample_parquet),
        "--sql", "SELECT * FROM data WHERE cat = 'a'",
    ])
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
