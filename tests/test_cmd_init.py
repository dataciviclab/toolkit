"""Tests for toolkit init --url (scout + generate dataset.yml)."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lab_connectors.http import HttpClient
from lab_connectors.testing import http_ok

from toolkit.cli.app import app
from typer.testing import CliRunner


@pytest.mark.policy
def test_init_url_generates_dataset_yml(monkeypatch, tmp_path: Path) -> None:
    """init --url downloads sample, sniffs, generates valid dataset.yml."""
    csv_content = b"nome,eta,citta\nMario,30,Roma\nLucia,25,Milano\n"

    def fake_get(self, url, **kwargs):
        return http_ok(
            content=csv_content,
            headers={"Content-Type": "text/csv"},
            url=url,
        )

    monkeypatch.setattr(HttpClient, "get", fake_get)

    runner = CliRunner()
    # Use tmp_path for output to avoid polluting repo
    with runner.isolated_filesystem(temp_dir=tmp_path) as td:
        r = runner.invoke(app, ["init", "--url", "https://example.test/dati.csv"])
        assert r.exit_code == 0, f"init failed: {r.output}"

        # Find generated slug directory (slug_<hash>)
        dirs = [d for d in Path(td).iterdir() if d.is_dir()]
        assert len(dirs) == 1, f"expected 1 directory, got {dirs}"
        slug_dir = dirs[0]

        # Check dataset.yml was created
        yml_path = slug_dir / "dataset.yml"
        assert yml_path.exists(), f"dataset.yml not found at {yml_path}"
        data = yaml.safe_load(yml_path.read_text(encoding="utf-8"))

        # Verify structure
        assert data["root"] == "../../out"
        assert data["dataset"]["name"].startswith("dati")
        assert len(data["raw"]["sources"]) == 1
        src = data["raw"]["sources"][0]
        assert src["type"] == "http_file"
        assert src["args"]["url"] == "https://example.test/dati.csv"

        # Verify sniffed config (in clean.read, per contratto)
        assert data["clean"]["read"]["encoding"] in ("utf-8", "ascii")
        assert data["clean"]["read"]["delim"] == ","

        # Verify columns
        cols = data["clean"]["read"]["columns"]
        assert "nome" in cols
        assert "eta" in cols
        assert "citta" in cols

        # Verify mart section
        assert len(data["mart"]["tables"]) == 1
        assert data["mart"]["tables"][0]["sql"] == "sql/mart.sql"

        # Verify generated SQL files
        assert (slug_dir / "sql" / "clean.sql").exists()
        assert (slug_dir / "sql" / "mart.sql").exists()
        mart_sql = (slug_dir / "sql" / "mart.sql").read_text()
        assert "SELECT * FROM clean" in mart_sql


@pytest.mark.policy
def test_init_url_requires_url_or_config() -> None:
    """init without --url or --config must fail with clear error."""
    runner = CliRunner()
    r = runner.invoke(app, ["init"])
    assert r.exit_code != 0
    assert "--url" in r.output or "--config" in r.output


@pytest.mark.policy
def test_init_url_rejects_both_url_and_config() -> None:
    """init with both --url and --config must fail."""
    runner = CliRunner()
    r = runner.invoke(app, ["init", "--url", "https://x", "--config", "dataset.yml"])
    assert r.exit_code != 0
    assert "non entrambi" in r.output
