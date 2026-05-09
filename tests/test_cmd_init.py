"""Tests for toolkit init --url (scout + generate dataset.yml)."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import ANY

import pytest
import yaml

from lab_connectors.http import HttpClient, HttpResult

from toolkit.cli.app import app
from typer.testing import CliRunner


class _FakeResp:
    def __init__(self, content: bytes = b"", headers: dict | None = None, status_code: int = 200, url: str = ""):
        self.content = content
        self.headers = headers or {}
        self.status_code = status_code
        self.url = url


def test_init_url_generates_dataset_yml(monkeypatch, tmp_path: Path) -> None:
    """init --url downloads sample, sniffs, generates valid dataset.yml."""
    csv_content = b"nome,eta,citta\nMario,30,Roma\nLucia,25,Milano\n"

    def fake_get(self, url, **kwargs):
        return HttpResult(
            response=_FakeResp(
                content=csv_content,
                headers={"Content-Type": "text/csv"},
                status_code=200,
                url=url,
            ),
            err=None,
        )

    monkeypatch.setattr(HttpClient, "get", fake_get)

    runner = CliRunner()
    # Use tmp_path for output to avoid polluting repo
    with runner.isolated_filesystem(temp_dir=tmp_path) as td:
        r = runner.invoke(app, ["init", "--url", "https://example.test/dati.csv"])
        assert r.exit_code == 0, f"init failed: {r.output}"

        # Check dataset.yml was created
        yml_path = Path(td) / "dati" / "dataset.yml"
        assert yml_path.exists(), f"dataset.yml not found at {yml_path}"
        data = yaml.safe_load(yml_path.read_text(encoding="utf-8"))

        # Verify structure
        assert data["root"] == "../../out"
        assert data["dataset"]["name"] == "dati"
        assert len(data["raw"]["sources"]) == 1
        src = data["raw"]["sources"][0]
        assert src["type"] == "http_file"
        assert src["args"]["url"] == "https://example.test/dati.csv"

        # Verify sniffed config
        assert data["raw"]["read"]["encoding"] in ("utf-8", "ascii")
        assert data["raw"]["read"]["delim"] == ","

        # Verify columns
        cols = data["clean"]["read"]["columns"]
        assert len(cols) == 3
        assert cols[0]["name"] == "nome"
        assert cols[1]["name"] == "eta"
        assert cols[2]["name"] == "citta"

        # Verify mart section
        assert data["mart"]["sql"] == "sql/mart.sql"
        assert len(data["mart"]["tables"]) == 1

        # Verify generated SQL files
        assert (Path(td) / "dati" / "sql" / "clean.sql").exists()
        assert (Path(td) / "dati" / "sql" / "mart.sql").exists()
        mart_sql = (Path(td) / "dati" / "sql" / "mart.sql").read_text()
        assert "SELECT * FROM clean" in mart_sql


def test_init_url_requires_url_or_config() -> None:
    """init without --url or --config must fail with clear error."""
    runner = CliRunner()
    r = runner.invoke(app, ["init"])
    assert r.exit_code != 0
    assert "--url" in r.output or "--config" in r.output


def test_init_url_rejects_both_url_and_config() -> None:
    """init with both --url and --config must fail."""
    runner = CliRunner()
    r = runner.invoke(app, ["init", "--url", "https://x", "--config", "dataset.yml"])
    assert r.exit_code != 0
    assert "non entrambi" in r.output
