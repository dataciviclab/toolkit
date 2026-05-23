"""Integration tests per toolkit scout CLI command.

Usa lo stesso test HTTP server di test_cli_scout_url.py.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import yaml

from toolkit.cli.app import app
from typer.testing import CliRunner


class _ScoutHandler(BaseHTTPRequestHandler):
    """Test HTTP server con URL prevedibili."""

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/redirect-file":
            self.send_response(302)
            self.send_header("Location", "/files/demo.csv")
            self.end_headers()
            return

        if self.path == "/files/demo.csv":
            body = b"id,value\n1,10\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="demo.csv"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/html":
            body = b"""
<html>
  <body>
    <a href="/downloads/data.csv">CSV</a>
    <a href="reports/report.xlsx">XLSX</a>
  </body>
</html>
"""
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/files/multi_col.csv":
            body = b"nome,eta,citta\nMario,30,Roma\nLucia,25,Milano\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args) -> None:
        return


def _serve() -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ScoutHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


# ---------------------------------------------------------------------------
# policy: toolkit scout su URL file diretto
# ---------------------------------------------------------------------------


@pytest.mark.policy
def test_scout_file_url_shows_probe_info() -> None:
    """toolkit scout su URL CSV mostra source_type, status, format."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["scout", f"{base_url}/files/demo.csv"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0, f"scout failed: {result.output}"
    assert "Source type: file" in result.output
    assert "HTTP status: 200" in result.output
    assert "csv" in result.output
    assert "Next: toolkit init --url" in result.output


@pytest.mark.policy
def test_scout_html_shows_candidate_links() -> None:
    """toolkit scout su HTML mostra candidate_links."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["scout", f"{base_url}/html"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0, f"scout failed: {result.output}"
    assert "Source type: html" in result.output
    assert "Candidate links:" in result.output
    assert f"{base_url}/downloads/data.csv" in result.output
    assert "Next: toolkit init --url" in result.output


@pytest.mark.policy
def test_scout_json_output() -> None:
    """toolkit scout --json restituisce JSON valido con source_type."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["scout", "--json", f"{base_url}/files/demo.csv"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0, f"scout --json failed: {result.output}"
    data = json.loads(result.output)
    assert data["source_type"] == "file"
    assert data["status_code"] == 200
    assert "final_url" in data


# ---------------------------------------------------------------------------
# policy: toolkit scout --scaffold genera file candidate
# ---------------------------------------------------------------------------


@pytest.mark.policy
def test_scout_scaffold_generates_dataset_yml(tmp_path: Path) -> None:
    """toolkit scout --scaffold genera dataset.yml valido (YAML parsabile)."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            result = runner.invoke(app, ["scout", "--scaffold", f"{base_url}/files/multi_col.csv"])
            assert result.exit_code == 0, f"scout --scaffold failed: {result.output}"

            # Trova la directory generata
            dirs = [d for d in Path(td).iterdir() if d.is_dir()]
            assert len(dirs) >= 1, f"no directories created in {td}"
            slug_dir = dirs[0]

            # dataset.yml valido
            yml_path = slug_dir / "dataset.yml"
            assert yml_path.exists(), f"dataset.yml not found at {yml_path}"
            data = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
            assert data["dataset"]["name"].startswith("multi_col")
            assert data["raw"]["sources"][0]["type"] == "http_file"
            assert "clean" in data
            assert "mart" in data

            # SQL files
            assert (slug_dir / "sql" / "clean.sql").exists()
            assert (slug_dir / "sql" / "mart.sql").exists()
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.policy
def test_scout_scaffold_has_type_casts_in_clean_sql(tmp_path: Path) -> None:
    """clean.sql generato da scout --scaffold ha TRY_CAST per colonne numeriche."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        with runner.isolated_filesystem(temp_dir=tmp_path) as td:
            result = runner.invoke(app, ["scout", "--scaffold", f"{base_url}/files/multi_col.csv"])
            assert result.exit_code == 0

            dirs = [d for d in Path(td).iterdir() if d.is_dir()]
            slug_dir = dirs[0]
            clean_sql = (slug_dir / "sql" / "clean.sql").read_text()
            mart_sql = (slug_dir / "sql" / "mart.sql").read_text()

            # Verifica cast
            assert clean_sql, "clean.sql is empty"
            assert mart_sql, "mart.sql is empty"
    finally:
        server.shutdown()
        server.server_close()


# ---------------------------------------------------------------------------
# policy: error cases
# ---------------------------------------------------------------------------


@pytest.mark.policy
def test_scout_shows_404_status() -> None:
    """toolkit scout su URL inesistente mostra codice 404."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["scout", f"{base_url}/nonexistent.csv"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0
    assert "HTTP status: 404" in result.output
    assert "Source type: file" in result.output


@pytest.mark.policy
def test_scout_shows_next_steps_for_file() -> None:
    """toolkit scout per file mostra suggerimento init --url."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["scout", f"{base_url}/files/demo.csv"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0
    assert "Next: toolkit init --url" in result.output
