"""Integration tests per toolkit scout CLI command.

Usa lo stesso test HTTP server di test_cli_scout_url.py.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

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
    assert "Next: toolkit preview" in result.output


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
    assert "Next: toolkit preview" in result.output


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
    """toolkit scout per file suggerisce toolkit preview."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["scout", f"{base_url}/files/demo.csv"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0
    assert "Next: toolkit preview" in result.output


# ---------------------------------------------------------------------------
# contract: toolkit preview (profilo CSV/TSV remoto)
# ---------------------------------------------------------------------------


@pytest.mark.contract
def test_preview_profiles_remote_csv() -> None:
    """toolkit preview <URL> profila il file remoto e stampa le colonne."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["preview", f"{base_url}/files/multi_col.csv"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0, result.output
    assert "Columns (" in result.output
    assert "nome" in result.output


@pytest.mark.contract
def test_preview_json_output() -> None:
    """toolkit preview --json restituisce un payload con le colonne."""
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["preview", f"{base_url}/files/multi_col.csv", "--json"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "success"
    assert "nome" in payload["columns"]
