from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from typer.testing import CliRunner

from toolkit.cli.app import app
from toolkit.cli.cmd_scout_url import probe_url


class _ScoutHandler(BaseHTTPRequestHandler):
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
    <a href="../exports/out.csv">Parent CSV</a>
    <a href="//cdn.example.com/file.zip">CDN ZIP</a>
    <a href="https://example.org/api/data.json">JSON</a>
    <a href="/page">Page</a>
  </body>
</html>
"""
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/opaque":
            body = b"opaque-bytes"
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def _serve() -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ScoutHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}"


def test_scout_url_reports_file_headers_after_redirect() -> None:
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["scout-url", f"{base_url}/redirect-file"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0
    assert f"requested_url: {base_url}/redirect-file" in result.output
    assert f"final_url: {base_url}/files/demo.csv" in result.output
    assert "status_code: 200" in result.output
    assert "content_type: text/csv; charset=utf-8" in result.output
    assert 'content_disposition: attachment; filename="demo.csv"' in result.output
    assert "kind: file" in result.output
    assert "candidate_links: none" in result.output


def test_scout_url_extracts_candidate_links_from_html() -> None:
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["scout-url", f"{base_url}/html"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0
    assert f"final_url: {base_url}/html" in result.output
    assert "content_type: text/html; charset=utf-8" in result.output
    assert "kind: html" in result.output
    assert "candidate_links:" in result.output
    assert f"  - {base_url}/downloads/data.csv" in result.output
    assert f"  - {base_url}/reports/report.xlsx" in result.output
    assert f"  - {base_url}/exports/out.csv" in result.output
    assert "  - http://cdn.example.com/file.zip" in result.output
    assert "  - https://example.org/api/data.json" in result.output


def test_scout_url_marks_opaque_non_html_response() -> None:
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["scout-url", f"{base_url}/opaque"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0
    assert f"final_url: {base_url}/opaque" in result.output
    assert "content_type: application/octet-stream" in result.output
    assert "content_disposition: None" in result.output
    assert "kind: opaque" in result.output
    assert "candidate_links: none" in result.output


def test_probe_url_uses_streaming_and_reads_body_only_for_html(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    class _FakeResponse:
        def __init__(self, *, content_type: str, text: str = "") -> None:
            self.headers = {"Content-Type": content_type}
            self.url = "https://example.org/resource"
            self.status_code = 200
            self.encoding = None
            self.apparent_encoding = "utf-8"
            self._text = text
            self.text_reads = 0

        @property
        def text(self) -> str:
            self.text_reads += 1
            return self._text

        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    responses = [
        _FakeResponse(content_type="application/octet-stream"),
        _FakeResponse(content_type="text/html; charset=utf-8", text='<a href="/data.csv">CSV</a>'),
    ]

    def _fake_get(*args, **kwargs):
        calls.append(kwargs)
        return responses[len(calls) - 1]

    monkeypatch.setattr("toolkit.cli.cmd_scout_url.requests.get", _fake_get)

    opaque = probe_url("https://example.org/opaque", timeout=7)
    html = probe_url("https://example.org/html", timeout=7)

    assert opaque["kind"] == "opaque"
    assert html["kind"] == "html"
    assert html["candidate_links"] == ["https://example.org/data.csv"]
    assert calls[0]["stream"] is True
    assert calls[1]["stream"] is True
    assert calls[0]["timeout"] == 7
    assert calls[1]["timeout"] == 7
    assert responses[0].text_reads == 0
    assert responses[1].text_reads == 1


def test_probe_url_passes_custom_user_agent(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    class _FakeResponse:
        def __init__(self) -> None:
            self.headers = {"Content-Type": "application/octet-stream"}
            self.url = "https://example.org/resource"
            self.status_code = 200

        def __enter__(self) -> "_FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def _fake_get(*args, **kwargs):
        calls.append(kwargs)
        return _FakeResponse()

    monkeypatch.setattr("toolkit.cli.cmd_scout_url.requests.get", _fake_get)

    custom_ua = "Mozilla/5.0 (DataCivicLab Custom)"
    probe_url("https://example.org/test", user_agent=custom_ua)

    assert len(calls) == 1
    assert calls[0]["headers"] == {"User-Agent": custom_ua}
