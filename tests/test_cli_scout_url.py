from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typer.testing import CliRunner

from lab_connectors.http import HttpClient, HttpResult

from toolkit.cli.app import app
from toolkit.scout.http import detect_ckan_in_html, extract_ckan_dataset_id
from toolkit.scout.probe import probe_url
from toolkit.scout.scaffold import generate_yaml_scaffold


class _ScoutHandler(BaseHTTPRequestHandler):
    def do_HEAD(self) -> None:  # noqa: N802
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
        result = runner.invoke(app, ["inspect", "url", f"{base_url}/redirect-file"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0
    assert f"requested_url: {base_url}/redirect-file" in result.output
    assert f"final_url: {base_url}/files/demo.csv" in result.output
    assert "status_code: 200" in result.output
    assert "content_type: text/csv; charset=utf-8" in result.output
    assert 'content_disposition: attachment; filename="demo.csv"' in result.output
    assert "source_type: file" in result.output
    assert "candidate_links: 0" in result.output


def test_scout_url_extracts_candidate_links_from_html() -> None:
    server, base_url = _serve()
    runner = CliRunner()
    try:
        result = runner.invoke(app, ["inspect", "url", f"{base_url}/html"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0
    assert f"final_url: {base_url}/html" in result.output
    assert "content_type: text/html; charset=utf-8" in result.output
    assert "source_type: html" in result.output
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
        result = runner.invoke(app, ["inspect", "url", f"{base_url}/opaque"])
    finally:
        server.shutdown()
        server.server_close()

    assert result.exit_code == 0
    assert f"final_url: {base_url}/opaque" in result.output
    assert "content_type: application/octet-stream" in result.output
    assert "content_disposition: None" in result.output
    assert "source_type: opaque" in result.output
    assert "candidate_links: 0" in result.output


def test_probe_url_uses_head_then_get_only_for_html(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []  # (method, url)

    class _FakeResponse:
        def __init__(self, *, content_type: str, text: str = "", url: str = "https://example.org/resource") -> None:
            self.headers = {"Content-Type": content_type}
            self.url = url
            self.status_code = 200
            self.encoding = "utf-8"
            self._text = text
            self.text = text

    def _fake_head(self, url, **kwargs):
        calls.append(("head", url))
        ct = "text/html; charset=utf-8" if "html" in url else "application/octet-stream"
        return HttpResult(response=_FakeResponse(content_type=ct, url=url), err=None)

    def _fake_get(self, url, **kwargs):
        calls.append(("get", url))
        if "html" in url:
            return HttpResult(
                response=_FakeResponse(
                    content_type="text/html; charset=utf-8",
                    text='<a href="/data.csv">CSV</a>',
                    url=url,
                ),
                err=None,
            )
        return HttpResult(
            response=_FakeResponse(content_type="application/octet-stream", url=url),
            err=None,
        )

    monkeypatch.setattr(HttpClient, "head", _fake_head)
    monkeypatch.setattr(HttpClient, "get", _fake_get)

    opaque = probe_url("https://example.org/opaque", timeout=7)
    html = probe_url("https://example.org/html", timeout=7)

    assert opaque["kind"] == "opaque"
    assert html["kind"] == "html"
    assert html["candidate_links"] == ["https://example.org/data.csv"]
    # Opaque: only HEAD, no GET
    assert calls[0] == ("head", "https://example.org/opaque")
    # HTML: HEAD + GET
    assert calls[1] == ("head", "https://example.org/html")
    assert calls[2] == ("get", "https://example.org/html")


def test_probe_url_falls_back_to_get_when_head_fails(monkeypatch) -> None:
    """HEAD fails with error, GET with Range succeeds → probe returns file info."""
    head_called = False
    get_called = False

    class _FileResp:
        headers = {"Content-Type": "text/csv; charset=utf-8"}
        url = "https://example.org/data.csv"
        status_code = 200

    def _fake_head(self, url, **kwargs):
        nonlocal head_called
        head_called = True
        return HttpResult(response=None, err=ConnectionError("HEAD refused"))

    def _fake_get(self, url, **kwargs):
        nonlocal get_called
        get_called = True
        return HttpResult(response=_FileResp(), err=None)

    monkeypatch.setattr(HttpClient, "head", _fake_head)
    monkeypatch.setattr(HttpClient, "get", _fake_get)

    result = probe_url("https://example.org/data.csv", timeout=10)

    assert head_called
    assert get_called
    assert result["kind"] == "file"
    assert result["status_code"] == 200
    assert result["final_url"] == "https://example.org/data.csv"
    assert "csv" in (result["content_type"] or "")


def test_probe_url_passes_timeout_and_user_agent(monkeypatch) -> None:
    """Verify probe_url creates HttpClient with the correct timeout and user-agent."""
    init_captured: dict = {}
    real_init = HttpClient.__init__

    def _fake_init(self, **kwargs):
        init_captured.update(kwargs)
        real_init(self, **kwargs)

    monkeypatch.setattr(HttpClient, "__init__", _fake_init)

    class _FakeResp:
        headers = {"Content-Type": "application/octet-stream"}
        url = "https://example.org/resource"
        status_code = 200

    def _fake_head(self, url, **kwargs):
        return HttpResult(response=_FakeResp(), err=None)

    monkeypatch.setattr(HttpClient, "head", _fake_head)

    probe_url("https://example.org/test", user_agent="CustomAgent/1.0", timeout=42)

    assert init_captured.get("timeout") == 42
    assert init_captured.get("user_agent") == "CustomAgent/1.0"


# ── Tests for CKAN detection and scaffold ───────────────────────────────────────

class TestExtractCkanDatasetId:
    def test_uuid_from_id_param(self) -> None:
        url = "https://www.dati.gov.it/view-dataset/dataset?id=bef11a2c-300b-4578-8143-c1ce08f46fff"
        assert extract_ckan_dataset_id(url) == "bef11a2c-300b-4578-8143-c1ce08f46fff"

    def test_dataset_path_with_uuid(self) -> None:
        url = "https://example.com/dataset/bef11a2c-300b-4578-8143-c1ce08f46fff"
        assert extract_ckan_dataset_id(url) == "bef11a2c-300b-4578-8143-c1ce08f46fff"

    def test_dataset_path_with_slug(self) -> None:
        url = "https://example.com/dataset/mio-dataset-slug"
        assert extract_ckan_dataset_id(url) == "mio-dataset-slug"

    def test_non_ckan_url_returns_none(self) -> None:
        url = "https://example.com/data/file.csv"
        assert extract_ckan_dataset_id(url) is None

    def test_html_api_reference(self) -> None:
        html = '<a href="/api/3/action/package_show?id=abc-123">Package</a>'
        url = "https://example.com/other"
        assert extract_ckan_dataset_id(url, html) == "abc-123"


class TestDetectCkan:
    def test_detects_data_view_embed(self) -> None:
        html = b'<div data-view-embed="/dataset/...">CKAN</div>'
        assert detect_ckan_in_html(html) is True

    def test_detects_api_action(self) -> None:
        html = b'/api/3/action/package_show'
        assert detect_ckan_in_html(html) is True

    def test_detects_ckan_css_class(self) -> None:
        html = b'<div class="ckan-1000">Content</div>'
        assert detect_ckan_in_html(html) is True

    def test_detects_package_id(self) -> None:
        html = b'{"package_id": "abc-123"}'
        assert detect_ckan_in_html(html) is True

    def test_rejects_non_ckan_html(self) -> None:
        html = b'<html><body><p>Plain HTML page</p></body></html>'
        assert detect_ckan_in_html(html) is False


class TestGenerateYamlScaffold:
    def test_http_file_scaffold(self) -> None:
        probe_result = {
            "final_url": "https://example.com/data/file.csv",
            "requested_url": "https://example.com/data/file.csv",
        }
        yaml = generate_yaml_scaffold(probe_result)
        assert '_source"' in yaml  # source name derived from slug with hash
        assert 'type: "http_file"' in yaml
        assert 'url: "https://example.com/data/file.csv"' in yaml
        assert 'filename: "file.csv"' in yaml
        assert 'years: [2024]' in yaml
        assert "root:" in yaml

    def test_scaffold_fallback_datastore_url_is_http_file(self) -> None:
        """Senza metadata CKAN, anche URL datastore produce http_file."""
        probe_result = {
            "final_url": "https://portal.com/api/3/datastore/dump/uuid.csv",
            "requested_url": "https://portal.com/api/3/datastore/dump/uuid.csv",
        }
        yaml = generate_yaml_scaffold(probe_result)
        assert 'type: "http_file"' in yaml
        assert 'type: "ckan"' not in yaml

    def test_scaffold_fallback_sdmx_url_is_http_file(self) -> None:
        """Senza metadata SDMX, anche URL /dataflow/ produce http_file."""
        probe_result = {
            "final_url": "https://example.com/data/flow/sdmx",
            "requested_url": "https://example.com/data/flow/sdmx",
        }
        yaml = generate_yaml_scaffold(probe_result)
        assert 'type: "http_file"' in yaml
        assert 'type: "sdmx"' not in yaml

    def test_ckan_resources_scaffold(self) -> None:
        probe_result = {
            "final_url": "https://portal.it/dataset/uuid",
            "requested_url": "https://portal.it/dataset/uuid",
        }
        ckan_resources = [
            {
                "id": "res-uuid-1",
                "name": "Main CSV",
                "format": "csv",
                "url": "https://portal.it/files/data.csv",
            },
            {
                "id": "res-uuid-2",
                "name": "Backup XLS",
                "format": "xls",
                "url": "https://portal.it/files/data.xls",
            },
        ]
        yaml = generate_yaml_scaffold(probe_result, ckan_resources)
        assert "type: \"ckan\"" in yaml
        assert 'resource_id: "res-uuid-1"' in yaml
        assert 'resource_id: "res-uuid-2"' in yaml
        assert 'filename: "data.csv"' in yaml

    def test_candidate_links_fallback(self) -> None:
        probe_result = {
            "final_url": "https://portal.it/page",
            "requested_url": "https://portal.it/page",
        }
        links = [
            "https://portal.it/download/data.csv",
            "https://portal.it/download/report.xlsx",
        ]
        yaml = generate_yaml_scaffold(probe_result, None, links)
        assert 'type: "http_file"' in yaml
        assert 'url: "https://portal.it/download/data.csv"' in yaml
        assert 'url: "https://portal.it/download/report.xlsx"' in yaml


# --scaffold e --run sono stati rimossi da inspect url.
# Usa: toolkit init --url <URL>  per scaffold
# Usa: toolkit init --url <URL> --run  per scaffold + raw run
