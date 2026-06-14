from __future__ import annotations

import pytest

from lab_connectors.http import HttpResult
from lab_connectors.testing import FakeHttpClient, fake_response

from toolkit.core.exceptions import DownloadError
from toolkit.plugins.ckan import CkanSource

pytestmark = pytest.mark.adapter


def test_ckan_fetch_resource_show_forces_https():
    """resource_show → download CSV (URL forzato a HTTPS)."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {"url": "http://portal.example.org/export/data.csv"},
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/export/data.csv"] = HttpResult(
        response=fake_response(200, text="a,b\n1,2\n"),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    payload, origin = source.fetch("https://portal.example.org/api/3", resource_id="abc")

    assert payload == b"a,b\n1,2\n"
    assert origin == "https://portal.example.org/export/data.csv"
    assert fake.requests[1][1] == "https://portal.example.org/export/data.csv"


def test_ckan_fetch_falls_back_to_package_show():
    """resource_show 404 → fallback a package_show → download CSV."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        response=fake_response(404),
        err=None,
    )
    fake.responses["https://portal.example.org/api/3/action/package_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "resources": [
                        {
                            "id": 33344,
                            "name": "csv dump",
                            "format": "CSV",
                            "url": "http://portal.example.org/api/3/datastore/dump/dataset.csv",
                        }
                    ]
                },
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/api/3/datastore/dump/dataset.csv"] = HttpResult(
        response=fake_response(200, text="a,b\n1,2\n"),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    payload, origin = source.fetch(
        "https://portal.example.org/api/3",
        resource_id="33344",
        dataset_id="dataset-id",
    )

    assert payload == b"a,b\n1,2\n"
    assert origin == "https://portal.example.org/api/3/datastore/dump/dataset.csv"
    assert any("package_show" in req[1] for req in fake.requests)


def test_ckan_fetch_requires_identifier():
    try:
        CkanSource().fetch("https://portal.example.org/api/3")
    except DownloadError as exc:
        assert "resource_id or dataset_id" in str(exc)
    else:
        raise AssertionError("Expected DownloadError")


def test_ckan_fetch_falls_back_to_second_resource_when_first_fails():
    """First resource URL fails (404), second resource succeeds."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/package_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "resources": [
                        {
                            "id": "first-res",
                            "name": "first csv",
                            "format": "CSV",
                            "url": "http://portal.example.org/export/first.csv",
                        },
                        {
                            "id": "second-res",
                            "name": "second csv",
                            "format": "CSV",
                            "url": "http://portal.example.org/export/second.csv",
                        },
                    ]
                },
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/export/first.csv"] = HttpResult(
        response=fake_response(404),
        err=None,
    )
    fake.responses["https://portal.example.org/export/second.csv"] = HttpResult(
        response=fake_response(200, text="ok,second"),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    payload, origin = source.fetch("https://portal.example.org/api/3", dataset_id="dataset-id")

    assert payload == b"ok,second"
    assert "second.csv" in origin
    urls = [req[1] for req in fake.requests]
    assert any("first.csv" in u for u in urls)
    assert any("second.csv" in u for u in urls)


def test_ckan_fetch_package_show_by_resource_name_raises_when_missing():
    """resource_name inesistente → DownloadError."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/package_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "resources": [
                        {
                            "id": "other",
                            "name": "auxiliary export",
                            "format": "CSV",
                            "url": "https://portal.example.org/export/aux.csv",
                        },
                    ]
                },
            },
        ),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    with pytest.raises(DownloadError, match="resource_name=target csv export"):
        source.fetch(
            "https://portal.example.org/api/3",
            dataset_id="dataset-id",
            resource_name="target csv export",
        )


def test_ckan_fetch_rejects_package_fallback_when_resource_id_missing():
    """resource_show 404 + package_show non trova resource_id → DownloadError."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        response=fake_response(404),
        err=None,
    )
    fake.responses["https://portal.example.org/api/3/action/package_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "resources": [
                        {
                            "id": 33344,
                            "name": "csv dump",
                            "format": "CSV",
                            "url": "http://portal.example.org/api/3/datastore/dump/dataset.csv",
                        },
                    ]
                },
            },
        ),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    with pytest.raises(DownloadError, match="resource_id=99999"):
        source.fetch(
            "https://portal.example.org/api/3", resource_id="99999", dataset_id="dataset-id"
        )


def test_ckan_download_bytes_retries_then_succeeds():
    """Retry behavior is in HttpClient; test end-to-end success."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {"url": "https://portal.example.org/export/retry.csv"},
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/export/retry.csv"] = HttpResult(
        response=fake_response(200, text="ok-after-retry"),
        err=None,
    )

    source = CkanSource(retries=3)
    source._client = fake

    payload, origin = source.fetch("https://portal.example.org/api/3", resource_id="abc")
    assert payload == b"ok-after-retry"
    assert origin == "https://portal.example.org/export/retry.csv"


def test_ckan_download_bytes_raises_on_http_error():
    """503 dal server → DownloadError."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {"url": "https://portal.example.org/export/unavailable.csv"},
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/export/unavailable.csv"] = HttpResult(
        response=fake_response(503),
        err=None,
    )
    # Anche package_show fallisce con 503 (stessa origine del download)
    fake.responses["https://portal.example.org/api/3/action/package_show"] = HttpResult(
        response=fake_response(503),
        err=None,
    )

    source = CkanSource(retries=1)
    source._client = fake

    with pytest.raises(DownloadError, match="HTTP 503"):
        source.fetch("https://portal.example.org/api/3", resource_id="abc")


def test_ckan_fetch_datastore_active_uses_datastore_search():
    """datastore_active=true → chiama datastore_search invece di download URL."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "id": "res-123",
                    "url": "http://portal.example.org/api/3/dump.csv",
                    "datastore_active": True,
                },
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/api/3/action/datastore_search"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "fields": [{"id": "nome"}, {"id": "valore"}],
                    "records": [
                        {"nome": "a", "valore": 1},
                        {"nome": "b", "valore": 2},
                    ],
                },
            },
        ),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    payload, origin = source.fetch("https://portal.example.org/api/3", resource_id="res-123")

    text = payload.decode("utf-8")
    assert "nome,valore" in text
    assert "a,1" in text
    assert "b,2" in text
    assert any("datastore_search" in req[1] for req in fake.requests)


def test_ckan_fetch_datastore_fallback_to_url_on_empty_records():
    """datastore_search vuoto → fallback a download URL."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "id": "res-456",
                    "url": "http://portal.example.org/api/3/dump.csv",
                    "datastore_active": True,
                },
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/api/3/action/datastore_search"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {"fields": [], "records": []},
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/api/3/dump.csv"] = HttpResult(
        response=fake_response(200, text="csv-from-url"),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    payload, origin = source.fetch("https://portal.example.org/api/3", resource_id="res-456")
    assert payload == b"csv-from-url"
    assert "dump.csv" in origin


def test_ckan_fetch_resource_no_url_no_datastore_raises():
    """Risorsa senza URL e datastore non attivo → DownloadError."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {"id": "no-url-res", "url": "", "datastore_active": False},
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/api/3/action/package_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {"id": "no-url-res", "resources": []},
            },
        ),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    with pytest.raises(DownloadError, match="no URL|no resources"):
        source.fetch("https://portal.example.org/api/3", resource_id="no-url-res")


def test_ckan_fetch_malformed_json_falls_back_to_package_show():
    """resource_show ritorna JSON non valido → fallback a package_show."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        # Simula risposta 200 con body non JSON
        response=fake_response(200, text="not json at all"),
        err=None,
    )
    fake.responses["https://portal.example.org/api/3/action/package_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "id": "malformed",
                    "resources": [
                        {
                            "id": "res-xyz",
                            "name": "data",
                            "format": "csv",
                            "url": "http://portal.example.org/data.csv",
                        },
                    ],
                },
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/data.csv"] = HttpResult(
        response=fake_response(200, text="a,b\n1,2\n"),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    payload, origin = source.fetch("https://portal.example.org/api/3", dataset_id="malformed")
    assert payload == b"a,b\n1,2\n"


def test_ckan_fetch_package_no_resources_raises():
    """Package senza risorse → DownloadError."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/package_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {"id": "empty-pkg", "resources": []},
            },
        ),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    with pytest.raises(DownloadError, match="no resources"):
        source.fetch("https://portal.example.org/api/3", dataset_id="empty-pkg")


def test_ckan_fetch_prefer_datastore_false_skips_datastore():
    """prefer_datastore=False → download URL anche se datastore e attivo."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/resource_show"] = HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "id": "ds-res",
                    "url": "http://portal.example.org/api/3/dump.csv",
                    "datastore_active": True,
                },
            },
        ),
        err=None,
    )
    fake.responses["https://portal.example.org/api/3/dump.csv"] = HttpResult(
        response=fake_response(200, text="csv-via-url"),
        err=None,
    )

    source = CkanSource()
    source._client = fake

    payload, origin = source.fetch(
        "https://portal.example.org/api/3", resource_id="ds-res", prefer_datastore=False
    )
    assert payload == b"csv-via-url"
    assert "csv-via-url" in origin or "dump.csv" in origin
    assert not any("datastore_search" in req[1] for req in fake.requests)


def _paginated_handler(url, **kwargs):
    """Handler per datastore_search con paginazione."""
    page1 = [{"id": i, "valore": f"record-{i}"} for i in range(3)]
    page2 = [{"id": i + 3, "valore": f"record-{i + 3}"} for i in range(2)]
    total = 5
    params = kwargs.get("params", {})
    offset = int(params.get("offset", 0))
    page = page1 if offset == 0 else page2
    return HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "fields": [{"id": "id"}, {"id": "valore"}],
                    "records": page,
                    "total": total,
                },
            },
        ),
        err=None,
    )


def test_ckan_datastore_search_paginates():
    """_datastore_search pagina quando total > records per chiamata."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/datastore_search"] = _paginated_handler

    source = CkanSource()
    source._client = fake

    csv_bytes = source._datastore_search(
        "res-paginated", "https://portal.example.org/api/3", page_size=3
    )

    text = csv_bytes.decode("utf-8")
    lines = text.strip().splitlines()
    assert lines[0] == "id,valore"
    assert len(lines) - 1 == 5, f"Expected 5 records (header escluso), got {len(lines) - 1}"
    assert lines[1] == "0,record-0"
    assert lines[-1] == "4,record-4"
    ds_calls = [r for r in fake.requests if "datastore_search" in r[1]]
    assert len(ds_calls) == 2, f"Expected 2 API calls, got {len(ds_calls)}"


def _partial_page_handler(url, **kwargs):
    """Handler che simula server che cappa a 2 record per chiamata."""
    params = kwargs.get("params", {})
    offset = int(params.get("offset", 0))
    remaining = 7 - offset
    batch_size = min(2, remaining)
    records = (
        [{"id": offset + i, "valore": f"r-{offset + i}"} for i in range(batch_size)]
        if batch_size > 0
        else []
    )
    return HttpResult(
        response=fake_response(
            200,
            json_data={
                "success": True,
                "result": {
                    "fields": [{"id": "id"}, {"id": "valore"}],
                    "records": records,
                    "total": 7,
                },
            },
        ),
        err=None,
    )


def test_ckan_datastore_search_partial_page():
    """Server cappa a 100 record: offset avanza per records reali."""
    fake = FakeHttpClient()
    fake.responses["https://portal.example.org/api/3/action/datastore_search"] = (
        _partial_page_handler
    )

    source = CkanSource()
    source._client = fake

    csv_bytes = source._datastore_search(
        "res-capped", "https://portal.example.org/api/3", page_size=32000
    )

    text = csv_bytes.decode("utf-8")
    lines = text.strip().splitlines()
    assert lines[0] == "id,valore"
    assert len(lines) - 1 == 7
    assert lines[1] == "0,r-0"
    assert lines[-1] == "6,r-6"
    ds_calls = [r for r in fake.requests if "datastore_search" in r[1]]
    assert len(ds_calls) == 4, f"Expected 4 API calls (capped at 2/page), got {len(ds_calls)}"
