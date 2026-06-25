"""Tests per toolkit.scout.http — fetch_ckan_datastore_schema e discover_ckan_resources."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from toolkit.scout.http import discover_ckan_resources

pytestmark = pytest.mark.contract


def _mock_http(fields: list | None = None, status: int = 200, success: bool = True) -> MagicMock:
    """Crea un HttpClient.mock con risposta DataStore."""
    client = MagicMock()
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = {"success": success, "result": {"fields": fields or [], "total": 0}}
    result = MagicMock()
    result.is_ok = True
    result.response = resp
    client.get.return_value = result
    return client


MOCK_FIELDS = [
    {"id": "nome", "type": "text", "info": {"label": "Nome"}},
    {"id": "valore", "type": "numeric"},
    {"id": "data", "type": "date"},
]


# ---------------------------------------------------------------------------
# contract: discover_ckan_resources — campo datastore_active
# ---------------------------------------------------------------------------


class TestDiscoverCkanResourcesDatastoreActive:
    """contract: discover_ckan_resources include datastore_active."""

    @pytest.mark.parametrize(
        "pkg,expected_active,expected_count",
        [
            (
                {
                    "resources": [
                        {
                            "id": "r1",
                            "name": "t.csv",
                            "format": "CSV",
                            "url": "https://e.t/t.csv",
                            "datastore_active": True,
                        }
                    ]
                },
                True,
                1,
            ),
            (
                {
                    "resources": [
                        {
                            "id": "r1",
                            "name": "t.csv",
                            "format": "CSV",
                            "url": "https://e.t/t.csv",
                            "datastore_active": False,
                        }
                    ]
                },
                False,
                1,
            ),
            (
                {
                    "resources": [
                        {"id": "r1", "name": "t.csv", "format": "CSV", "url": "https://e.t/t.csv"}
                    ]
                },
                False,
                1,
            ),
            (
                {
                    "resources": [
                        {
                            "id": "r1",
                            "name": "t.pdf",
                            "format": "PDF",
                            "url": "",
                            "datastore_active": True,
                        }
                    ]
                },
                None,
                0,
            ),
        ],
    )
    def test_datastore_active_flag(self, pkg, expected_active, expected_count) -> None:
        resources = discover_ckan_resources(pkg)
        assert len(resources) == expected_count
        if expected_count:
            assert resources[0]["datastore_active"] is expected_active

    def test_backward_compat_fields(self) -> None:
        """Campi legacy (id, name, format, url) ancora presenti."""
        pkg = {
            "resources": [
                {
                    "id": "abc",
                    "name": "t.csv",
                    "format": "CSV",
                    "url": "https://e.t/t.csv",
                    "datastore_active": True,
                }
            ]
        }
        r = discover_ckan_resources(pkg)[0]
        assert r == {
            "id": "abc",
            "name": "t.csv",
            "format": "csv",
            "url": "https://e.t/t.csv",
            "datastore_active": True,
        }


# ---------------------------------------------------------------------------
# contract: fetch_ckan_datastore_schema
# ---------------------------------------------------------------------------


class TestFetchCkanDatastoreSchema:
    """contract: fetch_ckan_datastore_schema torna fields o None."""

    def test_returns_fields(self) -> None:
        from toolkit.scout.http import fetch_ckan_datastore_schema

        result = fetch_ckan_datastore_schema(
            "https://example.test", "r1", client=_mock_http(MOCK_FIELDS)
        )
        assert result is not None
        assert [f["id"] for f in result] == ["nome", "valore", "data"]

    def test_filters_id_field(self) -> None:
        from toolkit.scout.http import fetch_ckan_datastore_schema

        result = fetch_ckan_datastore_schema(
            "https://example.test",
            "r1",
            client=_mock_http(MOCK_FIELDS + [{"id": "_id", "type": "int"}]),
        )
        assert result is not None
        assert "_id" not in {f["id"] for f in result}
        assert len(result) == 3

    @pytest.mark.parametrize(
        "client,reason",
        [
            (_mock_http(success=False), "success=False"),
            (_mock_http(status=500), "HTTP 500"),
            (_mock_http([]), "empty fields"),
        ],
    )
    def test_error_returns_none(self, client, reason) -> None:
        from toolkit.scout.http import fetch_ckan_datastore_schema

        result = fetch_ckan_datastore_schema("https://example.test", "r1", client=client)
        assert result is None, f"atteso None per {reason}"

    def test_normalizes_portal_url(self) -> None:
        from toolkit.scout.http import fetch_ckan_datastore_schema

        client = _mock_http(MOCK_FIELDS)
        fetch_ckan_datastore_schema("https://example.test/dataset/foo", "r1", client=client)
        called_url = client.get.call_args[0][0]
        assert called_url.startswith("https://example.test/api/3/action/")
