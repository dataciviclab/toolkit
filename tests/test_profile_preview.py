"""Tests per preview_url: probe + download sniff profile infer."""

from __future__ import annotations


import pytest

from toolkit.profile.preview import (
    _download_preview_chunk,
    _extract_year_values_from_sample,
    preview_url,
)


def _mock_probe(monkeypatch, result: dict | None = None):
    """Mock probe_url_headers per non fare HTTP reale."""
    default = {
        "requested_url": "https://example.test/data.csv",
        "final_url": "https://example.test/data.csv",
        "status_code": 200,
        "content_type": "text/csv",
        "content_disposition": None,
        "method": "head",
        "content_length": "500",
    }
    mock_result = {**default, **(result or {})}

    import toolkit.profile.preview as _preview_mod

    monkeypatch.setattr(
        _preview_mod,
        "probe_url_headers",
        lambda url, **kw: mock_result,
    )


def _mock_download(monkeypatch, content: bytes = b"", status: int = 200):
    """Mock HttpClient.get per non fare HTTP reale."""
    from lab_connectors.http import HttpResult
    from lab_connectors.testing import fake_response

    def fake_get(self, url, **kw):
        return HttpResult(
            response=fake_response(
                status,
                text=content.decode("utf-8", errors="replace"),
                headers={
                    "Content-Type": "text/csv",
                    "Content-Length": str(len(content)),
                },
            ),
            err=None,
        )

    monkeypatch.setattr("lab_connectors.http.HttpClient.get", fake_get)


class TestPreviewUrl:
    @pytest.mark.smoke
    def test_csv_preview_produces_columns(self, monkeypatch):
        """Preview CSV: deve restituire colonne e tipi."""
        csv_content = b"col1,col2,col3\n1,2,3\n4,5,6\n7,8,9\n"
        _mock_probe(monkeypatch, {"status_code": 200, "content_type": "text/csv"})
        _mock_download(monkeypatch, csv_content)

        result = preview_url("https://example.test/data.csv")

        assert result["reachable"] is True
        assert result["resource_format"] == "CSV"
        assert result["columns"] == ["col1", "col2", "col3"]
        assert isinstance(result["col_types"], dict)
        assert set(result["col_types"].keys()) == {"col1", "col2", "col3"}
        assert result["preview_row_count"] == 3
        assert result["enrich_method"] == "csv_preview"

    @pytest.mark.smoke
    def test_csv_preview_reachable_false_on_probe_fail(self, monkeypatch):
        """HEAD fallimento → reachable=False."""
        import toolkit.profile.preview as _preview_mod

        def _fail_probe(url, **kw):
            raise RuntimeError("timeout")

        monkeypatch.setattr(_preview_mod, "probe_url_headers", _fail_probe)

        result = preview_url("https://example.test/timeout.csv")
        assert result["reachable"] is False
        assert result["enrich_method"] == "probe_failed"

    @pytest.mark.smoke
    def test_csv_with_known_params_skips_sniff(self, monkeypatch):
        """known_encoding + known_delim → sniff saltato."""
        csv_content = b"a;b;c\n1;2;3\n"
        _mock_probe(monkeypatch, {"status_code": 200, "content_type": "text/csv"})

        def _fake_get(self, url, **kw):
            from lab_connectors.http import HttpResult
            from lab_connectors.testing import fake_response

            return HttpResult(
                response=fake_response(
                    200,
                    text=csv_content.decode(),
                    headers={"Content-Type": "text/csv", "Content-Length": str(len(csv_content))},
                ),
                err=None,
            )

        monkeypatch.setattr("lab_connectors.http.HttpClient.get", _fake_get)

        result = preview_url(
            "https://example.test/data.csv",
            known_encoding="utf-8",
            known_delim=";",
        )

        assert result["columns"] == ["a", "b", "c"]
        assert result["encoding_suggested"] == "utf-8"
        assert result["delim_suggested"] == ";"
        # sniff saltato → decimal e skip rimangono default
        assert result["skip_suggested"] == 0

    @pytest.mark.contract
    def test_preview_returns_granularity(self, monkeypatch):
        """Preview deve inferire granularità da nomi colonna."""
        csv_content = b"Comune,Popolazione,Anno\nRoma,1000000,2023\nMilano,500000,2023\n"
        _mock_probe(monkeypatch, {"status_code": 200, "content_type": "text/csv"})

        def _fake_get(self, url, **kw):
            from lab_connectors.http import HttpResult
            from lab_connectors.testing import fake_response

            return HttpResult(
                response=fake_response(
                    200,
                    text=csv_content.decode(),
                    headers={"Content-Type": "text/csv", "Content-Length": str(len(csv_content))},
                ),
                err=None,
            )

        monkeypatch.setattr("lab_connectors.http.HttpClient.get", _fake_get)

        result = preview_url("https://example.test/comuni.csv")
        assert result["granularity"] != "non_determinato"
        assert result["granularity"] is not None


class TestExtractYearValues:
    @pytest.mark.contract
    def test_multiple_years_in_column(self):
        sample = [
            {"Anno": 2020, "Regione": "Lombardia", "Valore": 100},
            {"Anno": 2021, "Regione": "Lombardia", "Valore": 110},
            {"Anno": 2022, "Regione": "Lombardia", "Valore": 120},
        ]
        columns = ["Anno", "Regione", "Valore"]
        result = _extract_year_values_from_sample(sample, columns)
        assert result == [2020, 2021, 2022]

    @pytest.mark.contract
    def test_no_year_values(self):
        sample = [{"nome": "Mario", "eta": 30}]
        assert _extract_year_values_from_sample(sample, ["nome", "eta"]) == []

    @pytest.mark.contract
    def test_nan_values_in_sample(self):
        sample = [
            {"Anno": float("nan"), "Regione": "Lombardia", "Valore": 100},
            {"Anno": 2021.0, "Regione": "Lombardia", "Valore": 110},
        ]
        columns = ["Anno", "Regione", "Valore"]
        result = _extract_year_values_from_sample(sample, columns)
        assert result == [2021]


class TestDownloadPreviewChunk:
    @pytest.mark.contract
    def test_download_csv_chunk(self, monkeypatch):
        """Range GET → bytes + file_size."""
        from lab_connectors.http import HttpResult
        from lab_connectors.testing import fake_response

        def fake_get(self, url, **kw):
            return HttpResult(
                response=fake_response(
                    200,
                    text="a,b,c\n1,2,3\n",
                    headers={"Content-Type": "text/csv", "Content-Length": "10"},
                ),
                err=None,
            )

        monkeypatch.setattr("lab_connectors.http.HttpClient.get", fake_get)

        content, file_size = _download_preview_chunk("https://example.test/data.csv", "csv")
        assert content is not None
        assert file_size == 10

    @pytest.mark.contract
    def test_download_4xx_returns_none(self, monkeypatch):
        """Range GET 4xx → None."""
        from lab_connectors.http import HttpResult
        from lab_connectors.testing import fake_response

        def fake_get(self, url, **kw):
            return HttpResult(
                response=fake_response(404, text="not found"),
                err=None,
            )

        monkeypatch.setattr("lab_connectors.http.HttpClient.get", fake_get)

        content, file_size = _download_preview_chunk("https://example.test/missing.csv", "csv")
        assert content is None


pytestmark = pytest.mark.contract
