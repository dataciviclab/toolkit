"""Tests per preview_url: probe + download sniff profile infer."""

from __future__ import annotations

import pytest

from toolkit.profile.preview import (
    PreviewResult,
    extract_year_values_from_sample,
    preview_url,
)


def _mock_probe(monkeypatch, result: dict | None = None, error: bool = False):
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

    import toolkit.profile.preview as mod

    if error:

        def _fail(url, **kw):
            raise RuntimeError("timeout")

        monkeypatch.setattr(mod, "probe_url_headers", _fail)
    else:
        mock_result = {**default, **(result or {})}
        monkeypatch.setattr(
            mod,
            "probe_url_headers",
            lambda url, **kw: mock_result,
        )


def _mock_fetch(monkeypatch, content: bytes = b"", error: bool = False, status_code: int = 200):
    """Mock fetch_content per non fare HTTP reale."""
    import toolkit.profile.preview as mod

    if error:

        def _fail(url, **kw):
            raise RuntimeError("fetch failed")

        monkeypatch.setattr(mod, "fetch_content", _fail)
    else:
        monkeypatch.setattr(
            mod,
            "fetch_content",
            lambda url, **kw: {
                "content": content,
                "content_type": "text/csv",
                "status_code": status_code,
                "final_url": url,
                "method": "range",
            },
        )


class TestPreviewUrl:
    @pytest.mark.smoke
    def test_csv_preview_produces_columns(self, monkeypatch):
        """Preview CSV: deve restituire colonne e tipi."""
        csv_content = b"col1,col2,col3\n1,2,3\n4,5,6\n7,8,9\n"
        _mock_probe(monkeypatch)
        _mock_fetch(monkeypatch, csv_content)

        result = preview_url("https://example.test/data.csv")

        assert result.status == "success"
        assert result.reachable is True
        assert result.resource_format == "CSV"
        assert result.columns == ["col1", "col2", "col3"]
        assert isinstance(result.col_types, dict)
        assert set(result.col_types.keys()) == {"col1", "col2", "col3"}
        assert result.preview_row_count == 3

    @pytest.mark.smoke
    def test_probe_failure_returns_error_status(self, monkeypatch):
        """HEAD fallimento → status=probe_failed."""
        _mock_probe(monkeypatch, error=True)

        result = preview_url("https://example.test/timeout.csv")
        assert result.status == "probe_failed"
        assert result.reachable is False

    @pytest.mark.contract
    @pytest.mark.parametrize("get_status", [200, 206])
    def test_head_404_but_get_success_marks_reachable(self, monkeypatch, get_status):
        """HEAD 404 ma Range GET 200/206 → fonte raggiungibile."""
        _mock_probe(monkeypatch, {"status_code": 404, "content_type": "text/csv"})
        _mock_fetch(monkeypatch, b"col1,col2\n1,2\n", status_code=get_status)

        result = preview_url("https://example.test/data.csv")

        assert result.status == "success"
        assert result.reachable is True
        assert result.http_status == get_status

    @pytest.mark.smoke
    def test_unsupported_format_returns_error(self, monkeypatch):
        """Formato non supportato → status=unsupported_format."""
        _mock_probe(monkeypatch, {"content_type": "application/pdf"})

        result = preview_url("https://example.test/report.pdf")
        assert result.status == "unsupported_format"

    @pytest.mark.smoke
    def test_csv_with_known_params_skips_sniff(self, monkeypatch):
        """known_encoding + known_delim → sniff saltato."""
        csv_content = b"a;b;c\n1;2;3\n"
        _mock_probe(monkeypatch, {"content_type": "text/csv"})
        _mock_fetch(monkeypatch, csv_content)

        result = preview_url(
            "https://example.test/data.csv",
            known_encoding="utf-8",
            known_delim=";",
        )

        assert result.columns == ["a", "b", "c"]
        assert result.encoding_suggested == "utf-8"
        assert result.delim_suggested == ";"
        assert result.skip_suggested == 0

    @pytest.mark.contract
    def test_preview_infers_granularity(self, monkeypatch):
        """Preview deve inferire granularità da nomi colonna."""
        csv_content = b"Comune,Popolazione,Anno\nRoma,1000000,2023\nMilano,500000,2023\n"
        _mock_probe(monkeypatch, {"content_type": "text/csv"})

        def _fake_fetch(url, **kw):
            return {
                "content": csv_content,
                "content_type": "text/csv",
                "status_code": 200,
                "final_url": url,
                "method": "range",
            }

        import toolkit.profile.preview as mod

        monkeypatch.setattr(mod, "fetch_content", _fake_fetch)

        result = preview_url("https://example.test/comuni.csv")
        assert result.status == "success"
        assert result.granularity is not None
        assert result.granularity != "non_determinato"

    @pytest.mark.regression
    def test_no_internal_fields_in_result(self, monkeypatch):
        """PreviewResult non deve contenere campi interni."""
        csv_content = b"a,b,c\n1,2,3\n"
        _mock_probe(monkeypatch)
        _mock_fetch(monkeypatch, csv_content)

        result = preview_url("https://example.test/data.csv")
        assert isinstance(result, PreviewResult)
        # Verifica che non ci siano attributi non definiti nel dataclass
        assert not hasattr(result, "_sample_rows")


class TestExtractYearValues:
    @pytest.mark.contract
    def test_multiple_years_in_column(self):
        sample = [
            {"Anno": 2020, "Regione": "Lombardia", "Valore": 100},
            {"Anno": 2021, "Regione": "Lombardia", "Valore": 110},
            {"Anno": 2022, "Regione": "Lombardia", "Valore": 120},
        ]
        columns = ["Anno", "Regione", "Valore"]
        result = extract_year_values_from_sample(sample, columns)
        assert result == [2020, 2021, 2022]

    @pytest.mark.contract
    def test_no_year_values(self):
        sample = [{"nome": "Mario", "eta": 30}]
        assert extract_year_values_from_sample(sample, ["nome", "eta"]) == []

    @pytest.mark.contract
    def test_nan_values_in_sample(self):
        sample = [
            {"Anno": float("nan"), "Regione": "Lombardia", "Valore": 100},
            {"Anno": 2021.0, "Regione": "Lombardia", "Valore": 110},
        ]
        columns = ["Anno", "Regione", "Valore"]
        result = extract_year_values_from_sample(sample, columns)
        assert result == [2021]

    @pytest.mark.regression
    def test_empty_sample(self):
        assert extract_year_values_from_sample([], ["A"]) == []

    @pytest.mark.contract
    def test_single_year_with_hint_column(self):
        """Un solo anno in colonna hint deve matchare."""
        sample = [
            {"anno": 2020, "valore": 100},
        ]
        result = extract_year_values_from_sample(sample, ["anno", "valore"])
        assert result == [2020]

    @pytest.mark.regression
    def test_inf_values_do_not_overflow(self):
        """float('inf') da parsing numeri italiani non deve lanciare OverflowError.

        Regressione: CSV con importi grandi (es. rendiconto gestionale INPS)
        producevano inf nello sniff e int(inf) esplodeva in preflight/run.
        """
        sample = [
            {"Anno": float("inf"), "Valore": 100},
            {"Anno": float("-inf"), "Valore": 110},
            {"Anno": float("nan"), "Valore": 120},
            {"Anno": 2023.0, "Valore": 130},
            {"Anno": 2024.0, "Valore": 140},
        ]
        columns = ["Anno", "Valore"]
        result = extract_year_values_from_sample(sample, columns)
        assert result == [2023, 2024]

    @pytest.mark.regression
    def test_huge_float_values_do_not_overflow(self):
        """Float oltre il range int non devono rompere l'estrazione anni."""
        sample = [
            {"Anno": 1e300, "Valore": 100},
            {"Anno": float(2**63), "Valore": 110},
            {"Anno": 2020.0, "Valore": 120},
            {"Anno": 2021.0, "Valore": 130},
        ]
        columns = ["Anno", "Valore"]
        result = extract_year_values_from_sample(sample, columns)
        assert result == [2020, 2021]


class TestPreviewResultDataclass:
    @pytest.mark.contract
    def test_default_values(self):
        """PreviewResult con default deve avere stato probe_failed."""
        r = PreviewResult(url="https://test.test/", status="probe_failed")
        assert r.url == "https://test.test/"
        assert r.status == "probe_failed"
        assert r.reachable is False
        assert r.columns is None
        assert r.granularity == "non_determinato"
        assert r.skip_suggested == 0

    @pytest.mark.contract
    def test_success_has_columns(self):
        """Success deve poter avere colonne."""
        r = PreviewResult(
            url="https://test.test/data.csv",
            status="success",
            reachable=True,
            columns=["a", "b"],
            col_types={"a": "BIGINT", "b": "VARCHAR"},
        )
        assert len(r.columns) == 2
        assert r.col_types["a"] == "BIGINT"


pytestmark = pytest.mark.contract
