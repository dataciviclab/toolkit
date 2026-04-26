"""Tests for raw/run.py utilities."""

from __future__ import annotations

from toolkit.raw.run import _format_args


class TestFormatArgs:
    def test_format_args_simple_year_substitution(self) -> None:
        """Standard {year} substitution works as before."""
        args = {"url": "https://example.com/data{year}.csv"}
        result = _format_args(args, 2023)
        assert result["url"] == "https://example.com/data2023.csv"

    def test_format_args_no_url_suffix_by_year(self) -> None:
        """Without url_suffix_by_year, output is unchanged."""
        args = {"url": "https://example.com/data{year}.csv", "other": "value"}
        result = _format_args(args, 2023)
        assert result["url"] == "https://example.com/data2023.csv"
        assert result["other"] == "value"

    def test_format_args_url_suffix_by_year(self) -> None:
        """url_suffix_by_year appends the correct suffix for the year."""
        args = {
            "url": "https://www.aifa.gov.it/documents/20142/847578/dati{year}",
            "url_suffix_by_year": {
                2018: "_23.09.2020.csv",
                2019: "_23.09.2020.csv",
                2020: "_22.10.2021.csv",
                2024: "_04.12.2025.csv",
            },
        }
        assert _format_args(args, 2018)["url"] == "https://www.aifa.gov.it/documents/20142/847578/dati2018_23.09.2020.csv"
        assert _format_args(args, 2019)["url"] == "https://www.aifa.gov.it/documents/20142/847578/dati2019_23.09.2020.csv"
        assert _format_args(args, 2020)["url"] == "https://www.aifa.gov.it/documents/20142/847578/dati2020_22.10.2021.csv"
        assert _format_args(args, 2024)["url"] == "https://www.aifa.gov.it/documents/20142/847578/dati2024_04.12.2025.csv"

    def test_format_args_url_suffix_removed_from_output(self) -> None:
        """url_suffix_by_year must not appear in formatted output dict."""
        args = {
            "url": "https://example.com/data{year}",
            "url_suffix_by_year": {2023: "_suffix.csv"},
        }
        result = _format_args(args, 2023)
        assert "url_suffix_by_year" not in result, "url_suffix_by_year is internal config, must not leak into output"

    def test_format_args_url_suffix_year_not_in_map(self) -> None:
        """Year not in url_suffix_by_year map appends empty string."""
        args = {
            "url": "https://example.com/data{year}",
            "url_suffix_by_year": {2020: "_v2.csv"},
        }
        result = _format_args(args, 2023)
        assert result["url"] == "https://example.com/data2023"

    def test_format_args_url_suffix_non_string_value(self) -> None:
        """Non-string suffix value is ignored."""
        args = {
            "url": "https://example.com/data{year}",
            "url_suffix_by_year": {2023: 123},  # type: ignore
        }
        result = _format_args(args, 2023)
        assert result["url"] == "https://example.com/data2023"

    def test_format_args_url_suffix_non_dict(self) -> None:
        """Non-dict url_suffix_by_year is ignored."""
        args = {
            "url": "https://example.com/data{year}",
            "url_suffix_by_year": "not_a_dict",  # type: ignore
        }
        result = _format_args(args, 2023)
        assert result["url"] == "https://example.com/data2023"

    def test_format_args_no_url_key(self) -> None:
        """Without 'url' key, url_suffix_by_year is ignored."""
        args = {
            "other": "value",
            "url_suffix_by_year": {2023: "_suffix.csv"},
        }
        result = _format_args(args, 2023)
        assert result["other"] == "value"
        assert "url" not in result

    def test_format_args_sparql_query_with_braces_no_year(self) -> None:
        """SPARQL query containing {} but no {year} must not be formatted.

        Regression test for issue #186: Python .format() treats {s} as a placeholder
        and raises KeyError. Only strings containing {year} should be formatted.
        """
        sparql_query = "SELECT ?s WHERE { ?s ?p ?o }"
        args = {"query": sparql_query}
        result = _format_args(args, 2024)
        assert result["query"] == sparql_query
