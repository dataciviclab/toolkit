"""Tests for toolkit/cli/cmd_url_inspect.py

contract: _generate_yaml_scaffold output format (used by CLI scaffold)
pure_unit: _is_html, _is_file_like, _candidate_links, _detect_ckan, _extract_ckan_dataset_id
"""

import pytest

from toolkit.cli.cmd_url_inspect import (
    _candidate_links,
    _detect_ckan,
    _extract_ckan_dataset_id,
    _generate_yaml_scaffold,
    _is_file_like,
    _is_html,
)


# ---------------------------------------------------------------------------
# contract — public scaffold output format
# ---------------------------------------------------------------------------

class TestGenerateYamlScaffold:
    """contract: _generate_yaml_scaffold produces valid YAML with expected fields."""

    @pytest.mark.contract
    def test_basic_probe_result(self) -> None:
        probe_result = {
            "final_url": "https://example.com/data/dataset.csv",
            "requested_url": "https://example.com/data/dataset.csv",
        }
        yaml = _generate_yaml_scaffold(probe_result)
        assert 'name: "dataset_' in yaml  # slug with uuid5 hash suffix
        assert '_source"' in yaml          # source name derived from slug
        assert 'type: "http_file"' in yaml
        assert 'url: "https://example.com/data/dataset.csv"' in yaml
        assert "schema_version: 1" in yaml

    @pytest.mark.contract
    def test_ckan_resources(self) -> None:
        probe_result = {"final_url": "https://example.com/dataset/test"}
        ckan_resources = [
            {
                "id": "res-123",
                "name": "Main Data",
                "format": "csv",
                "url": "https://cdn.example.com/file.csv",
            }
        ]
        yaml = _generate_yaml_scaffold(probe_result, ckan_resources=ckan_resources)
        assert 'type: "ckan"' in yaml
        assert 'resource_id: "res-123"' in yaml
        assert 'portal_url: "https://example.com"' in yaml

    @pytest.mark.contract
    def test_fallback_when_no_resources(self) -> None:
        probe_result = {
            "final_url": "https://example.com/page",
            "requested_url": "https://example.com/page",
        }
        yaml = _generate_yaml_scaffold(probe_result, ckan_resources=[], candidate_links=None)
        assert 'name: "page_' in yaml  # slug with uuid5 hash suffix
        assert '_source"' in yaml
        assert 'type: "http_file"' in yaml

    @pytest.mark.contract
    def test_slug_strips_special_chars(self) -> None:
        # %20 is not URL-decoded; % is stripped to _ giving _20_
        probe_result = {
            "final_url": "https://example.com/data/my-data-file%202023.csv",
            "requested_url": "https://example.com/data/my-data-file%202023.csv",
        }
        yaml = _generate_yaml_scaffold(probe_result)
        assert 'name: "my_data_file_202023_' in yaml  # slug with uuid5 hash suffix


# ---------------------------------------------------------------------------
# pure_unit — non-trivial pure logic (kept, not banale)
# ---------------------------------------------------------------------------

class TestIsHtml:
    """pure_unit: content-type classification for HTML detection."""

    @pytest.mark.pure_unit
    @pytest.mark.parametrize(
        "content_type,expected",
        [
            ("text/html", True),
            ("text/html; charset=utf-8", True),
            ("TEXT/HTML", True),
            ("application/json", False),
            ("application/xml", False),
            ("text/plain", False),
            ("", False),
            (None, False),
        ],
    )
    def test_is_html(self, content_type: str | None, expected: bool) -> None:
        assert _is_html(content_type) is expected


class TestIsFileLike:
    """pure_unit: file-like detection from URL, content-type, content-disposition."""

    @pytest.mark.pure_unit
    @pytest.mark.parametrize(
        "url,content_type,content_disposition,expected",
        [
            # By URL extension
            ("https://example.com/data.csv", None, None, True),
            ("https://example.com/data.xlsx", None, None, True),
            ("https://example.com/data.parquet", None, None, True),
            ("https://example.com/data.geojson", None, None, True),
            # By content-disposition
            ("https://example.com/data", None, "attachment; filename=data.csv", True),
            # By content-type
            ("https://example.com/data", "application/json", None, True),
            ("https://example.com/data", "text/csv", None, True),
            ("https://example.com/data", "application/vnd.ms-excel", None, True),
            (
                "https://example.com/data",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                None,
                True,
            ),
            # HTML is NOT file-like
            ("https://example.com/data", "text/html", None, False),
            ("https://example.com/data", "text/html; charset=utf-8", None, False),
            # No match
            ("https://example.com/page", None, None, False),
        ],
    )
    def test_is_file_like(
        self, url: str, content_type: str | None, content_disposition: str | None, expected: bool
    ) -> None:
        assert _is_file_like(url, content_type, content_disposition) is expected


class TestCandidateLinks:
    """pure_unit: HTML link extraction — relative→absolute, dedup, data-link filter."""

    @pytest.mark.pure_unit
    @pytest.mark.parametrize(
        "html,base_url,expected_count",
        [
            ("<html><body><p>No links here</p></body></html>", "https://example.com", 0),
            ('<html><body><a href="data.csv">CSV</a></body></html>', "https://example.com", 1),
            ('<html><body><a href="/files/data.csv">CSV</a></body></html>', "https://example.com", 1),
            # relative made absolute
            (
                '<html><body><a href="/files/data.csv">CSV</a></body></html>',
                "https://example.com",
                1,
            ),
            # dedup
            (
                '<html><body><a href="data.csv">CSV</a><a href="data.csv">CSV again</a></body></html>',
                "https://example.com",
                1,
            ),
            # filters non-data links
            (
                '<html><body><a href="page.html">Page</a><a href="data.csv">CSV</a></body></html>',
                "https://example.com",
                1,
            ),
            # multiple data links
            (
                '<html><body><a href="a.csv">A</a><a href="b.xlsx">B</a><a href="c.zip">C</a></body></html>',
                "https://example.com",
                3,
            ),
            # ignores non-anchor tags
            (
                '<html><body><img src="data.csv"/><a href="data.csv">CSV</a></body></html>',
                "https://example.com",
                1,
            ),
        ],
    )
    def test_candidate_links(self, html: str, base_url: str, expected_count: int) -> None:
        links = _candidate_links(base_url, html)
        assert len(links) == expected_count

    @pytest.mark.pure_unit
    def test_relative_link_made_absolute(self) -> None:
        html = '<html><body><a href="/files/data.csv">CSV</a></body></html>'
        links = _candidate_links("https://example.com", html)
        assert "https://example.com/files/data.csv" in links


class TestExtractCkanDatasetId:
    """pure_unit: CKAN dataset ID extraction from URL and optional HTML context."""

    @pytest.mark.pure_unit
    @pytest.mark.parametrize(
        "url,html,expected",
        [
            # UUID param
            (
                "https://example.com/dataset?id=12345678-1234-1234-1234-123456789012",
                None,
                "12345678-1234-1234-1234-123456789012",
            ),
            # UUID in path
            (
                "https://example.com/dataset/12345678-1234-1234-1234-123456789012",
                None,
                "12345678-1234-1234-1234-123456789012",
            ),
            # slug in path (non-UUID)
            ("https://example.com/dataset/my-dataset-name", None, "my-dataset-name"),
            # no match
            ("https://example.com/page", None, None),
            # short ID → falls back to path slug when HTML provided
            (
                "https://example.com/dataset/test",
                '<a href="/api/3/action/package_show?id=my-id-123">API</a>',
                "test",
            ),
            # short ID without HTML → None
            (
                "https://example.com/api/3/action/package_show?id=abc123",
                None,
                None,
            ),
        ],
    )
    def test_extract_ckan_dataset_id(
        self, url: str, html: str | None, expected: str | None
    ) -> None:
        result = _extract_ckan_dataset_id(url, html)
        assert result == expected


class TestDetectCkan:
    """pure_unit: CKAN signature detection in raw HTML bytes."""

    @pytest.mark.pure_unit
    @pytest.mark.parametrize(
        "html_bytes,expected",
        [
            (b'<div data-view-embed="...">', True),
            (b"/api/3/action", True),
            (b'<div class="ckan-btn">', True),
            (b'"package_id": "abc"', True),
            (b"<html><body>Generic page</body></html>", False),
            (b"", False),
        ],
    )
    def test_detect_ckan(self, html_bytes: bytes, expected: bool) -> None:
        assert _detect_ckan(html_bytes) is expected
