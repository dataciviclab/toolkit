"""Tests for toolkit/cli/cmd_url_inspect.py — pure functions."""

from toolkit.cli.cmd_url_inspect import (
    _candidate_links,
    _detect_ckan,
    _extract_ckan_dataset_id,
    _generate_yaml_scaffold,
    _is_file_like,
    _is_html,
)


class TestIsHtml:
    def test_none_returns_false(self) -> None:
        assert _is_html(None) is False

    def test_empty_string_returns_false(self) -> None:
        assert _is_html("") is False

    def test_text_html_returns_true(self) -> None:
        assert _is_html("text/html") is True

    def test_text_html_utf8_returns_true(self) -> None:
        assert _is_html("text/html; charset=utf-8") is True

    def test_uppercase_returns_true(self) -> None:
        assert _is_html("TEXT/HTML") is True

    def test_application_json_returns_false(self) -> None:
        assert _is_html("application/json") is False

    def test_application_xml_returns_false(self) -> None:
        assert _is_html("application/xml") is False

    def test_text_plain_returns_false(self) -> None:
        assert _is_html("text/plain") is False


class TestIsFileLike:
    def test_csv_in_url_returns_true(self) -> None:
        assert _is_file_like("https://example.com/data.csv", None, None) is True

    def test_xlsx_in_url_returns_true(self) -> None:
        assert _is_file_like("https://example.com/data.xlsx", None, None) is True

    def test_parquet_in_url_returns_true(self) -> None:
        assert _is_file_like("https://example.com/data.parquet", None, None) is True

    def test_geojson_in_url_returns_true(self) -> None:
        assert _is_file_like("https://example.com/data.geojson", None, None) is True

    def test_attachment_disposition_returns_true(self) -> None:
        assert _is_file_like("https://example.com/data", None, "attachment; filename=data.csv") is True

    def test_json_content_type_is_file_like(self) -> None:
        # json is in the token list, so non-HTML content-type with "json" is file-like
        assert _is_file_like("https://example.com/data", "application/json", None) is True

    def test_text_csv_is_file_like(self) -> None:
        assert _is_file_like("https://example.com/data", "text/csv", None) is True

    def test_html_content_type_returns_false(self) -> None:
        assert _is_file_like("https://example.com/data", "text/html", None) is False

    def test_html_in_content_type_returns_false(self) -> None:
        assert _is_file_like("https://example.com/data", "text/html; charset=utf-8", None) is False

    def test_excel_content_type_returns_true(self) -> None:
        assert _is_file_like("https://example.com/data", "application/vnd.ms-excel", None) is True

    def test_spreadsheetml_content_type_returns_true(self) -> None:
        assert _is_file_like("https://example.com/data", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", None) is True

    def test_no_match_returns_false(self) -> None:
        assert _is_file_like("https://example.com/page", None, None) is False


class TestCandidateLinks:
    def test_no_links(self) -> None:
        html = "<html><body><p>No links here</p></body></html>"
        assert _candidate_links("https://example.com", html) == []

    def test_csv_link_found(self) -> None:
        html = '<html><body><a href="data.csv">CSV</a></body></html>'
        links = _candidate_links("https://example.com", html)
        assert "https://example.com/data.csv" in links

    def test_relative_link_made_absolute(self) -> None:
        html = '<html><body><a href="/files/data.csv">CSV</a></body></html>'
        links = _candidate_links("https://example.com", html)
        assert "https://example.com/files/data.csv" in links

    def test_deduplicates_links(self) -> None:
        html = '<html><body><a href="data.csv">CSV</a><a href="data.csv">CSV again</a></body></html>'
        links = _candidate_links("https://example.com", html)
        assert links.count("https://example.com/data.csv") == 1

    def test_filters_non_data_links(self) -> None:
        html = '<html><body><a href="page.html">Page</a><a href="data.csv">CSV</a></body></html>'
        links = _candidate_links("https://example.com", html)
        assert "https://example.com/page.html" not in links
        assert "https://example.com/data.csv" in links

    def test_multiple_data_links(self) -> None:
        html = '<html><body><a href="a.csv">A</a><a href="b.xlsx">B</a><a href="c.zip">C</a></body></html>'
        links = _candidate_links("https://example.com", html)
        assert len(links) == 3

    def test_ignores_non_anchor_tags(self) -> None:
        html = '<html><body><img src="data.csv"/><a href="data.csv">CSV</a></body></html>'
        links = _candidate_links("https://example.com", html)
        assert len(links) == 1


class TestExtractCkanDatasetId:
    def test_uuid_id_param(self) -> None:
        url = "https://example.com/dataset?id=12345678-1234-1234-1234-123456789012"
        assert _extract_ckan_dataset_id(url) == "12345678-1234-1234-1234-123456789012"

    def test_dataset_path_with_uuid(self) -> None:
        url = "https://example.com/dataset/12345678-1234-1234-1234-123456789012"
        result = _extract_ckan_dataset_id(url)
        assert result == "12345678-1234-1234-1234-123456789012"

    def test_dataset_path_with_slug(self) -> None:
        url = "https://example.com/dataset/my-dataset-name"
        result = _extract_ckan_dataset_id(url)
        assert result == "my-dataset-name"

    def test_dataset_path_with_uuid_as_id(self) -> None:
        # ID must be 36+ chars (UUID format) to match _CKAN_ID_PARAM_RE
        url = "https://example.com/dataset?id=12345678-1234-1234-1234-123456789012"
        assert _extract_ckan_dataset_id(url) == "12345678-1234-1234-1234-123456789012"

    def test_short_id_no_html_returns_none(self) -> None:
        # short IDs don't satisfy {36,} UUID regex and URL has no /dataset/ path,
        # and no html_text is provided -> returns None
        url = "https://example.com/api/3/action/package_show?id=abc123"
        result = _extract_ckan_dataset_id(url)
        assert result is None

    def test_no_match_returns_none(self) -> None:
        url = "https://example.com/page"
        assert _extract_ckan_dataset_id(url) is None

    def test_html_text_api_match(self) -> None:
        # _CKAN_ID_PARAM_RE requires 36+ char UUID; short IDs like "my-id-123"
        # don't match, so the URL slug "test" is returned via _CKAN_DATASET_PATH_RE
        url = "https://example.com/dataset/test"
        html = '<a href="/api/3/action/package_show?id=my-id-123">API</a>'
        result = _extract_ckan_dataset_id(url, html)
        # short ?id= value doesn't satisfy {36,} -> falls back to path-based slug
        assert result == "test"


class TestDetectCkan:
    def test_data_view_embed_sig(self) -> None:
        assert _detect_ckan(b'<div data-view-embed="...">') is True

    def test_api_action_sig(self) -> None:
        assert _detect_ckan(b'/api/3/action') is True

    def test_ckan_class_sig(self) -> None:
        assert _detect_ckan(b'<div class="ckan-btn">') is True

    def test_package_id_sig(self) -> None:
        assert _detect_ckan(b'"package_id": "abc"') is True

    def test_no_sig_returns_false(self) -> None:
        assert _detect_ckan(b'<html><body>Generic page</body></html>') is False

    def test_empty_bytes_returns_false(self) -> None:
        assert _detect_ckan(b'') is False


class TestGenerateYamlScaffold:
    def test_basic_probe_result(self) -> None:
        probe_result = {
            "final_url": "https://example.com/data/dataset.csv",
            "requested_url": "https://example.com/data/dataset.csv",
        }
        yaml = _generate_yaml_scaffold(probe_result)
        assert 'name: "dataset"' in yaml
        assert 'type: "http_file"' in yaml
        assert 'url: "https://example.com/data/dataset.csv"' in yaml
        assert "schema_version: 1" in yaml

    def test_slug_strips_special_chars(self) -> None:
        # %20 is not URL-decoded; % is stripped to _ giving _20_
        probe_result = {
            "final_url": "https://example.com/data/my-data-file%202023.csv",
            "requested_url": "https://example.com/data/my-data-file%202023.csv",
        }
        yaml = _generate_yaml_scaffold(probe_result)
        assert 'name: "my_data_file_202023"' in yaml

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

    def test_fallback_when_no_resources(self) -> None:
        probe_result = {
            "final_url": "https://example.com/page",
            "requested_url": "https://example.com/page",
        }
        yaml = _generate_yaml_scaffold(probe_result, ckan_resources=[], candidate_links=None)
        assert 'name: "page_source"' in yaml
        assert 'type: "http_file"' in yaml
