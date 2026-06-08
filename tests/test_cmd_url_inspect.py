"""Tests per scout — format detection, candidate links, CKAN detection, slugify.

pure_unit: is_html_content, is_file_like, extract_candidate_links, detect_ckan_in_html, extract_ckan_dataset_id, slugify
"""

import pytest

from toolkit.scout.http import (
    detect_ckan_in_html,
    extract_candidate_links,
    extract_ckan_dataset_id,
    is_file_like,
    is_html_content,
)
from toolkit.scaffold.sources import slugify


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
    def testis_html_content(self, content_type: str | None, expected: bool) -> None:
        assert is_html_content(content_type) is expected


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
    def testis_file_like(
        self, url: str, content_type: str | None, content_disposition: str | None, expected: bool
    ) -> None:
        assert is_file_like(url, content_type, content_disposition) is expected


class TestCandidateLinks:
    """pure_unit: HTML link extraction — relative→absolute, dedup, data-link filter."""

    @pytest.mark.pure_unit
    @pytest.mark.parametrize(
        "html,base_url,expected_count",
        [
            ("<html><body><p>No links here</p></body></html>", "https://example.com", 0),
            ('<html><body><a href="data.csv">CSV</a></body></html>', "https://example.com", 1),
            (
                '<html><body><a href="/files/data.csv">CSV</a></body></html>',
                "https://example.com",
                1,
            ),
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
        links = extract_candidate_links(base_url, html)
        assert len(links) == expected_count

    @pytest.mark.pure_unit
    def test_relative_link_made_absolute(self) -> None:
        html = '<html><body><a href="/files/data.csv">CSV</a></body></html>'
        links = extract_candidate_links("https://example.com", html)
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
        result = extract_ckan_dataset_id(url, html)
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
    def testdetect_ckan_in_html(self, html_bytes: bytes, expected: bool) -> None:
        assert detect_ckan_in_html(html_bytes) is expected


# ---------------------------------------------------------------------------
# contract — slugify: stesso URL → stesso slug
# ---------------------------------------------------------------------------


class TestSlugify:
    """contract: slugify e` deterministica e stabile (uuid5)."""

    @pytest.mark.contract
    def test_slugify_deterministic(self) -> None:
        """Stesso URL produce sempre lo stesso slug."""
        url = "https://example.com/data/dataset.csv"
        assert slugify(url) == slugify(url)

    @pytest.mark.contract
    def test_slugify_appends_hash(self) -> None:
        """Lo slug contiene un suffisso hash di 6 caratteri."""
        slug = slugify("https://example.com/data/file.csv")
        # Formato: {stem}_{6hex}  es. file_abc123
        parts = slug.split("_")
        assert len(parts) >= 2, f"slug non ha suffisso hash: {slug}"
        hash_part = parts[-1]
        assert len(hash_part) == 6, f"hash non 6 caratteri: {hash_part}"
        assert all(c in "0123456789abcdef" for c in hash_part), f"hash non esadecimale: {hash_part}"

    @pytest.mark.contract
    def test_slugify_different_urls_different_slugs(self) -> None:
        """URL diversi producono slug diversi (anche se stesso stem)."""
        a = slugify("https://example.com/data/dataset.csv")
        b = slugify("https://other.org/data/dataset.csv")
        assert a != b

    @pytest.mark.contract
    def test_slugify_decodes_percent_encoding(self) -> None:
        """Il percent-encoding nell'URL viene decodificato prima di slugificare.
        %20  spazio, %2F  /, ecc.  In questo caso %202023  ' 2023'  my_data_file_2023_."""
        slug = slugify("https://example.com/data/posti%20per%20stabilimento.csv")
        assert slug.startswith("posti_per_stabilimento_"), f"slug inizio errato: {slug}"

    @pytest.mark.contract
    def test_slugify_preserves_hyphens_as_underscores(self) -> None:
        """Trattini nell'URL diventano underscore."""
        slug = slugify("https://example.com/data/my-data-file.csv")
        assert slug.startswith("my_data_file_"), f"slug inizio errato: {slug}"

    @pytest.mark.contract
    def test_slugify_deterministic_and_unique(self) -> None:
        """slugify e` deterministica e produce hash univoco per URL diversi."""
        a = slugify("https://example.com/data/dataset.csv")
        b = slugify("https://other.org/data/dataset.csv")
        assert len(a.split("_")[-1]) == 6
        assert a.startswith("dataset_")
        assert a != b
