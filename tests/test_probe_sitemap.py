"""Tests per probe — fetch_sitemap_pages con sitemap index.

pure_unit: fetch_sitemap_pages parsing XML (mock HTTP)
"""

from unittest.mock import patch, MagicMock

import pytest
from toolkit.scout.probe import fetch_sitemap_pages


# ---------------------------------------------------------------------------
# pure_unit — fetch_sitemap_pages
# ---------------------------------------------------------------------------


class TestFetchSitemapPages:
    """pure_unit: parsing sitemap XML."""

    @pytest.mark.pure_unit
    def test_standard_sitemap(self):
        """Sitemap standard <urlset><url><loc>."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://ex.it/page1</loc></url>
  <url><loc>https://ex.it/page2</loc></url>
</urlset>"""
        pages = self._mock_fetch(xml)
        assert pages == ["https://ex.it/page1", "https://ex.it/page2"]

    @pytest.mark.pure_unit
    def test_sitemap_index(self):
        """Sitemap index <sitemapindex><sitemap><loc> con fetch ricorsivo."""
        index_xml = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://ex.it/sub1.xml</loc></sitemap>
  <sitemap><loc>https://ex.it/sub2.xml</loc></sitemap>
</sitemapindex>"""
        sub1_xml = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://ex.it/page1</loc></url>
</urlset>"""
        sub2_xml = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://ex.it/page2</loc></url>
  <url><loc>https://ex.it/page3</loc></url>
</urlset>"""

        pages = self._mock_fetch_index(
            index_xml,
            {
                "https://ex.it/sub1.xml": sub1_xml,
                "https://ex.it/sub2.xml": sub2_xml,
            },
        )
        assert pages == ["https://ex.it/page1", "https://ex.it/page2", "https://ex.it/page3"]

    @pytest.mark.pure_unit
    def test_sitemap_index_no_namespace(self):
        """Sitemap index senza namespace XML."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex>
  <sitemap><loc>https://ex.it/sub.xml</loc></sitemap>
</sitemapindex>"""
        sub_xml = """<?xml version="1.0" encoding="UTF-8"?>
<urlset>
  <url><loc>https://ex.it/page</loc></url>
</urlset>"""
        pages = self._mock_fetch_index(xml, {"https://ex.it/sub.xml": sub_xml})
        assert pages == ["https://ex.it/page"]

    @pytest.mark.pure_unit
    def test_unreachable_sitemap(self):
        """Sitemap non raggiungibile → lista vuota."""
        with patch("toolkit.scout.probe.HttpClient") as MockClient:
            instance = MockClient.return_value
            instance.get.return_value = MagicMock(is_ok=False)
            pages = fetch_sitemap_pages("https://ex.it/sitemap.xml", timeout=5)
            assert pages == []

    @pytest.mark.pure_unit
    def test_malformed_xml(self):
        """XML malformato → lista vuota."""
        with patch("toolkit.scout.probe.HttpClient") as MockClient:
            instance = MockClient.return_value
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.text = "not xml"
            instance.get.return_value = MagicMock(
                is_ok=True, response=mock_resp, err=None, ssl_fallback_used=False
            )
            pages = fetch_sitemap_pages("https://ex.it/sitemap.xml", timeout=5)
            assert pages == []

    # -- helpers --

    def _mock_fetch(self, xml: str) -> list[str]:
        """Mock HttpClient per sitemap standard."""
        with patch("toolkit.scout.probe.HttpClient") as MockClient:
            instance = MockClient.return_value
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.text = xml
            instance.get.return_value = MagicMock(
                is_ok=True, response=mock_resp, err=None, ssl_fallback_used=False
            )
            return fetch_sitemap_pages("https://ex.it/sitemap.xml", timeout=5)

    def _mock_fetch_index(self, index_xml: str, subs: dict[str, str]) -> list[str]:
        """Mock HttpClient per sitemap index con sotto-sitemap."""

        def mock_get(url):
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            if url == "https://ex.it/sitemap.xml":
                mock_resp.text = index_xml
            elif url in subs:
                mock_resp.text = subs[url]
            else:
                mock_resp.text = ""
            return MagicMock(is_ok=True, response=mock_resp, err=None, ssl_fallback_used=False)

        with patch("toolkit.scout.probe.HttpClient") as MockClient:
            instance = MockClient.return_value
            instance.get.side_effect = mock_get
            return fetch_sitemap_pages("https://ex.it/sitemap.xml", timeout=5)
