"""Tests per link_extractor — contratto pubblico: extract_data_links, group_links.

pure_unit: extract_data_links, group_links
"""

import pytest
from toolkit.scout.link_extractor import (
    DataLink,
    extract_data_links,
    group_links,
)


class TestExtractDataLinks:
    """pure_unit: estrazione link dati da HTML."""

    @pytest.mark.pure_unit
    def test_basic_formats(self):
        """Estrae CSV, XLSX, JSON, ZIP, ignora PDF e HTML."""
        html = """
            <a href="/data.csv">CSV</a>
            <a href="/data.xlsx">XLSX</a>
            <a href="/data.json">JSON</a>
            <a href="/data.zip">ZIP</a>
            <a href="/about">HTML</a>
            <a href="/report.pdf">PDF</a>
        """
        links = extract_data_links("https://ex.it", html)
        assert len(links) == 4
        assert {lnk.format for lnk in links} == {"CSV", "XLSX", "JSON", "ZIP"}

    @pytest.mark.pure_unit
    def test_skips_anchors_and_protocols(self):
        """Ignora ancore, mailto, tel, javascript."""
        html = """
            <a href="#section">X</a>
            <a href="mailto:a@b.it">X</a>
            <a href="tel:+39">X</a>
            <a href="javascript:void(0)">X</a>
        """
        assert extract_data_links("https://ex.it", html) == []

    @pytest.mark.pure_unit
    def test_resolves_relative_and_title(self):
        """URL relativi risolti, aria-label come title."""
        links = extract_data_links(
            "https://ex.it/dir/",
            '<a href="data.csv" aria-label="Report">x</a>',
        )
        assert links[0].url == "https://ex.it/dir/data.csv"
        assert links[0].title == "Report"

    @pytest.mark.pure_unit
    def test_prefix_and_years(self):
        """Prefisso e anni estratti dal filename."""
        links = extract_data_links(
            "https://ex.it",
            '<a href="REG_tipo_reddito_2025.csv">x</a>',
        )
        assert links[0].prefix == "REG"
        assert links[0].years == [2025]


class TestGroupLinks:
    """pure_unit: raggruppamento link per prefisso."""

    @pytest.mark.pure_unit
    def test_single_group(self):
        """Stesso prefisso → un gruppo con anni e formati."""
        links = [
            DataLink(url="https://ex.it/REG_2024.csv", format="CSV", prefix="REG", years=[2024]),
            DataLink(url="https://ex.it/REG_2025.csv", format="CSV", prefix="REG", years=[2025]),
        ]
        groups = group_links(links)
        assert len(groups) == 1
        assert groups[0].group_id == "REG"
        assert groups[0].count == 2
        assert groups[0].year_range == [2024, 2025]

    @pytest.mark.pure_unit
    def test_multiple_groups(self):
        """Prefissi diversi → gruppi separati."""
        links = [
            DataLink(url="https://ex.it/REG_2024.csv", format="CSV", prefix="REG"),
            DataLink(url="https://ex.it/CLA_2024.csv", format="CSV", prefix="CLA"),
        ]
        groups = group_links(links)
        assert len(groups) == 2
        assert {g.group_id for g in groups} == {"REG", "CLA"}

    @pytest.mark.pure_unit
    def test_no_prefix_becomes_other(self):
        """Link senza prefisso → gruppo other."""
        groups = group_links(
            [
                DataLink(url="https://ex.it/data.csv", format="CSV"),
            ]
        )
        assert groups[0].group_id == "other"

    @pytest.mark.pure_unit
    def test_empty(self):
        """Lista vuota → lista vuota."""
        assert group_links([]) == []


class TestMcpFormatsQueryString:
    """pure_unit: MCP formats legacy usa path, non URL grezzo."""

    @pytest.mark.pure_unit
    def test_formats_from_path(self, monkeypatch):
        """URL con query string → formato senza query."""
        from toolkit.mcp.scout_ops import mcp_html_extract_links

        html = """
            <a href="https://ex.it/data.csv?download=1">CSV</a>
            <a href="https://ex.it/archive.zip?t=123">ZIP</a>
        """

        def mock_fetch(url, **kw):
            return {"html_text": html, "status_code": 200}

        monkeypatch.setattr("toolkit.mcp.scout_ops.fetch_html_body", mock_fetch)
        result = mcp_html_extract_links("https://ex.it/")
        assert result["formats"] == {"csv": 1, "zip": 1}
        assert result["data_links"][0]["format"] == "CSV"
