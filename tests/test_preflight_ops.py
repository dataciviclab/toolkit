"""Test per preflight_ops: run_preflight con fonti mockate.

Nessuna chiamata HTTP reale — tutto mockato.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from toolkit.cli.preflight_ops import run_preflight


@pytest.fixture
def _mock_http(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mocka preview_url e probe_url_headers per evitare HTTP reali."""

    class _FakePreviewResult:
        url = "https://example.org/data.csv"
        status = "success"
        reachable = True
        http_status = 200
        file_size = 1024
        resource_format = "CSV"
        encoding_suggested = "utf-8"
        delim_suggested = ","
        decimal_suggested = "."
        skip_suggested = 0
        columns = ["comune", "anno", "valore"]
        col_types = {"comune": "VARCHAR", "anno": "BIGINT", "valore": "DOUBLE"}
        preview_row_count = 10
        robust_read_suggested = False
        mapping_suggestions = {}
        granularity = "comune"
        year_min = 2020
        year_max = 2025
        quality_score = 85
        quality_structural_score = 85
        quality_semantic_score = 80
        quality_combined_score = 83
        quality_sampled = True
        quality_verdict = "buona"
        quality_flags = []
        quality_ontologies = {}
        quality_note = None

    monkeypatch.setattr(
        "toolkit.profile.preview.preview_url",
        lambda *args, **kwargs: _FakePreviewResult(),
    )

    monkeypatch.setattr(
        "toolkit.scout.http.probe_url_headers",
        lambda *args, **kwargs: {
            "requested_url": args[0],
            "final_url": args[0],
            "status_code": 200,
            "content_type": "text/html",
            "content_disposition": None,
            "content_length": 5000,
            "method": "head",
        },
    )


def _write_test_config(path: Path) -> Path:
    """Scrive dataset.yml con 3 source type diversi e 2 anni."""
    config_dir = path / "test_config"
    config_dir.mkdir(exist_ok=True)
    config_path = config_dir / "dataset.yml"
    config_path.write_text(
        "\n".join(
            [
                f'root: "{(config_dir / "out").as_posix()}"',
                "dataset:",
                '  name: "test_preflight"',
                "  source_id: test_local",
                "  years: [2022, 2023]",
                "raw:",
                "  sources:",
                "    - name: csv_source",
                "      type: http_file",
                "      primary: true",
                "      args:",
                '        url: "https://example.org/data_{year}.csv"',
                "    - name: ckan_portal",
                "      type: ckan",
                "      args:",
                '        portal_url: "https://ckan.example.org"',
                "    - name: local_data",
                "      type: local_file",
                "      args:",
                '        path: "data/local.csv"',
                "clean:",
                '  sql: "sql/clean.sql"',
                "mart:",
                "  tables:",
                '    - name: "mart_test"',
                '      sql: "sql/mart.sql"',
                "validation:",
                "  fail_on_error: false",
            ]
        ),
        encoding="utf-8",
    )
    return config_path


class TestPreflightOps:
    """Test per run_preflight con fonti mockate."""

    @pytest.mark.policy
    def test_preflight_returns_structured_report(self, tmp_path: Path, _mock_http) -> None:
        """Il report contiene config_check, sources, status."""
        config_path = _write_test_config(tmp_path)
        result = run_preflight(str(config_path))

        assert result["config_check"]["ok"] is True
        assert result["dataset"] == "test_preflight"
        assert result["years"] == [2022, 2023]
        assert result["status"] == "passed"

    @pytest.mark.policy
    def test_preflight_reports_http_file_source(self, tmp_path: Path, _mock_http) -> None:
        """http_file CSV: reachable, colonne, quality score."""
        config_path = _write_test_config(tmp_path)
        result = run_preflight(str(config_path))

        csv_sources = [s for s in result["sources"] if s["type"] == "http_file"]
        assert len(csv_sources) == 2  # una per anno

        src = csv_sources[0]
        assert src["reachable"] is True
        assert src["status"] == "success"
        assert src["resource_format"] == "CSV"
        assert src["encoding"] == "utf-8"
        assert src["delim"] == ","
        assert src["columns"] == ["comune", "anno", "valore"]
        assert src["quality_score"] == 85
        assert src["quality_verdict"] == "buona"

    @pytest.mark.policy
    def test_preflight_deduplicates_same_url_across_years(self, tmp_path: Path, _mock_http) -> None:
        """CKAN senza {year} nell'URL deve essere probe-ato una sola volta."""
        config_path = _write_test_config(tmp_path)
        result = run_preflight(str(config_path))

        ckan_sources = [s for s in result["sources"] if s["type"] == "ckan"]
        assert len(ckan_sources) == 2  # due anni

        # Primo anno: probe reale
        assert ckan_sources[0]["status"] == "reachable"

        # Secondo anno: stesso URL → cached
        assert ckan_sources[1]["status"] == "cached"

    @pytest.mark.policy
    def test_preflight_skips_local_file(self, tmp_path: Path, _mock_http) -> None:
        """local_file: saltato senza probe."""
        config_path = _write_test_config(tmp_path)
        result = run_preflight(str(config_path))

        local_sources = [s for s in result["sources"] if s["type"] == "local_file"]
        assert len(local_sources) == 2  # una per anno
        for src in local_sources:
            assert src["reachable"] is True
            assert src["status"] == "skipped"

    @pytest.mark.policy
    def test_preflight_http_file_with_placeholder_in_url(self, tmp_path: Path, _mock_http) -> None:
        """URL con {year} produce URL diversi per anno — nessuna deduplica."""
        config_path = _write_test_config(tmp_path)
        result = run_preflight(str(config_path))

        csv_sources = [s for s in result["sources"] if s["type"] == "http_file"]
        urls = {s["year"]: s["url"] for s in csv_sources}

        # {year} deve essere risolto
        assert urls[2022] == "https://example.org/data_2022.csv"
        assert urls[2023] == "https://example.org/data_2023.csv"

        # URL diversi → entrambi status success (non cached)
        assert all(s["status"] == "success" for s in csv_sources)
