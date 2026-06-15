"""Test per report_ops: build_dataset_readme e integrazione run full."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from toolkit.cli.inspect.report_ops import (
    build_dataset_readme,
    write_run_report,
)


# ---------------------------------------------------------------------------
# build_dataset_readme — test su output markdown
# ---------------------------------------------------------------------------


@pytest.mark.contract
@pytest.mark.pure_unit
class TestBuildDatasetReadme:
    """Verifica che il README markdown aggregato abbia la struttura attesa."""

    def _make_report(self, year: int, **overrides: object) -> dict:
        """Report sintetico minimale per test."""
        report: dict = {
            "dataset": "test_dataset",
            "config_path": "/path/to/dataset.yml",
            "year": year,
            "status": "SUCCESS",
            "duration_seconds": 2.5,
            "readiness": "ready",
            "readiness_checks": {"total": 5, "ok": 5, "fail": 0},
            "preflight": {
                "config_ok": True,
                "sources_reachable": 1,
                "sources_total": 1,
                "quality_score_avg": 88,
            },
            "layers": {
                "raw": {
                    "status": "SUCCESS",
                    "validation": {"ok": True, "errors": 0, "warnings": 0},
                    "warnings": [],
                    "errors": [],
                    "encoding": "utf-8",
                    "delim": ",",
                    "file_size_bytes": 1024,
                },
                "clean": {
                    "status": "SUCCESS",
                    "validation": {"ok": True, "errors": 0, "warnings": 2},
                    "warnings": ["colonna X rimossa", "colonna Y rinominata"],
                    "errors": [],
                    "rows": 150,
                    "columns": 12,
                    "file_size_bytes": 4096,
                },
                "mart": {
                    "status": "SUCCESS",
                    "validation": {"ok": True, "errors": 0, "warnings": 1},
                    "warnings": ["colonna Z rimossa"],
                    "errors": [],
                    "tables": [{"name": "t1", "rows": 50}, {"name": "t2", "rows": 100}],
                    "total_rows": 150,
                    "file_size_bytes": 2048,
                },
            },
            "support_datasets": [],
        }
        report.update(overrides)
        return report

    def test_ha_titolo_e_dataset(self) -> None:
        """Il README contiene titolo e nome dataset."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "# Run Report: `test`" in md
        assert "/c.yml" in md

    def test_tabella_ha_tutti_gli_anni(self) -> None:
        """La tabella elenca tutti gli anni presenti."""
        reports = [self._make_report(2023), self._make_report(2024)]
        md = build_dataset_readme("test", "/c.yml", reports)
        assert "| 2023 |" in md
        assert "| 2024 |" in md

    def test_tabella_mostra_righe_e_warning(self) -> None:
        """La tabella include righe, warning, dimensione file."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "150 righe" in md
        assert "2w" in md
        assert "1.0KB" in md or "4.0KB" in md or "4KB" in md

    def test_sezione_warning_per_anno(self) -> None:
        """Warning compaiono nella sezione dedicata per anno."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "### Anno 2023" in md
        assert "colonna X rimossa" in md
        assert "colonna Z rimossa" in md

    def test_sezione_readiness(self) -> None:
        """La sezione Review Readiness e' presente."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "Review Readiness" in md
        assert "ready" in md
        assert "5/5" in md

    def test_qualita_nella_tabella(self) -> None:
        """Il quality score compare nella tabella."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "**88**" in md

    def test_nessun_warning_silenziato(self) -> None:
        """Report senza warning non genera sezione warning."""
        r = self._make_report(2023)
        r["layers"]["clean"]["warnings"] = []
        r["layers"]["clean"]["validation"]["warnings"] = 0
        r["layers"]["mart"]["warnings"] = []
        r["layers"]["mart"]["validation"]["warnings"] = 0
        md = build_dataset_readme("test", "/c.yml", [r])
        assert "Warning ed errori" not in md

    def test_readiness_needs_review(self) -> None:
        """Verdetto needs-review ha icona appropriata."""
        r = self._make_report(
            2023, readiness="needs-review", readiness_checks={"total": 5, "ok": 4, "fail": 1}
        )
        md = build_dataset_readme("test", "/c.yml", [r])
        assert "needs-review" in md
        assert "4/5" in md

    def test_readiness_incomplete(self) -> None:
        r = self._make_report(
            2023, readiness="incomplete", readiness_checks={"total": 5, "ok": 2, "fail": 3}
        )
        md = build_dataset_readme("test", "/c.yml", [r])
        assert "incomplete" in md
        assert "2/5" in md

    def test_status_failed(self) -> None:
        r = self._make_report(2023, status="FAILED")
        md = build_dataset_readme("test", "/c.yml", [r], overall_status="failed")
        assert "FAILED" in md
        assert "failed" in md

    def test_quality_null_non_mostra_valore(self) -> None:
        r = self._make_report(2023)
        r["preflight"]["quality_score_avg"] = None
        md = build_dataset_readme("test", "/c.yml", [r])
        # La cella qualita' dovrebbe essere "-"
        lines = [line for line in md.split("\n") if "2023" in line and "|" in line]
        assert any("| - |" in line or "| - " in line for line in lines)

    def test_support_datasets(self) -> None:
        r = self._make_report(
            2023,
            support_datasets=[{"name": "istat-elenco-comuni", "year": 2026, "status": "SUCCESS"}],
        )
        md = build_dataset_readme("test", "/c.yml", [r])
        assert "Support Datasets" in md
        assert "istat-elenco-comuni" in md


# ---------------------------------------------------------------------------
# write_run_report — verifica che il JSON sia valido e abbia campi minimi
# ---------------------------------------------------------------------------


@pytest.mark.contract
class TestWriteRunReport:
    def test_write_e_lettura(self, tmp_path: Path) -> None:
        """Scrive un report e verifica che sia JSON valido con i campi base."""
        report = {
            "dataset": "prova",
            "config_path": "/c.yml",
            "year": 2024,
            "run_id": "test_123",
            "status": "SUCCESS",
            "readiness": "ready",
            "preflight": {"config_ok": True},
            "layers": {},
            "support_datasets": [],
        }
        path = write_run_report(report, tmp_path, "prova", 2024)
        assert path.exists()
        assert path.name == "2024_run_report.json"
        raw = path.read_text(encoding="utf-8")
        loaded = json.loads(raw)
        assert loaded["dataset"] == "prova"
        assert loaded["year"] == 2024
        assert loaded["status"] == "SUCCESS"


# (Lo smoke test di integrazione viene eseguito dalla suite smoke del toolkit
# tramite `run_full` con smoke template local_file_csv. I test unitari sopra
# coprono il contratto del markdown e del JSON.)
