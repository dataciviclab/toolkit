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

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_ha_titolo_e_dataset(self) -> None:
        """Il README contiene titolo e nome dataset."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "# Run Report: `test`" in md
        assert "/c.yml" in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_tabella_ha_tutti_gli_anni(self) -> None:
        """La tabella elenca tutti gli anni presenti."""
        reports = [self._make_report(2023), self._make_report(2024)]
        md = build_dataset_readme("test", "/c.yml", reports)
        assert "| 2023 |" in md
        assert "| 2024 |" in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_tabella_mostra_righe_e_warning(self) -> None:
        """La tabella include righe, warning, dimensione file."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "150 righe" in md
        assert "2w" in md
        assert "1.0KB" in md or "4.0KB" in md or "4KB" in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_sezione_warning_per_anno(self) -> None:
        """Warning compaiono nella sezione dedicata per anno."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "### Anno 2023" in md
        assert "colonna X rimossa" in md
        assert "colonna Z rimossa" in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_sezione_readiness(self) -> None:
        """La sezione Review Readiness e' presente."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "Review Readiness" in md
        assert "ready" in md
        assert "5/5" in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_qualita_nella_tabella(self) -> None:
        """Il quality score compare nella tabella."""
        md = build_dataset_readme("test", "/c.yml", [self._make_report(2023)])
        assert "**88**" in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_nessun_warning_silenziato(self) -> None:
        """Report senza warning non genera sezione warning."""
        r = self._make_report(2023)
        r["layers"]["clean"]["warnings"] = []
        r["layers"]["clean"]["validation"]["warnings"] = 0
        r["layers"]["mart"]["warnings"] = []
        r["layers"]["mart"]["validation"]["warnings"] = 0
        md = build_dataset_readme("test", "/c.yml", [r])
        assert "Warning ed errori" not in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_readiness_needs_review(self) -> None:
        """Verdetto needs-review ha icona appropriata."""
        r = self._make_report(
            2023, readiness="needs-review", readiness_checks={"total": 5, "ok": 4, "fail": 1}
        )
        md = build_dataset_readme("test", "/c.yml", [r])
        assert "needs-review" in md
        assert "4/5" in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_readiness_incomplete(self) -> None:
        r = self._make_report(
            2023, readiness="incomplete", readiness_checks={"total": 5, "ok": 2, "fail": 3}
        )
        md = build_dataset_readme("test", "/c.yml", [r])
        assert "incomplete" in md
        assert "2/5" in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_status_failed(self) -> None:
        r = self._make_report(2023, status="FAILED")
        md = build_dataset_readme("test", "/c.yml", [r], overall_status="failed")
        assert "FAILED" in md
        assert "failed" in md

    @pytest.mark.contract
    @pytest.mark.pure_unit
    def test_quality_null_non_mostra_valore(self) -> None:
        r = self._make_report(2023)
        r["preflight"]["quality_score_avg"] = None
        md = build_dataset_readme("test", "/c.yml", [r])
        lines = [line for line in md.split("\n") if "2023" in line and "|" in line]
        assert any("| - |" in line or "| - " in line for line in lines)

    @pytest.mark.contract
    @pytest.mark.pure_unit
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


class TestWriteRunReport:
    @pytest.mark.contract
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


# ---------------------------------------------------------------------------
# FAILED report — simulazione run fallito
# ---------------------------------------------------------------------------


@pytest.mark.contract
class TestRunFullFailedReport:
    """run_full con run_year che fallisce produce report FAILED e rilancia."""

    def _make_config(self, tmp_path: Path) -> Path:
        """Crea un dataset.yml minimale per test."""
        sql_dir = tmp_path / "sql" / "mart"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (tmp_path / "sql" / "clean.sql").write_text("select 1 as value", encoding="utf-8")
        (sql_dir / "mart_example.sql").write_text("select * from clean_input", encoding="utf-8")
        cfg = tmp_path / "dataset.yml"
        cfg.write_text(
            "\n".join(
                [
                    f'root: "{(tmp_path / "out").as_posix()}"',
                    "dataset:",
                    '  name: "test_fail"',
                    "  years: [2023, 2024]",
                    "raw: {}",
                    "clean:",
                    '  sql: "sql/clean.sql"',
                    "mart:",
                    "  tables:",
                    '    - name: "mart_example"',
                    '      sql: "sql/mart/mart_example.sql"',
                ]
            ),
            encoding="utf-8",
        )
        return cfg

    def test_run_full_fail_produce_report_failed_e_rilancia(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """run_full con run_year che fallisce:
        - scrive report FAILED per l'anno fallito
        - NON scrive report per l'anno successivo (non eseguito)
        - rilancia l'eccezione originale
        """
        from toolkit.cli import cmd_run

        config_path = self._make_config(tmp_path)

        # Mock preflight per saltare check rete
        monkeypatch.setattr(
            "toolkit.cli.preflight_ops.run_preflight",
            lambda *args, **kwargs: {
                "config_check": {"ok": True, "errors": [], "warnings": [], "slug": "test"},
                "sources": [],
                "status": "passed",
            },
        )

        # Mock review_readiness
        import toolkit.cli.inspect.readiness_ops as _readiness_ops

        monkeypatch.setattr(
            _readiness_ops,
            "review_readiness",
            lambda *args, **kwargs: {
                "readiness": "ready",
                "check_count": 5,
                "ok_count": 5,
                "fail_count": 0,
                "layers": {},
            },
        )

        # Mock run_year per fallire al primo anno
        call_count = {"n": 0}

        def _failing_run_year(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("simulated run failure")
            return None  # non dovrebbe arrivare qui

        monkeypatch.setattr(cmd_run, "run_year", _failing_run_year)

        # Chiamata: deve rilanciare l'eccezione
        with pytest.raises(RuntimeError, match="simulated run failure"):
            cmd_run.run_full(
                config=str(config_path),
                years=None,
                smoke=False,
                sample_rows=None,
                sample_bytes=None,
                root=None,
                json_output=False,
                dry_run=False,
            )

        # Verifica: report per anno 2023 esiste (fallito)
        report_dir = tmp_path / "out" / "data" / "_reports" / "test_fail"
        report_2023 = report_dir / "2023_run_report.json"
        assert report_2023.exists(), "Report per anno fallito non trovato"

        # Verifica: report per 2024 NON esiste (mai eseguito)
        report_2024 = report_dir / "2024_run_report.json"
        assert not report_2024.exists(), "Report per anno non eseguito non dovrebbe esistere"

        # run_year chiamato solo 1 volta (secondo anno skippato)
        assert call_count["n"] == 1, "run_year non dovrebbe essere chiamato per il secondo anno"

        # Verifica: README aggregato esiste e contiene lo stato
        readme = report_dir / "README.md"
        assert readme.exists()
        assert "test_fail" in readme.read_text(encoding="utf-8")


class TestFailedReport:
    """build_run_report chiamato con step_results minimale (come dopo eccezione)."""

    @pytest.mark.contract
    def test_failed_report_da_step_results_minimi(self, tmp_path: Path) -> None:
        """Con step_results={'run':'failed'}, build_run_report produce status FAILED
        e non solleva eccezioni (tollerante a layers assenti)."""
        from toolkit.cli.inspect.report_ops import build_run_report

        report = build_run_report(
            config_path="/c.yml",
            year=2024,
            root=tmp_path,
            dataset="fallito",
            step_results={"run": "failed", "validate": "failed"},
            run_mode="full",
        )
        assert report["dataset"] == "fallito"
        assert report["year"] == 2024
        assert report["readiness"] is None  # nessun readiness disponibile
        # I layer ci sono sempre (raw/clean/mart) ma con validation=None
        for lname in ("raw", "clean", "mart"):
            assert lname in report["layers"]
            assert report["layers"][lname]["validation"]["ok"] is None
        assert report["preflight"]["sources_total"] == 0
        assert report["status"] is None  # nessun run record su tmp_path

    @pytest.mark.contract
    def test_failed_report_scritto_su_disco(self, tmp_path: Path) -> None:
        """write_run_report con status FAILED produce JSON valido."""
        report = {
            "dataset": "fallito",
            "config_path": "/c.yml",
            "year": 2024,
            "run_id": None,
            "status": "FAILED",
            "readiness": None,
            "layers": {},
            "support_datasets": [],
        }
        path = write_run_report(report, tmp_path, "fallito", 2024)
        assert path.exists()
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert loaded["status"] == "FAILED"
        assert loaded["layers"] == {}

    @pytest.mark.contract
    def test_readme_con_anno_fallito(self) -> None:
        """Un report FAILED nel README mostra stato FAILED e readiness assente."""
        r = {
            "dataset": "test",
            "config_path": "/c.yml",
            "year": 2024,
            "status": "FAILED",
            "readiness": None,
            "layers": {},
            "support_datasets": [],
        }
        md = build_dataset_readme("test", "/c.yml", [r])
        assert "FAILED" in md
        assert "🔴" in md
