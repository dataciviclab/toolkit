"""Cache dei support `external`: URL GCS → file locale.

Contratto: con la cache attiva i payload puntano a file locali; con
`cache: false` o `DCL_EXTERNAL_CACHE=off` restano URL; un errore di cache non
rompe la pipeline (si legge remoto).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

pytest.importorskip(
    "lab_connectors.gcs.fetch_once",
    reason="richiede lab-connectors con fetch_once (PR #107)",
)

from toolkit.core.support import resolve_support_payloads  # noqa: E402

pytestmark = pytest.mark.contract

URL = "https://storage.googleapis.com/dataciviclab-clean/ds/{year}/ds_{year}_clean.parquet"


class _Spy:
    def __init__(self) -> None:
        self.infos: list[tuple[str, tuple[Any, ...]]] = []
        self.warnings: list[tuple[str, tuple[Any, ...]]] = []

    def info(self, msg: str, *args: Any) -> None:
        self.infos.append((msg, args))

    def warning(self, msg: str, *args: Any) -> None:
        self.warnings.append((msg, args))


def _entry(**over: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "name": "rna",
        "type": "external",
        "uri": URL,
        "years": [2025, 2026],
    }
    entry.update(over)
    return entry


def _fake_fetch(calls: list[str], *, fail: bool = False) -> Any:
    class _Res:
        def __init__(self, path: Path, cached: bool = False) -> None:
            self.path = path
            self.cached = cached
            self.bytes = 0 if cached else 1_000
            self.generation = "g"

    def fetch(url: str, cache_dir: Path, **kw: Any) -> _Res:
        calls.append(url)
        if fail:
            raise OSError("rete giù")
        return _Res(Path(cache_dir) / f"{len(calls)}.parquet", cached=len(calls) % 2 == 0)

    return fetch


def test_url_diventano_path_locali(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr("lab_connectors.gcs.fetch_once.fetch_once", _fake_fetch(calls))
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("DCL_EXTERNAL_CACHE", str(tmp_path / "cache"))

    payloads = resolve_support_payloads(
        [_entry()], require_exists=False, root=tmp_path, localize=True
    )

    payload = payloads[0]
    assert len(calls) == 2  # un fetch per anno, non per citazione
    assert str(tmp_path / "cache") in payload["outputs"][0]
    assert payload["clean"] and str(tmp_path / "cache") in payload["clean"][0]
    assert all(str(tmp_path / "cache") in y["path"] for y in payload["years_resolved"])


def test_default_non_localizza(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Regressione: dry-run/validazione/inspect non devono scaricare nulla.

    `resolve_support_payloads` è chiamato anche da sql_validation, inspect e
    path resolver: senza `localize=True` (solo runtime) restano URL remoti.
    """
    calls: list[str] = []
    monkeypatch.setattr("lab_connectors.gcs.fetch_once.fetch_once", _fake_fetch(calls))
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("DCL_EXTERNAL_CACHE", str(tmp_path / "cache"))

    payload = resolve_support_payloads([_entry()], require_exists=False, root=tmp_path)[0]

    assert calls == []
    assert payload["outputs"][0].startswith("https://")


def test_smoke_non_localizza(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`smoke=True` vince su `localize=True`: nessun download in smoke run."""
    calls: list[str] = []
    monkeypatch.setattr("lab_connectors.gcs.fetch_once.fetch_once", _fake_fetch(calls))
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("DCL_EXTERNAL_CACHE", str(tmp_path / "cache"))

    payload = resolve_support_payloads(
        [_entry()], require_exists=False, root=tmp_path, smoke=True, localize=True
    )[0]

    assert calls == []
    assert payload["outputs"][0].startswith("https://")


def test_load_clean_sql_non_localizza_di_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regressione: `load_clean_sql` è usato da sql_validation (dry-run).

    Il default deve essere "nessun download": solo `run_clean` passa
    `localize=True`. Se questo test fallisce, il dry-run scarica i support.
    """
    from toolkit.clean.run import load_clean_sql

    calls: list[str] = []
    monkeypatch.setattr("lab_connectors.gcs.fetch_once.fetch_once", _fake_fetch(calls))
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("DCL_EXTERNAL_CACHE", str(tmp_path / "cache"))
    sql = tmp_path / "clean.sql"
    sql.write_text("SELECT 1 FROM read_parquet({support.rna.clean})")

    load_clean_sql(
        {"sql": str(sql)},
        dataset="ds",
        year=2026,
        root=tmp_path,
        base_dir=tmp_path,
        support_cfg=[_entry()],
    )

    assert calls == [], "dry-run/validazione non deve scaricare i support external"


def test_cache_false_lascia_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr("lab_connectors.gcs.fetch_once.fetch_once", _fake_fetch(calls))
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("DCL_EXTERNAL_CACHE", str(tmp_path / "cache"))

    payload = resolve_support_payloads([_entry(cache=False)], require_exists=False, root=tmp_path)[
        0
    ]

    assert calls == []
    assert payload["outputs"][0].startswith("https://storage.googleapis.com/")


def test_env_off_lascia_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr("lab_connectors.gcs.fetch_once.fetch_once", _fake_fetch(calls))
    monkeypatch.setenv("DCL_EXTERNAL_CACHE", "off")

    payload = resolve_support_payloads(
        [_entry()], require_exists=False, root=tmp_path, localize=True
    )[0]

    assert calls == []
    assert payload["outputs"][0].startswith("https://")


def test_errore_non_rompe_e_lascia_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _Spy()
    monkeypatch.setattr("lab_connectors.gcs.fetch_once.fetch_once", _fake_fetch([], fail=True))
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("DCL_EXTERNAL_CACHE", str(tmp_path / "cache"))

    payload = resolve_support_payloads(
        [_entry()], require_exists=False, root=tmp_path, logger=spy, localize=True
    )[0]

    assert payload["outputs"][0].startswith("https://storage.googleapis.com/")
    assert spy.warnings  # warning esplicito, mai silenzioso


def test_url_non_gcs_intatto(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr("lab_connectors.gcs.fetch_once.fetch_once", _fake_fetch(calls))
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("DCL_EXTERNAL_CACHE", str(tmp_path / "cache"))
    raw = "https://raw.githubusercontent.com/dataciviclab/italia-corpus/main/x.parquet"

    payload = resolve_support_payloads([_entry(uri=raw)], require_exists=False, root=tmp_path)[0]

    assert calls == []
    assert payload["outputs"] == [raw]


def test_log_hit_e_miss(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _Spy()
    monkeypatch.setattr("lab_connectors.gcs.fetch_once.fetch_once", _fake_fetch([]))
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.setenv("DCL_EXTERNAL_CACHE", str(tmp_path / "cache"))

    resolve_support_payloads(
        [_entry()], require_exists=False, root=tmp_path, logger=spy, localize=True
    )

    assert spy.infos, "la cache deve dichiarare hit/miss nei log del run"
