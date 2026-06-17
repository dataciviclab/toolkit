"""Test per il source type script (raw plugin).

Verifica che _fetch_script:
1. Esegua un comando shell e legga l'output
2. Sollevi errore se il comando fallisce
3. Sollevi errore se il file output non esiste
4. Supporti {year} nei placeholder (via _format_args)
5. Rifiuti path traversal nell'output
6. Rifiuti output assoluto fuori base_dir
7. Richieda TOOLKIT_ALLOW_SCRIPT_SOURCE=1
"""

import os
from pathlib import Path

import pytest

from toolkit.core.exceptions import DownloadError
from toolkit.raw._fetch_utils import _fetch_payload, _format_args

pytestmark = pytest.mark.contract


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _allow_script_source(monkeypatch):
    """Abilita il source type script per tutti i test della suite."""
    monkeypatch.setenv("TOOLKIT_ALLOW_SCRIPT_SOURCE", "1")


@pytest.fixture
def script_dir(tmp_path: Path) -> Path:
    d = tmp_path / "candidate"
    d.mkdir()
    return d


def _make_script(script_dir: Path, output_file: Path, body: str) -> Path:
    """Crea uno script shell nel candidate e lo rende eseguibile."""
    script_path = script_dir / "gen.sh"
    script_path.write_text(f"#!/bin/bash\n{body}\n")
    script_path.chmod(0o755)
    return script_path


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_script_echo(script_dir: Path):
    """Comando semplice: scrive un file e lo rilegge."""
    output_file = script_dir / "output.csv"
    script = _make_script(
        script_dir,
        output_file,
        f'echo "anno,valore" > {output_file}\necho "2023,42.5" >> {output_file}',
    )

    payload, origin = _fetch_payload(
        "script",
        {},
        {"command": f"bash {script}", "output": str(output_file)},
        base_dir=script_dir,
    )

    assert payload == b"anno,valore\n2023,42.5\n"
    assert origin == str(output_file)


def test_script_year_placeholder_through_format_args(script_dir: Path):
    """{year} placeholder risolto da _format_args prima di _fetch_payload."""
    output_file = script_dir / "data_2024.csv"
    script = _make_script(
        script_dir,
        output_file,
        f'echo "year,val" > {output_file}\necho "2024,99.9" >> {output_file}',
    )

    # Simula ciò che fa run_raw: _format_args risolve {year} prima del dispatch
    raw_args = {"command": f"bash {script}", "output": str(output_file)}
    formatted = _format_args(raw_args, year=2024)

    payload, origin = _fetch_payload(
        "script",
        {},
        formatted,
        base_dir=script_dir,
    )

    assert b"2024" in payload
    assert origin == str(output_file)


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_script_command_fails(script_dir: Path):
    """Comando con exit code != 0 → DownloadError."""
    with pytest.raises(DownloadError, match="Script failed"):
        _fetch_payload(
            "script",
            {},
            {"command": "exit 1", "output": "nonexistent.csv"},
            base_dir=script_dir,
        )


def test_script_output_missing(script_dir: Path):
    """Comando ok ma file output non generato → DownloadError."""
    with pytest.raises(DownloadError, match="did not produce expected output"):
        _fetch_payload(
            "script",
            {},
            {"command": "echo hello", "output": "missing.csv"},
            base_dir=script_dir,
        )


def test_script_no_command():
    """Nessun comando specificato → DownloadError."""
    with pytest.raises(DownloadError, match="requires a 'command'"):
        _fetch_payload("script", {}, {"output": "out.csv"}, base_dir=Path("/tmp"))


# ---------------------------------------------------------------------------
# Security guardrails
# ---------------------------------------------------------------------------


def test_script_disabled_by_default(script_dir: Path):
    """Senza TOOLKIT_ALLOW_SCRIPT_SOURCE → DownloadError."""
    old = os.environ.pop("TOOLKIT_ALLOW_SCRIPT_SOURCE", None)
    try:
        with pytest.raises(DownloadError, match="disabled by default"):
            _fetch_payload(
                "script",
                {},
                {"command": "echo ok", "output": "out.csv"},
                base_dir=script_dir,
            )
    finally:
        if old is not None:
            os.environ["TOOLKIT_ALLOW_SCRIPT_SOURCE"] = old


def test_script_rejects_absolute_output(script_dir: Path):
    """Output con path assoluto fuori base_dir → DownloadError."""
    with pytest.raises(DownloadError, match="outside candidate base directory"):
        _fetch_payload(
            "script",
            {},
            {"command": "echo ok", "output": "/etc/passwd"},
            base_dir=script_dir,
        )


def test_script_rejects_path_traversal(script_dir: Path):
    """Output con ../ per uscire da base_dir → DownloadError."""
    with pytest.raises(DownloadError, match="outside candidate base directory"):
        _fetch_payload(
            "script",
            {},
            {"command": "echo ok", "output": "../outside.csv"},
            base_dir=script_dir,
        )


def test_script_rejects_prefix_collision(tmp_path: Path):
    """Output con path che inizia come base_dir ma non è dentro → DownloadError.

    Regressione: str.startswith() è bypassabile.
    Esempio: base_dir=/tmp/candidate, output=/tmp/candidate_evil/out.csv
    """
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    # Crea una directory sibling che inizia con lo stesso prefisso
    evil = tmp_path / "candidate_evil"
    evil.mkdir()
    evil_file = evil / "out.csv"

    with pytest.raises(DownloadError, match="outside candidate base directory"):
        _fetch_payload(
            "script",
            {},
            {"command": "echo ok", "output": str(evil_file)},
            base_dir=candidate,
        )
