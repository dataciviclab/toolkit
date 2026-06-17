"""Test per il source type script (raw plugin).

Verifica che _fetch_script:
1. Esegua un comando shell e legga l'output
2. Sollevi errore se il comando fallisce
3. Sollevi errore se il file output non esiste
4. Supporti {year} nei placeholder
"""

from pathlib import Path

import pytest

from toolkit.core.exceptions import DownloadError
from toolkit.raw._fetch_utils import _fetch_payload

pytestmark = pytest.mark.contract


def test_script_echo(tmp_path: Path):
    """Comando semplice: scrive un file e lo rilegge."""
    script_dir = tmp_path / "candidate"
    script_dir.mkdir()
    output_file = script_dir / "output.csv"

    # Script che genera un CSV
    script_content = f"""#!/bin/bash
echo "anno,valore" > {output_file}
echo "2023,42.5" >> {output_file}
"""
    script_path = script_dir / "gen.sh"
    script_path.write_text(script_content)
    script_path.chmod(0o755)

    payload, origin = _fetch_payload(
        "script",
        {},
        {
            "command": f"bash {script_path}",
            "output": str(output_file),
        },
        base_dir=script_dir,
    )

    assert payload == b"anno,valore\n2023,42.5\n"
    assert origin == str(output_file)


def test_script_with_year_placeholder(tmp_path: Path):
    """Placeholder {year} risolto correttamente."""
    script_dir = tmp_path / "candidate"
    script_dir.mkdir()

    output_file = script_dir / "data_2024.csv"
    script_content = f"""#!/bin/bash
echo "year,val" > {output_file}
echo "2024,99.9" >> {output_file}
"""
    script_path = script_dir / "gen.sh"
    script_path.write_text(script_content)
    script_path.chmod(0o755)

    payload, origin = _fetch_payload(
        "script",
        {},
        {
            "command": f"bash {script_path}",
            "output": str(output_file),
        },
        base_dir=script_dir,
    )

    assert b"2024" in payload
    assert origin == str(output_file)


def test_script_command_fails(tmp_path: Path):
    """Comando con exit code != 0 → DownloadError."""
    script_dir = tmp_path / "candidate"
    script_dir.mkdir()

    with pytest.raises(DownloadError, match="Script failed"):
        _fetch_payload(
            "script",
            {},
            {
                "command": "exit 1",
                "output": "nonexistent.csv",
            },
            base_dir=script_dir,
        )


def test_script_output_missing(tmp_path: Path):
    """Comando ok ma file output non generato → DownloadError."""
    script_dir = tmp_path / "candidate"
    script_dir.mkdir()

    with pytest.raises(DownloadError, match="did not produce expected output"):
        _fetch_payload(
            "script",
            {},
            {
                "command": "echo hello",
                "output": "missing.csv",
            },
            base_dir=script_dir,
        )


def test_script_no_command():
    """Nessun comando specificato → DownloadError."""
    with pytest.raises(DownloadError, match="requires a 'command'"):
        _fetch_payload("script", {}, {"output": "out.csv"}, base_dir=Path("/tmp"))
