"""Il CLI dichiara la propria identità per i usage log GCS.

Senza `DCL_CLIENT_NAME`, tutte le letture del Lab risultano
`duckdb/vX python/3.12`: build, dashboard, MCP e pipeline diventano
indistinguibili nei log (ed è già costato un'attribuzione sbagliata).
"""

from __future__ import annotations

import importlib
import os

import pytest

# Contratto verso i usage log GCS: se il nome sparisce, l'attribuzione
# dell'egress torna impossibile.
pytestmark = pytest.mark.contract


def test_cli_imposta_client_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DCL_CLIENT_NAME", raising=False)
    import toolkit.cli.app as cli

    try:
        importlib.reload(cli)
        value = os.environ.get("DCL_CLIENT_NAME", "")
        assert value.startswith("dcl-toolkit/")
    finally:
        # reload esegue `setdefault`: senza cleanup la variabile resterebbe
        # nel processo e influenzerebbe i test successivi.
        os.environ.pop("DCL_CLIENT_NAME", None)


def test_env_esistente_non_viene_sovrascritta(monkeypatch: pytest.MonkeyPatch) -> None:
    """Un override esplicito (es. dcl-toolkit/debug) deve vincere."""
    monkeypatch.setenv("DCL_CLIENT_NAME", "custom/9.9")
    import toolkit.cli.app as cli

    try:
        importlib.reload(cli)
        assert os.environ["DCL_CLIENT_NAME"] == "custom/9.9"
    finally:
        os.environ.pop("DCL_CLIENT_NAME", None)
