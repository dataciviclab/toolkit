"""Test: contratto di pipeline (toolkit/contracts/pipeline.py).

Protegge la struttura pubblica del contratto: chiavi stabili,
valori attesi, regole di policy (es. warning normalize_italian_number).

Le macro sono AUTO-GENERATE da macros.sql — il test
"macros_auto_generated" verifica che la generazione funzioni (i test
sulla macro_reader stessa sono in test_macro_reader.py).
"""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.contract


def test_contracts_structure() -> None:
    """CONTRACTS ha struttura stabile con tutte le sezioni obbligatorie."""
    from toolkit.contracts.pipeline import CONTRACTS

    assert isinstance(CONTRACTS, dict)
    assert CONTRACTS["version"] == "1"

    for key in ("pipeline", "raw", "clean", "mart", "cli", "config", "constants", "tldr"):
        assert key in CONTRACTS, f"Chiave '{key}' mancante in CONTRACTS"

    assert isinstance(CONTRACTS["tldr"], str)
    assert "http_file" in CONTRACTS["tldr"] or "raw" in CONTRACTS["tldr"]
    assert "raw_input" in CONTRACTS["tldr"]
    assert "clean_input" in CONTRACTS["tldr"]
    assert "toolkit contract" in CONTRACTS["tldr"]

    # CLI commands list
    assert isinstance(CONTRACTS["cli"], list)
    assert len(CONTRACTS["cli"]) >= 3
    assert any("run all" in c["command"] for c in CONTRACTS["cli"])

    # Config quick reference
    assert "required_top_level_fields" in CONTRACTS["config"]
    assert "minimal_example" in CONTRACTS["config"]


def test_contracts_view_names_from_constants() -> None:
    """I view name nel contratto corrispondono alle costanti core."""
    from toolkit.contracts.pipeline import CONTRACTS
    from toolkit.core.constants import RAW_INPUT_VIEW, CLEAN_INPUT_VIEW, YEAR_PLACEHOLDER

    assert CONTRACTS["clean"]["sql_source"]["view"] == RAW_INPUT_VIEW
    assert CONTRACTS["mart"]["sql_source"]["view"] == CLEAN_INPUT_VIEW
    assert CONTRACTS["clean"]["year_placeholder"]["syntax"] == YEAR_PLACEHOLDER
    assert CONTRACTS["constants"]["RAW_INPUT_VIEW"] == RAW_INPUT_VIEW
    assert CONTRACTS["constants"]["CLEAN_INPUT_VIEW"] == CLEAN_INPUT_VIEW


def test_contracts_normalize_italian_warning_policy() -> None:
    """La macro normalize_italian_number ha un warning quando decimal=','.

    Questa e' una regola di policy non ovvia — va protetta da test.
    L'annotazione @contract warning: in macros.sql la produce.
    """
    from toolkit.contracts.pipeline import CONTRACTS

    macros = CONTRACTS["clean"]["macros"]
    italian_macro = [m for m in macros if m["name"] == "normalize_italian_number"]
    assert len(italian_macro) == 1
    macro = italian_macro[0]

    assert "warning" in macro, (
        "normalize_italian_number deve avere un warning che spiega "
        "di non usarla quando read.decimal=',' e' configurato"
    )
    assert "read.decimal" in macro["warning"], (
        "Il warning deve menzionare read.decimal per essere trovabile"
    )
    assert "CAST" in macro["warning"] or "DOUBLE" in macro["warning"], (
        "Il warning deve suggerire CAST(x AS DOUBLE) come alternativa"
    )


def test_contracts_required_columns_documented() -> None:
    """La regola required_columns (nomi output, non raw) e' documentata."""
    from toolkit.contracts.pipeline import CONTRACTS

    val = CONTRACTS["clean"]["validation"]["required_columns"]
    assert "scope" in val
    assert "output" in val["scope"].lower() or "alias" in val["scope"].lower(), (
        "required_columns.scope deve specificare che usa i nomi OUTPUT del clean"
    )


def test_contracts_macros_auto_generated() -> None:
    """Le macro nel contratto sono auto-generate da macros.sql.

    Il test verifica che:
    - ogni macro in macros.sql compaia nel contratto
    - ogni macro nel contratto abbia chiavi obbligatorie
    - il parser abbia prodotto risultati coerenti
    """
    from toolkit.contracts.pipeline import CONTRACTS
    from toolkit.core.macro_reader import macro_names

    contract_names = {m["name"] for m in CONTRACTS["clean"]["macros"]}
    sql_names = macro_names()

    # Ogni macro SQL deve essere nel contratto
    missing = sql_names - contract_names
    assert not missing, (
        f"Macro(e) in macros.sql ma non nel contratto: {missing}. "
        "Hai dimenticato un header '-- -- nome ---' in macros.sql?"
    )
    # Ogni macro contrattuale deve esistere in SQL (warning se extra)
    extra = contract_names - sql_names
    if extra:
        import warnings

        warnings.warn(
            f"Macro(e) nel contratto ma non in macros.sql: {extra}. "
            "Potrebbero essere state rimosse da macros.sql."
        )

    # Ogni macro deve avere le chiavi obbligatorie
    for macro in CONTRACTS["clean"]["macros"]:
        for key in ("name", "signature", "params", "returns", "description", "example"):
            assert key in macro, f"Macro '{macro.get('name', '?')}' manca chiave '{key}'"


def test_contracts_macros_return_types_known() -> None:
    """Tutte le macro hanno un returns type esplicito (non default VARCHAR)."""
    from toolkit.contracts.pipeline import CONTRACTS

    for macro in CONTRACTS["clean"]["macros"]:
        assert macro["returns"] != "VARCHAR" or macro["name"] == "normalize_string", (
            f"Macro '{macro['name']}' manca '@contract returns:' in macros.sql "
            f"(ha '{macro['returns']}' di default)"
        )


def test_contracts_json_serializable() -> None:
    """CONTRACTS deve essere serializzabile in JSON per MCP/CLI --json."""
    from toolkit.contracts.pipeline import CONTRACTS

    dumped = json.dumps(CONTRACTS, ensure_ascii=False, default=str)
    parsed = json.loads(dumped)
    assert parsed["version"] == "1"
    assert len(parsed["clean"]["macros"]) >= 8


def test_contracts_source_types_cover_plugins() -> None:
    """Ogni plugin in toolkit/plugins/ ha una voce in source_types.

    Test di manutenzione: se si aggiunge un plugin, il contratto raw
    deve essere aggiornato con il nuovo tipo fonte.
    """
    from pathlib import Path

    from toolkit.contracts.pipeline import CONTRACTS

    plugins_dir = Path(__file__).parent.parent / "toolkit" / "plugins"
    plugin_files = sorted(
        p.stem for p in plugins_dir.glob("*.py") if p.stem not in ("__init__", "_http_utils")
    )

    contract_types = {s["type"] for s in CONTRACTS["raw"]["source_types"]}

    missing = set(plugin_files) - contract_types
    assert not missing, (
        f"Plugin(s) senza corrispondente in source_types: {missing}. "
        "Aggiungili a toolkit/contracts/pipeline.py _SOURCE_TYPES."
    )


def test_contract_cli_output() -> None:
    """CLI toolkit contract produce output senza errori."""
    from typer.testing import CliRunner
    from toolkit.cli.app import app

    runner = CliRunner()

    result = runner.invoke(app, ["contract"])
    assert result.exit_code == 0, f"CLI contract fallito: {result.output}"
    assert "raw_input" in result.output
    assert "clean_input" in result.output
    assert "normalize_italian_number" in result.output

    result_raw = runner.invoke(app, ["contract", "--layer", "raw"])
    assert result_raw.exit_code == 0
    assert "http_file" in result_raw.output
    assert "LAYER RAW" in result_raw.output

    result_clean = runner.invoke(app, ["contract", "--layer", "clean"])
    assert result_clean.exit_code == 0
    assert "raw_input" in result_clean.output

    result_mart = runner.invoke(app, ["contract", "--layer", "mart"])
    assert result_mart.exit_code == 0
    assert "clean_input" in result_mart.output

    result_json = runner.invoke(app, ["contract", "--json"])
    assert result_json.exit_code == 0
    parsed = json.loads(result_json.output)
    assert parsed["version"] == "1"
    assert "clean" in parsed
    assert "mart" in parsed
