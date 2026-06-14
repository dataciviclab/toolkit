from __future__ import annotations

import pytest

from _smoke_registry import (
    EXPECTED_OFFLINE_SMOKE_NAMES,
    EXPECTED_SMOKE_NAMES,
    discover_smokes,
    discover_testable_offline_smokes,
)


@pytest.mark.contract
def test_smoke_discovery_finds_all_expected_smokes() -> None:
    """Nessuno smoke atteso va perso silenziosamente dalla discovery."""
    found = {s.name for s in discover_smokes()}
    missing = EXPECTED_SMOKE_NAMES - found
    extra = found - EXPECTED_SMOKE_NAMES

    errors: list[str] = []
    if missing:
        errors.append(
            f"Smoke attesi ma non trovati: {sorted(missing)}. "
            "Verifica che il dataset.yml/dataset.offline.yml sia presente e ben formato."
        )
    if extra:
        errors.append(
            f"Smoke trovati ma non attesi: {sorted(extra)}. "
            "Aggiorna EXPECTED_SMOKE_NAMES in _smoke_registry.py."
        )
    if errors:
        pytest.fail("\n".join(errors))


@pytest.mark.contract
def test_smoke_discovery_finds_all_expected_offline() -> None:
    """Gli smoke testabili offline devono essere tutti scopribili."""
    found = {s.name for s in discover_testable_offline_smokes()}
    missing = EXPECTED_OFFLINE_SMOKE_NAMES - found
    extra = found - EXPECTED_OFFLINE_SMOKE_NAMES

    errors: list[str] = []
    if missing:
        errors.append(f"Offline smoke attesi ma non trovati: {sorted(missing)}.")
    if extra:
        errors.append(
            f"Offline smoke trovati ma non attesi: {sorted(extra)}. "
            "Aggiorna EXPECTED_OFFLINE_SMOKE_NAMES."
        )
    if errors:
        pytest.fail("\n".join(errors))
