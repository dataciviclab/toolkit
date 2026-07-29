"""Wrapper CLI-specifico per _payload_for_year.

Le funzioni di dominio sono in ``toolkit.domain.inspect_utils``.
Qui resta solo il wrapper ``_payload_for_year`` che arricchisce il payload
con i layer_profiles (dipende da ``toolkit.cli.common``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from toolkit.domain.common import load_layer_profile_summaries
from toolkit.domain.path_resolver import payload_for_year as _domain_payload_for_year


def _payload_for_year(cfg, year: int) -> dict[str, Any]:
    """Payload diagnostico per dataset/year (CLI enriched)."""
    payload = _domain_payload_for_year(cfg, year)
    payload["layer_profiles"] = load_layer_profile_summaries(Path(cfg.root), cfg.dataset, year)
    return payload
