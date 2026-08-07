"""Test del lettore artifact registry (CLI/MCP: toolkit registry).

Fixture: mini-workspace con due repo, uno con registry/committati.

Marker: contract (lettura cross-repo), pure_unit (conteggio entries).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from toolkit.registry.reader import list_registries, show_registry


@pytest.fixture
def mini_workspace(tmp_path: Path) -> Path:
    """Workspace con due repo: uno con registry, uno senza."""
    euro = tmp_path / "eurostat"
    (euro / "registry").mkdir(parents=True)
    (euro / "registry" / "clean_catalog.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "datasets": [{"slug": "eurostat_gdp_nuts3", "name": "GDP"}],
            }
        ),
        encoding="utf-8",
    )
    (euro / "registry" / "pipeline_signals.json").write_text(
        json.dumps({"schema_version": "1", "signals": [{"id": "x"}, {"id": "y"}]}),
        encoding="utf-8",
    )
    # Schema: escluso dalla vista
    (euro / "registry" / "clean_catalog.schema.json").write_text("{}", encoding="utf-8")

    (tmp_path / "no-registry-repo").mkdir()
    return tmp_path


def test_list_registries(mini_workspace: Path) -> None:
    """Lista i repo con artifact, escludendo gli schemi (contract)."""
    data = list_registries(mini_workspace)
    assert data["total_repos"] == 1
    repo = data["repos"][0]
    assert repo["repo"] == "eurostat"
    names = {a["name"] for a in repo["artifacts"]}
    assert names == {"clean_catalog", "pipeline_signals"}
    assert "clean_catalog.schema" not in names
    cc = next(a for a in repo["artifacts"] if a["name"] == "clean_catalog")
    assert cc["entries"] == 1


def test_show_registry_full(mini_workspace: Path) -> None:
    """show senza slug ritorna il payload completo (contract)."""
    data = show_registry("eurostat", "clean_catalog", workspace=mini_workspace)
    assert data["artifact"] == "clean_catalog"
    assert len(data["data"]["datasets"]) == 1


def test_show_registry_filter_slug(mini_workspace: Path) -> None:
    """show con slug filtra l'entry richiesta (contract)."""
    data = show_registry(
        "eurostat", "clean_catalog", slug="eurostat_gdp_nuts3", workspace=mini_workspace
    )
    assert data["entry"]["slug"] == "eurostat_gdp_nuts3"


def test_show_registry_not_found(mini_workspace: Path) -> None:
    """Artifact o repo inesistente → FileNotFoundError (contract)."""
    with pytest.raises(FileNotFoundError):
        show_registry("eurostat", "mart_catalog", workspace=mini_workspace)
    with pytest.raises(FileNotFoundError):
        show_registry("no-registry-repo", "clean_catalog", workspace=mini_workspace)


def test_show_registry_slug_missing(mini_workspace: Path) -> None:
    """Slug inesistente → FileNotFoundError (pure_unit)."""
    with pytest.raises(FileNotFoundError):
        show_registry("eurostat", "pipeline_signals", slug="missing", workspace=mini_workspace)


def test_entries_count_pure() -> None:
    """Conteggio entries per tipo artifact (pure_unit)."""
    from toolkit.registry.reader import _entries_count

    assert _entries_count("clean_catalog", {"datasets": [1, 2]}) == 2
    assert _entries_count("mart_catalog", {"marts": [1]}) == 1
    assert _entries_count("pipeline_signals", {"signals": [1, 2, 3]}) == 3
    assert _entries_count("codelists", {"codelists": {"geo": [], "units": []}}) == 2
    assert _entries_count("entity_graph", {}) is None
