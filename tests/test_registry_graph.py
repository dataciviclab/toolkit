"""Test del grafo entità → dataset aggregato cross-repo.

Fixture: mini-workspace con due repo, ognuno con la sezione entities nel
registry (fusion e legacy).

Marker: contract (output pubblico del tool MCP toolkit_graph).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from toolkit.registry.graph import DOMAIN_KEYWORDS, filter_graph, load_workspace_graph


@pytest.fixture
def graph_workspace(tmp_path: Path) -> Path:
    """Due repo con entities: DI (legacy entity_graph) + eurostat (fusion)."""
    # DI legacy: entity_graph.json separato
    di = tmp_path / "dataset-incubator" / "registry"
    di.mkdir(parents=True)
    (di / "entity_graph.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "entities": {
                    "Comune": {
                        "entity": "Comune",
                        "label": "Comune (ISTAT)",
                        "datasets": [
                            {
                                "slug": "irpef_comunale",
                                "name": "Irpef Comunale",
                                "column": "codice_istat_comune",
                                "semantic_type": "municipality_code",
                            }
                        ],
                        "types": {"municipality_code": 1},
                    },
                    "Gara": {
                        "entity": "Gara",
                        "label": "Gara / Appalto",
                        "datasets": [
                            {
                                "slug": "anac_bandi_gara",
                                "name": "Bandi ANAC",
                                "column": "cig",
                                "semantic_type": "cig_code",
                            }
                        ],
                        "types": {"cig_code": 1},
                    },
                },
                "bridges": [
                    {
                        "from": {
                            "entity": "Gara",
                            "dataset": "anac_aggiudicatari",
                            "via": "cig_code",
                        },
                        "to": {
                            "entity": "Comune",
                            "bridge": "anac_bandi_gara",
                            "on": "cig",
                            "semantic_type": "municipality_code",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    # eurostat fusion: registry.json unico con entities (stessa entity "Comune" + nuova)
    eu = tmp_path / "eurostat" / "registry"
    eu.mkdir(parents=True)
    (eu / "registry.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "repo": "eurostat",
                "datasets": [],
                "marts": [],
                "signals": [],
                "codelists": {},
                "entities": {
                    "generated_from": "registry",
                    "entities": {
                        "Comune": {
                            "entity": "Comune",
                            "label": "Comune (ISTAT)",
                            "datasets": [
                                {
                                    "slug": "eurostat_demo_balance_nuts3",
                                    "name": "Demo Balance",
                                    "column": "geo",
                                    "semantic_type": "nuts_code",
                                }
                            ],
                            "types": {"nuts_code": 1},
                        }
                    },
                    "bridges": [],
                },
            }
        ),
        encoding="utf-8",
    )

    (tmp_path / "no-registry-repo").mkdir()
    return tmp_path


@pytest.mark.contract
def test_load_workspace_graph_merges_entities(graph_workspace: Path) -> None:
    """Entity con lo stesso nome si fondono (datasets concatenati, types sommati)."""
    graph = load_workspace_graph(graph_workspace)

    assert graph["repos"] == ["dataset-incubator", "eurostat"]
    # Comune fusa: 1 (DI) + 1 (EU)
    comune = graph["entities"]["Comune"]
    assert len(comune["datasets"]) == 2
    assert comune["types"] == {"municipality_code": 1, "nuts_code": 1}
    # Gara solo in DI
    assert len(graph["entities"]["Gara"]["datasets"]) == 1
    # Bridge da DI preservato
    assert len(graph["bridges"]) == 1


@pytest.mark.contract
def test_filter_by_key(graph_workspace: Path) -> None:
    """Filtro per tipo semantico su graph aggregato."""
    graph = load_workspace_graph(graph_workspace)
    res = filter_graph(graph, by_key="nuts_code")

    assert list(res["entities"].keys()) == ["Comune"]
    assert res["summary"]["total_datasets"] == 1
    assert res["entities"]["Comune"]["datasets"][0]["slug"] == "eurostat_demo_balance_nuts3"


@pytest.mark.contract
def test_filter_by_registry(graph_workspace: Path) -> None:
    """Filtro per entità."""
    graph = load_workspace_graph(graph_workspace)
    res = filter_graph(graph, by_registry="Gara")

    assert list(res["entities"].keys()) == ["Gara"]
    assert res["summary"]["total_datasets"] == 1


@pytest.mark.contract
def test_filter_by_domain(graph_workspace: Path) -> None:
    """Filtro per dominio logico → solo bridge pertinenti."""
    graph = load_workspace_graph(graph_workspace)
    res = filter_graph(graph, by_domain="appalti")

    assert res["domain"] == "appalti"
    assert res["count"] == 1  # bridge cig_code → Comune


@pytest.mark.contract
def test_filter_by_domain_unknown(graph_workspace: Path) -> None:
    """Dominio non riconosciuto → error con domini disponibili."""
    graph = load_workspace_graph(graph_workspace)
    res = filter_graph(graph, by_domain="xyz")

    assert "error" in res
    assert "appalti" in res["error"]


@pytest.mark.contract
def test_domain_keywords_cover_clean_query_contract() -> None:
    """I domini del graph coprono quelli di clean-query (parità di contratto)."""
    expected = {"appalti", "enti", "territorio", "giustizia", "scuola", "progetti", "economia"}
    assert set(DOMAIN_KEYWORDS) == expected
