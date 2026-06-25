"""Tests per toolkit/scout/infer.py — inferenze pure.

pure_unit: infer_years, suggest_years, infer_granularity, infer_topics,
           generate_clean_sql, suggest_mart_sql, suggest_validation
"""

from __future__ import annotations

from typing import Any

import pytest

from toolkit.scout.infer import (
    infer_granularity,
    infer_granularity_from_name_and_columns,
    infer_topics,
    infer_years,
    suggest_years,
)
from toolkit.scaffold.clean import generate_clean_sql
from toolkit.scaffold.full import (
    suggest_mart_sql,
    suggest_validation,
)


# ---------------------------------------------------------------------------
# pure_unit: infer_years
# ---------------------------------------------------------------------------


class TestInferYears:
    """pure_unit: estrazione anni minimo/massimo da testo."""

    @pytest.mark.pure_unit
    @pytest.mark.parametrize(
        "text,expected_min,expected_max",
        [
            ("dataset-2015-2023.csv", 2015, 2023),
            ("data_2020", 2020, 2020),
            ("serie_storica_2010_2025", 2010, 2025),
            ("no-years-here", None, None),
            ("report_2022", 2022, 2022),
            ("anni 2018 2019 2020", 2018, 2020),
            ("", None, None),
        ],
    )
    def test_infer_years(
        self, text: str, expected_min: int | None, expected_max: int | None
    ) -> None:
        result = infer_years(text)
        assert result == (expected_min, expected_max), (
            f"infer_years({text!r}) = {result}, expected ({expected_min}, {expected_max})"
        )


# ---------------------------------------------------------------------------
# pure_unit: suggest_years
# ---------------------------------------------------------------------------


class TestSuggestYears:
    """pure_unit: suggest_years da colonne e profilo (NON da URL)."""

    @pytest.mark.pure_unit
    def test_from_columns(self) -> None:
        years = suggest_years(column_names=["anno", "valore"])
        assert years == [2024], f"got {years}"  # "anno" doesn't contain year numbers

    @pytest.mark.pure_unit
    def test_from_columns_with_numbers(self) -> None:
        years = suggest_years(column_names=["2020", "2021", "2022", "valore"])
        assert years == [2020, 2021, 2022], f"got {years}"

    @pytest.mark.pure_unit
    def test_fallback(self) -> None:
        years = suggest_years()
        assert years == [2024], f"got {years}"


# ---------------------------------------------------------------------------
# pure_unit: infer_granularity
# ---------------------------------------------------------------------------


class TestInferGranularity:
    """pure_unit: infer_granularity da testo."""

    @pytest.mark.pure_unit
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("popolazione residente nei comuni", "comune"),
            ("dati provinciali istat", "provincia"),
            ("statistiche nazionali italia", "nazionale"),
            ("indicatori a livello europeo", "europeo"),
            ("dati globali mondo", "mondiale"),
            ("rumore bianco", "non_determinato"),
            ("", "non_determinato"),
        ],
    )
    def test_granularity(self, text: str, expected: str) -> None:
        assert infer_granularity(text) == expected


class TestInferGranularityFromNameAndColumns:
    """pure_unit: infer_granularity_from_name_and_columns."""

    @pytest.mark.pure_unit
    def test_comune_from_codice_comune_column(self) -> None:
        """CODICE_COMUNE come colonna → granularità comune."""
        result = infer_granularity_from_name_and_columns("", ["CODICE_COMUNE"])
        assert result == "comune"

    @pytest.mark.pure_unit
    def test_comune_from_column_name(self) -> None:
        """Colonna 'Comune' → granularità comune."""
        result = infer_granularity_from_name_and_columns("", ["Comune", "Anno"])
        assert result == "comune"

    @pytest.mark.pure_unit
    def test_provincia_from_column(self) -> None:
        """Colonna 'Provincia' → granularità provincia."""
        result = infer_granularity_from_name_and_columns("", ["Provincia", "Importo"])
        assert result == "provincia"

    @pytest.mark.pure_unit
    def test_regione_from_column(self) -> None:
        """Colonna 'REGIONE' → granularità regione."""
        result = infer_granularity_from_name_and_columns("", ["REGIONE", "Anno"])
        assert result == "regione"

    @pytest.mark.pure_unit
    def test_fallback_on_name_when_no_geo_columns(self) -> None:
        """Senza colonne geografiche, usa il nome per inferire."""
        result = infer_granularity_from_name_and_columns(
            "Popolazione residente nei comuni", ["Reddito", "Anno"]
        )
        assert result == "comune"

    @pytest.mark.pure_unit
    def test_non_determinato_without_geo_hints(self) -> None:
        """Senza colonne geografiche né nome appropriato → non_determinato."""
        result = infer_granularity_from_name_and_columns("Dati generici", ["Nome", "Valore"])
        assert result == "non_determinato"


# ---------------------------------------------------------------------------
# pure_unit: infer_topics
# ---------------------------------------------------------------------------


class TestInferTopics:
    """pure_unit: infer_topics da testo."""

    @pytest.mark.pure_unit
    def test_demografia(self) -> None:
        topics = infer_topics("popolazione residente per regione")
        assert len(topics) > 0
        assert any(t["topic"] == "demografia" for t in topics)

    @pytest.mark.pure_unit
    def test_multiple_topics(self) -> None:
        topics = infer_topics("lavoro e economia in italia")
        topics_found = [t["topic"] for t in topics]
        assert "lavoro" in topics_found
        assert "economia" in topics_found

    @pytest.mark.pure_unit
    def test_no_match(self) -> None:
        topics = infer_topics("xyzabc123")
        assert topics == []

    @pytest.mark.pure_unit
    def test_ordered_by_score(self) -> None:
        topics = infer_topics("lavoro economia lavoro economia lavoro")
        scores = [t["score"] for t in topics]
        assert scores == sorted(scores, reverse=True), "topics should be ordered by score desc"


# ---------------------------------------------------------------------------
# pure_unit: suggest_clean_sql
# ---------------------------------------------------------------------------


class TestGenerateCleanSql:
    """pure_unit: clean.sql con TRY_CAST basato su mapping_suggestions."""

    @pytest.mark.pure_unit
    def test_with_casts(self) -> None:
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "nome": {"type": "str"},
                "valore": {"type": "float"},
                "anno": {"type": "int"},
            },
        }
        sql = generate_clean_sql(profile, "candidate", 2024)
        assert 'TRY_CAST("valore" AS DOUBLE)' in sql
        assert 'TRY_CAST("anno" AS BIGINT)' in sql
        assert '"nome"' in sql
        assert "FROM raw_input" in sql

    @pytest.mark.pure_unit
    def test_no_mapping(self) -> None:
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "a": {"type": "string"},
                "b": {"type": "string"},
                "c": {"type": "string"},
            },
        }
        sql = generate_clean_sql(profile, "candidate", 2024)
        assert "TRY_CAST" not in sql
        assert '"a"' in sql
        assert '"b"' in sql
        assert '"c"' in sql

    @pytest.mark.pure_unit
    def test_empty_profile(self) -> None:
        profile: dict[str, Any] = {"mapping_suggestions": {}}
        sql = generate_clean_sql(profile, "candidate", 2024)
        assert "FROM raw_input" in sql


# ---------------------------------------------------------------------------
# pure_unit: suggest_mart_sql
# ---------------------------------------------------------------------------


@pytest.mark.pure_unit
class TestSuggestMartSql:
    """pure_unit: mart.sql come scheletro commentato (niente aggregazioni automatiche)."""

    @pytest.mark.pure_unit
    def test_skeleton_with_columns(self) -> None:
        """Con colonne: produce scheletro commentato con tipi."""
        cols = ["anno", "categoria", "valore"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "anno": {"type": "int"},
                "categoria": {"type": "str"},
                "valore": {"type": "float"},
            },
        }
        sql = suggest_mart_sql(cols, profile)
        assert "Sostituisci con la tua aggregazione" in sql
        assert "SELECT * FROM clean_input" in sql
        assert "GROUP BY" not in sql
        assert "anno: int" in sql
        assert "valore: float" in sql

    @pytest.mark.pure_unit
    def test_skeleton_no_mapping(self) -> None:
        """Senza mapping: produce scheletro con tipi '?'."""
        cols = ["a", "b"]
        profile: dict[str, Any] = {"mapping_suggestions": {}}
        sql = suggest_mart_sql(cols, profile)
        assert "Sostituisci con la tua aggregazione" in sql
        assert "SELECT * FROM clean_input" in sql
        assert "a: ?" in sql
        assert "b: ?" in sql

    @pytest.mark.pure_unit
    def test_empty_fallback(self) -> None:
        """Senza colonne: placeholder."""
        cols: list[str] = []
        profile: dict[str, Any] = {"mapping_suggestions": {}}
        sql = suggest_mart_sql(cols, profile)
        assert "Sostituisci" in sql
        assert "SELECT * FROM clean_input" in sql

    @pytest.mark.pure_unit
    def test_shorthand_types_in_skeleton(self) -> None:
        """Tipi shorthand (int, float) appaiono nel commento."""
        cols = ["anno", "valore"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "anno": {"type": "int"},
                "valore": {"type": "float"},
            },
        }
        sql = suggest_mart_sql(cols, profile)
        assert "anno: int" in sql
        assert "valore: float" in sql
        assert "SELECT * FROM clean_input" in sql


# ---------------------------------------------------------------------------
# pure_unit: suggest_validation
# ---------------------------------------------------------------------------


class TestSuggestValidation:
    """pure_unit: validation rules suggerite dal profilo."""

    @pytest.mark.pure_unit
    def test_with_columns_and_rows(self) -> None:
        profile: dict[str, Any] = {
            "columns_norm": ["a", "b", "c"],
            "row_count": 100,
        }
        val = suggest_validation(profile)
        assert "clean" in val
        assert val["clean"]["validate"]["required_columns"] == ["a", "b", "c"]
        assert val["clean"]["validate"]["min_rows"] <= 100

    @pytest.mark.pure_unit
    def test_empty_profile(self) -> None:
        val = suggest_validation({})
        assert val == {}


# ---------------------------------------------------------------------------
# pure_unit: shorthand types support
# ---------------------------------------------------------------------------


class TestShorthandTypes:
    """pure_unit: mapping_suggestions con tipi shorthand (int, float, str)."""

    @pytest.mark.pure_unit
    def test_clean_sql_with_shorthand_types(self) -> None:
        """generate_clean_sql riconosce int/float come tipi numerici."""
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "eta": {"type": "int"},
                "reddito": {"type": "float"},
            },
        }
        sql = generate_clean_sql(profile, "candidate", 2024)
        assert 'TRY_CAST("eta" AS BIGINT)' in sql, f"int type not recognized: {sql}"
        assert 'TRY_CAST("reddito" AS DOUBLE)' in sql


# ── Routing SPARQL in probe_url_routed ───────────────────────────────────────


class TestProbeUrlRoutedSparql:
    """probe_url_routed() must recognize SPARQL endpoints and return sparql_info.

    Uses monkeypatch to avoid real HTTP calls (deterministic, runs in CI).
    """

    @pytest.mark.regression
    def test_is_sparql_endpoint_detected_by_url(self) -> None:
        """URL con /sparql nel path deve essere riconosciuto."""
        from toolkit.scout.http import is_sparql_endpoint

        assert is_sparql_endpoint("https://dati.camera.it/sparql")
        assert is_sparql_endpoint("https://example.org/sparql/query")
        assert not is_sparql_endpoint("https://example.org/data.csv")
        assert not is_sparql_endpoint("https://dati.consip.it/dataset/foo")

    @pytest.mark.regression
    def test_is_sparql_endpoint_detected_by_content_type(self) -> None:
        """Content-Type application/sparql-results+json deve attivare il rilevamento."""
        from toolkit.scout.http import is_sparql_endpoint

        assert is_sparql_endpoint("https://example.org/data", "application/sparql-results+json")
        assert not is_sparql_endpoint("https://example.org/data", "text/html")

    @pytest.mark.regression
    def test_probe_url_routed_returns_sparql_info(self, monkeypatch) -> None:
        """probe_url_routed deve restituire source_type=sparql e sparql_info."""
        from toolkit.scout.probe import probe_url_routed

        # Mock probe_url_headers per evitare HTTP reali
        def _mock_headers(url, **kw):
            return {
                "status_code": 200,
                "content_type": "text/html",
                "content_disposition": None,
                "final_url": url,
            }

        monkeypatch.setattr("toolkit.scout.probe.probe_url_headers", _mock_headers)

        # Mock SparqlSource.fetch() per evitare query SPARQL reali
        _fetch_results = [
            # Primo call (ASK) → CSV vuoto
            (b"", "https://dati.camera.it/sparql"),
            # Secondo call (DCAT) → CSV con un dataset
            (
                b"dataset,title,description\r\nhttp://example.org/ds1,Dataset 1,Test dataset\r\n",
                "https://dati.camera.it/sparql",
            ),
        ]

        def _mock_fetch(self, endpoint, query, accept_format="csv"):
            return _fetch_results.pop(0)

        monkeypatch.setattr("toolkit.plugins.sparql.SparqlSource.fetch", _mock_fetch)

        result = probe_url_routed("https://dati.camera.it/sparql", timeout=5)
        assert result["source_type"] == "sparql"
        si = result.get("sparql_info") or {}
        assert si.get("responded") is True
        assert si.get("dataset_count", 0) >= 1
        assert si.get("datasets", [{}])[0].get("title") == "Dataset 1"

    @pytest.mark.regression
    def test_probe_url_routed_sparql_ask_failure_falls_to_opaque(self, monkeypatch) -> None:
        """Se l'ASK probe fallisce (timeout/refused), source_type deve essere opaco."""
        from toolkit.scout.probe import probe_url_routed

        def _mock_headers(url, **kw):
            return {
                "status_code": 200,
                "content_type": "text/html",
                "content_disposition": None,
                "final_url": url,
            }

        monkeypatch.setattr("toolkit.scout.probe.probe_url_headers", _mock_headers)

        # Mock SparqlSource.fetch() per simulare un timeout
        def _mock_fetch_fail(self, endpoint, query, accept_format="csv"):
            raise RuntimeError("timeout connecting to SPARQL endpoint")

        monkeypatch.setattr("toolkit.plugins.sparql.SparqlSource.fetch", _mock_fetch_fail)

        result = probe_url_routed("https://slow-endpoint.org/sparql", timeout=5)
        assert result["source_type"] == "opaque"
        assert "timeout" in str(result.get("sparql_info", {}).get("error", ""))


class TestProbeUrlRoutedProtocolHint:
    """contract: probe_url_routed(protocol=...) routing deterministico."""

    @pytest.mark.contract
    def test_protocol_http_returns_file(self, monkeypatch) -> None:
        """Con protocol='http', source_type='file' senza euristica."""
        from toolkit.scout.probe import probe_url_routed

        def _mock_headers(url, **kw):
            return {
                "status_code": 200,
                "content_type": "text/plain",
                "content_disposition": None,
                "final_url": url,
            }

        monkeypatch.setattr("toolkit.scout.probe.probe_url_headers", _mock_headers)

        result = probe_url_routed("https://example.org/data.txt", protocol="http")
        assert result["source_type"] == "file"
        assert result["status_code"] == 200
        assert result["ckan_resources"] is None
        assert result["candidate_links"] == []
        assert result["sdmx_info"] is None
        assert result["sparql_info"] is None

    @pytest.mark.contract
    def test_protocol_none_falls_to_auto_detect(self, monkeypatch) -> None:
        """Senza protocol, usa auto-detect come prima."""
        from toolkit.scout.probe import probe_url_routed

        def _mock_headers(url, **kw):
            return {
                "status_code": 200,
                "content_type": "text/html",
                "content_disposition": None,
                "final_url": url,
            }

        monkeypatch.setattr("toolkit.scout.probe.probe_url_headers", _mock_headers)

        result = probe_url_routed("https://example.org/")
        # URL senza /sparql, content_type=html → _route_html
        # La route HTML non ha body HTML (mockato) → source_type="html"
        assert result["source_type"] == "html"

    @pytest.mark.contract
    def test_protocol_unknown_falls_to_auto_detect(self, monkeypatch) -> None:
        """Protocol non in _PROTOCOL_ROUTER casca su auto-detect."""
        from toolkit.scout.probe import probe_url_routed

        def _mock_headers(url, **kw):
            return {
                "status_code": 200,
                "content_type": "text/html",
                "content_disposition": None,
                "final_url": url,
            }

        monkeypatch.setattr("toolkit.scout.probe.probe_url_headers", _mock_headers)

        # "ckan" è in _PROTOCOL_ROUTER ma mapped=None → casca su auto-detect
        result = probe_url_routed("https://example.org/", protocol="ckan")
        assert result["source_type"] == "html"  # auto-detect vince


class TestMcpProbeUrlRouted:
    """contract: mcp_probe_url_routed forwarda protocol a probe_url_routed."""

    @pytest.mark.contract
    def test_protocol_forwarded_to_core(self, monkeypatch) -> None:
        """mcp_probe_url_routed(protocol=...) deve passare protocol al core."""
        from toolkit.mcp.scout_ops import mcp_probe_url_routed

        captured_kwargs = {}

        def _mock_probe(url, **kwargs):
            captured_kwargs.update(kwargs)
            return {"source_type": "mock"}

        monkeypatch.setattr("toolkit.mcp.scout_ops.probe_url_routed", _mock_probe)

        result = mcp_probe_url_routed("https://example.org/data.csv", protocol="http")
        assert captured_kwargs.get("protocol") == "http"
        assert result["source_type"] == "mock"

    @pytest.mark.contract
    def test_no_protocol_still_works(self, monkeypatch) -> None:
        """mcp_probe_url_routed senza protocol deve funzionare (backward compat)."""
        from toolkit.mcp.scout_ops import mcp_probe_url_routed

        captured_kwargs = {}

        def _mock_probe(url, **kwargs):
            captured_kwargs.update(kwargs)
            return {"source_type": "mock"}

        monkeypatch.setattr("toolkit.mcp.scout_ops.probe_url_routed", _mock_probe)

        result = mcp_probe_url_routed("https://example.org/")
        assert "protocol" not in captured_kwargs or captured_kwargs.get("protocol") is None
        assert result["source_type"] == "mock"
