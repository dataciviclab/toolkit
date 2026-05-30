"""Tests per toolkit/scout/infer.py — inferenze pure.

pure_unit: infer_years, suggest_years, infer_granularity, infer_topics,
           suggest_clean_sql, suggest_mart_sql, suggest_validation
"""

from __future__ import annotations

from typing import Any

import pytest

from toolkit.scout.infer import (
    infer_granularity,
    infer_topics,
    infer_years,
    suggest_years,
)
from toolkit.scaffold.full import (
    suggest_clean_sql,
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
    def test_infer_years(self, text: str, expected_min: int | None, expected_max: int | None) -> None:
        result = infer_years(text)
        assert result == (expected_min, expected_max), f"infer_years({text!r}) = {result}, expected ({expected_min}, {expected_max})"


# ---------------------------------------------------------------------------
# pure_unit: suggest_years
# ---------------------------------------------------------------------------


class TestSuggestYears:
    """pure_unit: suggest_years combina URL + colonne + profilo."""

    @pytest.mark.pure_unit
    def test_from_url(self) -> None:
        years = suggest_years(url="https://example.com/data-2020-2022.csv")
        assert years == [2020, 2021, 2022], f"got {years}"

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


class TestSuggestCleanSql:
    """pure_unit: clean.sql con TRY_CAST basato su mapping_suggestions."""

    @pytest.mark.pure_unit
    def test_with_casts(self) -> None:
        cols = ["nome", "valore", "anno"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "nome": {"type": "str"},
                "valore": {"type": "float"},
                "anno": {"type": "int"},
            },
        }
        sql = suggest_clean_sql(cols, profile)
        assert 'TRY_CAST("valore" AS DOUBLE)' in sql
        assert 'TRY_CAST("anno" AS BIGINT)' in sql
        assert '"nome"' in sql
        assert "FROM raw_input" in sql

    @pytest.mark.pure_unit
    def test_no_mapping(self) -> None:
        cols = ["a", "b", "c"]
        profile: dict[str, Any] = {"mapping_suggestions": {}}
        sql = suggest_clean_sql(cols, profile)
        assert "TRY_CAST" not in sql
        assert '"a"' in sql
        assert '"b"' in sql
        assert '"c"' in sql

    @pytest.mark.pure_unit
    def test_empty_columns(self) -> None:
        sql = suggest_clean_sql([], {})
        assert "placeholder" in sql or "FROM raw_input" in sql


# ---------------------------------------------------------------------------
# pure_unit: suggest_mart_sql
# ---------------------------------------------------------------------------


@pytest.mark.pure_unit
class TestSuggestMartSql:
    """pure_unit: mart.sql con GROUP BY basato su colonne e tipi."""

    @pytest.mark.pure_unit
    def test_with_year_and_numeric(self) -> None:
        """Con anno + colonna numerica: GROUP BY anno, SUM(numerico)."""
        cols = ["anno", "categoria", "valore"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "anno": {"type": "int"},
                "categoria": {"type": "str"},
                "valore": {"type": "float"},
            },
        }
        sql = suggest_mart_sql(cols, profile)
        assert "GROUP BY" in sql
        assert 'SUM("valore")' in sql
        assert '"anno"' in sql
        assert '"categoria"' in sql

    @pytest.mark.pure_unit
    def test_year_only_with_numeric_columns(self) -> None:
        """Con anno + altre colonne non numeriche: SUM(anno) (fallback)."""
        cols = ["anno", "categoria"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "anno": {"type": "int"},
                "categoria": {"type": "str"},
            },
        }
        sql = suggest_mart_sql(cols, profile)
        assert "GROUP BY" in sql
        # "anno" e' l'unica colonna numerica, viene aggregata
        assert 'SUM("anno")' in sql

    @pytest.mark.pure_unit
    def test_no_numeric_column_with_year(self) -> None:
        """Colonna anno + nessuna colonna numerica: COUNT per anno."""
        cols = ["anno", "categoria"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "anno": {"type": "str"},
                "categoria": {"type": "str"},
            },
        }
        sql = suggest_mart_sql(cols, profile)
        # "anno" word triggers year-based count aggregation
        assert "COUNT" in sql
        assert '"anno"' in sql

    @pytest.mark.pure_unit
    def test_region_without_year(self) -> None:
        """Colonna regione senza anno: COUNT per regione."""
        cols = ["regione", "valore"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "regione": {"type": "str"},
                "valore": {"type": "float"},
            },
        }
        sql = suggest_mart_sql(cols, profile)
        assert "regione" in sql

    @pytest.mark.pure_unit
    def test_empty_fallback(self) -> None:
        """Senza colonne riconoscibili: SELECT * FROM clean."""
        cols = ["a", "b"]
        profile: dict[str, Any] = {"mapping_suggestions": {}}
        sql = suggest_mart_sql(cols, profile)
        assert "SELECT * FROM clean" in sql


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
        assert "mart" in val
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
        """suggest_clean_sql riconosce int/float come tipi numerici."""
        cols = ["eta", "reddito"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "eta": {"type": "int"},
                "reddito": {"type": "float"},
            },
        }
        sql = suggest_clean_sql(cols, profile)
        assert 'TRY_CAST("eta" AS BIGINT)' in sql, f"int type not recognized: {sql}"
        assert 'TRY_CAST("reddito" AS DOUBLE)' in sql

    @pytest.mark.pure_unit
    def test_mart_sql_with_shorthand_types(self) -> None:
        """suggest_mart_sql riconosce int/float come tipi numerici."""
        cols = ["anno", "valore"]
        profile: dict[str, Any] = {
            "mapping_suggestions": {
                "anno": {"type": "int"},
                "valore": {"type": "float"},
            },
        }
        sql = suggest_mart_sql(cols, profile)
        assert 'SUM("valore")' in sql


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
            (b"dataset,title,description\r\nhttp://example.org/ds1,Dataset 1,Test dataset\r\n", "https://dati.camera.it/sparql"),
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
