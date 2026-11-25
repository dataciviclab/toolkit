from __future__ import annotations

from pathlib import Path

from lab_connectors.duckdb import safe_connect
import pytest

from toolkit.domain.sql_validation import (
    _build_clean_preview,
    _create_placeholder_raw_input_with_columns,
    _declared_columns,
    _dedupe_preserve_order,
    _extract_missing_binder_column,
    _normalize_sql,
    _placeholder_columns,
    _validate_mart_sql,
    schema_from_preview,
    validate_sql_dry_run,
    validate_sql_syntax,
)


# --- Helpers ---


def _make_cfg(
    tmp_path: Path,
    *,
    clean_sql: str | None = None,
    clean_read: dict | None = None,
    mart_tables: list[dict] | None = None,
    support: list[dict] | None = None,
    root: Path | None = None,
) -> object:
    """Write SQL files to disk and build a ToolkitConfig via make_config()."""
    from tests.helpers import make_config

    if clean_sql:
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        (sql_dir / "clean.sql").write_text(clean_sql, encoding="utf-8")

    clean: dict = {"sql": "sql/clean.sql" if clean_sql else None}
    if clean_read:
        clean["read"] = clean_read

    return make_config(
        base_dir=tmp_path,
        root=root,
        dataset="demo_ds",
        years=[2022],
        clean=clean if clean.get("sql") else None,
        mart={"tables": mart_tables or []} if mart_tables else None,
        support=support,
    )


# --- Unit tests for utility functions ---


class TestDedupePreserveOrder:
    @pytest.mark.pure_unit
    def test_removes_duplicates(self):
        assert _dedupe_preserve_order(["a", "b", "a", "c", "b"]) == ["a", "b", "c"]

    @pytest.mark.pure_unit
    def test_skips_empty(self):
        assert _dedupe_preserve_order(["", "a", "", "b"]) == ["a", "b"]

    @pytest.mark.pure_unit
    def test_preserves_order(self):
        assert _dedupe_preserve_order(["z", "a", "z", "m"]) == ["z", "a", "m"]


class TestNormalizeSql:
    @pytest.mark.pure_unit
    def test_strips_whitespace(self):
        assert _normalize_sql("  select 1  ") == "select 1"

    @pytest.mark.pure_unit
    def test_removes_trailing_semicolon(self):
        assert _normalize_sql("select 1;") == "select 1"

    @pytest.mark.pure_unit
    def test_removes_semicolon_then_space(self):
        assert _normalize_sql("select 1;  ") == "select 1"


class TestExtractMissingBinderColumn:
    @pytest.mark.pure_unit
    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            ('Referenced column "my_col" not found in FROM clause', "my_col"),
            (
                "Referenced column deputato not found in FROM clause and can't find in alias map",
                "deputato",
            ),
            (
                'Column "anno" referenced that exists in the SELECT clause - but this column '
                "cannot be referenced before it is defined",
                "anno",
            ),
            (
                'Values list "r" does not have a column named "TIME_PERIOD"',
                "TIME_PERIOD",
            ),
            (
                'Referenced column r."data" not found in FROM clause and can\'t find in alias map',
                "data",
            ),
        ],
    )
    def test_matches_duckdb_error_variants(self, message, expected):
        """DuckDB cambia messaggio con la forma della query: vanno tutti riconosciuti."""
        assert _extract_missing_binder_column(Exception(message)) == expected

    @pytest.mark.pure_unit
    def test_returns_none_for_unrelated_error(self):
        exc = Exception("syntax error at or near SELECT")
        assert _extract_missing_binder_column(exc) is None


class TestPlaceholderColumns:
    @pytest.mark.pure_unit
    def test_declared_columns_parse_types(self):
        names, types = _declared_columns(
            {"read": {"columns": {"c0": "data_ora:VARCHAR", "c1": "valore"}}}
        )
        assert names == ["data_ora", "valore"]
        assert types == {"data_ora": "VARCHAR"}

    @pytest.mark.pure_unit
    def test_uses_read_columns_output_names(self):
        """Con ``read.columns`` il runtime espone i nomi di output, non le chiavi.

        Verificato su ``read_raw_to_relation``: ``{"a": "alpha:VARCHAR"}``
        produce una view con la colonna ``alpha``.
        """
        clean_cfg = {"read": {"columns": {"col_a": "a_norm:VARCHAR", "col_b": "b_norm:INT"}}}
        cols = _placeholder_columns(clean_cfg, "")
        assert cols == ["a_norm", "b_norm"]

    @pytest.mark.pure_unit
    def test_falls_back_to_quoted_identifiers(self):
        clean_cfg = {"read": {}}
        sql = 'select "foo", "bar" from raw_input'
        cols = _placeholder_columns(clean_cfg, sql)
        assert cols == ["foo", "bar"]

    @pytest.mark.pure_unit
    def test_combines_read_columns_and_identifiers(self):
        clean_cfg = {"read": {"columns": {"raw_a": "a:VARCHAR"}}}
        sql = 'select a, "b" from raw_input'
        cols = _placeholder_columns(clean_cfg, sql)
        assert cols == ["a", "b"]

    @pytest.mark.pure_unit
    def test_dedupes(self):
        clean_cfg = {"read": {"columns": {"raw_a": "a:VARCHAR"}}}
        sql = 'select "a", "b", "a" from raw_input'
        cols = _placeholder_columns(clean_cfg, sql)
        assert cols == ["a", "b"]


class TestCreatePlaceholderRawInput:
    @pytest.mark.policy
    def test_creates_view_with_columns(self):
        with safe_connect() as con:
            _create_placeholder_raw_input_with_columns(con, ["x", "y"])
            result = con.execute("DESCRIBE raw_input").fetchall()
            names = [row[0] for row in result]
            assert names == ["x", "y"]

    @pytest.mark.policy
    def test_creates_fallback_placeholder(self):
        with safe_connect() as con:
            _create_placeholder_raw_input_with_columns(con, [])
            result = con.execute("DESCRIBE raw_input").fetchall()
            names = [row[0] for row in result]
            assert names == ["__dry_run_placeholder"]

    @pytest.mark.policy
    def test_infers_columns_from_sql(self):
        with safe_connect() as con:
            clean_cfg = {"read": {}}
            sql = 'select "val" from raw_input'
            _create_placeholder_raw_input_with_columns(con, _placeholder_columns(clean_cfg, sql))
            result = con.execute("DESCRIBE raw_input").fetchall()
            names = [row[0] for row in result]
            assert names == ["val"]


# --- Unit/integration tests for validate_sql_syntax and schema_from_preview ---


class TestSqlSyntax:
    @pytest.mark.policy
    def test_valid_sql_passes_without_schema(self, tmp_path: Path):
        cfg = _make_cfg(tmp_path, clean_sql="select comune from raw_input")
        result = validate_sql_syntax(cfg, layers=["clean"])
        assert result["ok"] is True
        assert result["checked"][0]["label"] == "clean"

    @pytest.mark.policy
    def test_syntax_error_reports_label_and_path(self, tmp_path: Path):
        cfg = _make_cfg(tmp_path, clean_sql="select from raw_input where (")
        result = validate_sql_syntax(cfg, layers=["clean"])
        assert result["ok"] is False
        assert "clean" in result["errors"][0]
        assert "clean.sql" in result["checked"][0]["path"]

    @pytest.mark.policy
    def test_unresolved_template_placeholders_are_neutralized(self, tmp_path: Path):
        """In PR il support non e' materializzato: il parse-only non deve fallire."""
        cfg = _make_cfg(
            tmp_path,
            clean_sql="select * from read_csv('{support.territori.path}', header = true)",
            support=[{"name": "territori", "type": "file", "path": "mapping/t.csv"}],
        )
        result = validate_sql_syntax(cfg, layers=["clean"])
        assert result["ok"] is True

    @pytest.mark.policy
    def test_unknown_support_name_is_an_error(self, tmp_path: Path):
        """Un typo in {support.NOME} fallirebbe a runtime: va visto in PR."""
        cfg = _make_cfg(
            tmp_path,
            clean_sql="select * from read_csv('{support.territoriX.path}', header = true)",
            support=[{"name": "territori", "type": "file", "path": "mapping/t.csv"}],
        )
        result = validate_sql_syntax(cfg, layers=["clean"])
        assert result["ok"] is False
        assert "territoriX" in result["errors"][0]

    @pytest.mark.policy
    def test_support_ref_in_comment_is_not_flagged(self, tmp_path: Path):
        """Un esempio in un commento non e' un riferimento reale."""
        cfg = _make_cfg(
            tmp_path,
            clean_sql="-- esempio: {support.altro.path}\nselect x from raw_input",
        )
        result = validate_sql_syntax(cfg, layers=["clean"])
        assert result["ok"] is True

    @pytest.mark.policy
    def test_non_serializable_statement_is_skipped(self, tmp_path: Path):
        """DuckDB non serializza PIVOT: il file va saltato, non bocciato."""
        cfg = _make_cfg(
            tmp_path,
            clean_sql="PIVOT raw_input ON categoria USING count(*) GROUP BY comune",
        )
        result = validate_sql_syntax(cfg, layers=["clean"])
        assert result["ok"] is True
        assert result["skipped"]

    @pytest.mark.policy
    def test_unknown_template_key_is_not_checked(self, tmp_path: Path):
        """Placeholder fuori dal contratto support non sono verificabili senza parser."""
        cfg = _make_cfg(tmp_path, clean_sql="select * from raw_input where anno = {anno}")
        result = validate_sql_syntax(cfg, layers=["clean"])
        assert result["ok"] is True

    @pytest.mark.policy
    def test_missing_sql_file_is_an_error(self, tmp_path: Path):
        cfg = _make_cfg(tmp_path)  # nessun clean.sql scritto
        cfg.clean.sql = "sql/clean.sql"  # type: ignore[union-attr]
        result = validate_sql_syntax(cfg, layers=["clean"])
        assert result["ok"] is False
        assert "file non trovato" in result["errors"][0]


class TestSchemaFromPreview:
    @pytest.mark.pure_unit
    def test_uses_preview_types(self):
        schema = schema_from_preview({}, ["comune", "anno"], {"anno": "BIGINT"})
        assert schema == {"comune": "VARCHAR", "anno": "BIGINT"}

    @pytest.mark.pure_unit
    def test_applies_declared_mapping_and_types(self):
        clean_cfg = {"read": {"columns": {"col0": "data_ora:VARCHAR", "col1": "valore"}}}
        schema = schema_from_preview(
            clean_cfg, ["col0", "col1"], {"col0": "TIMESTAMP", "col1": "DOUBLE"}
        )
        assert schema == {"data_ora": "VARCHAR", "valore": "DOUBLE"}

    @pytest.mark.pure_unit
    def test_none_without_columns(self):
        assert schema_from_preview({}, None, {}) is None

    @pytest.mark.pure_unit
    def test_none_for_normalize_rows(self):
        clean_cfg = {"read": {"normalize_rows_to_columns": True}}
        assert schema_from_preview(clean_cfg, ["a"], {"a": "VARCHAR"}) is None


# --- Integration tests for raw_schema (preflight/profile) ---


class TestRawSchema:
    @pytest.mark.policy
    def test_schema_types_make_aggregates_bind(self, tmp_path: Path):
        """Con lo schema reale avg(DOUBLE) binda; con l'euristica fallirebbe."""
        cfg = _make_cfg(tmp_path, clean_sql="select avg(amount) as media from raw_input")
        with safe_connect() as con:
            is_placeholder = _build_clean_preview(
                cfg, year=2022, con=con, dry_run=True, raw_schema={"amount": "DOUBLE"}
            )
            assert is_placeholder is False
            con.execute("SELECT * FROM __dry_run_clean_preview")

    @pytest.mark.policy
    def test_schema_column_missing_is_a_real_error(self, tmp_path: Path):
        """Con schema noto non si inventano colonne: l'errore e' reale."""
        cfg = _make_cfg(tmp_path, clean_sql="select codice from raw_input")
        with safe_connect() as con:
            with pytest.raises(ValueError, match="CLEAN SQL dry-run failed"):
                _build_clean_preview(
                    cfg, year=2022, con=con, dry_run=True, raw_schema={"altro": "VARCHAR"}
                )

    @pytest.mark.policy
    def test_declared_types_bind_before_inference(self, tmp_path: Path):
        """I tipi di clean.read.columns sono la prima ipotesi, non l'ultima."""
        cfg = _make_cfg(
            tmp_path,
            clean_sql="select avg(amount) as media from raw_input",
            clean_read={"columns": {"raw_amount": "amount:DOUBLE"}},
        )
        with safe_connect() as con:
            is_placeholder = _build_clean_preview(cfg, year=2022, con=con, dry_run=True)
            assert is_placeholder is False


# --- Integration tests for _build_clean_preview ---


class TestBuildCleanPreview:
    @pytest.mark.policy
    def test_simple_sql_passes_immediately(self, tmp_path: Path):
        cfg = _make_cfg(tmp_path, clean_sql="select 1 as value")
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            # Table should exist after successful build
            con.execute("SELECT * FROM __dry_run_clean_preview")

    @pytest.mark.policy
    def test_sql_with_unquoted_column_infers_incrementally(self, tmp_path: Path):
        """Clean SQL uses unquoted column name not in read.columns."""
        cfg = _make_cfg(tmp_path, clean_sql="select x from raw_input")
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")

    @pytest.mark.policy
    def test_sql_with_read_columns(self, tmp_path: Path):
        cfg = _make_cfg(
            tmp_path,
            clean_sql='select "amount" from raw_input',
            clean_read={"columns": {"amount": "DOUBLE"}},
        )
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")

    @pytest.mark.policy
    def test_sql_with_multiple_columns_and_casts(self, tmp_path: Path):
        sql = (
            'select TRY_CAST("id" AS BIGINT) AS id, '
            'TRY_CAST("name" AS VARCHAR) AS name '
            "from raw_input"
        )
        cfg = _make_cfg(
            tmp_path,
            clean_sql=sql,
            clean_read={"columns": {"id": "VARCHAR", "name": "VARCHAR"}},
        )
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")

    @pytest.mark.policy
    def test_non_binder_error_raises_clean_dry_run_failure(self, tmp_path: Path):
        """Non-binder SQL errors should surface as CLEAN SQL dry-run failures."""
        cfg = _make_cfg(tmp_path, clean_sql="select from raw_input")
        with safe_connect() as con:
            with pytest.raises(ValueError, match="CLEAN SQL dry-run failed"):
                _build_clean_preview(cfg, year=2022, con=con)

    @pytest.mark.policy
    def test_missing_column_error_includes_path(self, tmp_path: Path):
        """SQL syntax error should include file path in the message."""
        cfg = _make_cfg(tmp_path, clean_sql="select from raw_input")
        with safe_connect() as con:
            with pytest.raises(ValueError) as exc_info:
                _build_clean_preview(cfg, year=2022, con=con)
        msg = str(exc_info.value)
        assert "CLEAN SQL dry-run failed" in msg
        assert "clean.sql" in msg

    @pytest.mark.policy
    def test_sql_ending_with_line_comment_passes(self, tmp_path: Path):
        """Un commento finale non deve commentare la parentesi del wrapper.

        Regressione osservata su clean.sql reali: ``... FROM raw_input``
        seguito da una riga ``-- nota`` produceva ``syntax error at end of
        input`` perche' il wrapper chiudeva il subquery sulla stessa riga.
        """
        cfg = _make_cfg(tmp_path, clean_sql="select x from raw_input\n-- nota finale")
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")

    @pytest.mark.policy
    def test_numeric_expressions_bind_without_known_types(self, tmp_path: Path):
        """Il placeholder non dichiara tipi: sum/confronti numerici non devono fallire."""
        sql = (
            "select sum(amount) as tot, amount - 1 as meno_uno "
            "from raw_input where amount > 0 group by amount"
        )
        cfg = _make_cfg(tmp_path, clean_sql=sql)
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")

    @pytest.mark.policy
    def test_string_functions_bind(self, tmp_path: Path):
        """length/trim/LIKE devono bindare con l'ipotesi VARCHAR."""
        sql = "select length(nome) as l, trim(nome) as n from raw_input where nome like 'A%'"
        cfg = _make_cfg(tmp_path, clean_sql=sql)
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")

    @pytest.mark.policy
    def test_date_functions_bind_with_type_fallback(self, tmp_path: Path):
        """date_part richiede un tipo data: la cascata di tipi deve arrivarci."""
        sql = "select date_part('year', data) as anno from raw_input"
        cfg = _make_cfg(tmp_path, clean_sql=sql)
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")

    @pytest.mark.policy
    def test_infers_more_than_25_distinct_columns(self, tmp_path: Path):
        """Il cap del loop di inferenza non deve fermarsi a 25 colonne raw."""
        cols = ", ".join(f"c{i}" for i in range(30))
        cfg = _make_cfg(tmp_path, clean_sql=f"select {cols} from raw_input")
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            con.execute("SELECT * FROM __dry_run_clean_preview")


# --- Integration tests for _validate_mart_sql ---


class TestValidateMartSql:
    @pytest.mark.policy
    def test_mart_can_read_a_previous_mart_table(self, tmp_path: Path):
        """I mart si creano in ordine: uno puo' leggere il precedente (come nel run)."""
        mart_dir = tmp_path / "sql" / "mart"
        mart_dir.mkdir(parents=True, exist_ok=True)
        (mart_dir / "mart_base.sql").write_text("select 1 as id", encoding="utf-8")
        (mart_dir / "mart_derived.sql").write_text("select id from mart_base", encoding="utf-8")
        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[
                {"name": "mart_base", "sql": "sql/mart/mart_base.sql"},
                {"name": "mart_derived", "sql": "sql/mart/mart_derived.sql"},
            ],
        )
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            _validate_mart_sql(cfg, year=2022, con=con)

    @pytest.mark.policy
    def test_missing_support_marks_preview_as_placeholder(self, tmp_path: Path):
        """Support non materializzato: il preview e' un segnaposto, non un errore."""
        root = tmp_path / "out"
        cfg = _make_cfg(
            tmp_path,
            clean_sql="select * from read_csv('{support.territori.path}', header = true)",
            support=[{"name": "territori", "type": "file", "path": "mapping/assente.csv"}],
            root=root,
        )
        with safe_connect() as con:
            is_placeholder = _build_clean_preview(
                cfg,
                year=2022,
                con=con,
                support_cfg=[{"name": "territori", "type": "file", "path": "mapping/assente.csv"}],
                dry_run=True,
            )
            assert is_placeholder is True

    @pytest.mark.policy
    def test_mart_skipped_when_clean_preview_is_placeholder(self, tmp_path: Path):
        """Con preview segnaposto il MART non e' validabile: si salta, non si boccia."""
        mart_dir = tmp_path / "sql" / "mart"
        mart_dir.mkdir(parents=True, exist_ok=True)
        (mart_dir / "mart_out.sql").write_text("select codice from clean_input", encoding="utf-8")
        root = tmp_path / "out"
        cfg = _make_cfg(
            tmp_path,
            clean_sql="select * from read_csv('{support.territori.path}', header = true)",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
            support=[{"name": "territori", "type": "file", "path": "mapping/assente.csv"}],
            root=root,
        )
        # Non deve sollevare: il MART viene saltato, non bocciato.
        validate_sql_dry_run(cfg, year=2022, layers=["clean", "mart"], dry_run=True)

    @pytest.mark.policy
    def test_simple_mart_sql_passes(self, tmp_path: Path):
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text("select * from clean_input", encoding="utf-8")

        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        with safe_connect() as con:
            # Build clean preview first (required by mart validation)
            _build_clean_preview(cfg, year=2022, con=con)
            _validate_mart_sql(cfg, year=2022, con=con)

    @pytest.mark.policy
    def test_file_support_resolves_against_config_root(self, tmp_path: Path):
        """Il support ``type: file`` va risolto sul root del config, non su cwd.

        Regressione: ``_validate_mart_sql`` chiamava ``resolve_support_payloads``
        senza ``root``, quindi ogni compose con support file/codelist falliva
        con "requires the candidate root" prima ancora di validare il SQL.
        """
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text("select * from clean_input", encoding="utf-8")

        out_root = tmp_path / "out"
        mapping = out_root / "mapping"
        mapping.mkdir(parents=True, exist_ok=True)
        (mapping / "territori.csv").write_text("codice,nome\n01,Abruzzo\n", encoding="utf-8")

        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
            support=[{"name": "territori", "type": "file", "path": "mapping/territori.csv"}],
            root=out_root,
        )
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            _validate_mart_sql(cfg, year=2022, con=con)

    @pytest.mark.policy
    def test_mart_sql_with_missing_column_fails(self, tmp_path: Path):
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text(
            "select nonexistent_col from clean_input", encoding="utf-8"
        )

        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            with pytest.raises(ValueError, match="MART SQL dry-run failed"):
                _validate_mart_sql(cfg, year=2022, con=con)

    @pytest.mark.policy
    def test_mart_sql_with_template_placeholder_resolved(self, tmp_path: Path):
        """Mart SQL with {year} placeholder should be resolved by template rendering."""
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text(
            "select * from clean_input where anno = {year}", encoding="utf-8"
        )

        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as anno",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            _validate_mart_sql(cfg, year=2022, con=con)

    @pytest.mark.policy
    def test_mart_sql_with_from_clean_fails(self, tmp_path: Path):
        """FROM clean non e' piu' supportato — solo clean_input funziona."""
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text("select * from clean", encoding="utf-8")

        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            with pytest.raises(ValueError, match="MART SQL dry-run failed"):
                _validate_mart_sql(cfg, year=2022, con=con)

    @pytest.mark.policy
    def test_mart_sql_with_clean_prefix_fails(self, tmp_path: Path):
        """Prefisso clean. non e' piu' supportato — solo clean_input. funziona."""
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text(
            "select clean.value from clean_input", encoding="utf-8"
        )

        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            with pytest.raises(ValueError, match="MART SQL dry-run failed"):
                _validate_mart_sql(cfg, year=2022, con=con)

    @pytest.mark.policy
    def test_mart_sql_with_unresolved_placeholder_fails(self, tmp_path: Path):
        """Mart SQL with unresolved placeholder should fail."""
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "mart_out.sql").write_text(
            "select * from clean_input where col = {unknown_placeholder}", encoding="utf-8"
        )

        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "mart_out", "sql": "sql/mart/mart_out.sql"}],
        )
        with safe_connect() as con:
            _build_clean_preview(cfg, year=2022, con=con)
            with pytest.raises(ValueError, match="unresolved"):
                _validate_mart_sql(cfg, year=2022, con=con)


# --- Top-level validate_sql_dry_run ---
# Support dataset paths remain covered indirectly in test_run_dry_run.py.


class TestValidateSqlDryRun:
    @pytest.mark.policy
    def test_clean_and_mart_pass(self, tmp_path: Path):
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "out.sql").write_text("select * from clean_input", encoding="utf-8")

        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "out", "sql": "sql/mart/out.sql"}],
        )
        # Should not raise
        validate_sql_dry_run(cfg, year=2022, layers=["clean", "mart"])

    @pytest.mark.policy
    def test_mart_only_pass(self, tmp_path: Path):
        """Without clean.sql, mart validation skips clean preview."""
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "out.sql").write_text("select 1 as value", encoding="utf-8")

        cfg = _make_cfg(
            tmp_path,
            clean_sql=None,
            mart_tables=[{"name": "out", "sql": "sql/mart/out.sql"}],
        )
        validate_sql_dry_run(cfg, year=2022, layers=["mart"])

    @pytest.mark.policy
    def test_no_matching_layers_returns_early(self, tmp_path: Path):
        """If layers don't include clean or mart, function returns without doing anything."""
        cfg = _make_cfg(tmp_path)
        # Should not raise even though no SQL files exist
        validate_sql_dry_run(cfg, year=2022, layers=["cross_year"])

    @pytest.mark.policy
    def test_fails_on_clean_sql_error(self, tmp_path: Path):
        cfg = _make_cfg(tmp_path, clean_sql="select from raw_input")
        with pytest.raises(ValueError, match="CLEAN SQL dry-run failed"):
            validate_sql_dry_run(cfg, year=2022, layers=["clean"])

    @pytest.mark.policy
    def test_fails_on_mart_sql_error(self, tmp_path: Path):
        mart_sql_dir = tmp_path / "sql" / "mart"
        mart_sql_dir.mkdir(parents=True, exist_ok=True)
        (mart_sql_dir / "out.sql").write_text("select bad_col from clean_input", encoding="utf-8")

        cfg = _make_cfg(
            tmp_path,
            clean_sql="select 1 as value",
            mart_tables=[{"name": "out", "sql": "sql/mart/out.sql"}],
        )
        with pytest.raises(ValueError, match="MART SQL dry-run failed"):
            validate_sql_dry_run(cfg, year=2022, layers=["clean", "mart"])
