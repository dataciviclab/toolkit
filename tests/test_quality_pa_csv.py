"""Test: quality check CSV PA (toolkit/quality/pa_csv_quality.py)."""

from toolkit.quality.pa_csv_quality import assess_quality, _parse_csv, _detect_sep

# ── Helpers ───────────────────────────────────────────────────────────────────

_csv_ok = """codice_istat,denominazione,regione,sigla_provincia,popolazione,anno
001002,Airasca,Piemonte,TO,3200,2024
001003,Ala di Stura,Piemonte,TO,450,2024
001004,Albiano d'Ivrea,Piemonte,TO,1650,2024
001006,Almese,Piemonte,TO,6400,2024
001007,Alpette,Piemonte,TO,250,2024
"""


def _score(csv_text: str, title: str = "") -> int:
    return assess_quality(csv_text, title=title).score


def _verdict(csv_text: str, title: str = "") -> str:
    return assess_quality(csv_text, title=title).verdict


def _flags(csv_text: str, title: str = "") -> list[str]:
    return assess_quality(csv_text, title=title).flags


def _ontologies(csv_text: str, title: str = "") -> dict:
    return assess_quality(csv_text, title=title).ontologies


def _checks(csv_text: str, title: str = "") -> list:
    r = assess_quality(csv_text, title=title)
    return [c for cat in r.checks.values() for c in cat]


# ── Parser ────────────────────────────────────────────────────────────────────


class TestParseCsv:
    def test_detect_sep_comma(self):
        assert _detect_sep("a,b,c\n1,2,3") == ","

    def test_detect_sep_semicolon(self):
        assert _detect_sep("a;b;c\n1;2;3") == ";"

    def test_detect_sep_tab(self):
        assert _detect_sep("a\tb\tc\n1\t2\t3") == "\t"

    def test_parse_simple(self):
        rows = _parse_csv("a,b\n1,2", ",")
        assert rows == [["a", "b"], ["1", "2"]]

    def test_parse_quoted_newline(self):
        """Newline dentro virgolette NON deve spezzare la riga."""
        csv = 'a,b\n1,"hello\nworld",2'
        rows = _parse_csv(csv, ",")
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
        assert rows[1][1] == "hello\nworld"

    def test_parse_quoted_comma(self):
        csv = 'a,b\n1,"hello, world"'
        rows = _parse_csv(csv, ",")
        assert rows[1][1] == "hello, world"

    def test_parse_escaped_quote(self):
        csv = 'a,b\n1,"say ""hello""",2'
        rows = _parse_csv(csv, ",")
        assert rows[1][1] == 'say "hello"'

    def test_parse_empty_lines_skipped(self):
        csv = "a,b\n\n1,2\n\n3,4"
        rows = _parse_csv(csv, ",")
        assert len(rows) == 3
        assert rows[-1] == ["3", "4"]

    def test_parse_crlf(self):
        csv = "a,b\r\n1,2\r\n3,4"
        rows = _parse_csv(csv, ",")
        assert len(rows) == 3

    def test_parse_semicolon_delim(self):
        csv = "a;b;c\n1;2;3"
        rows = _parse_csv(csv, ";")
        assert rows[1] == ["1", "2", "3"]


# ── Check: Struttura ─────────────────────────────────────────────────────────


class TestChecksStruttura:
    def test_empty_file(self):
        _ = assess_quality("")
        c = _checks("")
        c1 = [x for x in c if x.id == "S1"]
        assert len(c1) == 1
        assert c1[0].status == "fail"

    def test_no_header(self):
        csv = "abc,def\n"
        r = _checks(csv)
        s3 = [x for x in r if x.id == "S3"][0]
        assert s3.status == "pass"  # header presente (ma col nome strano)

    def test_empty_header_fail(self):
        csv = "a,,c\n1,2,3"
        r = _checks(csv)
        s5 = [x for x in r if x.id == "S5"][0]
        assert s5.status == "warn"

    def test_duplicate_columns_fail(self):
        csv = "a,a,b\n1,2,3"
        r = _checks(csv)
        s4 = [x for x in r if x.id == "S4"][0]
        assert s4.status == "fail"

    def test_consistent_ok(self):
        r = _checks(_csv_ok)
        s6 = [x for x in r if x.id == "S6"][0]
        assert s6.status == "pass"

    def test_inconsistent_fail(self):
        csv = "a,b,c\n1,2,3\n4,5"
        r = _checks(csv)
        s6 = [x for x in r if x.id == "S6"][0]
        assert s6.status == "fail"


# ── Check: Contenuto ─────────────────────────────────────────────────────────


class TestChecksContenuto:
    def test_high_missing_fail(self):
        csv = "a,b,c\n1,,3\n,,,"
        r = _checks(csv)
        c2 = [x for x in r if x.id == "C2"][0]
        assert c2.status == "fail"

    def test_no_duplicates(self):
        r = _checks(_csv_ok)
        c1 = [x for x in r if x.id == "C1"][0]
        assert c1.status == "pass"

    def test_duplicates_warn(self):
        csv = "a,b\n1,2\n1,2\n3,4"
        r = _checks(csv)
        c1 = [x for x in r if x.id == "C1"][0]
        assert c1.status == "warn"

    def test_no_id_column_warn(self):
        csv = "a,b\n1,2\n3,4"
        r = _checks(csv)
        c3 = [x for x in r if x.id == "C3"][0]
        assert c3.status == "warn"

    def test_id_column_detected(self):
        csv = "id,nome\n1,pippo\n2,pluto\n3,paperino"
        r = _checks(csv)
        c3 = [x for x in r if x.id == "C3"][0]
        assert c3.status == "pass"


# ── Check: Open Data ─────────────────────────────────────────────────────────


class TestChecksOpendata:
    def test_small_dataset_warn(self):
        csv = "a,b\n1,2"
        r = _checks(csv)
        o1 = [x for x in r if x.id == "O1"][0]
        assert o1.status == "warn"

    def test_few_columns_warn(self):
        csv = "a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"
        r = _checks(csv)
        o2 = [x for x in r if x.id == "O2"][0]
        assert o2.status == "warn"

    def test_geo_keyword_detected(self):
        csv = "lat,lon,valore\n45.0,9.0,100\n46.0,10.0,200"
        r = _checks(csv)
        o5 = [x for x in r if x.id == "O5"][0]
        assert o5.status == "pass"

    def test_time_keyword_detected(self):
        csv = "anno,valore\n2023,100\n2024,200"
        r = _checks(csv)
        o6 = [x for x in r if x.id == "O6"][0]
        assert o6.status == "pass"

    def test_uri_detected(self):
        csv = "id,url\n1,https://schema.gov.it/foo\n2,https://w3.org/bar"
        r = _checks(csv)
        o8 = [x for x in r if x.id == "O8"][0]
        assert o8.status == "pass"


# ── Check: Linked Data / Ontologie ────────────────────────────────────────────


class TestChecksLinkeddata:
    def test_uuid_detected(self):
        csv = "id,val\n550e8400-e29b-41d4-a716-446655440000,abc"
        r = _checks(csv)
        l1 = [x for x in r if x.id == "L1"][0]
        assert l1.status == "pass"

    def test_istat_code_detected(self):
        csv = "codice_istat,nome\n075035,Lecce"
        r = _checks(csv)
        l3 = [x for x in r if x.id == "L3"][0]
        assert l3.status == "pass"

    def test_cig_detected(self):
        csv = "cig,oggetto\nZ3A1234567,Lavori"
        r = _checks(csv)
        l4 = [x for x in r if x.id == "L4"][0]
        assert l4.status == "pass"

    def test_cup_detected(self):
        csv = "cup,titolo\nF21B23000000006,Progetto"
        r = _checks(csv)
        l4 = [x for x in r if x.id == "L4"][0]
        assert l4.status == "pass"

    def test_schema_uri_in_data(self):
        csv = "id,uri\n1,https://schema.gov.it/CLV"
        r = _checks(csv)
        l5 = [x for x in r if x.id == "L5"][0]
        assert l5.status == "pass"


# ── Ontologia detection ──────────────────────────────────────────────────────


class TestOntologyDetection:
    def test_clv_from_lat_lon(self):
        csv = "lat,lon,valore\n45.0,9.0,1"
        onto = _ontologies(csv)
        assert "CLV" in onto

    def test_pc_from_cig_importo(self):
        csv = "cig,importo\nZ3A1234567,1000"
        onto = _ontologies(csv)
        assert "PC" in onto

    def test_cov_from_denominazione(self):
        csv = "denominazione,piva\nComune X,12345"
        onto = _ontologies(csv)
        assert "COV" in onto

    def test_cpv_from_nome_cognome(self):
        csv = "nome,cognome,cf\nMario,Rossi,RSSMRA80A01H501U"
        onto = _ontologies(csv)
        assert "CPV" in onto

    def test_ti_from_anno_data(self):
        csv = "anno,valore\n2024,100"
        onto = _ontologies(csv)
        assert "TI" in onto

    def test_multiple_ontologies(self):
        csv = "cig,importo,lat,lon,anno,denominazione,piva\nX,1,45,9,2024,A,1"
        onto = _ontologies(csv)
        assert len(onto) >= 4


# ── Score e verdetto ────────────────────────────────────────────────────────


class TestScore:
    def test_clean_csv_high_score(self):
        s = _score(_csv_ok)
        assert s >= 90

    def test_clean_csv_buona_qualita(self):
        v = _verdict(_csv_ok)
        assert v == "buona_qualita"

    def test_empty_csv_fail(self):
        v = _verdict("")
        assert v == "non_accettabile"

    def test_critical_fail_on_s1(self):
        """S1 (file vuoto) = critical fail = non_accettabile."""
        v = _verdict("")
        assert v == "non_accettabile"
        assert assess_quality("").critical_fail is True

    def test_high_missing_lowers_score(self):
        csv_dirty = "a,b,c\n1,,3\n,,,\n2,3,4"
        csv_clean = "a,b,c\n1,2,3\n4,5,6\n7,8,9"
        assert _score(csv_dirty) < _score(csv_clean)

    def test_ipa_entire_file(self):
        """Test con porzione del CSV IPA reale — si aspetta score ≥ 80."""
        import urllib.request

        url = "https://indicepa.gov.it/ipa-dati/datastore/dump/d09adf99-dc10-4349-8c53-27b1e5aa97b6?bom=True"
        with urllib.request.urlopen(url) as f:
            raw = f.read().decode("utf-8")
        # Prime 100 righe
        lines = raw.split("\n")[:100]
        sample = "\n".join(lines)
        s = _score(sample)
        assert s >= 80, f"IPA CSV score {s} < 80"

    def test_newline_in_quotes_does_not_break(self):
        """Newline dentro virgolette non deve generare falso S6."""
        csv = 'a,b\n1,"hello\nworld"\n2,"foo\nbar"\n3,single'
        r = _checks(csv)
        s6 = [x for x in r if x.id == "S6"]
        assert len(s6) == 1
        assert s6[0].status == "pass", f"S6 status: {s6[0].status} (dovrebbe pass)"
