"""Parità di lettura RAW: il profilo deve leggere come il layer CLEAN.

Il profiler aveva un proprio parser DuckDB (`read_csv` costruito a mano) invece
di usare la pipeline del clean (`read_raw_to_relation`). Le due copie sono
divergite e il profilo falliva — in silenzio — su ogni dataset che usava:

- il formato compatto ``columns: {column00: "nome:VARCHAR"}``;
- ``normalize_rows_to_columns`` con colonne dichiarate per nome;
- ``align_by_header``.

Risultato misurato sul workspace: 193 profili vuoti su 219 (dati reali), con
``profile_failed`` registrato solo dentro l'artefatto. Questi test fissano il
contratto: una sola pipeline di lettura, e il profilo la segue.
"""

from __future__ import annotations

import json

import logging
from pathlib import Path

import pytest
from lab_connectors.duckdb import safe_connect

from toolkit.core.constants import RAW_INPUT_VIEW
from toolkit.core.duckdb_read import read_raw_to_relation
from toolkit.core.input_file import RawInputFile
from toolkit.profile.raw import profile_raw, profile_with_read_cfg, sniff_source_file


def _clean_columns(csv_path: Path, read_cfg: dict) -> list[str]:
    """Colonne prodotte dal layer CLEAN con la stessa read config."""
    cfg = dict(read_cfg)
    cfg.pop("source", None)
    with safe_connect() as con:
        read_raw_to_relation(
            con,
            [RawInputFile(path=csv_path.resolve())],
            cfg,
            "fallback",
            logging.getLogger("test.parity"),
        )
        return [str(r[0]) for r in con.execute(f"DESCRIBE {RAW_INPUT_VIEW}").fetchall()]


@pytest.mark.regression
def test_profile_handles_compact_columns_format(tmp_path: Path):
    """`columns: {column00: "nome:VARCHAR"}` non è un tipo DuckDB — ma è config valida.

    Regressione (dichiarazioni-italia, 9 dataset): il profilo passava
    ``column00: "regione:VARCHAR"`` come tipo a DuckDB e falliva con
    ``Value "regione:VARCHAR" can not be converted to a DuckDB Type``.
    Il clean, che parsa il formato compatto, funzionava: il profilo mentiva.
    """
    (tmp_path / "mef.csv").write_text(
        "Redditi - tutte le tipologie - Regione\n"
        "Anno;Regione;Numero contribuenti\n"
        "2014;Lombardia;1.234.567\n"
        "2014;Lazio;987.654\n",
        encoding="utf-8",
    )

    read_cfg = {
        "header": False,
        "skip": 2,
        "delim": ";",
        "encoding": "utf-8",
        "columns": {
            "column00": "anno:VARCHAR",
            "column01": "regione:VARCHAR",
            "column02": "numero_contribuenti:VARCHAR",
        },
        "normalize_rows_to_columns": True,
        "strict_mode": False,
        "null_padding": True,
    }

    profile = profile_raw(tmp_path, "mef", 2014, read_cfg, primary_file=tmp_path / "mef.csv")

    assert profile.columns_raw, f"profilo vuoto: {profile.warnings}"
    assert not any("profile_failed" in w for w in profile.warnings), profile.warnings
    assert profile.row_count == 2
    assert profile.columns_raw == _clean_columns(tmp_path / "mef.csv", {**read_cfg, "skip": 2})


@pytest.mark.regression
def test_profile_matches_clean_columns_for_plain_header_csv(tmp_path: Path):
    """Con header e sniff, profilo e clean vedono le stesse colonne."""
    csv_path = tmp_path / "plain.csv"
    csv_path.write_text("id;valore\n1;10\n2;20\n", encoding="utf-8")

    profile = profile_raw(tmp_path, "plain", 2024, {"delim": ";"})

    assert profile.columns_raw == ["id", "valore"]
    assert profile.columns_raw == _clean_columns(csv_path, {"delim": ";"})


@pytest.mark.regression
def test_profile_handles_align_by_header(tmp_path: Path):
    """`align_by_header` riallinea per nome: il profilo deve vedere le stesse colonne."""
    csv_path = tmp_path / "drift.csv"
    # ordine diverso dall'atteso + una colonna assente
    csv_path.write_text("anno;regione\n2014;Lombardia\n", encoding="utf-8")

    read_cfg = {
        "header": True,
        "delim": ";",
        "encoding": "utf-8",
        "columns": {"anno": "VARCHAR", "regione": "VARCHAR", "comune": "VARCHAR"},
        "normalize_rows_to_columns": True,
        "align_by_header": True,
    }

    profile = profile_with_read_cfg(csv_path, sniff_source_file(csv_path), dict(read_cfg))

    assert not any("profile_failed" in w for w in profile["warnings"]), profile["warnings"]
    assert set(profile["columns_raw"]) == set(_clean_columns(csv_path, dict(read_cfg)))


@pytest.mark.regression
def test_profile_respects_clean_read_mode_strict(tmp_path: Path):
    """Con `read_mode: strict` il profilo non deve salvare il clean con il robusto.

    Se il profilo facesse fallback mentre il clean è strict, direbbe "ok" per un
    dataset che poi fallisce a runtime.
    """
    csv_path = tmp_path / "dirty.csv"
    csv_path.write_text("col1;col2\n1;2;3\n4;5\n", encoding="utf-8")
    sniff = sniff_source_file(csv_path)
    cfg = {"delim": ";", "encoding": "utf-8", "header": True}

    strict = profile_with_read_cfg(csv_path, sniff, dict(cfg), read_mode="strict")
    fallback = profile_with_read_cfg(csv_path, sniff, dict(cfg), read_mode="fallback")

    assert not strict["columns_raw"]
    assert any("profile_failed" in w for w in strict["warnings"])
    assert fallback["columns_raw"]
    assert fallback["robust_read_suggested"] is True


@pytest.mark.policy
def test_profile_returns_row_count_without_second_scans(tmp_path: Path):
    """`row_count` arriva dal profilo, non da una seconda connessione."""
    (tmp_path / "n.csv").write_text("a;b\n" + "\n".join(f"{i};x" for i in range(7)), "utf-8")

    profile = profile_raw(tmp_path, "n", 2024, {"delim": ";"}, primary_file=tmp_path / "n.csv")

    assert profile.row_count == 7


# ── Selezione dei file: il profilo deve leggere la stessa relazione del clean ──


@pytest.mark.regression
def test_profile_reads_union_of_files_in_mode_all(tmp_path: Path):
    """`mode: all` → il clean fa union_by_name di N file, il profilo deve fare lo stesso.

    Regressione: il profilo guardava solo `primary_output_file`. Su `bdap_ni_azioni`
    (dlb/lb/rnd) e `ga_ricorsi_pervenuti_class` (31 file) descriveva una relazione
    diversa da quella letta dal clean (row_count e colonne sbagliati).
    """
    (tmp_path / "a.csv").write_text("id;valore\n1;10\n2;20\n", encoding="utf-8")
    (tmp_path / "b.csv").write_text("id;altro\n3;x\n", encoding="utf-8")

    profile = profile_raw(
        tmp_path,
        "ds",
        2024,
        {"mode": "all", "delim": ";"},
        primary_file=tmp_path / "a.csv",
    )

    assert set(profile.columns_raw) == {"id", "valore", "altro"}
    assert profile.row_count == 3
    assert not [w for w in profile.warnings if "mismatch" in w], profile.warnings


@pytest.mark.regression
def test_profile_honors_include_pattern(tmp_path: Path):
    """`mode: explicit` + `include` → profila i file del pattern, non il primary."""
    (tmp_path / "keep_1.csv").write_text("id;valore\n1;10\n", encoding="utf-8")
    (tmp_path / "other_1.csv").write_text("x;y;z\n9;8;7\n", encoding="utf-8")

    profile = profile_raw(
        tmp_path,
        "ds",
        2024,
        {"mode": "explicit", "include": ["keep_*.csv"], "delim": ";"},
        primary_file=tmp_path / "other_1.csv",
    )

    assert profile.columns_raw == ["id", "valore"]
    assert profile.row_count == 1


@pytest.mark.policy
def test_profile_latest_keeps_manifest_primary(tmp_path: Path):
    """`mode: latest` resta sul primary del manifest: nessuna selezione multi-file."""
    (tmp_path / "a.csv").write_text("id;valore\n1;10\n", encoding="utf-8")
    (tmp_path / "b.csv").write_text("x;y;z\n9;8;7\n", encoding="utf-8")

    profile = profile_raw(
        tmp_path,
        "ds",
        2024,
        {"mode": "latest", "delim": ";"},
        primary_file=tmp_path / "b.csv",
    )

    assert profile.columns_raw == ["x", "y", "z"]
    assert profile.row_count == 1


@pytest.mark.regression
def test_profile_uses_manifest_files_not_directory_listing(tmp_path: Path):
    """Un file sciolto nella directory non deve entrare nel profilo.

    Regressione: `select_raw_input` invocato senza `root/dataset/year` ricade
    su `list_input_files(raw_dir)` (la directory), mentre il clean usa le entry
    del manifest. Su `anac_stati_avanzamento` una copia del full
    (`stati-avanzamento_csv.csv`) non elencata nel manifest gonfiava il profilo
    a 3.459.926 righe contro le 2.185.125 lette dal clean.
    """
    root = tmp_path / "root"
    raw_year = root / "data" / "raw" / "ds" / "2024"
    raw_year.mkdir(parents=True)
    (raw_year / "a.csv").write_text("id;valore\n1;10\n", encoding="utf-8")
    (raw_year / "b.csv").write_text("id;altro\n2;x\n", encoding="utf-8")
    # file presente su disco ma NON scritto da questo run (artefatto sciolto)
    (raw_year / "raw_input.csv").write_text("id;valore\n1;10\n2;20\n3;30\n", encoding="utf-8")

    files = [{"file": "a.csv"}, {"file": "b.csv"}]
    (raw_year / "metadata.json").write_text(
        json.dumps({"layer": "raw", "dataset": "ds", "year": 2024, "files": files}),
        encoding="utf-8",
    )
    # il clean usa il manifest solo se l'ultimo run RAW è SUCCESS
    run_dir = root / "data" / "_runs" / "ds" / "2024"
    run_dir.mkdir(parents=True)
    (run_dir / "run1.json").write_text(
        json.dumps({"run_id": "run1", "layers": {"raw": {"status": "SUCCESS"}}}),
        encoding="utf-8",
    )

    profile = profile_raw(
        raw_year,
        "ds",
        2024,
        {"mode": "all", "delim": ";"},
        primary_file=raw_year / "a.csv",
        # run_raw passa i file del run corrente: ground truth
        source_files=[raw_year / "a.csv", raw_year / "b.csv"],
    )

    # 1 riga da a.csv + 1 da b.csv. Il file sciolto ne ha 3: se entrasse,
    # row_count sarebbe 5 (e comparirebbe la sua colonna extra).
    assert profile.row_count == 2, f"letti file fuori dal run: {profile.row_count}"
    assert set(profile.columns_raw) == {"id", "valore", "altro"}


@pytest.mark.regression
def test_inspect_profile_passes_read_mode(tmp_path: Path):
    """`inspect profile` deve usare il `read_mode` del dataset, non il default.

    Regressione: `run_profile` chiamava `profile_raw` senza `read_mode`, quindi
    usava `fallback` anche su dataset `strict` — la frase "il profilo non può
    dichiarare leggibile un file che il clean rifiuterebbe" non valeva per
    questo percorso.
    """
    from toolkit.cli.inspect.profile_ops import run_profile
    from toolkit.core.config import load_config
    from toolkit.core.paths import RAW_PROFILE, RAW_PROFILE_DIR, layer_year_dir

    def _project(read_mode: str) -> int:
        proj = tmp_path / read_mode
        (proj / "sql").mkdir(parents=True)
        (proj / "sql" / "clean.sql").write_text("select 1 as x", encoding="utf-8")
        (proj / "dataset.yml").write_text(
            "\n".join(
                [
                    f'root: "{(proj / "out").as_posix()}"',
                    "dataset:",
                    '  name: "ds"',
                    "  years: [2024]",
                    "raw: {}",
                    "clean:",
                    '  sql: "sql/clean.sql"',
                    f"  read_mode: {read_mode}",
                    "  read:",
                    '    delim: ";"',
                    "mart: {}",
                ]
            ),
            encoding="utf-8",
        )
        raw_year = layer_year_dir(proj / "out", "raw", "ds", 2024)
        raw_year.mkdir(parents=True)
        (raw_year / "dirty.csv").write_text("col1;col2\n1;2;3\n4;5\n", encoding="utf-8")

        cfg = load_config(str(proj / "dataset.yml"), strict_config=False)
        run_profile(cfg, [2024], logging.getLogger("test.inspect"))

        profile = json.loads((raw_year / RAW_PROFILE_DIR / RAW_PROFILE).read_text())
        return len(profile["columns_raw"])

    assert _project("fallback") > 0
    assert _project("strict") == 0


@pytest.mark.regression
def test_profile_falls_back_to_manifest_without_source_files(tmp_path: Path):
    """Senza `source_files` (chiamante diretto) la selezione è quella del manifest."""
    root = tmp_path / "root"
    raw_year = root / "data" / "raw" / "ds" / "2024"
    raw_year.mkdir(parents=True)
    (raw_year / "a.csv").write_text("id;valore\n1;10\n", encoding="utf-8")
    (raw_year / "b.csv").write_text("id;altro\n2;x\n", encoding="utf-8")
    (raw_year / "raw_input.csv").write_text("id;valore\n1;10\n2;20\n3;30\n", encoding="utf-8")
    (raw_year / "metadata.json").write_text(
        json.dumps(
            {
                "layer": "raw",
                "dataset": "ds",
                "year": 2024,
                "files": [{"file": "a.csv"}, {"file": "b.csv"}],
            }
        ),
        encoding="utf-8",
    )
    run_dir = root / "data" / "_runs" / "ds" / "2024"
    run_dir.mkdir(parents=True)
    (run_dir / "run1.json").write_text(
        json.dumps({"run_id": "run1", "layers": {"raw": {"status": "SUCCESS"}}}),
        encoding="utf-8",
    )

    profile = profile_raw(raw_year, "ds", 2024, {"mode": "all", "delim": ";"})

    assert profile.row_count == 2
    assert set(profile.columns_raw) == {"id", "valore", "altro"}
