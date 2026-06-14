"""Helper per assert su path e struttura degli artifact.

Centralizza i pattern di path assertion che appaiono in 35+ file di test.
Ogni funzione verifica che un percorso esista e restituisce il path per
ulteriori assert.

Usage::

    from tests.helpers_assert_paths import (
        assert_raw_dir,
        assert_clean_parquet,
        assert_mart_parquet,
        assert_metadata_file,
        assert_validation_file,
        assert_layer_year_dir,
        assert_no_absolute_paths,
        assert_file_replaceable,
        assert_golden_path_artifacts,
    )
"""

from __future__ import annotations

import json
import re
from pathlib import Path


# ── Esistenza layer dir ─────────────────────────────────────────────


def assert_raw_dir(root: Path, dataset: str, year: int) -> Path:
    """Verifica che il directory ``raw/{dataset}/{year}`` esista.

    Returns:
        Il path al directory.
    """
    path = root / "data" / "raw" / dataset / str(year)
    assert path.is_dir(), f"RAW dir non trovato: {path}"
    return path


def assert_clean_dir(root: Path, dataset: str, year: int) -> Path:
    """Verifica che il directory ``clean/{dataset}/{year}`` esista."""
    path = root / "data" / "clean" / dataset / str(year)
    assert path.is_dir(), f"CLEAN dir non trovato: {path}"
    return path


def assert_mart_dir(root: Path, dataset: str, year: int) -> Path:
    """Verifica che il directory ``mart/{dataset}/{year}`` esista."""
    path = root / "data" / "mart" / dataset / str(year)
    assert path.is_dir(), f"MART dir non trovato: {path}"
    return path


# ── Esistenza file singoli ───────────────────────────────────────────


def assert_raw_file(root: Path, dataset: str, year: int, filename: str) -> Path:
    """Verifica che un file raw esista (es. ``data.csv``)."""
    path = root / "data" / "raw" / dataset / str(year) / filename
    assert path.exists(), f"Raw file non trovato: {path}"
    return path


def assert_clean_parquet(root: Path, dataset: str, year: int) -> Path:
    """Verifica che il parquet clean ``{dataset}_{year}_clean.parquet`` esista.

    Returns:
        Il path al parquet.
    """
    path = root / "data" / "clean" / dataset / str(year) / f"{dataset}_{year}_clean.parquet"
    assert path.exists(), f"CLEAN parquet non trovato: {path}"
    return path


def assert_mart_parquet(root: Path, dataset: str, year: int, table: str) -> Path:
    """Verifica che una tabella mart ``{table}.parquet`` esista.

    Args:
        table: Nome della tabella (senza estensione).
    """
    path = root / "data" / "mart" / dataset / str(year) / f"{table}.parquet"
    assert path.exists(), f"MART parquet non trovato: {path}"
    return path


def assert_metadata_file(root: Path, dataset: str, layer: str, year: int) -> dict:
    """Legge e restituisce il ``metadata.json`` di un layer.

    Args:
        layer: ``"raw"``, ``"clean"`` o ``"mart"``.

    Returns:
        Contenuto del JSON parsato.
    """
    path = root / "data" / layer / dataset / str(year) / "metadata.json"
    assert path.exists(), f"metadata.json non trovato: {path}"
    return json.loads(path.read_text(encoding="utf-8"))


def assert_validation_file(
    root: Path, dataset: str, layer: str, year: int, filename: str | None = None
) -> dict:
    """Legge e restituisce il JSON di validazione di un layer.

    Args:
        layer: ``"raw"``, ``"clean"`` o ``"mart"``.
        filename: Nome del file (default ``{layer}_validation.json``).

    Returns:
        Contenuto del JSON parsato.
    """
    fname = filename or f"{layer}_validation.json"
    path = root / "data" / layer / dataset / str(year) / "_validate" / fname
    assert path.exists(), f"File validazione non trovato: {path}"
    return json.loads(path.read_text(encoding="utf-8"))


def assert_layer_year_dir(root: Path, layer: str, dataset: str, year: int) -> Path:
    """Verifica che un directory ``{layer}/{dataset}/{year}`` esista.

    Versione generica per quando il layer è dinamico.
    """
    path = root / "data" / layer / dataset / str(year)
    assert path.is_dir(), f"Directory {layer} non trovata: {path}"
    return path


# ── Asserzioni speciali (deep metadata) ──────────────────────────────


def assert_no_absolute_paths(payload: dict, root: Path) -> None:
    """Verifica che un dict JSON non contenga path assoluti.

    Usato per garantire che i metadata siano portabili.
    """
    serialized = json.dumps(payload, ensure_ascii=False)
    assert not re.search(r"[A-Za-z]:\\\\", serialized), "Trovato path Windows assoluto nel payload"
    assert '": "/' not in serialized, "Trovato path Unix assoluto nel payload"
    assert str(root.resolve()) not in serialized, (
        f"Trovato root assoluto nel payload: {root.resolve()}"
    )


def assert_file_replaceable(path: Path) -> None:
    """Verifica che un file possa essere cancellato e riscritto.

    Utile per test che simulano la sostituibilità degli artifact dopo un run.
    """
    assert path.exists(), f"File non trovato prima della sostituzione: {path}"
    original = path.read_bytes()
    path.unlink()
    path.write_bytes(original)
    assert path.exists(), f"File non ripristinato dopo sostituzione: {path}"


# ── Golden path aggregato ────────────────────────────────────────────


def assert_golden_path_artifacts(
    root: Path,
    dataset: str,
    years: list[int],
    mart_tables: list[str] | None = None,
) -> None:
    """Verifica la struttura completa degli artifact dopo un golden path.

    Controlla che per ogni anno esistano:
    - RAW dir con raw_validation.json
    - CLEAN dir con almeno un parquet e metadata.json
    - MART dir con ogni tabella dichiarata e mart_validation.json

    Args:
        mart_tables: Lista di nomi tabelle mart (senza estensione).
    """
    tables = mart_tables or []

    for year in years:
        # RAW
        raw_dir = assert_raw_dir(root, dataset, year)
        assert (raw_dir / "raw_validation.json").exists(), (
            f"raw_validation.json mancante in {raw_dir}"
        )

        # CLEAN
        clean_dir = assert_clean_dir(root, dataset, year)
        parquets = list(clean_dir.glob("*.parquet"))
        assert len(parquets) >= 1, f"Nessun parquet CLEAN in {clean_dir}"
        assert (clean_dir / "metadata.json").exists(), f"metadata.json mancante in {clean_dir}"

        # MART
        if tables:
            mart_dir = assert_mart_dir(root, dataset, year)
            for table in tables:
                assert (mart_dir / f"{table}.parquet").exists(), (
                    f"MART table {table}.parquet mancante in {mart_dir}"
                )
            assert (mart_dir / "_validate" / "mart_validation.json").exists(), (
                f"mart_validation.json mancante in {mart_dir}"
            )


def assert_run_record_dir(root: Path, dataset: str, year: int) -> Path:
    """Verifica che il directory dei run record esista."""
    path = root / "data" / "_runs" / dataset / str(year)
    assert path.is_dir(), f"Run record dir non trovato: {path}"
    return path
