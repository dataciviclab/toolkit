from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from toolkit.cli.app import app


def _get_dataset_name(smoke_dir: Path) -> str:
    """Legge il nome del dataset dal dataset.yml."""
    cfg_path = smoke_dir / "dataset.yml"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["dataset"]["name"]


def _get_years(smoke_dir: Path) -> list[int]:
    """Legge gli anni configurati dal dataset.yml."""
    cfg_path = smoke_dir / "dataset.yml"
    with open(cfg_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["dataset"].get("years", [])


def _root(smoke_dir: Path) -> Path:
    """Ritorna il path a _smoke_out dentro lo smoke."""
    return smoke_dir / "_smoke_out"


@pytest.mark.contract
def test_smoke_years_filter_run_all_supports_years_filter(
    smoke_offline: Path, runner, chdir_tmp: Path
) -> None:
    """--years filtra gli anni eseguiti — solo l'anno richiesto deve avere output."""
    config_path = smoke_offline / "dataset.yml"
    years = _get_years(smoke_offline)
    dataset_name = _get_dataset_name(smoke_offline)

    # Prendiamo l'ultimo anno disponibile come filtro
    target_year = max(years)

    result = runner.invoke(
        app,
        [
            "run",
            "all",
            "--config",
            str(config_path),
            "--years",
            str(target_year),
        ],
    )
    assert result.exit_code == 0, result.output

    root = _root(smoke_offline)

    # Verifica: l'anno non target NON deve esistere
    for y in years:
        if y != target_year:
            assert not (root / "data" / "raw" / dataset_name / str(y)).exists(), (
                f"Anno {y} non doveva essere eseguito"
            )

    # Verifica: l'anno target DEVE esistere
    assert (root / "data" / "raw" / dataset_name / str(target_year)).exists(), (
        f"Anno {target_year} doveva essere eseguito"
    )


@pytest.mark.contract
def test_smoke_years_filter_validate_all_supports_years_filter(
    smoke_offline: Path, runner, chdir_tmp: Path
) -> None:
    """--years funziona anche con validate."""
    config_path = smoke_offline / "dataset.yml"
    years = _get_years(smoke_offline)
    target_year = max(years)

    # Prima run
    run_result = runner.invoke(app, ["run", "all", "--config", str(config_path)])
    assert run_result.exit_code == 0, run_result.output

    # Validate con filtro anno
    validate_result = runner.invoke(
        app,
        [
            "validate",
            "all",
            "--config",
            str(config_path),
            "--years",
            str(target_year),
        ],
    )
    assert validate_result.exit_code == 0, validate_result.output


@pytest.mark.policy
def test_smoke_years_filter_rejects_unconfigured_year(
    smoke_offline: Path, runner, chdir_tmp: Path
) -> None:
    """Un anno non configurato deve essere rifiutato."""
    config_path = smoke_offline / "dataset.yml"
    years = _get_years(smoke_offline)
    fake_year = max(years) + 1  # anno sicuramente non presente

    result = runner.invoke(
        app,
        [
            "run",
            "all",
            "--config",
            str(config_path),
            "--years",
            str(fake_year),
        ],
    )
    assert result.exit_code != 0
    assert result.exception is not None
    assert "Year(s) not configured" in str(result.exception)
