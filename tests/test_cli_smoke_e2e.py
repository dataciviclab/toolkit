from pathlib import Path
import shutil

from toolkit.cli.cmd_run import run as run_cmd
from toolkit.cli.cmd_validate import validate as validate_cmd


def test_cli_run_all_and_validate_all_on_project_example(tmp_path: Path, monkeypatch):
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)

    monkeypatch.chdir(dst)
    config_path = dst / "dataset.yml"

    run_cmd(step="all", config=str(config_path))
    validate_cmd(step="all", config=str(config_path))

    root = dst / "_smoke_out"
    year = "2022"
    dataset = "project_example"

    assert (root / "data" / "raw" / dataset / year / "raw_validation.json").exists()
    assert (root / "data" / "clean" / dataset / year / "project_example_2022_clean.parquet").exists()
    assert (root / "data" / "clean" / dataset / year / "_validate" / "clean_validation.json").exists()
    assert (root / "data" / "mart" / dataset / year / "rd_by_regione.parquet").exists()
    assert (root / "data" / "mart" / dataset / year / "rd_by_provincia.parquet").exists()
    assert (root / "data" / "mart" / dataset / year / "_validate" / "mart_validation.json").exists()


def test_cli_run_raw_resolves_paths_from_config_dir_not_cwd(tmp_path: Path, monkeypatch):
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)

    monkeypatch.chdir(tmp_path)
    config_path = dst / "dataset.yml"

    run_cmd(step="raw", config=str(config_path))

    raw_dir = dst / "_smoke_out" / "data" / "raw" / "project_example" / "2022"
    assert (raw_dir / "ispra_dettaglio_comunale_2022.csv").exists()
    assert (raw_dir / "raw_validation.json").exists()
