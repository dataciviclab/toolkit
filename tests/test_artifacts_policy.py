import shutil
from pathlib import Path

from toolkit.clean.run import run_clean
from toolkit.cli.cmd_profile import profile as profile_cmd
from toolkit.core.config import load_config
from toolkit.clean.validate import run_clean_validation
from toolkit.mart.run import run_mart
from toolkit.mart.validate import run_mart_validation
from toolkit.raw.run import run_raw


class _NoopLogger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def error(self, *_args, **_kwargs):
        return None


def _append_output_cfg(config_path: Path, *, artifacts: str, legacy_aliases: bool) -> None:
    with config_path.open("a", encoding="utf-8") as fh:
        fh.write("\noutput:\n")
        fh.write(f"  artifacts: {artifacts}\n")
        fh.write(f"  legacy_aliases: {'true' if legacy_aliases else 'false'}\n")


def _simplify_sql_project(dst: Path) -> None:
    shutil.rmtree(dst / "_smoke_out", ignore_errors=True)
    (dst / "sql" / "clean.sql").write_text("SELECT 1 AS ok FROM raw_input LIMIT 1\n", encoding="utf-8")
    mart_dir = dst / "sql" / "mart"
    for sql_path in mart_dir.glob("*.sql"):
        sql_path.write_text("SELECT * FROM clean_input\n", encoding="utf-8")


def test_artifacts_policy_minimal_skips_optional_outputs(tmp_path: Path, monkeypatch):
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    _simplify_sql_project(dst)
    config_path = dst / "dataset.yml"
    _append_output_cfg(config_path, artifacts="minimal", legacy_aliases=False)

    monkeypatch.chdir(dst)
    cfg = load_config(config_path)
    year = cfg.years[0]
    logger = _NoopLogger()

    run_raw(
        cfg.dataset,
        year,
        cfg.root,
        cfg.raw,
        logger,
        base_dir=cfg.base_dir,
        output_cfg=cfg.output,
        clean_cfg=cfg.clean,
    )
    profile_cmd(step="raw", config=str(config_path))
    run_clean(cfg.dataset, year, cfg.root, cfg.clean, logger, base_dir=cfg.base_dir, output_cfg=cfg.output)
    run_mart(cfg.dataset, year, cfg.root, cfg.mart, logger, base_dir=cfg.base_dir, output_cfg=cfg.output)
    run_clean_validation(cfg, year, logger)
    run_mart_validation(cfg, year, logger)

    root = Path(cfg.root)
    raw_dir = root / "data" / "raw" / cfg.dataset / str(year)
    profile_dir = raw_dir / "_profile"
    clean_dir = root / "data" / "clean" / cfg.dataset / str(year)
    mart_dir = root / "data" / "mart" / cfg.dataset / str(year)

    assert (raw_dir / "manifest.json").exists()
    assert (raw_dir / "metadata.json").exists()
    assert (raw_dir / "raw_validation.json").exists()
    assert (profile_dir / "suggested_read.yml").exists()

    assert not (profile_dir / "raw_profile.json").exists()
    assert not (profile_dir / "profile.json").exists()
    assert not (profile_dir / "profile.md").exists()
    assert not (profile_dir / "suggested_mapping.yml").exists()
    assert not (clean_dir / "_run" / "clean_rendered.sql").exists()
    if (mart_dir / "_run").exists():
        assert not any((mart_dir / "_run").glob("*_rendered.sql"))

    assert (clean_dir / "manifest.json").exists()
    assert (clean_dir / "metadata.json").exists()
    assert (clean_dir / "_validate" / "clean_validation.json").exists()
    assert (mart_dir / "manifest.json").exists()
    assert (mart_dir / "metadata.json").exists()
    assert (mart_dir / "_validate" / "mart_validation.json").exists()


def test_artifacts_policy_standard_keeps_current_debug_artifacts(tmp_path: Path, monkeypatch):
    src = Path("project-example")
    dst = tmp_path / "project-example"
    shutil.copytree(src, dst)
    _simplify_sql_project(dst)
    config_path = dst / "dataset.yml"
    _append_output_cfg(config_path, artifacts="standard", legacy_aliases=True)

    monkeypatch.chdir(dst)
    cfg = load_config(config_path)
    year = cfg.years[0]
    logger = _NoopLogger()

    run_raw(
        cfg.dataset,
        year,
        cfg.root,
        cfg.raw,
        logger,
        base_dir=cfg.base_dir,
        output_cfg=cfg.output,
        clean_cfg=cfg.clean,
    )
    profile_cmd(step="raw", config=str(config_path))
    run_clean(cfg.dataset, year, cfg.root, cfg.clean, logger, base_dir=cfg.base_dir, output_cfg=cfg.output)
    run_mart(cfg.dataset, year, cfg.root, cfg.mart, logger, base_dir=cfg.base_dir, output_cfg=cfg.output)
    run_clean_validation(cfg, year, logger)
    run_mart_validation(cfg, year, logger)

    root = Path(cfg.root)
    raw_dir = root / "data" / "raw" / cfg.dataset / str(year)
    profile_dir = raw_dir / "_profile"
    clean_dir = root / "data" / "clean" / cfg.dataset / str(year)
    mart_dir = root / "data" / "mart" / cfg.dataset / str(year)

    assert (profile_dir / "raw_profile.json").exists()
    assert (profile_dir / "profile.json").exists()
    assert not (profile_dir / "profile.md").exists()
    assert not (profile_dir / "suggested_mapping.yml").exists()
    assert (profile_dir / "suggested_read.yml").exists()
    assert (clean_dir / "_run" / "clean_rendered.sql").exists()
    assert any((mart_dir / "_run").glob("*_rendered.sql"))
