from __future__ import annotations

import typer

from toolkit.core.config import load_config
from toolkit.core.logging import get_logger
from toolkit.core.paths import layer_year_dir

from toolkit.profile.raw import profile_raw, write_raw_profile
from toolkit.profile.report import render_profile_md
from toolkit.profile.suggest import write_suggested_mapping_yml, write_suggested_read_yml


def profile(
    step: str = typer.Argument(..., help="raw"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
):
    """
    Profiling (assist) per i layer. Per ora: raw.
    """
    cfg = load_config(config)
    logger = get_logger()

    if step != "raw":
        raise typer.BadParameter("step must be: raw")

    for year in cfg.years:
        raw_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, year)
        out_dir = raw_dir / "_profile"
        out_dir.mkdir(parents=True, exist_ok=True)

        prof = profile_raw(raw_dir, cfg.dataset, year, read_cfg=(cfg.clean or {}).get("read"))
        paths = write_raw_profile(out_dir, prof)

        md_path = out_dir / "profile.md"
        md_path.write_text(render_profile_md(prof.__dict__), encoding="utf-8")

        yml_path = write_suggested_read_yml(out_dir, prof.__dict__)
        mapping_path = write_suggested_mapping_yml(out_dir, prof.__dict__)

        logger.info(f"PROFILE RAW → {paths['json']} | {md_path} | {yml_path} | {mapping_path}")


def register(app: typer.Typer) -> None:
    app.command("profile")(profile)