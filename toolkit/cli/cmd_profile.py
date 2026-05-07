from __future__ import annotations

import typer

from toolkit.cli.common import iter_years, load_cfg_and_logger
from toolkit.core.artifacts import resolve_artifact_policy, should_write
from toolkit.core.paths import layer_year_dir
from toolkit.profile.raw import profile_raw, write_raw_profile, write_suggested_read_yml


def profile(
    step: str = typer.Argument(..., help="raw"),
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    strict_config: bool = typer.Option(False, "--strict-config", help="Treat deprecated config forms as errors"),
):
    """
    Profiling (assist) per i layer. Per ora: raw.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_config_flag)

    if step != "raw":
        raise typer.BadParameter("step must be: raw")

    for year in iter_years(cfg, None):
        raw_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, year)
        out_dir = raw_dir / "_profile"
        out_dir.mkdir(parents=True, exist_ok=True)
        policy = resolve_artifact_policy(cfg.output)

        prof = profile_raw(raw_dir, cfg.dataset, year, read_cfg=(cfg.clean or {}).get("read"))
        # Explicit `toolkit profile raw` should always emit the canonical profile JSON.
        # Always emit canonical profile JSON for the assist workflow,
        # regardless of any discard-happy output configuration.
        paths = write_raw_profile(
            out_dir,
            prof,
            write_canonical=True,
            write_legacy_alias=should_write("profile", "profile_alias", policy, cfg),
        )

        written_paths = list(paths.values())

        if should_write("profile", "suggested_read", policy, cfg):
            written_paths.append(write_suggested_read_yml(out_dir, prof.__dict__))

        if written_paths:
            logger.info("PROFILE RAW -> %s", " | ".join(str(path) for path in written_paths))
        else:
            logger.info("PROFILE RAW -> no optional artifacts written for current policy")


def register(app: typer.Typer) -> None:
    app.command("profile")(profile)
