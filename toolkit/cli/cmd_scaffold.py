from __future__ import annotations

import json
from pathlib import Path

import typer

from toolkit.cli.common import iter_years, load_cfg_and_logger
from toolkit.core.paths import layer_year_dir
from toolkit.scaffold.clean import generate_clean_sql


def scaffold_clean(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", help="Dataset year"),
    output: str | None = typer.Option(
        None,
        "--output",
        "-o",
        help="Output path for clean.sql (default: from dataset.yml clean.sql, or sql/clean.sql)",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print to stdout without writing"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Genera una bozza iniziale di clean.sql a partire dal profilo RAW esistente.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg, logger = load_cfg_and_logger(config, strict_config=strict_config_flag)

    years = iter_years(cfg, year)
    if len(years) > 1 and year is None:
        raise typer.BadParameter(
            "Multiple years configured. Use --year to select one for scaffolding."
        )

    selected_year = years[0]

    raw_profile_dir = layer_year_dir(cfg.root, "raw", cfg.dataset, selected_year) / "_profile"
    profile_path = raw_profile_dir / "profile.json"

    if not profile_path.exists():
        # Also check raw_profile.json as fallback
        profile_path = raw_profile_dir / "raw_profile.json"
        if not profile_path.exists():
            raise typer.Exit(
                f"Profili RAW non trovato in {raw_profile_dir}\n"
                f"Esegui prima: toolkit profile raw -c {config}"
            )

    try:
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise typer.BadParameter(f"Impossibile leggere il profilo RAW: {exc}")

    # Default output: use configured clean.sql path, or fall back to sql/clean.sql
    # cfg.clean is a dict from load_config (via _compat_clean -> model_dump)
    clean_cfg = cfg.clean or {}
    configured_sql = clean_cfg.get("sql") or "sql/clean.sql"
    default_output = Path(cfg.base_dir) / configured_sql
    target_path = Path(output) if output else default_output

    sql = generate_clean_sql(profile, cfg.dataset, selected_year, cfg.root)

    if dry_run:
        typer.echo(sql)
        return

    if target_path.exists():
        logger.warning(
            f"File {target_path} gia esistente. "
            f"Usa --output per un path diverso o --dry-run per vedere la bozza."
        )
        return

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(sql, encoding="utf-8")
    logger.info(f"clean.sql scritto in {target_path}")


def register(app: typer.Typer) -> None:
    scaffold_app = typer.Typer(no_args_is_help=True, add_completion=False)
    scaffold_app.command("clean")(scaffold_clean)
    app.add_typer(scaffold_app, name="scaffold")
