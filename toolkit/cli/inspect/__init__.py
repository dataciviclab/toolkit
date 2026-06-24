"""inspect subcommand package — config, summary, runs, paths, profile."""

from __future__ import annotations

import typer

from toolkit.cli.inspect.config_ops import config
from toolkit.cli.inspect.summary_ops import summary
from toolkit.cli.inspect.runs_ops import runs
from toolkit.cli.inspect.paths_ops import paths
from toolkit.cli.inspect.profile_ops import profile


def register(app: typer.Typer) -> None:
    """Register all inspect subcommands on the parent app."""
    inspect_app = typer.Typer(no_args_is_help=True, add_completion=False)
    inspect_app.command("config")(config)
    inspect_app.command("summary")(summary)
    inspect_app.command("runs")(runs)
    inspect_app.command("paths")(paths)
    inspect_app.command("profile")(profile)
    app.add_typer(
        inspect_app,
        name="inspect",
        help="Config, summary, runs, paths, profile — ispeziona un dataset.",
    )
