"""inspect subcommand package — config, summary, runs."""

from __future__ import annotations

import typer

from toolkit.cli.inspect.config_ops import config
from toolkit.cli.inspect.summary_ops import summary
from toolkit.cli.inspect.runs_ops import runs
from toolkit.cli.inspect.paths_ops import paths
from toolkit.cli.inspect.profile_ops import profile
from toolkit.cli.inspect.schema_diff_ops import schema_diff
from toolkit.cli.inspect.schema_ops import schema
from toolkit.cli.inspect.sparql_ops import sparql


def register(app: typer.Typer) -> None:
    """Register all inspect subcommands on the parent app."""
    inspect_app = typer.Typer(no_args_is_help=True, add_completion=False)
    inspect_app.command("config")(config)
    inspect_app.command("summary")(summary)
    inspect_app.command("runs")(runs)
    # Legacy subcommands (hidden, backward compat)
    inspect_app.command("paths", hidden=True)(paths)
    inspect_app.command("profile", hidden=True)(profile)
    inspect_app.command("schema", hidden=True)(schema)
    inspect_app.command("schema-diff", hidden=True)(schema_diff)
    inspect_app.command("sparql", hidden=True)(sparql)
    app.add_typer(inspect_app, name="inspect", help="Config, summary, runs — ispeziona un dataset.")
