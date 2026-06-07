"""inspect subcommand package — paths, schema-diff, sparql, schema, profile."""
from __future__ import annotations

import typer

from toolkit.cli.inspect.paths_ops import paths
from toolkit.cli.inspect.profile_ops import profile
from toolkit.cli.inspect.schema_diff_ops import schema_diff
from toolkit.cli.inspect.schema_ops import schema
from toolkit.cli.inspect.sparql_ops import sparql


def register(app: typer.Typer) -> None:
    """Register all inspect subcommands on the parent app."""
    inspect_app = typer.Typer(no_args_is_help=True, add_completion=False)
    inspect_app.command("paths")(paths)
    inspect_app.command("profile")(profile)
    inspect_app.command("schema-diff")(schema_diff)
    inspect_app.command("schema")(schema)
    inspect_app.command("sparql")(sparql)
    app.add_typer(inspect_app, name="inspect", help="Ispeziona path, schema, readiness e URL del dataset.")
