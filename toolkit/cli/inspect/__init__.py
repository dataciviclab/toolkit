"""inspect subcommand package — paths, schema-diff, url, probe."""

from __future__ import annotations

import typer

from toolkit.cli.inspect.paths_ops import paths
from toolkit.cli.inspect.schema_diff_ops import schema_diff
from toolkit.cli.inspect.url_ops import url
from toolkit.cli.inspect.probe_ops import probe


def register(app: typer.Typer) -> None:
    """Register all inspect subcommands on the parent app."""
    inspect_app = typer.Typer(no_args_is_help=True, add_completion=False)
    inspect_app.command("paths")(paths)
    inspect_app.command("schema-diff")(schema_diff)
    inspect_app.command("url")(url)
    inspect_app.command("probe")(probe)
    app.add_typer(inspect_app, name="inspect")