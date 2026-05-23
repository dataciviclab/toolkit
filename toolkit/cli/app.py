from __future__ import annotations

import typer

from toolkit.cli.cmd_run import register as register_run
from toolkit.cli.cmd_profile import register as register_profile
from toolkit.cli.cmd_resume import register as register_resume
from toolkit.cli.cmd_status import register as register_status
from toolkit.cli.cmd_validate import register as register_validate
from toolkit.cli.cmd_inspect import register as register_inspect
from toolkit.cli.cmd_scaffold import register as register_scaffold
from toolkit.cli.cmd_batch import register as register_batch
from toolkit.cli.cmd_review_readiness import register as register_review_readiness
from toolkit.cli.cmd_init import register as register_init
from toolkit.cli.cmd_scout import register as register_scout
from toolkit.version import __version__

app = typer.Typer(no_args_is_help=True, add_completion=False)


@app.callback(invoke_without_command=True)
def _main_options(
    version: bool = typer.Option(False, "--version", help="Mostra versione ed esci"),
) -> None:
    """Toolkit: pipeline RAW → CLEAN → MART per il DataCivicLab."""
    if version:
        typer.echo(f"toolkit {__version__}")
        raise typer.Exit()


# registra comandi
register_run(app)
register_profile(app)
register_resume(app)
register_status(app)
register_validate(app)
register_inspect(app)
register_scaffold(app)
register_batch(app)
register_review_readiness(app)
register_init(app)
register_scout(app)


def main():
    app()


if __name__ == "__main__":
    main()
