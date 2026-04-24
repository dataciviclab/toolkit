from __future__ import annotations

import typer

from toolkit.cli.cmd_run import register as register_run
from toolkit.cli.cmd_profile import register as register_profile
from toolkit.cli.cmd_resume import register as register_resume
from toolkit.cli.cmd_scout_url import register as register_scout_url
from toolkit.cli.cmd_status import register as register_status
from toolkit.cli.cmd_validate import register as register_validate
from toolkit.cli.cmd_inspect import register as register_inspect
from toolkit.cli.cmd_scaffold import register as register_scaffold
from toolkit.cli.cmd_batch import register as register_batch

app = typer.Typer(no_args_is_help=True, add_completion=False)

# registra comandi
register_run(app)
register_profile(app)
register_resume(app)
register_scout_url(app)
register_status(app)
register_validate(app)
register_inspect(app)
register_scaffold(app)
register_batch(app)


def main():
    app()


if __name__ == "__main__":
    main()
