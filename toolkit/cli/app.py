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
from toolkit.cli.cmd_blocker_hints import register as register_blocker_hints
from toolkit.cli.cmd_init import register as register_init

app = typer.Typer(no_args_is_help=True, add_completion=False)

# registra comandi
register_run(app)
register_profile(app)
register_resume(app)
register_status(app)
register_validate(app)
register_inspect(app)
register_scaffold(app)
register_batch(app)
register_blocker_hints(app)
register_init(app)


def main():
    app()


if __name__ == "__main__":
    main()
