from __future__ import annotations

import typer

from toolkit.cli.cmd_run import register as register_run
from toolkit.cli.cmd_profile import register as register_profile
from toolkit.cli.cmd_resume import register as register_resume
from toolkit.cli.cmd_status import register as register_status
from toolkit.cli.cmd_validate import register as register_validate
from toolkit.cli.cmd_gen_sql import register as register_gen_sql
from toolkit.cli.cmd_inspect import register as register_inspect

app = typer.Typer(no_args_is_help=True, add_completion=False)

# registra comandi
register_run(app)
register_profile(app)
register_resume(app)
register_status(app)
register_validate(app)
register_gen_sql(app)
register_inspect(app)

def main():
    app()

if __name__ == "__main__":
    main()
