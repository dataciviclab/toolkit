from __future__ import annotations

import typer

from toolkit.cli.cmd_run import register as register_run
from toolkit.cli.cmd_profile import register as register_profile
from toolkit.cli.cmd_validate import register as register_validate
from toolkit.cli.cmd_gen_sql import register as register_gen_sql

app = typer.Typer(no_args_is_help=True, add_completion=False)

# registra comandi
register_run(app)
register_profile(app)
register_validate(app)
register_gen_sql(app)

def main():
    app()

if __name__ == "__main__":
    main()