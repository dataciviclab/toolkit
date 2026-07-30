"""inspect — ispeziona un dataset.

``toolkit inspect`` (default) → riassunto stato (ex summary).
Subcomandi: config, runs.
"""

from __future__ import annotations

import typer


def register(app: typer.Typer) -> None:
    """Register ``toolkit inspect`` e subcomandi."""
    from toolkit.cli.inspect.config_ops import config as _config
    from toolkit.cli.inspect.summary_ops import summary as _summary
    from toolkit.cli.inspect.runs_ops import runs as _runs

    inspect_app = typer.Typer(no_args_is_help=False, add_completion=False)

    @inspect_app.callback(invoke_without_command=True)
    def inspect_default(
        ctx: typer.Context,
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        year: int | None = typer.Option(None, "--year", "-y", help="Dataset year"),
        json_output: bool = typer.Option(False, "--json", help="Output JSON"),
    ):
        """Mostra lo stato del dataset (riassunto)."""
        if ctx.invoked_subcommand is not None:
            return
        try:
            _summary(
                config=config,
                year=year,
                dataset=None,
                run_id=None,
                latest=True,
                as_json=json_output,
            )
        except FileNotFoundError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1)

    @inspect_app.command("config")
    def inspect_config(
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        layer: str = typer.Option("clean", "--layer", "-l", help="Layer: raw, clean, mart"),
        mode: str = typer.Option(
            "schema", "--mode", "-m", help="Modalità: schema, preview, profile, sql"
        ),
        year: int = typer.Option(0, "--year", "-y", help="Anno"),
        sql: str | None = typer.Option(None, "--sql", help="SQL query (mode=sql)"),
        limit: int = typer.Option(20, "--limit", help="Max righe (mode=preview/sql)"),
        mart_index: int = typer.Option(0, "--mart-index", help="Indice tabella mart"),
        diff: bool = typer.Option(False, "--diff", help="Schema-diff RAW tra anni"),
        json_output: bool = typer.Option(False, "--json", help="Output JSON"),
    ):
        """Ispeziona configurazione e dati: schema, preview, profile, SQL o diff."""
        _config(
            config_path=config,
            layer=layer,
            mode=mode,
            year=year,
            sql=sql,
            limit=limit,
            mart_index=mart_index,
            diff=diff,
            json_output=json_output,
        )

    @inspect_app.command("runs")
    def inspect_runs(
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        year: int | None = typer.Option(None, "--year", "-y", help="Dataset year"),
        resume: bool = typer.Option(False, "--resume", help="Riprendi run fallito"),
        run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
        from_layer: str | None = typer.Option(
            None, "--from-layer", help="Forza ripartenza raw|clean|mart"
        ),
        limit: int = typer.Option(10, "--limit", help="Max runs"),
        json_output: bool = typer.Option(False, "--json", help="Output JSON"),
    ):
        """Mostra storico run o riprende un run fallito."""
        _runs(
            config=config,
            year=year,
            resume=resume,
            run_id=run_id,
            from_layer=from_layer,
            limit=limit,
            json_output=json_output,
        )

    app.add_typer(
        inspect_app,
        name="inspect",
        help="Ispeziona un dataset: stato, schema, storico run.",
    )
