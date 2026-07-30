"""inspect — comando unico per ispezionare un dataset.

``toolkit inspect`` (default) → riassunto stato (ex summary).
Flag di modo: --schema, --preview, --profile, --diff, --runs, --resume.

Backward compat: i vecchi subcomandi (config, summary, runs, paths, profile)
restano funzionanti con deprecation warning.
"""

from __future__ import annotations

import typer


def _deprecated_subcommand(old_name: str, hint: str):
    """Factory: produce una funzione Typer che mostra deprecation e delega."""

    def wrapper(*args, **kwargs):
        typer.echo(
            f"⚠️  'inspect {old_name}' è deprecato, usa '{hint}'",
            err=True,
        )
        # Dopo il warning, esegue il comportamento originale
        # (la funzione originale fa tutto via Typer, ma qui possiamo
        # solo mostrare il warning — il comando è già stato avviato)

    return wrapper


def register(app: typer.Typer) -> None:
    """Register ``toolkit inspect`` (unico comando con flag di modo)."""
    from toolkit.cli.inspect.config_ops import config as _config
    from toolkit.cli.inspect.summary_ops import summary as _summary
    from toolkit.cli.inspect.runs_ops import runs as _runs

    inspect_app = typer.Typer(no_args_is_help=False, add_completion=False)

    @inspect_app.callback(invoke_without_command=True)
    def inspect_cmd(
        ctx: typer.Context,
        # Common options
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        year: int | None = typer.Option(None, "--year", "-y", help="Dataset year"),
        json_output: bool = typer.Option(False, "--json", help="Output JSON"),
        # Mode flags
        schema: bool = typer.Option(False, "--schema", help="Mostra schema colonne"),
        preview: bool = typer.Option(False, "--preview", help="Anteprima dati"),
        profile: bool = typer.Option(False, "--profile", help="Profilo raw (encoding/delim)"),
        diff: bool = typer.Option(False, "--diff", help="Schema-diff RAW tra anni"),
        runs: bool = typer.Option(False, "--runs", help="Mostra storico run"),
        resume: bool = typer.Option(False, "--resume", help="Riprendi run fallito"),
        # Config-specific
        layer: str = typer.Option("clean", "--layer", "-l", help="Layer: raw, clean, mart"),
        sql: str | None = typer.Option(None, "--sql", help="SQL query (con --schema)"),
        limit: int = typer.Option(20, "--limit", help="Max righe (con --preview o --sql)"),
        # Runs-specific
        run_id: str | None = typer.Option(None, "--run-id", help="Specific run id (con --runs)"),
        from_layer: str | None = typer.Option(
            None, "--from-layer", help="Forza ripartenza raw|clean|mart (con --resume)"
        ),
    ):
        """Ispeziona un dataset: stato, schema, dati, storico run.

        Di default mostra il riassunto dello stato del dataset.
        Usa i flag --schema, --preview, --profile, --diff, --runs o --resume
        per cambiare modalità.
        """
        if ctx.invoked_subcommand is not None:
            return

        # Determina modalità
        if schema:
            _config(
                config_path=config,
                layer=layer,
                mode="schema",
                year=year or 0,
                sql=sql,
                limit=limit,
                mart_index=0,
                diff=False,
                json_output=json_output,
            )
        elif preview:
            _config(
                config_path=config,
                layer=layer,
                mode="preview",
                year=year or 0,
                sql=sql,
                limit=limit,
                mart_index=0,
                diff=False,
                json_output=json_output,
            )
        elif profile:
            _config(
                config_path=config,
                layer="raw",
                mode="profile",
                year=year or 0,
                sql=sql,
                limit=limit,
                mart_index=0,
                diff=False,
                json_output=json_output,
            )
        elif diff:
            _config(
                config_path=config,
                layer=layer,
                mode="schema",
                year=year or 0,
                sql=sql,
                limit=limit,
                mart_index=0,
                diff=True,
                json_output=json_output,
            )
        elif runs:
            _runs(
                config=config,
                year=year,
                resume=False,
                run_id=run_id,
                from_layer=None,
                limit=limit,
                json_output=json_output,
            )
        elif resume:
            _runs(
                config=config,
                year=year,
                resume=True,
                run_id=run_id,
                from_layer=from_layer,
                limit=limit,
                json_output=json_output,
            )
        else:
            # Default: summary
            _summary(
                config=config,
                year=year,
                dataset=None,
                run_id=run_id,
                latest=(run_id is None),
                as_json=json_output,
            )

    # ── Backward compat: subcomandi deprecati ──────────────────────────

    @inspect_app.command("summary", hidden=True)
    def summary_deprecated(
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        year: int | None = typer.Option(None, "--year", "-y", help="Dataset year"),
        dataset: str | None = typer.Option(None, "--dataset", help="Dataset name (auto-da-config)"),
        run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
        latest: bool = typer.Option(False, "--latest", help="Show latest run"),
        as_json: bool = typer.Option(False, "--json", help="Output JSON"),
    ):
        """⚠️ Deprecato: usa 'toolkit inspect' (default)."""
        if not as_json:
            typer.echo("⚠️  'inspect summary' è deprecato, usa 'toolkit inspect'", err=True)
        _summary(
            config=config, year=year, dataset=dataset, run_id=run_id, latest=latest, as_json=as_json
        )

    @inspect_app.command("config", hidden=True)
    def config_deprecated(
        config_path: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        layer: str = typer.Option("clean", "--layer", "-l", help="Layer: raw, clean, mart"),
        mode: str = typer.Option(
            "schema", "--mode", "-m", help="Modalità: schema, preview, profile, sql"
        ),
        year: int = typer.Option(0, "--year", "-y", help="Anno"),
        sql: str | None = typer.Option(None, "--sql", help="SQL query"),
        limit: int = typer.Option(20, "--limit", help="Max righe"),
        mart_index: int = typer.Option(0, "--mart-index", help="Indice tabella mart"),
        diff: bool = typer.Option(False, "--diff", help="Schema-diff RAW"),
        json_output: bool = typer.Option(False, "--json", help="Output JSON"),
    ):
        """⚠️ Deprecato: usa 'toolkit inspect --schema|--preview|--profile|--diff'."""
        if not json_output:
            hint = {
                "schema": "--schema",
                "preview": "--preview",
                "profile": "--profile",
                "sql": "--schema --sql",
            }.get(mode, f"--{mode}")
            typer.echo(f"⚠️  'inspect config' è deprecato, usa 'toolkit inspect {hint}'", err=True)
        _config(
            config_path=config_path,
            layer=layer,
            mode=mode,
            year=year,
            sql=sql,
            limit=limit,
            mart_index=mart_index,
            diff=diff,
            json_output=json_output,
        )

    @inspect_app.command("runs", hidden=True)
    def runs_deprecated(
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        year: int | None = typer.Option(None, "--year", "-y", help="Dataset year"),
        resume: bool = typer.Option(False, "--resume", help="Resume latest/failed run"),
        run_id: str | None = typer.Option(None, "--run-id", help="Specific run id"),
        from_layer: str | None = typer.Option(None, "--from-layer", help="Force restart layer"),
        limit: int = typer.Option(10, "--limit", help="Max runs da elencare"),
        json_output: bool = typer.Option(False, "--json", help="Output JSON"),
    ):
        """⚠️ Deprecato: usa 'toolkit inspect --runs' o '--resume'."""
        if not json_output:
            hint = "--resume" if resume else "--runs"
            typer.echo(f"⚠️  'inspect runs' è deprecato, usa 'toolkit inspect {hint}'", err=True)
        _runs(
            config=config,
            year=year,
            resume=resume,
            run_id=run_id,
            from_layer=from_layer,
            limit=limit,
            json_output=json_output,
        )

    @inspect_app.command("paths", hidden=True)
    def paths_deprecated(
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        year: int | None = typer.Option(None, "--year", help="Dataset year"),
        as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
    ):
        """⚠️ Deprecato: usa 'toolkit inspect'."""
        if not as_json:
            typer.echo("⚠️  'inspect paths' è deprecato, usa 'toolkit inspect'", err=True)
        from toolkit.cli.inspect.paths_ops import paths as _paths

        _paths(config=config, year=year, as_json=as_json)

    @inspect_app.command("profile", hidden=True)
    def profile_deprecated(
        config: str | None = typer.Option(
            None, "--config", "-c", help="Path or slug to dataset.yml"
        ),
        csv_path: str | None = typer.Option(None, "--csv-path", help="CSV file to preview"),
        year: int | None = typer.Option(None, "--year", "-y", help="Dataset year"),
        years: str | None = typer.Option(None, "--years", help="Comma-separated years"),
        json_output: bool = typer.Option(False, "--json", help="Output JSON"),
    ):
        """⚠️ Deprecato: usa 'toolkit inspect --profile'."""
        from toolkit.cli.inspect.profile_ops import profile as _profile

        if not json_output:
            typer.echo(
                "⚠️  'inspect profile' è deprecato, usa 'toolkit inspect --profile'", err=True
            )
        _profile(config=config, csv_path=csv_path, year=year, years=years, json_output=json_output)

    app.add_typer(
        inspect_app,
        name="inspect",
        help="Ispeziona un dataset: stato, schema, dati, storico run.",
    )
