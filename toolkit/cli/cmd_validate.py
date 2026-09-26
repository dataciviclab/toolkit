"""``toolkit validate`` — standalone validation of existing layer outputs."""

from __future__ import annotations

from typing import Any

import typer

from toolkit.cli.common import load_cfg_and_logger
from toolkit.domain.common import iter_selected_years


def validate(
    config: str | None = typer.Option(None, "--config", "-c", help="Path or slug to dataset.yml"),
    year: int | None = typer.Option(None, "--year", "-y", help="Single year to validate"),
    years: str | None = typer.Option(None, "--years", help="Comma-separated years"),
    layer: str | None = typer.Option(
        None, "--layer", "-l", help="Layer to validate (raw, clean, mart). Default: all available"
    ),
    json_output: bool = typer.Option(False, "--json", help="Output JSON report"),
):
    """Validate existing layer outputs without re-running the pipeline.

    Reads parquet/metadata from disk and applies validation rules defined
    in dataset.yml (required_columns, primary_key, not_null, ranges, etc.).

    Examples::

        toolkit validate -c dataset.yml -y 2024
        toolkit validate -c dataset.yml --layer clean
        toolkit validate -c dataset.yml --years 2022,2023,2024 --json
    """
    cfg, logger = load_cfg_and_logger(config, quiet=json_output)
    years_arg = years if isinstance(years, str) else None
    year_arg = year if isinstance(year, int) else None
    selected_years = iter_selected_years(cfg, year_arg=year_arg, years_arg=years_arg)

    layers_to_validate = _resolve_layers(layer, cfg)
    results: dict[str, Any] = {
        "dataset": cfg.dataset,
        "config": str(config) if config else None,
        "years": selected_years,
        "layers": layers_to_validate,
        "results": {},
        "status": "passed",
    }

    for y in selected_years:
        year_results: dict[str, Any] = {}
        for layer_name in layers_to_validate:
            summary = _run_layer_validation(layer_name, cfg, y, logger)
            year_results[layer_name] = summary
            if not summary.get("passed", False):
                results["status"] = "failed"
        results["results"][str(y)] = year_results

    if json_output:
        from toolkit.cli.common import echo_json

        echo_json(results)
    else:
        _print_report(results)

    if results["status"] != "passed":
        raise typer.Exit(code=1)


def _resolve_layers(layer: str | None, cfg) -> list[str]:
    """Resolve which layers to validate."""
    if layer:
        valid = {"raw", "clean", "mart"}
        if layer not in valid:
            raise typer.BadParameter(
                f"Invalid layer: {layer}. Must be one of: {', '.join(sorted(valid))}"
            )
        return [layer]

    layers = ["raw"]
    if not cfg.is_mart_only:
        layers.append("clean")
    if cfg.mart.tables:
        layers.append("mart")
    return layers


def _run_layer_validation(layer_name: str, cfg, year: int, logger) -> dict[str, Any]:
    """Run validation for a single layer. Returns validation summary."""
    try:
        if layer_name == "raw":
            from toolkit.raw.validate import run_raw_validation

            return run_raw_validation(cfg.root, cfg.dataset, year, logger)
        elif layer_name == "clean":
            from toolkit.clean.validate import run_clean_validation

            return run_clean_validation(cfg, year, logger)
        elif layer_name == "mart":
            from toolkit.mart.validate import run_mart_validation

            return run_mart_validation(cfg, year, logger)
    except FileNotFoundError as exc:
        return {
            "passed": False,
            "errors_count": 1,
            "warnings_count": 0,
            "quality_score": None,
            "quality_verdict": "missing",
            "errors": [str(exc)],
            "warnings": [],
            "checks": [],
            "summary": {"dir": "not found"},
        }
    except Exception as exc:
        return {
            "passed": False,
            "errors_count": 1,
            "warnings_count": 0,
            "quality_score": None,
            "quality_verdict": "error",
            "errors": [f"Validation error: {exc}"],
            "warnings": [],
            "checks": [],
            "summary": {},
        }
    raise ValueError(f"Unknown layer: {layer_name}")


def _print_report(results: dict[str, Any]) -> None:
    """Print human-readable validation report with errors/warnings inline."""
    status_icon = "✅" if results["status"] == "passed" else "🔴"
    typer.echo(f"{status_icon} Validate: {results['dataset']}")
    typer.echo(f"   years: {', '.join(str(y) for y in results['years'])}")
    typer.echo(f"   layers: {', '.join(results['layers'])}")
    typer.echo("")

    for year_str, year_results in results["results"].items():
        typer.echo(f"  Year {year_str}:")
        for layer_name, summary in year_results.items():
            _print_layer_result(layer_name, summary)
        typer.echo("")


def _print_layer_result(layer_name: str, summary: dict[str, Any]) -> None:
    """Print validation result for a single layer."""
    passed = summary.get("passed", False)
    icon = "✅" if passed else "🔴"
    score = summary.get("quality_score")
    score_str = f"  qs={score}" if score is not None else ""

    # Header line
    typer.echo(f"    {layer_name}:{score_str}  {icon}")

    # Errors — always visible when present
    for err in summary.get("errors", []):
        typer.echo(f"      ✗ {err}")

    # Warnings — always visible when present
    for warn in summary.get("warnings", []):
        typer.echo(f"      ⚠ {warn}")

    # Layer-specific summary details
    data = summary.get("summary", {})
    if layer_name == "raw":
        _print_raw_summary(data)
    elif layer_name == "clean":
        _print_clean_summary(data)
    elif layer_name == "mart":
        _print_mart_summary(data)

    # Transition (clean→mart)
    transition = (summary.get("sections") or {}).get("transition") or {}
    _print_transition(transition)


def _print_raw_summary(data: dict[str, Any]) -> None:
    """Print raw layer summary: file, encoding, delim."""
    parts = []
    if data.get("file"):
        parts.append(data["file"])
    if data.get("encoding"):
        parts.append(f"enc={data['encoding']}")
    if data.get("delim"):
        parts.append(f"delim={data['delim']}")
    file_count = data.get("files")
    if file_count is not None:
        parts.append(f"{file_count} file(s)")
    total_bytes = data.get("total_bytes")
    if total_bytes is not None:
        parts.append(f"{total_bytes:,} bytes")
    if parts:
        typer.echo(f"      {'  '.join(parts)}")


def _print_clean_summary(data: dict[str, Any]) -> None:
    """Print clean layer summary: rows, cols, transition stats."""
    stats = data.get("stats") or {}
    raw_rows = stats.get("raw_rows")
    clean_rows = stats.get("clean_rows")
    row_drop = stats.get("row_drop_pct")
    clean_cols = stats.get("clean_cols")

    parts = []
    if clean_rows is not None:
        parts.append(f"{clean_rows:,} righe")
    if clean_cols is not None:
        parts.append(f"{clean_cols} colonne")
    if raw_rows is not None and clean_rows is not None and row_drop is not None:
        if row_drop > 0:
            parts.append(f"raw→clean: -{row_drop}% righe")
        else:
            parts.append("raw→clean: 0% righe")
    if parts:
        typer.echo(f"      {'  '.join(parts)}")

    # Rules applied
    rules = data.get("rules") or {}
    rule_parts = []
    if rules.get("required"):
        rule_parts.append(f"required={len(rules['required'])}")
    if rules.get("not_null"):
        rule_parts.append(f"not_null={len(rules['not_null'])}")
    if rules.get("primary_key"):
        rule_parts.append(f"pk={rules['primary_key']}")
    if rules.get("min_rows"):
        rule_parts.append(f"min_rows={rules['min_rows']}")
    if rule_parts:
        typer.echo(f"      rules: {', '.join(rule_parts)}")


def _print_mart_summary(data: dict[str, Any]) -> None:
    """Print mart layer summary: tables, row counts, rules."""
    tables = data.get("tables") or []
    required = data.get("required_tables") or []
    declared = data.get("declared_tables") or []
    row_counts = data.get("row_counts") or {}
    per_table = data.get("per_table") or {}

    # Tables status
    if declared:
        found_names = set(tables)
        required_names = set(required)
        missing = required_names - found_names
        extra = set(declared) - found_names
        typer.echo(f"      tabelle: {len(tables)}/{len(declared)} trovate")
        if missing:
            typer.echo(f"        mancanti: {', '.join(sorted(missing))}")
        if extra:
            typer.echo(f"        non prodotte: {', '.join(sorted(extra))}")

    # Row counts per table
    if row_counts:
        for tname, count in row_counts.items():
            min_rows = (per_table.get(tname, {}).get("rules") or {}).get("min_rows")
            status = ""
            if min_rows is not None and count < min_rows:
                status = f"  ✗ min_rows={min_rows}"
            typer.echo(f"        {tname}: {count:,} righe{status}")


def _print_transition(transition: dict[str, Any]) -> None:
    """Print clean→mart transition info if available."""
    if not transition or not transition.get("enabled"):
        return
    row_drop = transition.get("row_drop_pct")
    removed = transition.get("removed_columns") or []
    parts = []
    if row_drop is not None:
        parts.append(f"row_drop={row_drop}%")
    if removed:
        parts.append(f"removed_cols={len(removed)}")
    if parts:
        typer.echo(f"      transition: {', '.join(parts)}")


def register(app: typer.Typer) -> None:
    """Register ``toolkit validate`` command."""
    app.command("validate")(validate)
