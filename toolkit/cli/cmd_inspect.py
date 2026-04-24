from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests
import typer

from toolkit.cli.common import format_profile_preview, iter_years, load_layer_profile_summaries
from toolkit.cli.cmd_scout_url import (  # noqa: F401 — re-exported for compatibility
    _EXTENDED_EXTENSIONS,
    _MAX_PRINTED_LINKS,
    _candidate_links,
    _DEFAULT_TIMEOUT,
    _DEFAULT_USER_AGENT,
    _detect_ckan,
    _discover_ckan_resources,
    _extract_ckan_dataset_id,
    _generate_yaml_scaffold,
    _is_file_like,
    _is_html,
    probe_url,
)
from toolkit.core.config import load_config
from toolkit.core.metadata import read_layer_metadata
from toolkit.core.paths import layer_year_dir
from toolkit.core.support import resolve_support_payloads
from toolkit.profile.raw import build_profile_hints
from toolkit.core.run_context import get_run_dir, latest_run


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _raw_primary_file(raw_dir: Path, metadata: dict[str, Any]) -> Path | None:
    primary_output_file = metadata.get("primary_output_file")
    if isinstance(primary_output_file, str):
        candidate = raw_dir / primary_output_file
        if candidate.exists():
            return candidate
    return None


def _raw_schema_payload(cfg, year: int) -> dict[str, Any]:
    root = Path(cfg.root)
    raw_dir = layer_year_dir(root, "raw", cfg.dataset, year)
    raw_meta = read_layer_metadata(raw_dir)
    primary_file = _raw_primary_file(raw_dir, raw_meta)

    profile_hints = raw_meta.get("profile_hints") or {}
    profile_source = "metadata" if profile_hints else None
    sniff_error: str | None = None

    if not profile_hints and primary_file is not None:
        try:
            profile_hints = build_profile_hints(primary_file)
            profile_source = "sniff"
        except Exception as exc:
            sniff_error = f"{type(exc).__name__}: {exc}"

    columns_preview = profile_hints.get("columns_preview") or []
    warnings = list(profile_hints.get("warnings") or [])
    if sniff_error is not None:
        warnings.append(f"profile_hint_fallback_failed: {sniff_error}")

    return {
        "year": year,
        "raw_dir": str(raw_dir),
        "raw_exists": raw_dir.exists(),
        "primary_output_file": raw_meta.get("primary_output_file"),
        "file_used": profile_hints.get("file_used"),
        "profile_source": profile_source,
        "encoding": profile_hints.get("encoding_suggested"),
        "delim": profile_hints.get("delim_suggested"),
        "decimal": profile_hints.get("decimal_suggested"),
        "skip": profile_hints.get("skip_suggested"),
        "header_line": profile_hints.get("header_line"),
        "columns_count": len(columns_preview),
        "columns_preview": columns_preview,
        "warnings": warnings,
    }


def _compare_schema_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    comparisons: list[dict[str, Any]] = []
    for previous, current in zip(entries, entries[1:]):
        previous_columns = set(previous.get("columns_preview") or [])
        current_columns = set(current.get("columns_preview") or [])
        comparisons.append(
            {
                "from_year": previous["year"],
                "to_year": current["year"],
                "from_columns_count": previous.get("columns_count") or 0,
                "to_columns_count": current.get("columns_count") or 0,
                "added_columns": sorted(current_columns - previous_columns),
                "removed_columns": sorted(previous_columns - current_columns),
                "changed": previous_columns != current_columns,
            }
        )
    return comparisons


def _raw_output_paths(root: Path, dataset: str, year: int) -> dict[str, str]:
    raw_dir = layer_year_dir(root, "raw", dataset, year)
    return {
        "dir": str(raw_dir),
        "metadata": str(raw_dir / "metadata.json"),
        "manifest": str(raw_dir / "manifest.json"),
        "validation": str(raw_dir / "raw_validation.json"),
    }


def _clean_output_path(root: Path, dataset: str, year: int) -> Path:
    return layer_year_dir(root, "clean", dataset, year) / f"{dataset}_{year}_clean.parquet"


def _clean_paths(root: Path, dataset: str, year: int) -> dict[str, str]:
    clean_dir = layer_year_dir(root, "clean", dataset, year)
    return {
        "dir": str(clean_dir),
        "output": str(_clean_output_path(root, dataset, year)),
        "metadata": str(clean_dir / "metadata.json"),
        "manifest": str(clean_dir / "manifest.json"),
        "validation": str(clean_dir / "_validate" / "clean_validation.json"),
    }


def _mart_output_paths(root: Path, year_dir: Path, tables: list[dict[str, Any]]) -> list[Path]:
    return [
        year_dir / f"{table['name']}.parquet"
        for table in tables
        if isinstance(table, dict) and table.get("name")
    ]


def _mart_paths(
    root: Path, dataset: str, year: int, tables: list[dict[str, Any]]
) -> dict[str, Any]:
    mart_dir = layer_year_dir(root, "mart", dataset, year)
    return {
        "dir": str(mart_dir),
        "outputs": [str(path) for path in _mart_output_paths(root, mart_dir, tables)],
        "metadata": str(mart_dir / "metadata.json"),
        "manifest": str(mart_dir / "manifest.json"),
        "validation": str(mart_dir / "_validate" / "mart_validation.json"),
    }


def _payload_for_year(cfg, year: int) -> dict[str, Any]:
    root = Path(cfg.root)
    run_dir = get_run_dir(root, cfg.dataset, year)
    mart_tables = cfg.mart.get("tables") or []
    raw_dir = layer_year_dir(root, "raw", cfg.dataset, year)
    raw_meta = read_layer_metadata(raw_dir)
    suggested_read_path = raw_dir / "_profile" / "suggested_read.yml"
    profile_hints = raw_meta.get("profile_hints") or {}

    latest_payload: dict[str, Any] | None = None
    try:
        latest_record = latest_run(run_dir)
        latest_payload = {
            "run_id": latest_record.get("run_id"),
            "status": latest_record.get("status"),
            "started_at": latest_record.get("started_at"),
            "path": str(run_dir / f"{latest_record.get('run_id')}.json"),
        }
    except FileNotFoundError:
        latest_payload = None

    return {
        "dataset": cfg.dataset,
        "year": year,
        "config_path": str(cfg.base_dir / "dataset.yml"),
        "root": str(root),
        "paths": {
            "raw": _raw_output_paths(root, cfg.dataset, year),
            "clean": _clean_paths(root, cfg.dataset, year),
            "mart": _mart_paths(root, cfg.dataset, year, mart_tables),
            "support": resolve_support_payloads(cfg.support, require_exists=False),
            "run_dir": str(run_dir),
        },
        "raw_hints": {
            "primary_output_file": raw_meta.get("primary_output_file"),
            "suggested_read_path": str(suggested_read_path),
            "suggested_read_exists": suggested_read_path.exists(),
            "encoding": profile_hints.get("encoding_suggested"),
            "delim": profile_hints.get("delim_suggested"),
            "decimal": profile_hints.get("decimal_suggested"),
            "skip": profile_hints.get("skip_suggested"),
            "warnings": profile_hints.get("warnings") or [],
        },
        "layer_profiles": load_layer_profile_summaries(root, cfg.dataset, year),
        "latest_run": latest_payload,
    }


def paths(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    year: int | None = typer.Option(None, "--year", help="Dataset year"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output for notebooks/scripts"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Mostra i path stabili di output e l'ultimo run record per dataset/year.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg = load_config(config, strict_config=strict_config_flag)
    years = iter_years(cfg, year)
    payload = [_payload_for_year(cfg, selected_year) for selected_year in years]

    if as_json:
        typer.echo(
            json.dumps(payload if len(payload) > 1 else payload[0], indent=2, ensure_ascii=False)
        )
        return

    for item in payload:
        typer.echo(f"dataset: {item['dataset']}")
        typer.echo(f"year: {item['year']}")
        typer.echo(f"config_path: {item['config_path']}")
        typer.echo(f"root: {item['root']}")
        typer.echo(f"raw_dir: {item['paths']['raw']['dir']}")
        typer.echo(f"raw_metadata: {item['paths']['raw']['metadata']}")
        typer.echo(f"raw_manifest: {item['paths']['raw']['manifest']}")
        typer.echo(f"raw_validation: {item['paths']['raw']['validation']}")
        typer.echo("raw_hints:")
        typer.echo(f"  - primary_output_file: {item['raw_hints']['primary_output_file']}")
        typer.echo(f"  - suggested_read_exists: {item['raw_hints']['suggested_read_exists']}")
        typer.echo(f"  - suggested_read_path: {item['raw_hints']['suggested_read_path']}")
        typer.echo(f"  - encoding: {item['raw_hints']['encoding']}")
        typer.echo(f"  - delim: {item['raw_hints']['delim']}")
        typer.echo(f"  - decimal: {item['raw_hints']['decimal']}")
        typer.echo(f"  - skip: {item['raw_hints']['skip']}")
        if item["raw_hints"]["warnings"]:
            typer.echo("  - warnings:")
            for warning in item["raw_hints"]["warnings"]:
                typer.echo(f"    - {warning}")
        if item["layer_profiles"]:
            typer.echo("layer_profiles:")
            clean_output = item["layer_profiles"].get("clean_output")
            if clean_output is not None:
                typer.echo(f"  clean_output: {format_profile_preview(clean_output)}")
            mart_clean_input = item["layer_profiles"].get("mart_clean_input")
            if mart_clean_input is not None:
                typer.echo(f"  mart_clean_input: {format_profile_preview(mart_clean_input)}")
            mart_tables = item["layer_profiles"].get("mart_tables") or []
            if mart_tables:
                typer.echo("  mart_tables:")
                for table in mart_tables:
                    typer.echo(f"    {table['name']}: {format_profile_preview(table)}")
            transitions = item["layer_profiles"].get("clean_to_mart") or []
            if transitions:
                typer.echo("  clean_to_mart:")
                for transition in transitions:
                    typer.echo(
                        f"    {transition['target_name']}: "
                        f"rows {transition['source_row_count']} -> {transition['target_row_count']} "
                        f"added={len(transition['added_columns'])} "
                        f"removed={len(transition['removed_columns'])} "
                        f"type_changes={transition['type_change_count']}"
                    )
        typer.echo(f"clean_dir: {item['paths']['clean']['dir']}")
        typer.echo(f"clean_output: {item['paths']['clean']['output']}")
        typer.echo(f"clean_manifest: {item['paths']['clean']['manifest']}")
        typer.echo(f"clean_metadata: {item['paths']['clean']['metadata']}")
        typer.echo(f"clean_validation: {item['paths']['clean']['validation']}")
        typer.echo(f"mart_dir: {item['paths']['mart']['dir']}")
        typer.echo("mart_outputs:")
        for output in item["paths"]["mart"]["outputs"]:
            typer.echo(f"  - {output}")
        typer.echo(f"mart_manifest: {item['paths']['mart']['manifest']}")
        typer.echo(f"mart_metadata: {item['paths']['mart']['metadata']}")
        typer.echo(f"mart_validation: {item['paths']['mart']['validation']}")
        if item["paths"]["support"]:
            typer.echo("support:")
            for support in item["paths"]["support"]:
                typer.echo(f"  - name: {support['name']}")
                typer.echo(f"    dataset: {support['dataset']}")
                typer.echo(f"    config_path: {support['config_path']}")
                typer.echo(
                    f"    years: {', '.join(str(year_value) for year_value in support['years'])}"
                )
                typer.echo(f"    mart: {support['mart']}")
                typer.echo("    outputs:")
                for output in support["outputs"]:
                    typer.echo(f"      - {output}")
        typer.echo(f"run_dir: {item['paths']['run_dir']}")
        latest_info = item.get("latest_run")
        if latest_info is None:
            typer.echo("latest_run: none")
        else:
            typer.echo(f"latest_run_id: {latest_info['run_id']}")
            typer.echo(f"latest_run_status: {latest_info['status']}")
            typer.echo(f"latest_run_record: {latest_info['path']}")
        typer.echo("")


def schema_diff(
    config: str = typer.Option(..., "--config", "-c", help="Path to dataset.yml"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
    strict_config: bool = typer.Option(
        False, "--strict-config", help="Treat deprecated config forms as errors"
    ),
):
    """
    Confronta i principali segnali di schema RAW tra gli anni configurati.
    """
    strict_config_flag = strict_config if isinstance(strict_config, bool) else False
    cfg = load_config(config, strict_config=strict_config_flag)
    entries = [_raw_schema_payload(cfg, selected_year) for selected_year in iter_years(cfg, None)]
    comparisons = _compare_schema_entries(entries)
    payload = {
        "dataset": cfg.dataset,
        "config_path": str(cfg.base_dir / "dataset.yml"),
        "years": [entry["year"] for entry in entries],
        "entries": entries,
        "comparisons": comparisons,
    }

    if as_json:
        typer.echo(json.dumps(payload, indent=2, ensure_ascii=False))
        return

    typer.echo(f"dataset: {payload['dataset']}")
    typer.echo(f"config_path: {payload['config_path']}")
    typer.echo(f"years: {', '.join(str(year) for year in payload['years'])}")
    typer.echo("")

    for entry in entries:
        typer.echo(f"year: {entry['year']}")
        typer.echo(f"  raw_exists: {entry['raw_exists']}")
        typer.echo(f"  raw_dir: {entry['raw_dir']}")
        typer.echo(f"  primary_output_file: {entry['primary_output_file']}")
        typer.echo(f"  profile_source: {entry['profile_source']}")
        typer.echo(f"  encoding: {entry['encoding']}")
        typer.echo(f"  delim: {entry['delim']}")
        typer.echo(f"  decimal: {entry['decimal']}")
        typer.echo(f"  skip: {entry['skip']}")
        typer.echo(f"  columns_count: {entry['columns_count']}")
        typer.echo(f"  header_line: {entry['header_line']}")
        if entry["columns_preview"]:
            typer.echo("  columns_preview:")
            for column in entry["columns_preview"]:
                typer.echo(f"    - {column}")
        if entry["warnings"]:
            typer.echo("  warnings:")
            for warning in entry["warnings"]:
                typer.echo(f"    - {warning}")
        typer.echo("")

    if comparisons:
        typer.echo("comparisons:")
        for comparison in comparisons:
            typer.echo(f"  {comparison['from_year']} -> {comparison['to_year']}:")
            typer.echo(
                f"    counts: {comparison['from_columns_count']} -> {comparison['to_columns_count']}"
            )
            typer.echo(f"    changed: {comparison['changed']}")
            if comparison["added_columns"]:
                typer.echo("    added_columns:")
                for column in comparison["added_columns"]:
                    typer.echo(f"      - {column}")
            if comparison["removed_columns"]:
                typer.echo("    removed_columns:")
                for column in comparison["removed_columns"]:
                    typer.echo(f"      - {column}")
    else:
        typer.echo("comparisons: none")


def url(
    url: str = typer.Argument(..., help="URL da ispezionare"),
    scaffold: bool = typer.Option(False, "--scaffold", help="Genera scaffold YAML (blocchi dataset + raw)"),
    timeout: int = typer.Option(_DEFAULT_TIMEOUT, "--timeout", min=1, help="Timeout HTTP in secondi"),
    user_agent: str = typer.Option(_DEFAULT_USER_AGENT, "--user-agent"),
    as_json: bool = typer.Option(False, "--json", help="Emit JSON output"),
) -> None:
    """
    Ispeziona un URL per dataset scouting: probe HTTP e generazione scaffold YAML.
    """
    try:
        result = probe_url(url, timeout=timeout, user_agent=user_agent, capture_html=scaffold)
    except requests.RequestException as exc:
        typer.echo(f"error: {type(exc).__name__}: {exc}")
        raise typer.Exit(code=1) from exc

    if scaffold:
        ckan_resources: list[dict[str, Any]] | None = None
        candidate_file_links: list[str] | None = None

        if result["kind"] == "html":
            html_content = result.get("html_content", b"")
            html_text = html_content.decode("utf-8", errors="replace") if html_content else ""
            dataset_id = _extract_ckan_dataset_id(result["final_url"], html_text)
            is_ckan = _detect_ckan(html_content) if html_content else False

            if dataset_id and html_content and is_ckan:
                ckan_resources = _discover_ckan_resources(
                    result["final_url"],
                    dataset_id,
                    timeout=timeout,
                    user_agent=user_agent,
                )

            if not ckan_resources and html_content:
                candidate_file_links = [
                    link for link in result.get("candidate_links", [])
                    if any(ext in link.lower() for ext in _EXTENDED_EXTENSIONS)
                ]

        yaml_scaffold = _generate_yaml_scaffold(result, ckan_resources, candidate_file_links)
        typer.echo(yaml_scaffold)
        return

    if as_json:
        typer.echo(json.dumps(result, indent=2, ensure_ascii=False))
        return

    typer.echo(f"requested_url: {result['requested_url']}")
    typer.echo(f"final_url: {result['final_url']}")
    typer.echo(f"status_code: {result['status_code']}")
    typer.echo(f"content_type: {result['content_type']}")
    typer.echo(f"content_disposition: {result['content_disposition']}")
    typer.echo(f"kind: {result['kind']}")

    if result["candidate_links"]:
        typer.echo("candidate_links:")
        for link in result["candidate_links"][:_MAX_PRINTED_LINKS]:
            typer.echo(f"  - {link}")
        remaining = len(result["candidate_links"]) - _MAX_PRINTED_LINKS
        if remaining > 0:
            typer.echo(f"candidate_links_more: {remaining}")
    else:
        typer.echo("candidate_links: none")


def register(app: typer.Typer) -> None:
    inspect_app = typer.Typer(no_args_is_help=True, add_completion=False)
    inspect_app.command("paths")(paths)
    inspect_app.command("schema-diff")(schema_diff)
    inspect_app.command("url")(url)
    app.add_typer(inspect_app, name="inspect")
