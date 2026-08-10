"""toolkit registry — consulta gli artifact registry committati nel workspace.

Legge i cataloghi generati dal registry builder e committati nei repo
(``{repo}/registry/*.json``): clean_catalog, mart_catalog, pipeline_signals,
codelists. Cross-repo, senza riprogettare la discovery.

Speculare al tool MCP toolkit_registry.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer


def _print_list(data: dict[str, Any]) -> None:
    repos = data.get("repos", [])
    typer.echo(f"Registry artifacts nel workspace ({data.get('total_repos', 0)} repo):")
    for repo_info in repos:
        typer.echo(f"  {repo_info['repo']}:")
        for art in repo_info["artifacts"]:
            size = art.get("size_bytes")
            size_txt = f"  [{size // 1024} KB]" if size is not None else ""
            sections = art.get("sections") or {}
            counts = ", ".join(f"{k}: {v}" for k, v in sections.items() if v is not None)
            legacy = " (legacy)" if art.get("legacy") else ""
            typer.echo(f"    registry.json{size_txt}{legacy}  {counts}")


def registry_list(json_output: bool = typer.Option(False, "--json", help="Output JSON")) -> None:
    """Elenca gli artifact registry committati nei repo del workspace."""
    from toolkit.registry.reader import list_registries

    data = list_registries()
    if json_output:
        typer.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        return
    _print_list(data)


def registry_show(
    repo: str = typer.Argument(..., help="Repo (es. 'eurostat')"),
    artifact: str = typer.Argument(..., help="Artifact (es. 'clean_catalog')"),
    slug: str = typer.Option(None, "--slug", help="Filtra un'entry (dataset/mart/signal/codelist)"),
    json_output: bool = typer.Option(False, "--json", help="Output JSON"),
) -> None:
    """Mostra un artifact registry di un repo del workspace."""
    from toolkit.registry.reader import show_registry

    try:
        data = show_registry(repo, artifact, slug=slug)
    except (FileNotFoundError, ValueError) as exc:
        typer.echo(f"ERRORE: {exc}", err=True)
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        return

    entry = data.get("entry")
    if entry is not None:
        typer.echo(json.dumps(entry, indent=2, ensure_ascii=False, default=str))
        return
    payload = data["data"]
    typer.echo(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


def _git_source_repo(repo_root: Path) -> str:
    """Deriva ``owner/repo`` dal git remote origin (fallback: repo name)."""
    import subprocess

    try:
        out = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return repo_root.name
    # Accetta ssh (git@host:owner/repo.git) e https (https://host/owner/repo.git)
    url = out.rstrip("/").removesuffix(".git")
    if "@" in url and ":" in url:
        # git@host:owner/repo.git → owner/repo (toglie host: e porta).
        rest = url.split("@", 1)[1].replace(":", "/")
        parts = [p for p in rest.split("/") if p and p != "github.com"]
        return "/".join(parts[-2:])
    parts = [p for p in url.split("/") if p and p != "github.com"]
    if len(parts) >= 2:
        return "/".join(parts[-2:])
    return repo_root.name


def registry_build(
    repo: str = typer.Option(None, "--repo", help="Root del repo (default: CWD)"),
    prefix: str = typer.Option("", "--prefix", help="Prefisso GCS (es. 'eurostat')"),
    flat: bool = typer.Option(False, "--flat", help="Layout flat per clean+mart (no year)"),
    write: bool = typer.Option(False, "--write", help="Scrive registry.json (default: dry-run)"),
    out: str = typer.Option("registry", "--out", help="Dir di output (default: registry)"),
) -> None:
    """Genera registry.json del repo (auto-discovery, fusion ADR).

    Sostituisce i wrapper ``scripts/build_registry.py`` per-repo: scopre le
    sezioni dati per convenzione (``repo_dataset_dirs``) e deriva ``source_repo``
    dal git remote. Il PathContract si configura con i flag (default: root/year).

    Uso:
        toolkit registry build                     # dry-run sul repo corrente
        toolkit registry build --write             # scrive registry/registry.json
        toolkit registry build --prefix eurostat --flat --write
    """
    from toolkit.registry.builders import build_registry
    from toolkit.registry.layout import RepoLayout, repo_dataset_dirs
    from toolkit.registry.paths import PathContract

    repo_root = Path(repo).resolve() if repo else Path.cwd()
    sections = repo_dataset_dirs(repo_root)
    if not sections:
        typer.echo(
            f"ERRORE: nessuna sezione dati in {repo_root} (nessuna dir con {{slug}}/dataset.yml)",
            err=True,
        )
        raise typer.Exit(code=1)

    layout = RepoLayout(
        repo_root=repo_root,
        dataset_dirs=sections,
        source_repo=_git_source_repo(repo_root),
    )
    contract = PathContract(
        prefix=prefix,
        clean_layout="flat" if flat else "year",
        mart_layout="flat" if flat else "year",
    )

    out_dir = repo_root / out
    existing_catalog = None
    existing_signals = None
    existing_path = out_dir / "registry.json"
    if existing_path.is_file():
        try:
            existing = json.loads(existing_path.read_text(encoding="utf-8"))
            existing_catalog = {
                "datasets": existing.get("datasets", []),
                "marts": existing.get("marts", []),
            }
            existing_signals = {"signals": existing.get("signals", [])}
        except json.JSONDecodeError:
            typer.echo("WARN: registry.json esistente illeggibile — riparto da zero", err=True)

    result = build_registry(
        layout,
        path_contract=contract,
        existing_catalog=existing_catalog,
        existing_signals=existing_signals,
    )

    all_warnings: list[str] = []
    all_real: list[str] = []
    for artifact, errors in result["errors"].items():
        all_warnings.extend(f"{artifact}: {e}" for e in errors["derive"])
        all_real.extend(f"{artifact}: {e}" for e in errors["validation"])
    for w in all_warnings:
        typer.echo(f"WARN: {w}", err=True)
    if all_real:
        for e in all_real:
            typer.echo(f"ERROR: {e}", err=True)
        typer.echo("Artifact NON scritto: errori di validazione.", err=True)
        raise typer.Exit(code=1)

    registry = result["registry"]
    s = registry["summary"]
    typer.echo(
        f"registry.json — datasets {s['datasets']}, marts {s['marts']}, "
        f"signals {s['signals']} (repo: {layout.source_repo})"
    )
    if not write:
        typer.echo("Dry-run: usa --write per scrivere il file.")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "registry.json"
    out_path.write_text(json.dumps(registry, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    typer.echo(f"scritto {out_path}")


def register(app: typer.Typer) -> None:
    registry = typer.Typer(no_args_is_help=True, help="Consulta e genera gli artifact registry")
    registry.command("list")(registry_list)
    registry.command("show")(registry_show)
    registry.command("build")(registry_build)
    app.add_typer(registry, name="registry")
